/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.seatunnel.connectors.seatunnel.influxdb.source;

import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;
import org.apache.seatunnel.shade.org.apache.commons.lang3.tuple.Pair;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.influxdb.exception.InfluxdbConnectorException;
import org.apache.seatunnel.connectors.seatunnel.influxdb.state.InfluxDBSourceState;

import org.influxdb.InfluxDB;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

class InfluxDBMultiTableSourceTest {
    private static final String ROOT = "url=\"http://localhost:8086\"\n";
    private static final String TEMPERATURE =
            "{ database=temperature, sql=\"select value from readings\", schema { table=temperature, fields { value=DOUBLE } } }";
    private static final String ALARMS =
            "{ database=alarms, sql=\"select active from alerts tz('Asia/Shanghai')\", schema { table=alarms, fields { active=BOOLEAN, label=STRING } } }";
    private static final String MULTI =
            ROOT + "tables_configs=[" + TEMPERATURE + "," + ALARMS + "]";

    @Test
    void createsIndependentCatalogTablesWithoutRootSqlOrSchema() {
        InfluxDBSource source = source(MULTI);
        Assertions.assertEquals(2, source.getProducedCatalogTables().size());
        Assertions.assertEquals(
                "temperature",
                source.getProducedCatalogTables().get(0).getTableId().toTablePath().toString());
        Assertions.assertEquals(
                "alarms",
                source.getProducedCatalogTables().get(1).getTableId().toTablePath().toString());
        Assertions.assertEquals(
                1, source.getProducedCatalogTables().get(0).getSeaTunnelRowType().getTotalFields());
        Assertions.assertEquals(
                2, source.getProducedCatalogTables().get(1).getSeaTunnelRowType().getTotalFields());
    }

    @Test
    void acceptsSingleEntryAndSharedDatabaseDefault() {
        Assertions.assertEquals(
                1,
                source(
                                ROOT
                                        + "database=temperature\ntables_configs=["
                                        + TEMPERATURE.replace("database=temperature,", "")
                                        + "]")
                        .getProducedCatalogTables()
                        .size());
    }

    @Test
    void preservesLegacyRootConfiguration() throws Exception {
        List<InfluxDBSourceSplit> splits =
                enumerate(
                        source(
                                ROOT
                                        + "database=temperature\nsql=\"select value from readings\"\nschema { fields { value=DOUBLE } }"));
        Assertions.assertEquals(1, splits.size());
        Assertions.assertNull(splits.get(0).getTableId());
        Assertions.assertEquals("0", splits.get(0).splitId());
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "tables_configs=[]",
                "tables_configs=[{ database=db, sql=\"select x from m\", schema { table=t, fields { x=INT } }, where=\"x > 0\" }]",
                "tables_configs=[{ database=db, sql=\"select x from m\", schema.fields.x=INT }]",
                "tables_configs=[{ database=db, sql=\" \", schema { table=t, fields.x=INT } }]",
                "tables_configs=[{ sql=\"select x from m\", schema { table=t, fields.x=INT } }]",
                "tables_configs=[{ database=db, sql=\"select x from m\", schema { table=t, fields.x=INT }, partition_num=2 }]",
                "tables_configs=[{ database=db, sql=\"select x from m\", schema { table=t, fields.x=INT }, url=\"http://other:8086\" }]",
                "tables_configs=[{ database=db, sql=\"select x from m\", schema { table=t, fields.x=INT } }, { database=other, sql=\"select x from m\", schema { table=t, fields.x=INT } }]",
                "sql=\"select x from m\"\ntables_configs=[]",
                "schema.fields.x=INT\ntables_configs=[]"
            })
    void rejectsInvalidOrAmbiguousTableConfiguration(String config) {
        Assertions.assertThrows(OptionValidationException.class, () -> source(ROOT + config));
    }

    @Test
    void reportsTableIndexForInvalidSchema() {
        OptionValidationException error =
                Assertions.assertThrows(
                        OptionValidationException.class,
                        () ->
                                source(
                                        ROOT
                                                + "tables_configs=[{ database=db, sql=\"select x from m\", schema { fields { x=INT } } }]"));
        Assertions.assertTrue(error.getMessage().contains("tables_configs[0]"));
        Assertions.assertTrue(error.getMessage().contains("schema.table"));
    }

    @Test
    void rootWhereErrorDirectsUsersToTheSqlPredicate() {
        OptionValidationException error =
                Assertions.assertThrows(
                        OptionValidationException.class,
                        () -> source(MULTI + "\nwhere=\"value > 0\""));
        Assertions.assertTrue(error.getMessage().contains("inside that entry's sql"));
    }

    @Test
    void optionRuleAcceptsBothModes() {
        ConfigValidator.of(config(MULTI)).validate(new InfluxDBSourceFactory().optionRule());
        ConfigValidator.of(
                        config(
                                ROOT
                                        + "database=db\nsql=\"select x from m\"\nschema { fields { x=INT } }"))
                .validate(new InfluxDBSourceFactory().optionRule());
    }

    @Test
    void enumeratesEachTableWithItsOwnQueryAndRange() throws Exception {
        String splitTable =
                TEMPERATURE.replace(
                        "database=temperature,",
                        "database=temperature, lower_bound=0, upper_bound=3, partition_num=2, split_column=value,");
        List<InfluxDBSourceSplit> splits =
                enumerate(source(ROOT + "tables_configs=[" + splitTable + "," + ALARMS + "]"));
        Assertions.assertEquals(3, splits.size());
        Assertions.assertEquals(
                3,
                splits.stream()
                        .map(InfluxDBSourceSplit::splitId)
                        .collect(Collectors.toSet())
                        .size());
        Assertions.assertEquals(
                2, splits.stream().filter(s -> "temperature".equals(s.getTableId())).count());
        Assertions.assertTrue(
                splits.stream().anyMatch(s -> s.getQuery().contains("value >= 0 and value < 2")));
        Assertions.assertTrue(
                splits.stream().anyMatch(s -> s.getQuery().contains("value >= 2 and value < 4")));
        Assertions.assertTrue(
                splits.stream()
                        .anyMatch(
                                s ->
                                        "alarms".equals(s.getTableId())
                                                && s.getQuery()
                                                        .equals(
                                                                "select active from alerts tz('Asia/Shanghai')")));
    }

    @Test
    void readsEachDatabaseWithIndependentColumnsAndRoutesRows() throws Exception {
        InfluxDB client = Mockito.mock(InfluxDB.class);
        Mockito.when(client.query(Mockito.any(Query.class)))
                .thenAnswer(
                        invocation -> {
                            Query query = invocation.getArgument(0);
                            if ("temperature".equals(query.getDatabase())) {
                                return result(
                                        Arrays.asList("time", "value"), Arrays.asList(100D, 12.5D));
                            }
                            Assertions.assertEquals("alarms", query.getDatabase());
                            Assertions.assertEquals(
                                    "select active from alerts tz('Asia/Shanghai')",
                                    query.getCommand());
                            return result(
                                    Arrays.asList("time", "label", "active"),
                                    Arrays.asList(101D, "overheat", true));
                        });
        SourceReader.Context context = readerContext();
        InfluxdbSourceReader reader = reader(source(MULTI), context, client);
        reader.addSplits(enumerate(source(MULTI)));
        reader.handleNoMoreSplits();
        List<SeaTunnelRow> rows = collect(reader);
        Assertions.assertEquals(2, rows.size());
        Assertions.assertArrayEquals(
                new Object[] {12.5D},
                rows.stream()
                        .filter(r -> "temperature".equals(r.getTableId()))
                        .findFirst()
                        .get()
                        .getFields());
        Assertions.assertArrayEquals(
                new Object[] {true, "overheat"},
                rows.stream()
                        .filter(r -> "alarms".equals(r.getTableId()))
                        .findFirst()
                        .get()
                        .getFields());
        Mockito.verify(context).signalNoMoreElement();
        reader.close();
        reader.close();
        Mockito.verify(client).close();
    }

    @Test
    void emptyTableDoesNotPreventReadingAnotherTable() throws Exception {
        InfluxDB client = Mockito.mock(InfluxDB.class);
        Mockito.when(client.query(Mockito.any(Query.class)))
                .thenAnswer(
                        invocation ->
                                "temperature"
                                                .equals(
                                                        ((Query) invocation.getArgument(0))
                                                                .getDatabase())
                                        ? new QueryResult()
                                        : result(
                                                Arrays.asList("active", "label"),
                                                Arrays.asList(false, "normal")));
        InfluxdbSourceReader reader = reader(source(MULTI), readerContext(), client);
        reader.addSplits(enumerate(source(MULTI)));
        List<SeaTunnelRow> rows = collect(reader);
        Assertions.assertEquals(1, rows.size());
        Assertions.assertEquals("alarms", rows.get(0).getTableId());
    }

    @Test
    void derivesColumnOrderForEverySeries() throws Exception {
        QueryResult first = result(Arrays.asList("value", "time"), Arrays.asList(12D, 1D));
        QueryResult second = result(Arrays.asList("time", "value"), Arrays.asList(2D, 13D));
        first.getResults()
                .get(0)
                .setSeries(
                        Arrays.asList(
                                first.getResults().get(0).getSeries().get(0),
                                second.getResults().get(0).getSeries().get(0)));
        InfluxDB client = Mockito.mock(InfluxDB.class);
        Mockito.when(client.query(Mockito.any(Query.class))).thenReturn(first);
        InfluxdbSourceReader reader = reader(source(MULTI), readerContext(), client);
        reader.addSplits(
                Collections.singletonList(
                        new InfluxDBSourceSplit("a", "select value from readings", "temperature")));
        List<SeaTunnelRow> rows = collect(reader);
        Assertions.assertEquals(12D, rows.get(0).getField(0));
        Assertions.assertEquals(13D, rows.get(1).getField(0));
    }

    @Test
    void failsOnMissingColumnsAndServerErrors() throws Exception {
        InfluxDB client = Mockito.mock(InfluxDB.class);
        QueryResult error = new QueryResult();
        error.setError("database not found");
        QueryResult resultError = new QueryResult();
        QueryResult.Result failed = new QueryResult.Result();
        failed.setError("permission denied");
        resultError.setResults(Collections.singletonList(failed));
        for (QueryResult result :
                Arrays.asList(
                        result(Collections.singletonList("wrong"), Collections.singletonList(1D)),
                        error,
                        resultError)) {
            Mockito.when(client.query(Mockito.any(Query.class))).thenReturn(result);
            InfluxdbSourceReader reader = reader(source(MULTI), readerContext(), client);
            reader.addSplits(
                    Collections.singletonList(
                            new InfluxDBSourceSplit(
                                    "a", "select value from readings", "temperature")));
            InfluxdbConnectorException exception =
                    Assertions.assertThrows(
                            InfluxdbConnectorException.class, () -> collect(reader));
            Assertions.assertTrue(exception.getMessage().contains("temperature"));
        }
    }

    @Test
    void rejectsUnroutableRestoredSplitsBeforeQuerying() throws Exception {
        InfluxDB client = Mockito.mock(InfluxDB.class);
        for (String id : Arrays.asList(null, "unknown")) {
            InfluxdbSourceReader reader = reader(source(MULTI), readerContext(), client);
            reader.addSplits(
                    Collections.singletonList(
                            new InfluxDBSourceSplit("old", "select value from readings", id)));
            Assertions.assertThrows(InfluxdbConnectorException.class, () -> collect(reader));
        }
        Mockito.verifyNoInteractions(client);
    }

    @Test
    void restoresEnumeratorAndReaderStateWithTableIdentity() throws Exception {
        InfluxDBSource source = roundTrip(source(MULTI));
        SourceSplitEnumerator.Context<InfluxDBSourceSplit> context = enumeratorContext();
        Mockito.when(context.registeredReaders()).thenReturn(Collections.emptySet());
        InfluxDBSourceSplitEnumerator enumerator =
                (InfluxDBSourceSplitEnumerator) source.createEnumerator(context);
        enumerator.run();
        InfluxDBSourceState state = roundTrip(enumerator.snapshotState(1));
        InfluxDBSourceSplitEnumerator restored =
                (InfluxDBSourceSplitEnumerator) source.restoreEnumerator(context, state);
        restored.registerReader(0);
        ArgumentCaptor<List<InfluxDBSourceSplit>> captor = ArgumentCaptor.forClass(List.class);
        Mockito.verify(context).assignSplit(Mockito.eq(0), captor.capture());
        Assertions.assertEquals(2, captor.getValue().size());
        InfluxdbSourceReader reader = reader(source, readerContext(), Mockito.mock(InfluxDB.class));
        reader.addSplits(captor.getValue());
        List<InfluxDBSourceSplit> readerState = roundTrip(reader.snapshotState(2));
        Assertions.assertEquals(2, readerState.size());
        Assertions.assertEquals(
                captor.getValue().get(0).getTableId(), readerState.get(0).getTableId());
        Assertions.assertEquals(captor.getValue().get(1).getQuery(), readerState.get(1).getQuery());
    }

    @Test
    void readsFrozenLegacyJava8Split() throws Exception {
        byte[] bytes =
                Base64.getDecoder()
                        .decode(
                                "rO0ABXNyAE1vcmcuYXBhY2hlLnNlYXR1bm5lbC5jb25uZWN0b3JzLnNlYXR1bm5lbC5pbmZsdXhkYi5zb3VyY2UuSW5mbHV4REJTb3VyY2VTcGxpdG4krO+p6M+SAgACTAAFcXVlcnl0ABJMamF2YS9sYW5nL1N0cmluZztMAAdzcGxpdElkcQB+AAF4cHQAH3NlbGVjdCB0ZW1wZXJhdHVyZSBmcm9tIHNlbnNvcnN0ABhsZWdhY3ktbWVhc3VyZW1lbnQtcmFuZ2U=");
        try (ObjectInputStream input = new ObjectInputStream(new ByteArrayInputStream(bytes))) {
            InfluxDBSourceSplit split = (InfluxDBSourceSplit) input.readObject();
            Assertions.assertNull(split.getTableId());
            Assertions.assertEquals("legacy-measurement-range", split.splitId());
            Assertions.assertEquals("select temperature from sensors", split.getQuery());
        }
    }

    @Test
    void tableRangesHaveNoGapsOrOverlap() {
        for (int[] range :
                Arrays.asList(
                        new int[] {0, 100, 4},
                        new int[] {7, 7, 4},
                        new int[] {-2, 2, 10},
                        new int[] {Integer.MIN_VALUE, Integer.MAX_VALUE, 3})) {
            List<Pair<Long, Long>> ranges =
                    InfluxDBSourceSplitEnumerator.genTableSplitRanges(range[0], range[1], range[2]);
            long next = range[0];
            for (Pair<Long, Long> part : ranges) {
                Assertions.assertEquals(next, part.getLeft());
                Assertions.assertTrue(part.getRight() > part.getLeft());
                next = part.getRight();
            }
            Assertions.assertEquals((long) range[1] + 1, next);
            Assertions.assertTrue(ranges.size() <= range[2]);
        }
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "lower_bound=4, upper_bound=0, partition_num=2, split_column=value,",
                "lower_bound=0, upper_bound=4, partition_num=-1, split_column=value,",
                "lower_bound=0, upper_bound=4, partition_num=2, split_column=\" \","
            })
    void rejectsInvalidRanges(String range) {
        Assertions.assertThrows(
                OptionValidationException.class,
                () ->
                        source(
                                ROOT
                                        + "tables_configs=["
                                        + TEMPERATURE.replace(
                                                "database=temperature,",
                                                "database=temperature," + range)
                                        + "]"));
    }

    @Test
    void restoredSplitsStillRouteAfterReorderingTables() throws Exception {
        List<InfluxDBSourceSplit> splits = roundTrip(enumerate(source(MULTI)));
        InfluxDB client = Mockito.mock(InfluxDB.class);
        Mockito.when(client.query(Mockito.any(Query.class)))
                .thenAnswer(
                        invocation ->
                                "temperature"
                                                .equals(
                                                        ((Query) invocation.getArgument(0))
                                                                .getDatabase())
                                        ? result(
                                                Collections.singletonList("value"),
                                                Collections.singletonList(8D))
                                        : result(
                                                Arrays.asList("active", "label"),
                                                Arrays.asList(true, "alarm")));
        InfluxdbSourceReader reader =
                reader(
                        source(ROOT + "tables_configs=[" + ALARMS + "," + TEMPERATURE + "]"),
                        readerContext(),
                        client);
        reader.addSplits(splits);
        List<SeaTunnelRow> rows = collect(reader);
        Assertions.assertEquals(2, rows.size());
        Assertions.assertEquals(
                8D,
                rows.stream()
                        .filter(r -> "temperature".equals(r.getTableId()))
                        .findFirst()
                        .get()
                        .getField(0));
        Assertions.assertEquals(
                "alarm",
                rows.stream()
                        .filter(r -> "alarms".equals(r.getTableId()))
                        .findFirst()
                        .get()
                        .getField(1));
    }

    @Test
    void preservesUppercaseWherePredicateInTableRangeQueries() throws Exception {
        String table =
                TEMPERATURE
                        .replace(
                                "select value from readings",
                                "SELECT value FROM readings WHERE value >= 0")
                        .replace(
                                "database=temperature,",
                                "database=temperature, lower_bound=0, upper_bound=4, partition_num=2, split_column=value,");
        List<InfluxDBSourceSplit> splits =
                enumerate(source(ROOT + "tables_configs=[" + table + "]"));
        Assertions.assertEquals(2, splits.size());
        Assertions.assertTrue(
                splits.stream().allMatch(s -> s.getQuery().contains("and ( value >= 0 )")));
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "select value from readings limit 1",
                "select value from readings tz('Asia/Shanghai')",
                "select value from readings; select value from readings",
                "select value from readings where label = 'where'",
                "select value from (select value from readings)"
            })
    void rejectsComplexRangeQueriesWithoutRestrictingUnpartitionedQueries(String sql) {
        String table = TEMPERATURE.replace("select value from readings", sql);
        Assertions.assertDoesNotThrow(() -> source(ROOT + "tables_configs=[" + table + "]"));
        String range =
                table.replace(
                        "database=temperature,",
                        "database=temperature, lower_bound=0, upper_bound=4, partition_num=2, split_column=value,");
        OptionValidationException error =
                Assertions.assertThrows(
                        OptionValidationException.class,
                        () -> source(ROOT + "tables_configs=[" + range + "]"));
        Assertions.assertTrue(error.getMessage().contains("unpartitioned query"));
    }

    private static ReadonlyConfig config(String config) {
        return ReadonlyConfig.fromConfig(ConfigFactory.parseString(config));
    }

    static InfluxDBSource source(String config) {
        return (InfluxDBSource)
                new InfluxDBSourceFactory()
                        .<SeaTunnelRow, InfluxDBSourceSplit, InfluxDBSourceState>createSource(
                                new TableSourceFactoryContext(
                                        config(config),
                                        InfluxDBMultiTableSourceTest.class.getClassLoader()))
                        .createSource();
    }

    private static SourceReader.Context readerContext() {
        SourceReader.Context context = Mockito.mock(SourceReader.Context.class);
        Mockito.when(context.getBoundedness()).thenReturn(Boundedness.BOUNDED);
        return context;
    }

    private static InfluxdbSourceReader reader(
            InfluxDBSource source, SourceReader.Context context, InfluxDB client) throws Exception {
        InfluxdbSourceReader reader = (InfluxdbSourceReader) source.createReader(context);
        Field field = InfluxdbSourceReader.class.getDeclaredField("influxdb");
        field.setAccessible(true);
        field.set(reader, client);
        return reader;
    }

    private static List<SeaTunnelRow> collect(InfluxdbSourceReader reader) {
        List<SeaTunnelRow> rows = new ArrayList<>();
        reader.pollNext(
                new Collector<SeaTunnelRow>() {
                    public void collect(SeaTunnelRow row) {
                        rows.add(row);
                    }

                    public Object getCheckpointLock() {
                        return this;
                    }
                });
        return rows;
    }

    private static SourceSplitEnumerator.Context<InfluxDBSourceSplit> enumeratorContext() {
        SourceSplitEnumerator.Context<InfluxDBSourceSplit> context =
                Mockito.mock(SourceSplitEnumerator.Context.class);
        Mockito.when(context.currentParallelism()).thenReturn(1);
        Mockito.when(context.registeredReaders()).thenReturn(Collections.singleton(0));
        return context;
    }

    private static List<InfluxDBSourceSplit> enumerate(InfluxDBSource source) throws Exception {
        SourceSplitEnumerator.Context<InfluxDBSourceSplit> context = enumeratorContext();
        source.createEnumerator(context).run();
        ArgumentCaptor<List<InfluxDBSourceSplit>> captor = ArgumentCaptor.forClass(List.class);
        Mockito.verify(context).assignSplit(Mockito.eq(0), captor.capture());
        return captor.getValue();
    }

    private static QueryResult result(List<String> columns, List<Object> values) {
        QueryResult.Series series = new QueryResult.Series();
        series.setColumns(columns);
        series.setValues(Collections.singletonList(values));
        QueryResult.Result result = new QueryResult.Result();
        result.setSeries(Collections.singletonList(series));
        QueryResult query = new QueryResult();
        query.setResults(Collections.singletonList(result));
        return query;
    }

    private static <T> T roundTrip(T value) throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ObjectOutputStream output = new ObjectOutputStream(bytes)) {
            output.writeObject(value);
        }
        try (ObjectInputStream input =
                new ObjectInputStream(new ByteArrayInputStream(bytes.toByteArray()))) {
            return (T) input.readObject();
        }
    }
}
