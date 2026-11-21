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

package org.apache.seatunnel.connectors.seatunnel.iotdb.source;

import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.iotdb.exception.IotdbConnectorException;
import org.apache.seatunnel.connectors.seatunnel.iotdb.state.IoTDBSourceState;

import org.apache.iotdb.session.Session;
import org.apache.iotdb.session.SessionDataSet;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.RowRecord;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

class IoTDBMultiTableSourceTest {

    @ParameterizedTest
    @CsvSource({"0,100,4", "1,2,20", "-10,10,4", "1,10,1"})
    void tableTimePartitionsCoverEachTimestampExactlyOnce(long lower, long upper, int count)
            throws Exception {
        IoTDBSource source =
                source(
                        "tables_configs = [{sql = \"select value from root.test\", lower_bound = "
                                + lower
                                + ", upper_bound = "
                                + upper
                                + ", num_partitions = "
                                + count
                                + ", schema {table = test, fields {ts = bigint, value = int}}}]");
        SourceSplitEnumerator.Context<IoTDBSourceSplit> context =
                mock(SourceSplitEnumerator.Context.class);
        when(context.currentParallelism()).thenReturn(1);
        when(context.registeredReaders()).thenReturn(Collections.emptySet());
        SourceSplitEnumerator<IoTDBSourceSplit, IoTDBSourceState> enumerator =
                source.createEnumerator(context);
        enumerator.run();
        List<IoTDBSourceSplit> splits = enumerator.snapshotState(1).getPendingSplit().get(0);
        Assertions.assertEquals(Math.min(count, upper - lower + 1), splits.size());
        Set<Long> covered = new HashSet<>();
        Pattern pattern = Pattern.compile("time >= (-?\\d+) and time < (-?\\d+)");
        for (IoTDBSourceSplit split : splits) {
            Matcher matcher = pattern.matcher(split.getQuery());
            Assertions.assertTrue(matcher.find());
            long start = Long.parseLong(matcher.group(1));
            long end = Long.parseLong(matcher.group(2));
            for (long timestamp = start; timestamp < end; timestamp++) {
                Assertions.assertTrue(covered.add(timestamp), "Overlapping time partitions");
                Assertions.assertTrue(timestamp >= lower && timestamp <= upper);
            }
        }
        Assertions.assertEquals(upper - lower + 1, covered.size());
    }

    private static final String CONNECTION =
            "node_urls = \"localhost:6667\"\nusername = root\npassword = root\n";
    private static final String TABLES =
            "tables_configs = ["
                    + "{sql = \"select temperature from root.weather\", lower_bound = 1, upper_bound = 10, num_partitions = 2, schema {table = weather, fields {ts = bigint, temperature = tinyint}}},"
                    + "{sql = \"select enabled from root.status\", schema {table = status, fields {ts = bigint, enabled = boolean}}}"
                    + "]";

    @Test
    void enumeratesIndependentQueriesAndRestoresTableIdentities() throws Exception {
        IoTDBSource source = roundTrip(source(TABLES));
        Assertions.assertEquals(Boundedness.BOUNDED, source.getBoundedness());
        SourceSplitEnumerator.Context<IoTDBSourceSplit> context =
                mock(SourceSplitEnumerator.Context.class);
        when(context.currentParallelism()).thenReturn(2);
        when(context.registeredReaders()).thenReturn(Collections.emptySet());
        SourceSplitEnumerator<IoTDBSourceSplit, IoTDBSourceState> enumerator =
                source.createEnumerator(context);
        enumerator.run();
        IoTDBSourceState state = roundTrip(enumerator.snapshotState(1));
        List<IoTDBSourceSplit> pending =
                state.getPendingSplit().values().stream()
                        .flatMap(List::stream)
                        .collect(Collectors.toList());
        Assertions.assertEquals(3, pending.size());
        Assertions.assertEquals(
                3, pending.stream().map(IoTDBSourceSplit::splitId).distinct().count());
        Assertions.assertEquals(
                2, pending.stream().filter(s -> "weather".equals(s.getTableId())).count());
        Assertions.assertTrue(
                pending.stream()
                        .anyMatch(
                                s ->
                                        s.getQuery().contains("time >= 1")
                                                && s.getQuery().contains("time < 6")));
        Assertions.assertTrue(
                pending.stream()
                        .anyMatch(
                                s ->
                                        s.getQuery().contains("time >= 6")
                                                && s.getQuery().contains("time < 11")));
        Assertions.assertTrue(
                pending.stream()
                        .anyMatch(
                                s ->
                                        "status".equals(s.getTableId())
                                                && "select enabled from root.status"
                                                        .equals(s.getQuery())));
        List<IoTDBSourceSplit> assigned = new ArrayList<>();
        doAnswer(
                        invocation -> {
                            assigned.addAll(invocation.getArgument(1));
                            return null;
                        })
                .when(context)
                .assignSplit(anyInt(), anyList());
        when(context.registeredReaders()).thenReturn(new HashSet<>(Arrays.asList(0, 1)));
        SourceSplitEnumerator<IoTDBSourceSplit, IoTDBSourceState> restored =
                source.restoreEnumerator(context, state);
        restored.registerReader(0);
        restored.registerReader(1);
        restored.run();
        Assertions.assertEquals(3, assigned.size());
        Assertions.assertTrue(restored.snapshotState(2).getPendingSplit().isEmpty());
        restored.addSplitsBack(Collections.singletonList(assigned.get(0)), 0);
        Assertions.assertEquals(4, assigned.size());
    }

    @Test
    void partitionSqlRecognizesKeywordsRatherThanFieldNames() throws Exception {
        IoTDBSource source =
                source(
                        "tables_configs = [{sql = \"SELECT somewhere FROM root.test WHERE somewhere > 0 ALIGN BY DEVICE\", lower_bound = 1, upper_bound = 10, num_partitions = 2, schema {table = a, fields {ts = bigint, device = string, somewhere = int}}}]");
        SourceSplitEnumerator.Context<IoTDBSourceSplit> context =
                mock(SourceSplitEnumerator.Context.class);
        when(context.currentParallelism()).thenReturn(1);
        when(context.registeredReaders()).thenReturn(Collections.emptySet());
        SourceSplitEnumerator<IoTDBSourceSplit, IoTDBSourceState> enumerator =
                source.createEnumerator(context);
        enumerator.run();
        for (IoTDBSourceSplit split : enumerator.snapshotState(1).getPendingSplit().get(0)) {
            Assertions.assertTrue(split.getQuery().startsWith("SELECT somewhere FROM root.test "));
            Assertions.assertTrue(split.getQuery().contains("somewhere > 0"));
            Assertions.assertTrue(split.getQuery().endsWith("align by  DEVICE"));
        }
    }

    @Test
    void readerUsesEachSchemaAndTableIdAndClosesResults() throws Exception {
        SourceReader.Context context = mock(SourceReader.Context.class);
        when(context.getBoundedness()).thenReturn(Boundedness.BOUNDED);
        IoTDBSourceReader reader = (IoTDBSourceReader) source(TABLES).createReader(context);
        Session session = mock(Session.class);
        setSession(reader, session);
        RowRecord weather = new RowRecord(1);
        weather.addField(7, TSDataType.INT32);
        RowRecord status = new RowRecord(2);
        status.addField(true, TSDataType.BOOLEAN);
        SessionDataSet first = result(weather);
        SessionDataSet second = result(status);
        when(session.executeQueryStatement("weather query")).thenReturn(first);
        when(session.executeQueryStatement("status query")).thenReturn(second);
        reader.addSplits(
                Arrays.asList(
                        new IoTDBSourceSplit("a", "weather query", "weather"),
                        new IoTDBSourceSplit("b", "status query", "status")));
        List<IoTDBSourceSplit> checkpoint = roundTrip(reader.snapshotState(1));
        Assertions.assertEquals("status", checkpoint.get(1).getTableId());
        reader =
                (IoTDBSourceReader)
                        source(
                                        "tables_configs = ["
                                                + "{sql = status, schema {table = status, fields {ts = bigint, enabled = boolean}}},"
                                                + "{sql = weather, schema {table = weather, fields {ts = bigint, temperature = tinyint}}}]")
                                .createReader(context);
        setSession(reader, session);
        reader.addSplits(checkpoint);
        List<SeaTunnelRow> rows = new ArrayList<>();
        reader.handleNoMoreSplits();
        reader.pollNext(collector(rows));
        Assertions.assertEquals(2, rows.size());
        Assertions.assertEquals("weather", rows.get(0).getTableId());
        Assertions.assertArrayEquals(new Object[] {1L, (byte) 7}, rows.get(0).getFields());
        Assertions.assertEquals("status", rows.get(1).getTableId());
        Assertions.assertArrayEquals(new Object[] {2L, true}, rows.get(1).getFields());
        Assertions.assertTrue(reader.snapshotState(2).isEmpty());
        verify(first).close();
        verify(second).close();
        verify(context).signalNoMoreElement();
        reader.close();
        verify(session).close();
    }

    @Test
    void readerClosesResultsAfterConversionFailure() throws Exception {
        IoTDBSourceReader reader =
                (IoTDBSourceReader) source(TABLES).createReader(mock(SourceReader.Context.class));
        Session session = mock(Session.class);
        setSession(reader, session);
        SessionDataSet result = result(new RowRecord(1));
        when(session.executeQueryStatement("invalid row")).thenReturn(result);
        reader.addSplits(
                Collections.singletonList(new IoTDBSourceSplit("a", "invalid row", "weather")));
        Assertions.assertThrows(
                IotdbConnectorException.class, () -> reader.pollNext(collector(new ArrayList<>())));
        verify(result).close();
    }

    @Test
    void readerRejectsUnknownOrMissingTableIdentity() throws Exception {
        for (String tableId : Arrays.asList(null, "unknown")) {
            IoTDBSourceReader reader =
                    (IoTDBSourceReader)
                            source(TABLES).createReader(mock(SourceReader.Context.class));
            Session session = mock(Session.class);
            setSession(reader, session);
            reader.addSplits(
                    Collections.singletonList(new IoTDBSourceSplit("a", "query", tableId)));
            Assertions.assertThrows(
                    IotdbConnectorException.class,
                    () -> reader.pollNext(collector(new ArrayList<>())));
            verifyNoInteractions(session);
        }
    }

    @Test
    void legacyReaderRejectsMultiTableCheckpoint() throws Exception {
        IoTDBSourceReader reader =
                (IoTDBSourceReader)
                        source(
                                        "sql = x\nschema {table = weather, fields {ts = bigint, temperature = int}}")
                                .createReader(mock(SourceReader.Context.class));
        Session session = mock(Session.class);
        setSession(reader, session);
        reader.addSplits(Collections.singletonList(new IoTDBSourceSplit("a", "query", "weather")));
        Assertions.assertThrows(
                IotdbConnectorException.class, () -> reader.pollNext(collector(new ArrayList<>())));
        verifyNoInteractions(session);
    }

    @Test
    void restoresSplitSerializedBeforeTableIdentityWasAdded() throws Exception {
        // Generated from the original two-field IoTDBSourceSplit with serialVersionUID = -1L.
        String fixture =
                "rO0ABXNyAEdvcmcuYXBhY2hlLnNlYXR1bm5lbC5jb25uZWN0b3JzLnNlYXR1bm5lbC5pb3RkYi5zb3VyY2UuSW9UREJTb3VyY2VTcGxpdP//////////AgACTAAFcXVlcnl0ABJMamF2YS9sYW5nL1N0cmluZztMAAdzcGxpdElkcQB+AAF4cHQAJHNlbGVjdCB0ZW1wZXJhdHVyZSBmcm9tIHJvb3QuZGV2aWNlc3QAE2xlZ2FjeS1kZXZpY2UtcmFuZ2U=";
        IoTDBSourceSplit split;
        try (ObjectInputStream input =
                new ObjectInputStream(
                        new ByteArrayInputStream(Base64.getDecoder().decode(fixture)))) {
            split = (IoTDBSourceSplit) input.readObject();
        }
        Assertions.assertEquals("legacy-device-range", split.splitId());
        Assertions.assertNull(split.getTableId());
        IoTDBSourceReader reader =
                (IoTDBSourceReader)
                        source("sql = x\nschema {fields {ts = bigint, temperature = int}}")
                                .createReader(mock(SourceReader.Context.class));
        Session session = mock(Session.class);
        setSession(reader, session);
        RowRecord row = new RowRecord(1);
        row.addField(7, TSDataType.INT32);
        SessionDataSet dataSet = result(row);
        when(session.executeQueryStatement(split.getQuery())).thenReturn(dataSet);
        reader.addSplits(Collections.singletonList(split));
        List<SeaTunnelRow> rows = new ArrayList<>();
        reader.pollNext(collector(rows));
        Assertions.assertArrayEquals(new Object[] {1L, 7}, rows.get(0).getFields());
        Assertions.assertEquals(
                new SeaTunnelRow(new Object[0]).getTableId(), rows.get(0).getTableId());
    }

    private static IoTDBSource source(String options) {
        ReadonlyConfig config =
                ReadonlyConfig.fromConfig(ConfigFactory.parseString(CONNECTION + options));
        Object result =
                new IoTDBSourceFactory()
                        .createSource(
                                new TableSourceFactoryContext(
                                        config, IoTDBMultiTableSourceTest.class.getClassLoader()))
                        .createSource();
        return (IoTDBSource) result;
    }

    private static SessionDataSet result(RowRecord row) throws Exception {
        SessionDataSet result = mock(SessionDataSet.class);
        when(result.hasNext()).thenReturn(true, false);
        when(result.next()).thenReturn(row);
        return result;
    }

    private static void setSession(IoTDBSourceReader reader, Session session) throws Exception {
        Field field = IoTDBSourceReader.class.getDeclaredField("session");
        field.setAccessible(true);
        field.set(reader, session);
    }

    private static Collector<SeaTunnelRow> collector(List<SeaTunnelRow> rows) {
        return new Collector<SeaTunnelRow>() {
            @Override
            public void collect(SeaTunnelRow row) {
                rows.add(row);
            }

            @Override
            public Object getCheckpointLock() {
                return this;
            }
        };
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
