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

package org.apache.seatunnel.connectors.seatunnel.couchbase.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplit;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitEnumeratorState;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.format.json.JsonDeserializationSchema;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.ReactiveCluster;
import com.couchbase.client.java.json.JsonObject;
import com.couchbase.client.java.query.QueryOptions;
import com.couchbase.client.java.query.ReactiveQueryResult;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class CouchbaseSourceTest {
    @Test
    void rejectsNonObjectDocuments() throws Exception {
        for (String value : new String[] {"null", "[]", "123", "\"text\""}) {
            withRawReader(
                    Flux.just(value.getBytes(StandardCharsets.UTF_8)),
                    (reader, context, cluster) -> {
                        assertThrows(
                                IllegalArgumentException.class,
                                () -> reader.pollNext(collector(new ArrayList<>())));
                        verify(context, never()).signalNoMoreElement();
                    });
        }
    }

    @Test
    void sourceAndSinkFactoriesAreBothDiscoverable() {
        List<String> classes = new ArrayList<>();
        java.util.ServiceLoader.load(org.apache.seatunnel.api.table.factory.Factory.class)
                .forEach(factory -> classes.add(factory.getClass().getName()));
        assertTrue(classes.contains(CouchbaseSourceFactory.class.getName()));
        assertTrue(
                classes.contains(
                        "org.apache.seatunnel.connectors.seatunnel.couchbase.sink.CouchbaseSinkFactory"));
    }

    @Test
    void restoredReaderReplaysCollectionFromBeginning() throws Exception {
        List<SingleSplit> state = new ArrayList<>();
        List<SeaTunnelRow> first = new ArrayList<>();
        withReader(
                Flux.just(JsonObject.create().put("id", 1)),
                (reader, context, cluster) -> {
                    state.addAll(reader.snapshotState(1));
                    reader.pollNext(collector(first));
                });
        withReader(
                Flux.just(JsonObject.create().put("id", 1)),
                (restored, restoredContext, restoredCluster) -> {
                    restored.addSplits(state);
                    List<SeaTunnelRow> replay = new ArrayList<>();
                    restored.pollNext(collector(replay));
                    assertEquals(first.get(0).getField(0), replay.get(0).getField(0));
                });
    }

    @Test
    @EnabledIfSystemProperty(named = "couchbase.test.connection-string", matches = ".+")
    void preservesRealServerNumericResponse() throws Exception {
        Map<String, Object> config = options();
        config.put("connection.string", System.getProperty("couchbase.test.connection-string"));
        config.put("username", "Administrator");
        config.put("password", "password");
        config.put("collection", "decimal_fixture");
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("amount", "decimal(38,9)");
        fields.put("large_integer", "bigint");
        config.put("schema", Collections.singletonMap("fields", fields));
        SourceReader.Context context = mock(SourceReader.Context.class);
        CouchbaseSourceReader reader = (CouchbaseSourceReader) source(config).createReader(context);
        try {
            reader.open();
            List<SeaTunnelRow> records = new ArrayList<>();
            reader.pollNext(collector(records));
            assertEquals(1, records.size());
            // This is the server's raw SQL++ response, not the original inserted decimal.
            assertEquals(new BigDecimal("12345678901234567000"), records.get(0).getField(0));
            assertEquals(9007199254740993L, records.get(0).getField(1));
        } finally {
            reader.close();
        }
    }

    @Test
    @EnabledIfSystemProperty(named = "couchbase.test.connection-string", matches = ".+")
    void readsRealLocalFixtureThroughFactoryAndReader() throws Exception {
        Map<String, Object> config = options();
        config.put("connection.string", System.getProperty("couchbase.test.connection-string"));
        config.put("username", "Administrator");
        config.put("password", "password");
        SourceReader.Context context = mock(SourceReader.Context.class);
        CouchbaseSourceReader reader = (CouchbaseSourceReader) source(config).createReader(context);
        List<SeaTunnelRow> records = new ArrayList<>();
        reader.addSplits(Collections.singletonList(new SingleSplit(null)));
        try {
            reader.open();
            reader.pollNext(collector(records));
            records.sort(java.util.Comparator.comparingInt(row -> (Integer) row.getField(0)));
            assertEquals(2, records.size());
            assertArrayEquals(
                    new Object[] {1, "first", true, 12.5, null}, records.get(0).getFields());
            assertArrayEquals(
                    new Object[] {2, "second", false, -3.25, null}, records.get(1).getFields());
            verify(context).signalNoMoreElement();
        } finally {
            reader.close();
        }
    }

    @Test
    void closeWakesBlockedReader() throws Exception {
        CountDownLatch subscribed = new CountDownLatch(1);
        AtomicBoolean cancelled = new AtomicBoolean();
        withReader(
                Flux.<JsonObject>never()
                        .doOnSubscribe(subscription -> subscribed.countDown())
                        .doOnCancel(() -> cancelled.set(true)),
                (reader, context, cluster) -> {
                    ExecutorService executor = Executors.newSingleThreadExecutor();
                    try {
                        Future<?> polling =
                                executor.submit(
                                        () -> {
                                            try {
                                                reader.pollNext(collector(new ArrayList<>()));
                                            } catch (Exception error) {
                                                throw new RuntimeException(error);
                                            }
                                        });
                        assertTrue(subscribed.await(5, TimeUnit.SECONDS));
                        reader.close();
                        polling.get(5, TimeUnit.SECONDS);
                        assertTrue(cancelled.get());
                        verify(context, never()).signalNoMoreElement();
                    } finally {
                        executor.shutdownNow();
                    }
                });
    }

    @Test
    void partialReadFailureIsNotReportedAsCompleted() throws Exception {
        Sinks.One<JsonObject> failure = Sinks.one();
        withReader(
                Flux.concat(Flux.just(JsonObject.create().put("id", 1)), failure.asMono()),
                (reader, context, cluster) -> {
                    List<SeaTunnelRow> records = new ArrayList<>();
                    Collector<SeaTunnelRow> output =
                            new Collector<SeaTunnelRow>() {
                                private final Object lock = new Object();

                                @Override
                                public void collect(SeaTunnelRow row) {
                                    records.add(row);
                                    failure.tryEmitError(
                                            new IllegalStateException("mid-query failure"));
                                }

                                @Override
                                public Object getCheckpointLock() {
                                    return lock;
                                }
                            };
                    assertThrows(IllegalStateException.class, () -> reader.pollNext(output));
                    assertEquals(1, records.size());
                    verify(context, never()).signalNoMoreElement();
                });
    }

    @Test
    void preservesRawQueryDecimalPrecision() throws Exception {
        byte[] response =
                "{\"amount\":12345678901234567890.123456789}".getBytes(StandardCharsets.UTF_8);
        byte[] rowBytes =
                CouchbaseSourceReader.RAW_JSON_SERIALIZER.deserialize(byte[].class, response);
        JsonDeserializationSchema decoder =
                new JsonDeserializationSchema(
                        false,
                        false,
                        new SeaTunnelRowType(
                                new String[] {"amount"},
                                new DecimalType[] {new DecimalType(38, 9)}));
        assertEquals(
                new BigDecimal("12345678901234567890.123456789"),
                decoder.deserialize(rowBytes).getField(0));
    }

    @Test
    @EnabledIfSystemProperty(named = "couchbase.test.connection-string", matches = ".+")
    void missingRealCollectionFailsInsteadOfReportingEmptyInput() throws Exception {
        Map<String, Object> config = options();
        config.put("connection.string", System.getProperty("couchbase.test.connection-string"));
        config.put("username", "Administrator");
        config.put("password", "password");
        config.put("collection", "missing_collection");
        SourceReader.Context context = mock(SourceReader.Context.class);
        CouchbaseSourceReader reader = (CouchbaseSourceReader) source(config).createReader(context);
        try {
            reader.open();
            assertThrows(Exception.class, () -> reader.pollNext(collector(new ArrayList<>())));
            verify(context, never()).signalNoMoreElement();
        } finally {
            reader.close();
        }
    }

    @Test
    void validatesRequiredSchemaAndTimeoutsWithoutConnecting() {
        Map<String, Object> options = options();
        options.remove("schema");
        Map<String, Object> missingSchema = options;
        assertThrows(OptionValidationException.class, () -> source(missingSchema));
        options = options();
        options.put("query.timeout", 0);
        Map<String, Object> invalidTimeout = options;
        assertThrows(OptionValidationException.class, () -> source(invalidTimeout));
    }

    @Test
    void quotesEachIdentifierIndependently() {
        Map<String, Object> options = options();
        options.put("bucket", "bucket`with`ticks");
        options.put("scope", "scope.with.dot");
        options.put("collection", "collection` WHERE true; DELETE");
        assertEquals(
                "SELECT RAW d FROM `bucket``with``ticks`.`scope.with.dot`."
                        + "`collection`` WHERE true; DELETE` AS d",
                CouchbaseSourceReader.statement(ReadonlyConfig.fromMap(options)));
    }

    @Test
    void sourceIsBoundedAndRejectsParallelReaders() throws Exception {
        CouchbaseSource source = source(options());
        assertEquals(Boundedness.BOUNDED, source.getBoundedness());
        assertEquals("Couchbase", source.getPluginName());
        SourceReader.Context context = mock(SourceReader.Context.class);
        when(context.getIndexOfSubtask()).thenReturn(1);
        assertThrows(IllegalArgumentException.class, () -> source.createReader(context));
    }

    @Test
    void mapsTypedRowsAndNullsAndSignalsCompletionOnce() throws Exception {
        JsonObject first =
                JsonObject.create()
                        .put("id", 1)
                        .put("name", "first")
                        .put("enabled", true)
                        .put("amount", 12.5)
                        .putNull("nullable");
        JsonObject second =
                JsonObject.create()
                        .put("id", 2)
                        .put("name", "second")
                        .put("enabled", false)
                        .put("amount", -3.25);
        withReader(
                Flux.just(first, second),
                (reader, context, cluster) -> {
                    List<SeaTunnelRow> rows = new ArrayList<>();
                    Collector<SeaTunnelRow> collector = collector(rows);
                    reader.pollNext(collector);
                    reader.pollNext(collector);
                    assertEquals(2, rows.size());
                    assertArrayEquals(
                            new Object[] {1, "first", true, 12.5, null}, rows.get(0).getFields());
                    assertArrayEquals(
                            new Object[] {2, "second", false, -3.25, null},
                            rows.get(1).getFields());
                    verify(context, times(1)).signalNoMoreElement();
                    assertEquals(1, reader.snapshotState(1).size());
                    assertNull(reader.snapshotState(1).get(0).getState());
                });
    }

    @Test
    void completesEmptyCollection() throws Exception {
        withReader(
                Flux.empty(),
                (reader, context, cluster) -> {
                    List<SeaTunnelRow> rows = new ArrayList<>();
                    reader.pollNext(collector(rows));
                    assertTrue(rows.isEmpty());
                    verify(context).signalNoMoreElement();
                });
    }

    @Test
    void failureDoesNotSignalSuccessfulCompletionAndClosesClient() throws Exception {
        withReader(
                Flux.error(new IllegalStateException("query failed")),
                (reader, context, cluster) -> {
                    assertThrows(
                            IllegalStateException.class,
                            () -> reader.pollNext(collector(new ArrayList<>())));
                    verify(context, never()).signalNoMoreElement();
                    verify(cluster).disconnect();
                });
    }

    @Test
    void malformedFieldFailsWithoutSilentlySkippingRows() throws Exception {
        withReader(
                Flux.just(JsonObject.create().put("id", "invalid")),
                (reader, context, cluster) -> {
                    assertThrows(
                            Exception.class, () -> reader.pollNext(collector(new ArrayList<>())));
                    verify(context, never()).signalNoMoreElement();
                    verify(cluster).disconnect();
                });
    }

    @Test
    void closeCancelsSubscriptionAndDoesNotSignalCompletion() throws Exception {
        AtomicBoolean cancelled = new AtomicBoolean();
        withReader(
                Flux.range(1, 100)
                        .map(id -> JsonObject.create().put("id", id))
                        .doOnCancel(() -> cancelled.set(true)),
                (reader, context, cluster) -> {
                    Collector<SeaTunnelRow> collector = mock(Collector.class);
                    when(collector.getCheckpointLock()).thenReturn(new Object());
                    doAnswer(
                                    call -> {
                                        reader.close();
                                        return null;
                                    })
                            .when(collector)
                            .collect(any(SeaTunnelRow.class));
                    reader.pollNext(collector);
                    assertTrue(cancelled.get());
                    verify(collector, times(1)).collect(any(SeaTunnelRow.class));
                    verify(context, never()).signalNoMoreElement();
                });
    }

    @Test
    void readinessFailureReleasesConnection() {
        Cluster cluster = mock(Cluster.class);
        Bucket bucket = mock(Bucket.class);
        when(cluster.bucket("test_bucket")).thenReturn(bucket);
        doThrow(new IllegalStateException("not ready")).when(bucket).waitUntilReady(any());
        try (MockedStatic<Cluster> connection = Mockito.mockStatic(Cluster.class)) {
            connection
                    .when(() -> Cluster.connect("couchbase://localhost", "user", "pass"))
                    .thenReturn(cluster);
            CouchbaseSourceReader reader =
                    (CouchbaseSourceReader)
                            source(options())
                                    .createReader(
                                            new SingleSplitReaderContext(
                                                    mock(SourceReader.Context.class)));
            assertThrows(IllegalStateException.class, reader::open);
            verify(cluster).disconnect();
        }
    }

    private void withReader(Flux<JsonObject> records, ReaderAssertion assertion) throws Exception {
        withRawReader(
                records.map(row -> row.toString().getBytes(StandardCharsets.UTF_8)), assertion);
    }

    private void withRawReader(Flux<byte[]> records, ReaderAssertion assertion) throws Exception {
        Cluster cluster = mock(Cluster.class);
        Bucket bucket = mock(Bucket.class);
        ReactiveCluster reactive = mock(ReactiveCluster.class);
        ReactiveQueryResult result = mock(ReactiveQueryResult.class);
        when(cluster.bucket("test_bucket")).thenReturn(bucket);
        when(cluster.reactive()).thenReturn(reactive);
        when(reactive.query(anyString(), any(QueryOptions.class))).thenReturn(Mono.just(result));
        when(result.rowsAs(byte[].class)).thenReturn(records);
        SourceReader.Context context = mock(SourceReader.Context.class);
        try (MockedStatic<Cluster> connection = Mockito.mockStatic(Cluster.class)) {
            connection
                    .when(() -> Cluster.connect("couchbase://localhost", "user", "pass"))
                    .thenReturn(cluster);
            CouchbaseSourceReader reader =
                    (CouchbaseSourceReader) source(options()).createReader(context);
            reader.addSplits(Collections.singletonList(new SingleSplit(null)));
            reader.open();
            try {
                assertion.run(reader, context, cluster);
            } finally {
                reader.close();
            }
            verify(cluster, times(1)).disconnect();
        }
    }

    static CouchbaseSource source(Map<String, Object> options) {
        return (CouchbaseSource)
                new CouchbaseSourceFactory()
                        .<SeaTunnelRow, SingleSplit, SingleSplitEnumeratorState>createSource(
                                new TableSourceFactoryContext(
                                        ReadonlyConfig.fromMap(options),
                                        CouchbaseSourceTest.class.getClassLoader()))
                        .createSource();
    }

    static Map<String, Object> options() {
        Map<String, Object> options = new HashMap<>();
        options.put("connection.string", "couchbase://localhost");
        options.put("username", "user");
        options.put("password", "pass");
        options.put("bucket", "test_bucket");
        options.put("collection", "_default");
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("id", "int");
        fields.put("name", "string");
        fields.put("enabled", "boolean");
        fields.put("amount", "double");
        fields.put("nullable", "string");
        options.put("schema", Collections.singletonMap("fields", fields));
        return options;
    }

    static Collector<SeaTunnelRow> collector(List<SeaTunnelRow> rows) {
        return new Collector<SeaTunnelRow>() {
            private final Object lock = new Object();

            @Override
            public void collect(SeaTunnelRow row) {
                rows.add(row);
            }

            @Override
            public Object getCheckpointLock() {
                return lock;
            }
        };
    }

    private interface ReaderAssertion {
        void run(CouchbaseSourceReader reader, SourceReader.Context context, Cluster cluster)
                throws Exception;
    }
}
