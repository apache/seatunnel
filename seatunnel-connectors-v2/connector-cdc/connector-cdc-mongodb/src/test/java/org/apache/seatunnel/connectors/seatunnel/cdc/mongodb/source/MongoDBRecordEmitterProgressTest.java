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

package org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.source;

import org.apache.seatunnel.api.cdc.CdcProgressAccuracy;
import org.apache.seatunnel.api.cdc.CdcReaderProgressReport;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.connectors.cdc.base.source.offset.OffsetFactory;
import org.apache.seatunnel.connectors.cdc.base.source.progress.CdcReaderProgressTracker;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceRecords;
import org.apache.seatunnel.connectors.cdc.base.source.split.state.IncrementalSplitState;
import org.apache.seatunnel.connectors.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.source.offset.ChangeStreamOffset;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import org.bson.BsonDocument;
import org.bson.BsonString;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.Mockito;

import io.debezium.relational.TableId;

import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceConstants.ID_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceConstants.RESUME_TOKEN_FIELD;

class MongoDBRecordEmitterProgressTest {

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void partialBatchFailureRetainsOnlyVerifiedSamples(boolean collectorFailure) throws Exception {
        for (boolean sampleFirstRow : new boolean[] {false, true}) {
            Fixture fixture = new Fixture();
            fixture.emit(1);
            CdcReaderProgressReport first = fixture.tracker.current();
            if (sampleFirstRow) {
                fixture.nanos.addAndGet(TimeUnit.SECONDS.toNanos(1));
            }
            AtomicInteger deserialized = new AtomicInteger();
            AtomicInteger collected = new AtomicInteger();
            AtomicReference<CdcReaderProgressReport> beforeFailure = new AtomicReference<>();
            IllegalStateException failure = new IllegalStateException("partial batch failure");
            Mockito.doAnswer(
                            invocation -> {
                                int row = deserialized.incrementAndGet();
                                if (row == 2) {
                                    beforeFailure.set(fixture.tracker.current());
                                    if (!collectorFailure) {
                                        throw failure;
                                    }
                                }
                                invocation.<Collector<String>>getArgument(1).collect("row");
                                return null;
                            })
                    .when(fixture.schema)
                    .deserialize(Mockito.any(), Mockito.any());
            Mockito.doAnswer(
                            invocation -> {
                                if (collected.incrementAndGet() == 2 && collectorFailure) {
                                    throw failure;
                                }
                                return null;
                            })
                    .when(fixture.collector)
                    .collect(Mockito.anyString());
            Assertions.assertSame(
                    failure,
                    Assertions.assertThrows(
                            IllegalStateException.class,
                            () ->
                                    fixture.emitter.emitRecord(
                                            new SourceRecords(
                                                    Arrays.asList(
                                                            fixture.record(2),
                                                            fixture.record(3),
                                                            fixture.record(4))),
                                            fixture.collector,
                                            fixture.state)));
            Assertions.assertEquals(2, deserialized.get());
            Assertions.assertEquals(token(3), fixture.offset.getResumeToken());
            Assertions.assertSame(fixture.offset, fixture.state.getStartupOffset());
            CdcReaderProgressReport actual = fixture.tracker.current();
            Assertions.assertEquals(
                    token(sampleFirstRow ? 2 : 1).toJson(),
                    actual.getCurrentConsumedPosition()
                            .getValue()
                            .getValues()
                            .get(RESUME_TOKEN_FIELD));
            Assertions.assertEquals(
                    (sampleFirstRow ? 2 : 1) * 1000L, actual.getLastSourceEventAt());
            assertPositionUnchanged(beforeFailure.get(), actual);
            Assertions.assertEquals(beforeFailure.get().getLifecycle(), actual.getLifecycle());
            Assertions.assertEquals(
                    token(1).toJson(),
                    first.getCurrentConsumedPosition()
                            .getValue()
                            .getValues()
                            .get(RESUME_TOKEN_FIELD));
        }
    }

    @Test
    void singletonBatchesDoNotBypassPublicationBudget() throws Exception {
        Fixture fixture = new Fixture();
        fixture.emit(1);
        for (int i = 2; i <= 100; i++) {
            fixture.emitter.emitRecord(
                    SourceRecords.fromSingleRecord(fixture.record(i)),
                    fixture.collector,
                    fixture.state);
        }
        Assertions.assertEquals(token(100), fixture.offset.getResumeToken());
        Assertions.assertEquals(
                token(1).toJson(),
                fixture.tracker
                        .current()
                        .getCurrentConsumedPosition()
                        .getValue()
                        .getValues()
                        .get(RESUME_TOKEN_FIELD));
        fixture.nanos.addAndGet(TimeUnit.SECONDS.toNanos(1));
        fixture.emitter.emitRecord(
                new SourceRecords(Arrays.asList(fixture.record(101), fixture.record(102))),
                fixture.collector,
                fixture.state);
        Assertions.assertEquals(
                token(101).toJson(),
                fixture.tracker
                        .current()
                        .getCurrentConsumedPosition()
                        .getValue()
                        .getValues()
                        .get(RESUME_TOKEN_FIELD));
        fixture.emitter.emitRecord(
                new SourceRecords(Collections.emptyList()), fixture.collector, fixture.state);
        Assertions.assertEquals(
                token(101).toJson(),
                fixture.tracker
                        .current()
                        .getCurrentConsumedPosition()
                        .getValue()
                        .getValues()
                        .get(RESUME_TOKEN_FIELD));
        Mockito.verify(fixture.collector, Mockito.times(102)).collect("row");
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void failedEmissionDoesNotPublishMutatedResumeToken(boolean collectorFailure) throws Exception {
        Fixture fixture = new Fixture();
        fixture.emit(1);
        CdcReaderProgressReport first = fixture.tracker.current();
        IllegalStateException failure = new IllegalStateException("fixture emission failure");
        if (collectorFailure) {
            Mockito.doThrow(failure).when(fixture.collector).collect(Mockito.anyString());
        } else {
            Mockito.doThrow(failure).when(fixture.schema).deserialize(Mockito.any(), Mockito.any());
        }

        Assertions.assertSame(
                failure,
                Assertions.assertThrows(IllegalStateException.class, () -> fixture.emit(2)));
        Assertions.assertSame(fixture.offset, fixture.state.getStartupOffset());
        Assertions.assertEquals(token(2), fixture.offset.getResumeToken());
        assertPositionUnchanged(first, fixture.tracker.current());
        Mockito.verify(fixture.collector, Mockito.times(collectorFailure ? 2 : 1)).collect("row");
    }

    @Test
    void pollingDuringEmissionReadsLastSuccessfulResumeToken() throws Exception {
        Fixture fixture = new Fixture();
        fixture.emit(1);
        CdcReaderProgressReport first = fixture.tracker.current();
        CountDownLatch processing = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        Mockito.doAnswer(
                        invocation -> {
                            processing.countDown();
                            Assertions.assertTrue(release.await(10, TimeUnit.SECONDS));
                            invocation.<Collector<String>>getArgument(1).collect("row");
                            return null;
                        })
                .when(fixture.schema)
                .deserialize(Mockito.any(), Mockito.any());
        ExecutorService executor = Executors.newSingleThreadExecutor();
        Future<?> emission =
                executor.submit(
                        () -> {
                            fixture.emit(2);
                            return null;
                        });
        try {
            Assertions.assertTrue(processing.await(10, TimeUnit.SECONDS));
            Assertions.assertEquals(token(2), fixture.offset.getResumeToken());
            for (int i = 0; i < 100; i++) {
                assertPositionUnchanged(first, fixture.tracker.current());
            }
            release.countDown();
            emission.get(10, TimeUnit.SECONDS);
            Assertions.assertEquals(
                    token(2).toJson(),
                    fixture.tracker
                            .current()
                            .getCurrentConsumedPosition()
                            .getValue()
                            .getValues()
                            .get(RESUME_TOKEN_FIELD));
            Assertions.assertEquals(
                    token(1).toJson(),
                    first.getCurrentConsumedPosition()
                            .getValue()
                            .getValues()
                            .get(RESUME_TOKEN_FIELD));
        } finally {
            release.countDown();
            executor.shutdownNow();
            Assertions.assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    private static void assertPositionUnchanged(
            CdcReaderProgressReport expected, CdcReaderProgressReport actual) {
        Assertions.assertEquals(
                CdcProgressAccuracy.EXACT, actual.getCurrentConsumedPosition().getAccuracy());
        Assertions.assertEquals(
                expected.getCurrentConsumedPosition().getValue().getValues(),
                actual.getCurrentConsumedPosition().getValue().getValues());
        Assertions.assertEquals(
                expected.getLastPositionChangeAt(), actual.getLastPositionChangeAt());
        Assertions.assertEquals(expected.getLastSourceEventAt(), actual.getLastSourceEventAt());
    }

    private static BsonDocument token(int increment) {
        // Timestamp prefix consumed by the real resume-token decoder; no MongoDB service needed.
        return new BsonDocument(
                "_data", new BsonString(String.format("8200000001%08X", increment)));
    }

    private static final class Fixture {
        private final AtomicLong nanos = new AtomicLong();
        private final CdcReaderProgressTracker tracker =
                new CdcReaderProgressTracker("MongoDB-CDC", "MONGODB_RESUME_TOKEN", nanos::get);
        private final ChangeStreamOffset offset = new ChangeStreamOffset(token(1));
        private final IncrementalSplitState state =
                new IncrementalSplitState(
                        new IncrementalSplit(
                                "mongo-stream",
                                Collections.singletonList(TableId.parse("inventory.orders")),
                                offset,
                                null,
                                Collections.emptyList()));
        private final DebeziumDeserializationSchema<String> schema =
                Mockito.mock(DebeziumDeserializationSchema.class);
        private final Collector<String> collector = Mockito.mock(Collector.class);
        private final MongoDBRecordEmitter<String> emitter =
                new MongoDBRecordEmitter<>(
                        schema,
                        Mockito.mock(OffsetFactory.class),
                        Mockito.mock(SourceReader.Context.class, Mockito.RETURNS_DEEP_STUBS));

        private Fixture() throws Exception {
            Mockito.doAnswer(
                            invocation -> {
                                invocation.<Collector<String>>getArgument(1).collect("row");
                                return null;
                            })
                    .when(schema)
                    .deserialize(Mockito.any(), Mockito.any());
            emitter.setCdcProgressTracker(tracker);
            tracker.recordSplitState(state);
        }

        private void emit(int increment) throws Exception {
            nanos.addAndGet(TimeUnit.SECONDS.toNanos(1));
            emitter.emitRecord(SourceRecords.fromSingleRecord(record(increment)), collector, state);
        }

        private SourceRecord record(int increment) {
            Schema sourceSchema =
                    SchemaBuilder.struct().field("ts_ms", Schema.INT64_SCHEMA).build();
            Schema valueSchema =
                    SchemaBuilder.struct()
                            .name("fixture.mongodb")
                            .field("source", sourceSchema)
                            .build();
            return new SourceRecord(
                    Collections.emptyMap(),
                    Collections.singletonMap(ID_FIELD, token(increment).toJson()),
                    "inventory.orders",
                    null,
                    null,
                    valueSchema,
                    new Struct(valueSchema)
                            .put(
                                    "source",
                                    new Struct(sourceSchema).put("ts_ms", increment * 1000L)));
        }
    }
}
