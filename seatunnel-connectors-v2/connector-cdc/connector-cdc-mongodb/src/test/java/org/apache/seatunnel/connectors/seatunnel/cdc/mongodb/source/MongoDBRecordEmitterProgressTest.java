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

import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceConstants.ID_FIELD;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceConstants.RESUME_TOKEN_FIELD;

class MongoDBRecordEmitterProgressTest {

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
        private final CdcReaderProgressTracker tracker =
                new CdcReaderProgressTracker("MongoDB-CDC", "MONGODB_RESUME_TOKEN");
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
            Schema valueSchema = SchemaBuilder.struct().name("fixture.mongodb").build();
            SourceRecord record =
                    new SourceRecord(
                            Collections.emptyMap(),
                            Collections.singletonMap(ID_FIELD, token(increment).toJson()),
                            "inventory.orders",
                            null,
                            null,
                            valueSchema,
                            new Struct(valueSchema));
            emitter.emitRecord(SourceRecords.fromSingleRecord(record), collector, state);
        }
    }
}
