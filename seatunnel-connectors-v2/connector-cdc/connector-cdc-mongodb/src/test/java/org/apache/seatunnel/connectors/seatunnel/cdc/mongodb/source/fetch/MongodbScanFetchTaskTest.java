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

package org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.source.fetch;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.cdc.base.source.split.SnapshotSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.wartermark.WatermarkEvent;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.source.dialect.MongodbDialect;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.source.offset.ChangeStreamOffset;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils.MongodbUtils;

import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonString;
import org.bson.BsonTimestamp;
import org.bson.RawBsonDocument;
import org.bson.codecs.BsonDocumentCodec;
import org.bson.codecs.EncoderContext;
import org.bson.io.BasicOutputBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.relational.TableId;
import io.debezium.util.LoggingContext;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class MongodbScanFetchTaskTest {

    @Mock private MongodbDialect dialect;
    @Mock private MongodbSourceConfig sourceConfig;
    @Mock private MongoClient mongoClient;
    @Mock private MongoDatabase mongoDatabase;
    @Mock private MongoCollection<RawBsonDocument> mongoCollection;
    @Mock private FindIterable<RawBsonDocument> findIterable;
    @Mock private MongoCursor<RawBsonDocument> mongoCursor;

    private ChangeEventQueue<DataChangeEvent> changeEventQueue;
    private SnapshotSplit snapshotSplit;
    private MongodbFetchTaskContext taskContext;

    private static final TableId TABLE_ID = TableId.parse("test_db.test_collection");
    private static final ChangeStreamOffset LOW_WATERMARK =
            new ChangeStreamOffset(new BsonTimestamp(1000, 1));
    private static final ChangeStreamOffset HIGH_WATERMARK =
            new ChangeStreamOffset(new BsonTimestamp(2000, 1));

    private static final BsonDocumentCodec BSON_CODEC = new BsonDocumentCodec();
    private static final EncoderContext ENCODER_CONTEXT = EncoderContext.builder().build();

    /** Helper to convert a BsonDocument to a RawBsonDocument. */
    private static RawBsonDocument toRaw(BsonDocument doc) {
        BasicOutputBuffer buffer = new BasicOutputBuffer();
        org.bson.BsonBinaryWriter writer = new org.bson.BsonBinaryWriter(buffer);
        BSON_CODEC.encode(writer, doc, ENCODER_CONTEXT);
        writer.close();
        return new RawBsonDocument(buffer.toByteArray());
    }

    @BeforeEach
    void setUp() {
        // Create a snapshot split
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(new String[] {"_id"}, new BasicType[] {BasicType.STRING_TYPE});
        BsonDocument splitKey = new BsonDocument("_id", new BsonInt32(1));
        Object[] splitStart = new Object[] {splitKey, new BsonDocument("_id", new BsonString(""))};
        Object[] splitEnd = new Object[] {splitKey, new BsonDocument("_id", new BsonString("zzz"))};
        snapshotSplit = new SnapshotSplit("split-1", TABLE_ID, rowType, splitStart, splitEnd);

        // Create a bounded ChangeEventQueue
        changeEventQueue =
                new ChangeEventQueue.Builder<DataChangeEvent>()
                        .pollInterval(Duration.ofMillis(100))
                        .maxBatchSize(1024)
                        .maxQueueSize(1024)
                        .loggingContextSupplier(
                                () ->
                                        LoggingContext.forConnector(
                                                "mongodb-cdc",
                                                "mongodb-cdc-connector",
                                                "mongodb-cdc-connector-task"))
                        .build();

        // Create the task context with isExactlyOnce = false
        // Mock MongodbUtils.createMongoClient to avoid creating a real MongoDB client
        try (MockedStatic<MongodbUtils> mockedStatic = mockStatic(MongodbUtils.class)) {
            mockedStatic
                    .when(() -> MongodbUtils.createMongoClient(any(MongodbSourceConfig.class)))
                    .thenReturn(mongoClient);
            taskContext =
                    new MongodbFetchTaskContext(dialect, sourceConfig, null) {
                        @Override
                        public boolean isExactlyOnce() {
                            return false;
                        }

                        @Override
                        public ChangeEventQueue<DataChangeEvent> getQueue() {
                            return changeEventQueue;
                        }

                        @Override
                        public MongoClient getMongoClient() {
                            return mongoClient;
                        }
                    };
        }
    }

    @Test
    void testNonExactlyOnceModeSkipsBackfillAndDoesNotSendEndWatermark() throws Exception {
        // Setup mock MongoDB interactions
        when(sourceConfig.getBatchSize()).thenReturn(1024);
        when(sourceConfig.getHosts()).thenReturn("localhost:27017");

        // Mock dialect to return watermarks
        when(dialect.displayCurrentOffset(sourceConfig)).thenReturn(LOW_WATERMARK, HIGH_WATERMARK);

        // Mock MongoClient chain
        when(mongoClient.getDatabase(anyString())).thenReturn(mongoDatabase);
        when(mongoDatabase.getCollection(anyString(), eq(RawBsonDocument.class)))
                .thenReturn(mongoCollection);

        // Mock the FindIterable chain: find() -> min() -> max() -> hint() -> batchSize() ->
        // noCursorTimeout() -> cursor()
        when(mongoCollection.find()).thenReturn(findIterable);
        when(findIterable.min(any(BsonDocument.class))).thenReturn(findIterable);
        when(findIterable.max(any(BsonDocument.class))).thenReturn(findIterable);
        when(findIterable.hint(any(BsonDocument.class))).thenReturn(findIterable);
        when(findIterable.batchSize(anyInt())).thenReturn(findIterable);
        when(findIterable.noCursorTimeout(anyBoolean())).thenReturn(findIterable);
        when(findIterable.cursor()).thenReturn(mongoCursor);

        // Mock the cursor to return one snapshot document then end
        BsonDocument snapshotDoc =
                new BsonDocument("_id", new BsonString("doc1"))
                        .append("name", new BsonString("test"));
        when(mongoCursor.hasNext()).thenReturn(true, false);
        when(mongoCursor.next()).thenReturn(toRaw(snapshotDoc));

        // Execute the task
        MongodbScanFetchTask task = new MongodbScanFetchTask(snapshotSplit);
        task.execute(taskContext);

        // Verify the queue contains the expected events
        // We should have: LOW watermark, snapshot data, HIGH watermark
        // (no backfill MongodbStreamFetchTask was created, no END watermark in non-exactly-once)
        List<DataChangeEvent> events = new ArrayList<>();
        try {
            List<DataChangeEvent> batch = changeEventQueue.poll();
            while (batch != null && !batch.isEmpty()) {
                events.addAll(batch);
                batch = changeEventQueue.poll();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }

        // We should have at least 3 events: LOW, snapshot data, HIGH
        assertFalse(events.isEmpty(), "Queue should contain events");
        assertTrue(
                events.size() >= 3,
                "Should have at least 3 events (LOW, data, HIGH), got " + events.size());

        // Verify the last event is a HIGH watermark (not END)
        DataChangeEvent lastEvent = events.get(events.size() - 1);
        assertTrue(
                WatermarkEvent.isHighWatermarkEvent(lastEvent.getRecord()),
                "Last event should be a HIGH watermark in non-exactly-once mode");

        // Verify HIGH watermark is present and END watermark is NOT present
        boolean foundHigh = false;
        boolean foundEnd = false;
        for (DataChangeEvent event : events) {
            if (WatermarkEvent.isWatermarkEvent(event.getRecord())) {
                if (WatermarkEvent.isHighWatermarkEvent(event.getRecord())) {
                    foundHigh = true;
                }
                if (WatermarkEvent.isEndWatermarkEvent(event.getRecord())) {
                    foundEnd = true;
                }
            }
        }
        assertTrue(foundHigh, "Should contain HIGH watermark");
        assertFalse(
                foundEnd,
                "Should NOT contain END watermark in non-exactly-once mode — END is only consumed by exactly-once reader path");

        // Verify the task is no longer running
        assertFalse(task.isRunning(), "Task should be completed");
    }
}
