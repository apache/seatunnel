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

import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.cdc.base.source.split.SnapshotSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.wartermark.WatermarkEvent;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.exception.MongodbConnectorException;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.source.dialect.MongodbDialect;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.source.offset.ChangeStreamOffset;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils.MongodbUtils;

import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.bson.BsonTimestamp;
import org.bson.RawBsonDocument;
import org.bson.codecs.BsonDocumentCodec;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;

import com.mongodb.MongoException;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.relational.TableId;
import io.debezium.util.LoggingContext;

import java.time.Duration;
import java.util.List;

import static org.apache.seatunnel.api.table.type.BasicType.INT_TYPE;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils.ChunkUtils.maxUpperBoundOfId;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils.ChunkUtils.minLowerBoundOfId;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.RETURNS_SELF;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockConstruction;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class MongodbScanFetchTaskTest {

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    @Timeout(10)
    void shouldSkipBackfillInNonExactlyOnceMode(boolean advancingWatermark) throws Exception {
        assertSnapshotWatermarks(false, advancingWatermark);
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    @Timeout(10)
    void shouldPreserveExactlyOnceBackfill(boolean advancingWatermark) throws Exception {
        assertSnapshotWatermarks(true, advancingWatermark);
    }

    private void assertSnapshotWatermarks(boolean exactlyOnce, boolean advancingWatermark)
            throws Exception {
        TableId collectionId = new TableId("inventory", null, "products");
        SnapshotSplit split =
                new SnapshotSplit(
                        "inventory.products:0",
                        collectionId,
                        new SeaTunnelRowType(
                                new String[] {"_id"}, new SeaTunnelDataType<?>[] {INT_TYPE}),
                        minLowerBoundOfId(),
                        maxUpperBoundOfId());
        MongodbFetchTaskContext context = mock(MongodbFetchTaskContext.class);
        MongodbSourceConfig config = mock(MongodbSourceConfig.class);
        MongodbDialect dialect = mock(MongodbDialect.class);
        MongoClient client = mock(MongoClient.class);
        @SuppressWarnings("unchecked")
        MongoCollection<RawBsonDocument> collection = mock(MongoCollection.class);
        @SuppressWarnings("unchecked")
        FindIterable<RawBsonDocument> iterable = mock(FindIterable.class, RETURNS_SELF);
        @SuppressWarnings("unchecked")
        MongoCursor<RawBsonDocument> cursor = mock(MongoCursor.class);
        ChangeEventQueue<DataChangeEvent> queue =
                new ChangeEventQueue.Builder<DataChangeEvent>()
                        .pollInterval(Duration.ofMillis(10))
                        .maxBatchSize(exactlyOnce ? 4 : 3)
                        // A fourth event would block a non-exactly-once snapshot task.
                        .maxQueueSize(exactlyOnce ? 4 : 3)
                        .loggingContextSupplier(
                                () -> LoggingContext.forConnector("mongodb", "test", "snapshot"))
                        .build();
        ChangeStreamOffset low = new ChangeStreamOffset(new BsonTimestamp(1000, 1));
        ChangeStreamOffset high =
                advancingWatermark ? new ChangeStreamOffset(new BsonTimestamp(2000, 1)) : low;
        when(context.getSourceConfig()).thenReturn(config);
        when(context.getDialect()).thenReturn(dialect);
        when(context.getMongoClient()).thenReturn(client);
        when(context.getQueue()).thenReturn(queue);
        when(context.isExactlyOnce()).thenReturn(exactlyOnce);
        when(config.getHosts()).thenReturn("localhost:27017");
        when(dialect.displayCurrentOffset(config)).thenReturn(low, high);
        when(collection.find()).thenReturn(iterable);
        when(iterable.cursor()).thenReturn(cursor);
        when(cursor.hasNext()).thenReturn(true, false);
        when(cursor.next())
                .thenReturn(
                        new RawBsonDocument(
                                new BsonDocument("_id", new BsonInt32(1)),
                                new BsonDocumentCodec()));

        try (MockedStatic<MongodbUtils> utils = mockStatic(MongodbUtils.class);
                MockedConstruction<MongodbStreamFetchTask> backfill =
                        mockConstruction(MongodbStreamFetchTask.class)) {
            utils.when(() -> MongodbUtils.buildConnectionNamespacePrefix("localhost:27017"))
                    .thenReturn("localhost:27017");
            utils.when(
                            () ->
                                    MongodbUtils.getMongoCollection(
                                            client, collectionId, RawBsonDocument.class))
                    .thenReturn(collection);
            MongodbScanFetchTask task = new MongodbScanFetchTask(split);
            task.execute(context);

            List<DataChangeEvent> events = queue.poll();
            boolean requiresBackfill = exactlyOnce && advancingWatermark;
            assertEquals(requiresBackfill ? 1 : 0, backfill.constructed().size());
            if (requiresBackfill) {
                verify(backfill.constructed().get(0)).execute(context);
            }
            assertEquals(exactlyOnce && !advancingWatermark ? 4 : 3, events.size());
            assertTrue(WatermarkEvent.isLowWatermarkEvent(events.get(0).getRecord()));
            assertFalse(WatermarkEvent.isWatermarkEvent(events.get(1).getRecord()));
            assertTrue(WatermarkEvent.isHighWatermarkEvent(events.get(2).getRecord()));
            if (events.size() == 4) {
                assertTrue(WatermarkEvent.isEndWatermarkEvent(events.get(3).getRecord()));
            }
            verify(cursor).close();
            assertFalse(task.isRunning());
        }
    }

    @Test
    void shouldPreserveSnapshotReadFailureCause() throws Exception {
        TableId collectionId = new TableId("inventory", null, "products");
        SnapshotSplit snapshotSplit =
                new SnapshotSplit(
                        "inventory.products:0",
                        collectionId,
                        new SeaTunnelRowType(
                                new String[] {"_id"}, new SeaTunnelDataType<?>[] {INT_TYPE}),
                        minLowerBoundOfId(),
                        maxUpperBoundOfId());
        MongodbScanFetchTask fetchTask = new MongodbScanFetchTask(snapshotSplit);

        MongodbFetchTaskContext taskContext = mock(MongodbFetchTaskContext.class);
        MongodbSourceConfig sourceConfig = mock(MongodbSourceConfig.class);
        MongodbDialect dialect = mock(MongodbDialect.class);
        MongoClient mongoClient = mock(MongoClient.class);
        @SuppressWarnings("unchecked")
        ChangeEventQueue<DataChangeEvent> queue = mock(ChangeEventQueue.class);
        @SuppressWarnings("unchecked")
        MongoCollection<RawBsonDocument> collection = mock(MongoCollection.class);
        ChangeStreamOffset lowWatermark = new ChangeStreamOffset(new BsonTimestamp(1));
        MongoException snapshotReadFailure = new MongoException(2, "invalid snapshot bounds");

        when(taskContext.getSourceConfig()).thenReturn(sourceConfig);
        when(taskContext.getDialect()).thenReturn(dialect);
        when(taskContext.getQueue()).thenReturn(queue);
        when(taskContext.getMongoClient()).thenReturn(mongoClient);
        when(dialect.displayCurrentOffset(sourceConfig)).thenReturn(lowWatermark);
        when(collection.find()).thenThrow(snapshotReadFailure);

        try (MockedStatic<MongodbUtils> mongodbUtils = mockStatic(MongodbUtils.class)) {
            mongodbUtils
                    .when(
                            () ->
                                    MongodbUtils.getMongoCollection(
                                            mongoClient, collectionId, RawBsonDocument.class))
                    .thenReturn(collection);

            MongodbConnectorException actual =
                    assertThrows(
                            MongodbConnectorException.class, () -> fetchTask.execute(taskContext));

            assertSame(snapshotReadFailure, actual.getCause());
            assertFalse(fetchTask.isRunning());
        }
    }
}
