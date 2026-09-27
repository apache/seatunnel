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
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.exception.MongodbConnectorException;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.source.dialect.MongodbDialect;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.source.offset.ChangeStreamOffset;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils.MongodbUtils;

import org.bson.BsonTimestamp;
import org.bson.RawBsonDocument;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import com.mongodb.MongoException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import io.debezium.connector.base.ChangeEventQueue;
import io.debezium.pipeline.DataChangeEvent;
import io.debezium.relational.TableId;

import static org.apache.seatunnel.api.table.type.BasicType.INT_TYPE;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils.ChunkUtils.maxUpperBoundOfId;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.utils.ChunkUtils.minLowerBoundOfId;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

class MongodbScanFetchTaskTest {

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
