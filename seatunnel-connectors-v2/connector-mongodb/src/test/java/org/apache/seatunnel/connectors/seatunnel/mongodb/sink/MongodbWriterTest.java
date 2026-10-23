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

package org.apache.seatunnel.connectors.seatunnel.mongodb.sink;

import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.utils.function.RunnableWithException;
import org.apache.seatunnel.connectors.seatunnel.mongodb.exception.MongodbConnectorException;
import org.apache.seatunnel.connectors.seatunnel.mongodb.internal.MongodbSingleCollectionProvider;
import org.apache.seatunnel.connectors.seatunnel.mongodb.serde.DocumentSerializer;

import org.bson.BsonDocument;
import org.bson.BsonInt32;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.BulkWriteOptions;
import com.mongodb.client.model.InsertOneModel;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class MongodbWriterTest {

    @Test
    void shouldRegisterAndExecuteTimerFlushForNonTransactionWriter() throws Exception {
        SinkWriter.Context context = mock(SinkWriter.Context.class);
        MongoCollection<BsonDocument> collection = mock(MongoCollection.class);
        ArgumentCaptor<RunnableWithException> actionCaptor =
                ArgumentCaptor.forClass(RunnableWithException.class);

        try (MockedConstruction<MongodbSingleCollectionProvider> ignored =
                mockCollectionProvider(collection)) {
            MongodbWriter writer = createWriter(context, false);
            writer.write(createRow());

            verify(context, times(1)).registerFlushAction(actionCaptor.capture());
            verify(collection, never())
                    .bulkWrite(Mockito.anyList(), Mockito.any(BulkWriteOptions.class));

            actionCaptor.getValue().run();

            verify(collection, times(1))
                    .bulkWrite(Mockito.anyList(), Mockito.any(BulkWriteOptions.class));
        }
    }

    @Test
    void shouldNotRegisterTimerFlushForTransactionWriter() {
        SinkWriter.Context context = mock(SinkWriter.Context.class);
        MongoCollection<BsonDocument> collection = mock(MongoCollection.class);

        try (MockedConstruction<MongodbSingleCollectionProvider> ignored =
                mockCollectionProvider(collection)) {
            createWriter(context, true);

            verify(context, never()).registerFlushAction(Mockito.any());
        }
    }

    @Test
    void shouldPropagateTimerFlushFailure() throws Exception {
        SinkWriter.Context context = mock(SinkWriter.Context.class);
        MongoCollection<BsonDocument> collection = mock(MongoCollection.class);
        ArgumentCaptor<RunnableWithException> actionCaptor =
                ArgumentCaptor.forClass(RunnableWithException.class);
        MongoException expected = new MongoException("timer flush failed");
        doThrow(expected)
                .when(collection)
                .bulkWrite(Mockito.anyList(), Mockito.any(BulkWriteOptions.class));

        try (MockedConstruction<MongodbSingleCollectionProvider> ignored =
                mockCollectionProvider(collection)) {
            MongodbWriter writer = createWriter(context, false);
            writer.write(createRow());
            verify(context).registerFlushAction(actionCaptor.capture());

            MongodbConnectorException actual =
                    Assertions.assertThrows(
                            MongodbConnectorException.class, actionCaptor.getValue()::run);

            Assertions.assertSame(expected, actual.getCause());
        }
    }

    private MockedConstruction<MongodbSingleCollectionProvider> mockCollectionProvider(
            MongoCollection<BsonDocument> collection) {
        return Mockito.mockConstruction(
                MongodbSingleCollectionProvider.class,
                (provider, context) ->
                        when(provider.getDefaultCollection()).thenReturn(collection));
    }

    private MongodbWriter createWriter(SinkWriter.Context context, boolean transaction) {
        DocumentSerializer<SeaTunnelRow> serializer = mock(DocumentSerializer.class);
        when(serializer.serializeToWriteModel(Mockito.any()))
                .thenReturn(new InsertOneModel<>(new BsonDocument("id", new BsonInt32(1))));
        MongodbWriterOptions options =
                MongodbWriterOptions.builder()
                        .withConnectString("mongodb://localhost:27017")
                        .withDatabase("timer_flush")
                        .withCollection("timer_flush")
                        .withFlushSize(100)
                        .withBatchIntervalMs(-1L)
                        .withRetryMax(0)
                        .withRetryInterval(0L)
                        .withTransaction(transaction)
                        .build();
        return new MongodbWriter(serializer, options, context);
    }

    private SeaTunnelRow createRow() {
        SeaTunnelRow row = mock(SeaTunnelRow.class);
        when(row.getRowKind()).thenReturn(RowKind.INSERT);
        return row;
    }
}
