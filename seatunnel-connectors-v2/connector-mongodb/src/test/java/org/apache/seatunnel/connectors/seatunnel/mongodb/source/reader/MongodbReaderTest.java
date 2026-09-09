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

package org.apache.seatunnel.connectors.seatunnel.mongodb.source.reader;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.mongodb.internal.MongodbClientProvider;
import org.apache.seatunnel.connectors.seatunnel.mongodb.serde.DocumentDeserializer;
import org.apache.seatunnel.connectors.seatunnel.mongodb.source.config.MongodbReadOptions;
import org.apache.seatunnel.connectors.seatunnel.mongodb.source.split.MongoSplit;

import org.bson.BsonDocument;
import org.junit.jupiter.api.Test;

import com.mongodb.ServerAddress;
import com.mongodb.ServerCursor;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;

import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class MongodbReaderTest {

    @Test
    void pollNextPreservesOriginalCursorCreationException() {
        RuntimeException cursorCreationFailure = new RuntimeException("cursor creation failed");
        MongodbClientProvider clientProvider = mock(MongodbClientProvider.class);
        when(clientProvider.getDefaultCollection()).thenThrow(cursorCreationFailure);

        SourceReader.Context context = mock(SourceReader.Context.class);
        DocumentDeserializer<SeaTunnelRow> deserializer = mock(DocumentDeserializer.class);
        Collector<SeaTunnelRow> collector = mock(Collector.class);
        when(collector.getCheckpointLock()).thenReturn(new Object());

        MongodbReader reader =
                new MongodbReader(
                        context,
                        clientProvider,
                        deserializer,
                        MongodbReadOptions.builder().build());
        reader.addSplits(
                Collections.singletonList(
                        new MongoSplit("split-1", new BsonDocument(), new BsonDocument(), 0L)));

        RuntimeException actual =
                assertThrows(RuntimeException.class, () -> reader.pollNext(collector));

        assertSame(cursorCreationFailure, actual);
    }

    @Test
    void pollNextClosesCursorWhenDeserializationFails() {
        RuntimeException deserializationFailure = new RuntimeException("deserialization failed");
        MongodbClientProvider clientProvider = mock(MongodbClientProvider.class);
        MongoCollection<BsonDocument> collection = mock(MongoCollection.class);
        FindIterable<BsonDocument> iterable = mock(FindIterable.class);
        AtomicBoolean cursorClosed = new AtomicBoolean();
        MongoCursor<BsonDocument> cursor =
                new MongoCursor<BsonDocument>() {
                    private boolean consumed;

                    @Override
                    public void close() {
                        cursorClosed.set(true);
                    }

                    @Override
                    public boolean hasNext() {
                        return !consumed;
                    }

                    @Override
                    public BsonDocument next() {
                        consumed = true;
                        return new BsonDocument();
                    }

                    @Override
                    public int available() {
                        return hasNext() ? 1 : 0;
                    }

                    @Override
                    public BsonDocument tryNext() {
                        return hasNext() ? next() : null;
                    }

                    @Override
                    public ServerCursor getServerCursor() {
                        return null;
                    }

                    @Override
                    public ServerAddress getServerAddress() {
                        return null;
                    }
                };

        when(clientProvider.getDefaultCollection()).thenReturn(collection);
        when(collection.find(any(BsonDocument.class))).thenReturn(iterable);
        when(iterable.projection(any(BsonDocument.class))).thenReturn(iterable);
        when(iterable.batchSize(anyInt())).thenReturn(iterable);
        when(iterable.noCursorTimeout(anyBoolean())).thenReturn(iterable);
        when(iterable.maxTime(anyLong(), any(TimeUnit.class))).thenReturn(iterable);
        when(iterable.iterator()).thenReturn(cursor);

        SourceReader.Context context = mock(SourceReader.Context.class);
        DocumentDeserializer<SeaTunnelRow> deserializer = mock(DocumentDeserializer.class);
        Collector<SeaTunnelRow> collector = mock(Collector.class);
        when(collector.getCheckpointLock()).thenReturn(new Object());
        when(deserializer.deserialize(any(BsonDocument.class))).thenThrow(deserializationFailure);

        MongodbReader reader =
                new MongodbReader(
                        context,
                        clientProvider,
                        deserializer,
                        MongodbReadOptions.builder().build());
        reader.addSplits(
                Collections.singletonList(
                        new MongoSplit("split-1", new BsonDocument(), new BsonDocument(), 0L)));

        RuntimeException actual =
                assertThrows(RuntimeException.class, () -> reader.pollNext(collector));

        assertSame(deserializationFailure, actual);
        assertTrue(cursorClosed.get());
    }
}
