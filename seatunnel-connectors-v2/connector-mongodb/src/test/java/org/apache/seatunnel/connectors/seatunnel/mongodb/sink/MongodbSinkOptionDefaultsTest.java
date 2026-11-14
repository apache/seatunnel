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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.mongodb.config.MongodbSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.mongodb.exception.MongodbConnectorException;
import org.apache.seatunnel.connectors.seatunnel.mongodb.internal.MongodbSingleCollectionProvider;

import org.bson.BsonDocument;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import com.mongodb.MongoException;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.BulkWriteOptions;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Regression tests for the declared defaults of the MongoDB sink buffer and retry options.
 *
 * <p>The sink is created through {@link MongodbSinkFactory} exactly as the engine does, and the
 * effective option values are observed through the writer behavior (number of bulk writes and retry
 * attempts) against a mocked collection.
 */
class MongodbSinkOptionDefaultsTest {

    private static final SeaTunnelRowType ROW_TYPE =
            new SeaTunnelRowType(
                    new String[] {"id"}, new SeaTunnelDataType<?>[] {BasicType.LONG_TYPE});

    @Test
    void omittedBufferOptionsUseDeclaredDefaults() throws Exception {
        // Default buffer-flush.max-rows = 1000 and buffer-flush.interval = 30000 ms: the first
        // row is flushed because no bulk write happened yet, the next 999 rows stay buffered.
        Assertions.assertEquals(1, bulkWritesAfterRows(baseConfig(), 1000));
    }

    @Test
    void explicitBufferOptionsAreStillApplied() throws Exception {
        Map<String, Object> config = baseConfig();
        config.put(MongodbSinkOptions.BUFFER_FLUSH_MAX_ROWS.key(), 10);
        config.put(MongodbSinkOptions.BUFFER_FLUSH_INTERVAL.key(), 60000L);
        // First row flushed, then one bulk write per 10 buffered rows: 1 + 999 / 10.
        Assertions.assertEquals(100, bulkWritesAfterRows(config, 1000));
    }

    @Test
    void omittedRetryMaxUsesDeclaredDefault() throws Exception {
        Map<String, Object> config = baseConfig();
        config.put(MongodbSinkOptions.RETRY_INTERVAL.key(), 1L);
        // Default retry.max = 3: one attempt plus three retries.
        Assertions.assertEquals(4, attemptsUntilFailure(config));
    }

    @Test
    void explicitRetryMaxIsStillApplied() throws Exception {
        Map<String, Object> config = baseConfig();
        config.put(MongodbSinkOptions.RETRY_MAX.key(), 1);
        config.put(MongodbSinkOptions.RETRY_INTERVAL.key(), 1L);
        Assertions.assertEquals(2, attemptsUntilFailure(config));
    }

    @Test
    void omittedRetryIntervalUsesDeclaredDefault() throws Exception {
        Map<String, Object> config = baseConfig();
        config.put(MongodbSinkOptions.RETRY_MAX.key(), 1);
        long start = System.nanoTime();
        Assertions.assertEquals(2, attemptsUntilFailure(config));
        long elapsedMs = (System.nanoTime() - start) / 1_000_000;
        // Default retry.interval = 1000 ms before the single retry.
        Assertions.assertTrue(
                elapsedMs >= 1000, "expected a retry back-off of 1000 ms, got " + elapsedMs);
    }

    private static Map<String, Object> baseConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put(MongodbSinkOptions.URI.key(), "mongodb://localhost:27017");
        config.put(MongodbSinkOptions.DATABASE.key(), "test_database");
        config.put(MongodbSinkOptions.COLLECTION.key(), "test_collection");
        return config;
    }

    private static MongodbSink createSink(Map<String, Object> config) {
        TableSinkFactoryContext context =
                new TableSinkFactoryContext(
                        CatalogTableUtil.getCatalogTable("test_table", ROW_TYPE),
                        ReadonlyConfig.fromMap(config),
                        MongodbSinkOptionDefaultsTest.class.getClassLoader());
        return (MongodbSink) new MongodbSinkFactory().createSink(context).createSink();
    }

    private static SeaTunnelRow row(long id) {
        return new SeaTunnelRow(new Object[] {id});
    }

    /** Writes {@code rows} rows (without closing the writer) and returns the bulk write count. */
    @SuppressWarnings("unchecked")
    private static int bulkWritesAfterRows(Map<String, Object> config, int rows) throws Exception {
        AtomicInteger bulkWrites = new AtomicInteger();
        MongoCollection<BsonDocument> collection = mock(MongoCollection.class);
        doAnswer(
                        invocation -> {
                            bulkWrites.incrementAndGet();
                            return null;
                        })
                .when(collection)
                .bulkWrite(anyList(), any(BulkWriteOptions.class));
        try (MockedConstruction<MongodbSingleCollectionProvider> ignored =
                mockCollectionProvider(collection)) {
            MongodbWriter writer = createSink(config).createWriter(mock(SinkWriter.Context.class));
            for (int i = 0; i < rows; i++) {
                writer.write(row(i));
            }
            return bulkWrites.get();
        }
    }

    /** Lets every bulk write fail and returns the number of attempts before the writer gives up. */
    @SuppressWarnings("unchecked")
    private static int attemptsUntilFailure(Map<String, Object> config) throws Exception {
        AtomicInteger attempts = new AtomicInteger();
        MongoCollection<BsonDocument> collection = mock(MongoCollection.class);
        doAnswer(
                        invocation -> {
                            attempts.incrementAndGet();
                            throw new MongoException("simulated failure");
                        })
                .when(collection)
                .bulkWrite(anyList(), any(BulkWriteOptions.class));
        try (MockedConstruction<MongodbSingleCollectionProvider> ignored =
                mockCollectionProvider(collection)) {
            MongodbWriter writer = createSink(config).createWriter(mock(SinkWriter.Context.class));
            Assertions.assertThrows(MongodbConnectorException.class, () -> writer.write(row(1)));
            return attempts.get();
        }
    }

    private static MockedConstruction<MongodbSingleCollectionProvider> mockCollectionProvider(
            MongoCollection<BsonDocument> collection) {
        return Mockito.mockConstruction(
                MongodbSingleCollectionProvider.class,
                (provider, context) ->
                        when(provider.getDefaultCollection()).thenReturn(collection));
    }
}
