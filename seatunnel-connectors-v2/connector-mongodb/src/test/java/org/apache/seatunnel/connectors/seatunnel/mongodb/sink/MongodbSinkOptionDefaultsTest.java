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

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
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
 * effective option values are observed through the writer behavior (number of bulk writes, retry
 * attempts and back-off) against a mocked collection.
 *
 * <p>The writer compares {@code System.currentTimeMillis()} with the time of its last bulk write.
 * To keep the tests independent of scheduling pauses, they set that time explicitly: far in the
 * future to isolate the size threshold, or a fixed distance in the past to test the interval.
 */
class MongodbSinkOptionDefaultsTest {

    private static final SeaTunnelRowType ROW_TYPE =
            new SeaTunnelRowType(
                    new String[] {"id"}, new SeaTunnelDataType<?>[] {BasicType.LONG_TYPE});

    /** A last-write time in the future: the interval threshold can never be reached. */
    private static final long INTERVAL_NEVER_REACHED = Long.MAX_VALUE / 2;

    @Test
    void omittedMaxRowsUsesDeclaredDefault() throws Exception {
        // Default buffer-flush.max-rows = 1000.
        Assertions.assertEquals(0, sizeTriggeredBulkWrites(baseConfig(), 999));
        Assertions.assertEquals(1, sizeTriggeredBulkWrites(baseConfig(), 1000));
        Assertions.assertEquals(2, sizeTriggeredBulkWrites(baseConfig(), 2000));
    }

    @Test
    void explicitMaxRowsIsStillApplied() throws Exception {
        Map<String, Object> config = baseConfig();
        config.put(MongodbSinkOptions.BUFFER_FLUSH_MAX_ROWS.key(), 10);
        config.put(MongodbSinkOptions.BUFFER_FLUSH_INTERVAL.key(), 60_000L);
        Assertions.assertEquals(100, sizeTriggeredBulkWrites(config, 1000));
    }

    @Test
    void omittedIntervalUsesDeclaredDefault() throws Exception {
        // Default buffer-flush.interval = 30000 ms, checked when a row is written.
        Assertions.assertFalse(rowFlushesAfterIdleMs(baseConfig(), 25_000));
        Assertions.assertTrue(rowFlushesAfterIdleMs(baseConfig(), 30_000));
    }

    @Test
    void explicitIntervalIsStillApplied() throws Exception {
        Map<String, Object> config = baseConfig();
        config.put(MongodbSinkOptions.BUFFER_FLUSH_INTERVAL.key(), 60_000L);
        Assertions.assertFalse(rowFlushesAfterIdleMs(config, 30_000));
        Assertions.assertTrue(rowFlushesAfterIdleMs(config, 60_000));

        config.put(MongodbSinkOptions.BUFFER_FLUSH_INTERVAL.key(), -1L);
        Assertions.assertFalse(rowFlushesAfterIdleMs(config, 3_600_000));
    }

    @Test
    void omittedRetryMaxUsesDeclaredDefault() throws Exception {
        Map<String, Object> config = baseConfig();
        config.put(MongodbSinkOptions.RETRY_INTERVAL.key(), 1L);
        // Default retry.max = 3: one attempt plus three retries.
        Assertions.assertEquals(4, attemptTimesUntilFailure(config).size());
    }

    @Test
    void explicitRetryMaxIsStillApplied() throws Exception {
        Map<String, Object> config = baseConfig();
        config.put(MongodbSinkOptions.RETRY_MAX.key(), 1);
        config.put(MongodbSinkOptions.RETRY_INTERVAL.key(), 1L);
        Assertions.assertEquals(2, attemptTimesUntilFailure(config).size());
    }

    @Test
    void omittedRetryIntervalUsesDeclaredDefault() throws Exception {
        Map<String, Object> config = baseConfig();
        config.put(MongodbSinkOptions.RETRY_MAX.key(), 1);
        List<Long> attempts = attemptTimesUntilFailure(config);
        Assertions.assertEquals(2, attempts.size());
        // Default retry.interval = 1000 ms before the first retry.
        assertBackOffAtLeast(attempts, 0, 1000);
    }

    @Test
    void explicitRetryIntervalIsStillApplied() throws Exception {
        Map<String, Object> config = baseConfig();
        config.put(MongodbSinkOptions.RETRY_MAX.key(), 2);
        config.put(MongodbSinkOptions.RETRY_INTERVAL.key(), 50L);
        List<Long> attempts = attemptTimesUntilFailure(config);
        Assertions.assertEquals(3, attempts.size());
        // Linear back-off: retry.interval * attempt number.
        assertBackOffAtLeast(attempts, 0, 50);
        assertBackOffAtLeast(attempts, 1, 100);
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

    /** Writes {@code rows} rows with the interval threshold disabled and counts bulk writes. */
    private static int sizeTriggeredBulkWrites(Map<String, Object> config, int rows)
            throws Exception {
        AtomicInteger bulkWrites = new AtomicInteger();
        try (MockedConstruction<MongodbSingleCollectionProvider> ignored =
                mockCollectionProvider(countingCollection(bulkWrites))) {
            MongodbWriter writer = createSink(config).createWriter(mock(SinkWriter.Context.class));
            try {
                setLastSendTime(writer, INTERVAL_NEVER_REACHED);
                for (int i = 0; i < rows; i++) {
                    writer.write(row(i));
                }
                return bulkWrites.get();
            } finally {
                closeQuietly(writer);
            }
        }
    }

    /** Returns whether one row is flushed when the last bulk write was {@code idleMs} ago. */
    private static boolean rowFlushesAfterIdleMs(Map<String, Object> config, long idleMs)
            throws Exception {
        // Keep the size threshold out of the way.
        config.put(MongodbSinkOptions.BUFFER_FLUSH_MAX_ROWS.key(), 1_000_000);
        AtomicInteger bulkWrites = new AtomicInteger();
        try (MockedConstruction<MongodbSingleCollectionProvider> ignored =
                mockCollectionProvider(countingCollection(bulkWrites))) {
            MongodbWriter writer = createSink(config).createWriter(mock(SinkWriter.Context.class));
            try {
                setLastSendTime(writer, System.currentTimeMillis() - idleMs);
                writer.write(row(1));
                return bulkWrites.get() == 1;
            } finally {
                closeQuietly(writer);
            }
        }
    }

    /** Lets every bulk write fail and returns the time of each attempt in nanoseconds. */
    @SuppressWarnings("unchecked")
    private static List<Long> attemptTimesUntilFailure(Map<String, Object> config)
            throws Exception {
        List<Long> attempts = Collections.synchronizedList(new ArrayList<>());
        MongoCollection<BsonDocument> collection = mock(MongoCollection.class);
        doAnswer(
                        invocation -> {
                            attempts.add(System.nanoTime());
                            throw new MongoException("simulated failure");
                        })
                .when(collection)
                .bulkWrite(anyList(), any(BulkWriteOptions.class));
        try (MockedConstruction<MongodbSingleCollectionProvider> ignored =
                mockCollectionProvider(collection)) {
            MongodbWriter writer = createSink(config).createWriter(mock(SinkWriter.Context.class));
            Assertions.assertThrows(MongodbConnectorException.class, () -> writer.write(row(1)));
            List<Long> result = new ArrayList<>(attempts);
            closeQuietly(writer);
            return result;
        }
    }

    private static void assertBackOffAtLeast(List<Long> attempts, int retry, long minMs) {
        long gapMs = (attempts.get(retry + 1) - attempts.get(retry)) / 1_000_000;
        Assertions.assertTrue(
                gapMs >= minMs,
                "expected a back-off of at least "
                        + minMs
                        + " ms before retry "
                        + (retry + 1)
                        + ", got "
                        + gapMs
                        + " ms");
    }

    @SuppressWarnings("unchecked")
    private static MongoCollection<BsonDocument> countingCollection(AtomicInteger bulkWrites) {
        MongoCollection<BsonDocument> collection = mock(MongoCollection.class);
        doAnswer(
                        invocation -> {
                            bulkWrites.incrementAndGet();
                            return null;
                        })
                .when(collection)
                .bulkWrite(anyList(), any(BulkWriteOptions.class));
        return collection;
    }

    private static void setLastSendTime(MongodbWriter writer, long millis) throws Exception {
        Field field = MongodbWriter.class.getDeclaredField("lastSendTime");
        field.setAccessible(true);
        field.setLong(writer, millis);
    }

    /** Closes the writer after the counts were taken; a flush failure on close is not relevant. */
    private static void closeQuietly(MongodbWriter writer) {
        try {
            writer.close();
        } catch (Exception ignored) {
            // the remaining buffered rows are flushed on close; failures were already asserted
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
