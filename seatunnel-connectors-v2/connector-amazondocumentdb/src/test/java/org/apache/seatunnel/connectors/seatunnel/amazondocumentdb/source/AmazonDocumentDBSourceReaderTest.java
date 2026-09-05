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

package org.apache.seatunnel.connectors.seatunnel.amazondocumentdb.source;

import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.amazondocumentdb.config.AmazonDocumentDBConfig;

import org.bson.BsonDocument;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import java.lang.reflect.Proxy;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class AmazonDocumentDBSourceReaderTest {

    @Test
    public void testUsesFilterProjectionAndFetchSize() {
        RecordingFetchReader reader = new RecordingFetchReader(createConfig(37), createRowType());
        AmazonDocumentDBSourceSplit split =
                new AmazonDocumentDBSourceSplit(0, "{\"status\": \"OPEN\"}", "{\"status\": 1}");
        RecordingCollector collector = new RecordingCollector();

        reader.addSplits(Collections.singletonList(split));
        reader.pollNext(collector);

        Assertions.assertEquals(37, reader.getFetchSize());
        Assertions.assertEquals(split.getMatchQuery(), reader.seenSplit.getMatchQuery());
        Assertions.assertEquals(split.getProjection(), reader.seenSplit.getProjection());
        Assertions.assertTrue(reader.snapshotState(1L).isEmpty());
    }

    @Test
    public void testSplitCopyPreservesQueryInformation() {
        AmazonDocumentDBSourceSplit split =
                new AmazonDocumentDBSourceSplit(0, "{\"status\": 1}", "{\"_id\": 0}");

        AmazonDocumentDBSourceSplit copy = split.copy();

        Assertions.assertEquals(split.getSplitId(), copy.getSplitId());
        Assertions.assertEquals(split.getMatchQuery(), copy.getMatchQuery());
        Assertions.assertEquals(split.getProjection(), copy.getProjection());
    }

    @Test
    public void testCloseReleasesMongoClient() {
        AtomicBoolean clientClosed = new AtomicBoolean();
        MongoClient client = createMongoClientProxy(clientClosed);
        AmazonDocumentDBSourceReader reader =
                new AmazonDocumentDBSourceReader(
                        new RecordingReaderContext(), createConfig(1), createRowType()) {
                    @Override
                    MongoClient createMongoClient() {
                        return client;
                    }
                };

        reader.open();
        reader.close();

        Assertions.assertTrue(clientClosed.get());
    }

    @Test
    public void testRemoteFetchDoesNotHoldCheckpointLock() throws Exception {
        CountDownLatch fetchStarted = new CountDownLatch(1);
        CountDownLatch releaseFetch = new CountDownLatch(1);
        RecordingCollector collector = new RecordingCollector();
        BlockingFetchReader reader =
                new BlockingFetchReader(
                        createConfig(1), createRowType(), fetchStarted, releaseFetch);
        AtomicReference<Throwable> pollFailure = new AtomicReference<>();
        Thread pollThread =
                new Thread(
                        () -> {
                            try {
                                reader.pollNext(collector);
                            } catch (Throwable throwable) {
                                pollFailure.set(throwable);
                            }
                        });

        reader.addSplits(Collections.singletonList(new AmazonDocumentDBSourceSplit(0, "{}", null)));
        pollThread.start();
        Assertions.assertTrue(fetchStarted.await(5, TimeUnit.SECONDS));

        ExecutorService checkpointThread = Executors.newSingleThreadExecutor();
        try {
            Future<Boolean> checkpointLockAcquired =
                    checkpointThread.submit(
                            () -> {
                                synchronized (collector.getCheckpointLock()) {
                                    return true;
                                }
                            });
            Assertions.assertTrue(checkpointLockAcquired.get(1, TimeUnit.SECONDS));
        } finally {
            releaseFetch.countDown();
            pollThread.join(TimeUnit.SECONDS.toMillis(5));
            checkpointThread.shutdownNow();
            reader.close();
        }

        Assertions.assertFalse(pollThread.isAlive());
        if (pollFailure.get() != null) {
            Assertions.fail(pollFailure.get());
        }
    }

    private static AmazonDocumentDBConfig createConfig(int fetchSize) {
        Map<String, Object> schema = new HashMap<>();
        schema.put("fields", new HashMap<String, Object>());

        Map<String, Object> options = new HashMap<>();
        options.put(
                "uri",
                "mongodb://reader:secret@cluster.example.docdb.amazonaws.com:27017/?retryWrites=false");
        options.put("database", "app-db");
        options.put("collection", "orders");
        options.put("tls", false);
        options.put("match.query", "{}");
        options.put("fetch.size", fetchSize);
        options.put("schema", schema);
        return new AmazonDocumentDBConfig(ReadonlyConfig.fromMap(options));
    }

    private static SeaTunnelRowType createRowType() {
        return new SeaTunnelRowType(
                new String[] {"id"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});
    }

    private static MongoClient createMongoClientProxy(AtomicBoolean clientClosed) {
        MongoCollection<?> collection =
                (MongoCollection<?>)
                        Proxy.newProxyInstance(
                                MongoCollection.class.getClassLoader(),
                                new Class<?>[] {MongoCollection.class},
                                (proxy, method, arguments) -> defaultValue(method.getReturnType()));
        MongoDatabase database =
                (MongoDatabase)
                        Proxy.newProxyInstance(
                                MongoDatabase.class.getClassLoader(),
                                new Class<?>[] {MongoDatabase.class},
                                (proxy, method, arguments) -> {
                                    if ("getCollection".equals(method.getName())) {
                                        return collection;
                                    }
                                    return defaultValue(method.getReturnType());
                                });
        return (MongoClient)
                Proxy.newProxyInstance(
                        MongoClient.class.getClassLoader(),
                        new Class<?>[] {MongoClient.class},
                        (proxy, method, arguments) -> {
                            if ("getDatabase".equals(method.getName())) {
                                return database;
                            }
                            if ("close".equals(method.getName())) {
                                clientClosed.set(true);
                            }
                            return defaultValue(method.getReturnType());
                        });
    }

    private static Object defaultValue(Class<?> type) {
        if (!type.isPrimitive()) {
            return null;
        }
        if (type == boolean.class) {
            return false;
        }
        if (type == char.class) {
            return '\0';
        }
        return 0;
    }

    private static class RecordingFetchReader extends AmazonDocumentDBSourceReader {
        private AmazonDocumentDBSourceSplit seenSplit;

        private RecordingFetchReader(AmazonDocumentDBConfig config, SeaTunnelRowType rowType) {
            super(new RecordingReaderContext(), config, rowType);
        }

        @Override
        BsonDocument fetchNextDocument(AmazonDocumentDBSourceSplit split) {
            seenSplit = split;
            return null;
        }
    }

    private static class BlockingFetchReader extends AmazonDocumentDBSourceReader {
        private final CountDownLatch fetchStarted;
        private final CountDownLatch releaseFetch;

        private BlockingFetchReader(
                AmazonDocumentDBConfig config,
                SeaTunnelRowType rowType,
                CountDownLatch fetchStarted,
                CountDownLatch releaseFetch) {
            super(new RecordingReaderContext(), config, rowType);
            this.fetchStarted = fetchStarted;
            this.releaseFetch = releaseFetch;
        }

        @Override
        BsonDocument fetchNextDocument(AmazonDocumentDBSourceSplit split) {
            fetchStarted.countDown();
            try {
                if (!releaseFetch.await(5, TimeUnit.SECONDS)) {
                    throw new IllegalStateException("Timed out waiting to release fetch");
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException("Interrupted while waiting to release fetch", e);
            }
            return null;
        }
    }

    private static class RecordingCollector implements Collector<SeaTunnelRow> {
        private final Object checkpointLock = new Object();

        @Override
        public void collect(SeaTunnelRow record) {}

        @Override
        public Object getCheckpointLock() {
            return checkpointLock;
        }
    }

    private static class RecordingReaderContext implements SourceReader.Context {
        @Override
        public int getIndexOfSubtask() {
            return 0;
        }

        @Override
        public Boundedness getBoundedness() {
            return Boundedness.BOUNDED;
        }

        @Override
        public void signalNoMoreElement() {}

        @Override
        public void sendSplitRequest() {}

        @Override
        public void sendSourceEventToEnumerator(SourceEvent sourceEvent) {}

        @Override
        public MetricsContext getMetricsContext() {
            return null;
        }

        @Override
        public EventListener getEventListener() {
            return null;
        }
    }
}
