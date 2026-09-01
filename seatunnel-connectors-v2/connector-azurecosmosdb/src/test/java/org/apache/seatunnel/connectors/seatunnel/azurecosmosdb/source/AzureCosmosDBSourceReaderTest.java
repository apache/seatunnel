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

package org.apache.seatunnel.connectors.seatunnel.azurecosmosdb.source;

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
import org.apache.seatunnel.connectors.seatunnel.azurecosmosdb.config.AzureCosmosDBConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.azure.cosmos.models.FeedResponse;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class AzureCosmosDBSourceReaderTest {

    @Test
    public void testUsesConfiguredQueryPageSize() {
        AzureCosmosDBSourceReader reader =
                new AzureCosmosDBSourceReader(null, createConfig(37), createRowType());

        try {
            Assertions.assertEquals(37, reader.getQueryPageSize());
        } finally {
            reader.close();
        }
    }

    @Test
    public void testSplitCopyPreservesContinuationToken() {
        AzureCosmosDBSourceSplit split = new AzureCosmosDBSourceSplit(0, "token-1");

        AzureCosmosDBSourceSplit copiedSplit = split.copy();

        Assertions.assertEquals(split.getSplitId(), copiedSplit.getSplitId());
        Assertions.assertEquals(split.getContinuationToken(), copiedSplit.getContinuationToken());
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

        reader.addSplits(Collections.singletonList(new AzureCosmosDBSourceSplit(0)));
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

    private AzureCosmosDBConfig createConfig(int maxItemCount) {
        Map<String, Object> schema = new HashMap<>();
        schema.put("fields", new HashMap<String, Object>());

        Map<String, Object> options = new HashMap<>();
        options.put("endpoint", "https://account.documents.azure.com:443/");
        options.put("primary_key", "account-key");
        options.put("database", "sales");
        options.put("container", "orders");
        options.put("query", "SELECT * FROM c");
        options.put("max_item_count", maxItemCount);
        options.put("schema", schema);
        return new AzureCosmosDBConfig(ReadonlyConfig.fromMap(options));
    }

    private SeaTunnelRowType createRowType() {
        return new SeaTunnelRowType(
                new String[] {"id"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});
    }

    private static class BlockingFetchReader extends AzureCosmosDBSourceReader {
        private final CountDownLatch fetchStarted;
        private final CountDownLatch releaseFetch;

        private BlockingFetchReader(
                AzureCosmosDBConfig config,
                SeaTunnelRowType rowType,
                CountDownLatch fetchStarted,
                CountDownLatch releaseFetch) {
            super(new RecordingReaderContext(), config, rowType);
            this.fetchStarted = fetchStarted;
            this.releaseFetch = releaseFetch;
        }

        @Override
        FeedResponse<Object> fetchPage(String continuationToken) {
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
