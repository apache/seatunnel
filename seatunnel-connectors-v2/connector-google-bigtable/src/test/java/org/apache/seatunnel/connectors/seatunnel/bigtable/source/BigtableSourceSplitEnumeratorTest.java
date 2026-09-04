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

package org.apache.seatunnel.connectors.seatunnel.bigtable.source;

import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.common.utils.HashUtils;
import org.apache.seatunnel.connectors.seatunnel.bigtable.client.BigtableClient;
import org.apache.seatunnel.connectors.seatunnel.bigtable.config.BigtableParameters;
import org.apache.seatunnel.connectors.seatunnel.bigtable.exception.BigtableConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.bigtable.exception.BigtableConnectorException;

import org.junit.jupiter.api.Test;

import com.google.cloud.bigtable.data.v2.models.KeyOffset;
import com.google.protobuf.ByteString;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BigtableSourceSplitEnumeratorTest {

    @Test
    void testSingleSplitFullRange() {
        BigtableSourceSplit split = new BigtableSourceSplit(0, "", "");
        assertEquals("bigtable_source_split_0", split.splitId());
        assertEquals("", split.getStartRowKey());
        assertEquals("", split.getEndRowKey());
    }

    @Test
    void testSplitWithRowKeyRange() {
        BigtableSourceSplit split = new BigtableSourceSplit(0, "aaa", "zzz");
        assertEquals("aaa", split.getStartRowKey());
        assertEquals("zzz", split.getEndRowKey());
    }

    /**
     * Regression for the returned-split recovery window: a split requeued via {@link
     * BigtableSourceSplitEnumerator#addSplitsBack} but not yet reassigned must survive enumerator
     * checkpoint / restore and be reassigned to the reader.
     */
    @Test
    void testReturnedSplitSurvivesCheckpointRestoreAndReassign() throws Exception {
        TestingContext context = new TestingContext(1);
        BigtableParameters parameters = testParameters("", "");
        BigtableClient client = mockClient(Collections.emptyList());

        BigtableSourceSplitEnumerator enumerator =
                new BigtableSourceSplitEnumerator(context, parameters, null, client);
        enumerator.open();

        context.registerReaderForTest(0);
        enumerator.registerReader(0);
        BigtableSourceSplit split = context.getLastAssignedSplit(0);
        assertEquals("bigtable_source_split_0", split.splitId());

        // Simulate failover: reader returns its split before the enumerator can reassign it.
        context.unregisterReaderForTest(0);
        enumerator.addSplitsBack(Collections.singletonList(split), 0);

        BigtableSourceState checkpoint = enumerator.snapshotState(1L);
        assertTrue(checkpoint.getPendingSplits().contains(split));
        assertTrue(checkpoint.getAssignedSplits().contains(split));

        BigtableSourceSplitEnumerator restored =
                new BigtableSourceSplitEnumerator(context, parameters, checkpoint, client);
        restored.open();

        context.clearAssignments();
        context.registerReaderForTest(0);
        restored.registerReader(0);

        assertEquals(1, context.getAssignedSplitCount(0));
        assertEquals(split.splitId(), context.getLastAssignedSplit(0).splitId());
        assertEquals(0, restored.currentUnassignedSplitSize());
    }

    @Test
    void testSnapshotStateDefensivelyCopiesPendingSplits() throws Exception {
        TestingContext context = new TestingContext(1);
        BigtableParameters parameters = testParameters("", "");
        BigtableClient client = mockClient(Collections.emptyList());

        BigtableSourceSplitEnumerator enumerator =
                new BigtableSourceSplitEnumerator(context, parameters, null, client);
        enumerator.open();

        context.registerReaderForTest(0);
        enumerator.registerReader(0);
        BigtableSourceSplit split = context.getLastAssignedSplit(0);

        context.unregisterReaderForTest(0);
        enumerator.addSplitsBack(Collections.singletonList(split), 0);

        BigtableSourceState checkpoint = enumerator.snapshotState(1L);
        assertEquals(1, checkpoint.getPendingSplits().size());
        checkpoint.getPendingSplits().clear();

        assertEquals(1, enumerator.currentUnassignedSplitSize());
    }

    @Test
    void testSampleRowKeysProducesThreeSplits() {
        BigtableSourceSplitEnumerator enumerator =
                enumeratorWithSamples(testParameters("", ""), Arrays.asList("m", "t", ""));

        Set<BigtableSourceSplit> splits = enumerator.buildSplits();
        assertEquals(3, splits.size());
        assertEquals(
                Arrays.asList("", "m", "t"),
                splits.stream()
                        .map(BigtableSourceSplit::getStartRowKey)
                        .collect(Collectors.toList()));
        assertEquals(
                Arrays.asList("m", "t", ""),
                splits.stream()
                        .map(BigtableSourceSplit::getEndRowKey)
                        .collect(Collectors.toList()));
    }

    @Test
    void testUserRangeIntersectsSampledSplits() {
        // Table tablets: ["","c") ["c","f") ["f",""); user range is [b,e)
        BigtableSourceSplitEnumerator enumerator =
                enumeratorWithSamples(testParameters("b", "e"), Arrays.asList("c", "f", ""));

        Set<BigtableSourceSplit> splits = enumerator.buildSplits();
        assertEquals(2, splits.size());
        List<String> starts =
                splits.stream()
                        .map(BigtableSourceSplit::getStartRowKey)
                        .collect(Collectors.toList());
        List<String> ends =
                splits.stream().map(BigtableSourceSplit::getEndRowKey).collect(Collectors.toList());
        assertEquals(Arrays.asList("b", "c"), starts);
        assertEquals(Arrays.asList("c", "e"), ends);
    }

    @Test
    void testSampleRowKeysFailureFallsBackToSingleSplit() {
        BigtableClient client = mock(BigtableClient.class);
        when(client.sampleRowKeys())
                .thenThrow(
                        new BigtableConnectorException(
                                BigtableConnectorErrorCode.TABLE_QUERY_FAILED, "boom"));
        BigtableParameters parameters = testParameters("start", "end");
        BigtableSourceSplitEnumerator enumerator =
                new BigtableSourceSplitEnumerator(new TestingContext(1), parameters, null, client);

        Set<BigtableSourceSplit> splits = enumerator.buildSplits();
        assertEquals(1, splits.size());
        BigtableSourceSplit split = splits.iterator().next();
        assertEquals("start", split.getStartRowKey());
        assertEquals("end", split.getEndRowKey());
    }

    @Test
    void testEmptySampleListFallsBackToSingleSplit() {
        BigtableSourceSplitEnumerator enumerator =
                enumeratorWithSamples(testParameters("s", "e"), Collections.emptyList());

        Set<BigtableSourceSplit> splits = enumerator.buildSplits();
        assertEquals(1, splits.size());
        BigtableSourceSplit split = splits.iterator().next();
        assertEquals("s", split.getStartRowKey());
        assertEquals("e", split.getEndRowKey());
    }

    @Test
    void testEmptyIntersectionFallsBackToSingleSplit() {
        // Inverted user range has no forward intersection with any sampled interval
        BigtableSourceSplitEnumerator enumerator =
                enumeratorWithSamples(testParameters("z", "a"), Arrays.asList("m", "t", ""));

        Set<BigtableSourceSplit> splits = enumerator.buildSplits();
        assertEquals(1, splits.size());
        BigtableSourceSplit split = splits.iterator().next();
        assertEquals("z", split.getStartRowKey());
        assertEquals("a", split.getEndRowKey());
    }

    @Test
    void testMissingTrailingEmptySampleStillCoversTableEnd() {
        BigtableSourceSplitEnumerator enumerator =
                enumeratorWithSamples(testParameters("", ""), Arrays.asList("m", "t"));

        Set<BigtableSourceSplit> splits = enumerator.buildSplits();
        assertEquals(3, splits.size());
        List<String> starts =
                splits.stream()
                        .map(BigtableSourceSplit::getStartRowKey)
                        .collect(Collectors.toList());
        List<String> ends =
                splits.stream().map(BigtableSourceSplit::getEndRowKey).collect(Collectors.toList());
        assertEquals(Arrays.asList("", "m", "t"), starts);
        assertEquals(Arrays.asList("m", "t", ""), ends);
    }

    @Test
    void testParallelismOneAssignsAllSampledSplits() throws Exception {
        TestingContext context = new TestingContext(1);
        BigtableParameters parameters = testParameters("", "");
        BigtableClient client = mockClient(Arrays.asList("m", "t", ""));
        BigtableSourceSplitEnumerator enumerator =
                new BigtableSourceSplitEnumerator(context, parameters, null, client);
        enumerator.open();

        context.registerReaderForTest(0);
        enumerator.registerReader(0);

        assertEquals(3, context.getAssignedSplitCount(0));
        assertEquals(0, enumerator.currentUnassignedSplitSize());
    }

    /**
     * Verifies that with parallelism N, each produced split is assigned to exactly one reader and
     * the assignment matches {@code hash(splitId) % N}.
     *
     * <p>Samples produce 3 splits (["","m"), ["m","t"), ["t","")). With parallelism 3 every reader
     * should receive at least one split, the union equals all splits, and no split appears twice.
     */
    @Test
    void testParallelismMultipleReadersEachGetDisjointHashedSplits() throws Exception {
        int parallelism = 3;
        TestingContext context = new TestingContext(parallelism);
        BigtableParameters parameters = testParameters("", "");
        BigtableClient client = mockClient(Arrays.asList("m", "t", ""));
        BigtableSourceSplitEnumerator enumerator =
                new BigtableSourceSplitEnumerator(context, parameters, null, client);
        enumerator.open();

        for (int i = 0; i < parallelism; i++) {
            context.registerReaderForTest(i);
            enumerator.registerReader(i);
        }

        // All 3 splits should have been assigned and nothing left pending.
        assertEquals(0, enumerator.currentUnassignedSplitSize());

        // Collect all assigned splits across all readers.
        List<BigtableSourceSplit> allAssigned = new ArrayList<>();
        for (int i = 0; i < parallelism; i++) {
            allAssigned.addAll(context.getAssignedSplits(i));
        }
        assertEquals(3, allAssigned.size(), "Total splits across all readers should be 3");

        // No split should appear more than once (no duplicates).
        Set<String> splitIds =
                allAssigned.stream().map(BigtableSourceSplit::splitId).collect(Collectors.toSet());
        assertEquals(3, splitIds.size(), "Each split must be assigned to exactly one reader");

        // Each split's owner must match hash(splitId) % parallelism.
        for (int i = 0; i < parallelism; i++) {
            for (BigtableSourceSplit split : context.getAssignedSplits(i)) {
                int expected = HashUtils.bucketIndex(split.splitId().hashCode(), parallelism);
                assertEquals(
                        expected,
                        i,
                        "Split "
                                + split.splitId()
                                + " should belong to reader "
                                + expected
                                + " but was assigned to reader "
                                + i);
            }
        }
    }

    /**
     * close() before open() must not fabricate a whole-range fallback split or commit pending
     * state. getBigtableClient() observes closed and discovery aborts without a misleading
     * sampleRowKeys-failure fallback.
     */
    @Test
    void testCloseBeforeOpenDoesNotCommitDiscoveryState() throws Exception {
        TestingContext context = new TestingContext(1);
        BigtableClient client = mockClient(Arrays.asList("m", "t", ""));
        BigtableSourceSplitEnumerator enumerator =
                new BigtableSourceSplitEnumerator(context, testParameters("", ""), null, client);

        enumerator.close();
        enumerator.open();

        assertEquals(0, enumerator.currentUnassignedSplitSize());
        BigtableSourceState state = enumerator.snapshotState(1L);
        assertTrue(state.getAssignedSplits().isEmpty());
        assertTrue(state.getPendingSplits().isEmpty());
        verify(client).close();
    }

    /**
     * close() racing an in-flight sampleRowKeys() must not commit pendingSplits/initialized after
     * the RPC returns. Without the closed check in initializePendingSplits(), empty samples would
     * fall back to a fabricated whole-range split and persist it.
     */
    @Test
    void testCloseDuringSampleRowKeysDoesNotCommitPendingSplits() throws Exception {
        CountDownLatch sampleStarted = new CountDownLatch(1);
        CountDownLatch allowSampleToFinish = new CountDownLatch(1);

        BigtableClient client = mock(BigtableClient.class);
        when(client.sampleRowKeys())
                .thenAnswer(
                        invocation -> {
                            sampleStarted.countDown();
                            assertTrue(allowSampleToFinish.await(10, TimeUnit.SECONDS));
                            // Would normally trigger the single-split empty-sample fallback.
                            return Collections.emptyList();
                        });

        TestingContext context = new TestingContext(1);
        BigtableSourceSplitEnumerator enumerator =
                new BigtableSourceSplitEnumerator(context, testParameters("", ""), null, client);

        ExecutorService pool = Executors.newSingleThreadExecutor();
        Future<?> openFuture =
                pool.submit(
                        () -> {
                            enumerator.open();
                            return null;
                        });

        assertTrue(sampleStarted.await(10, TimeUnit.SECONDS));
        enumerator.close();
        allowSampleToFinish.countDown();
        openFuture.get(10, TimeUnit.SECONDS);
        pool.shutdown();
        assertTrue(pool.awaitTermination(5, TimeUnit.SECONDS));

        assertEquals(0, enumerator.currentUnassignedSplitSize());
        BigtableSourceState state = enumerator.snapshotState(1L);
        assertTrue(state.getAssignedSplits().isEmpty());
        assertTrue(
                state.getPendingSplits().isEmpty(),
                "close() during discovery must not commit a fabricated fallback split");
        verify(client).close();
    }

    private static BigtableSourceSplitEnumerator enumeratorWithSamples(
            BigtableParameters parameters, List<String> sampleKeys) {
        return new BigtableSourceSplitEnumerator(
                new TestingContext(1), parameters, null, mockClient(sampleKeys));
    }

    private static BigtableParameters testParameters(String startRowkey, String endRowkey) {
        return BigtableParameters.builder()
                .projectId("test-project")
                .instanceId("test-instance")
                .table("test-table")
                .startRowkey(startRowkey.isEmpty() ? null : startRowkey)
                .endRowkey(endRowkey.isEmpty() ? null : endRowkey)
                .build();
    }

    private static BigtableClient mockClient(List<String> sampleKeys) {
        BigtableClient client = mock(BigtableClient.class);
        List<KeyOffset> samples = new ArrayList<>();
        for (String key : sampleKeys) {
            samples.add(keyOffset(key));
        }
        when(client.sampleRowKeys()).thenReturn(samples);
        return client;
    }

    private static KeyOffset keyOffset(String key) {
        KeyOffset offset = mock(KeyOffset.class);
        when(offset.getKey()).thenReturn(key == null ? null : ByteString.copyFromUtf8(key));
        return offset;
    }

    private static class TestingContext
            implements SourceSplitEnumerator.Context<BigtableSourceSplit> {

        private final int parallelism;
        private final Set<Integer> registeredReaders = new HashSet<>();
        private final Map<Integer, List<BigtableSourceSplit>> assignments = new HashMap<>();

        private TestingContext(int parallelism) {
            this.parallelism = parallelism;
        }

        void registerReaderForTest(int subtaskId) {
            registeredReaders.add(subtaskId);
        }

        void unregisterReaderForTest(int subtaskId) {
            registeredReaders.remove(subtaskId);
        }

        void clearAssignments() {
            assignments.clear();
        }

        int getAssignedSplitCount(int subtaskId) {
            return assignments.getOrDefault(subtaskId, Collections.emptyList()).size();
        }

        List<BigtableSourceSplit> getAssignedSplits(int subtaskId) {
            return Collections.unmodifiableList(
                    assignments.getOrDefault(subtaskId, Collections.emptyList()));
        }

        BigtableSourceSplit getLastAssignedSplit(int subtaskId) {
            List<BigtableSourceSplit> splits = assignments.get(subtaskId);
            return splits.get(splits.size() - 1);
        }

        @Override
        public int currentParallelism() {
            return parallelism;
        }

        @Override
        public Set<Integer> registeredReaders() {
            return registeredReaders;
        }

        @Override
        public void assignSplit(int subtaskId, List<BigtableSourceSplit> splits) {
            assignments.computeIfAbsent(subtaskId, ignored -> new ArrayList<>()).addAll(splits);
        }

        @Override
        public void signalNoMoreSplits(int subtask) {}

        @Override
        public void sendEventToSourceReader(int subtaskId, SourceEvent event) {}

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
