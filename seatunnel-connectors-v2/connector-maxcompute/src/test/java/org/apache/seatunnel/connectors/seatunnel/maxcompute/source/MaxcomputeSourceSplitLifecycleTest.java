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

package org.apache.seatunnel.connectors.seatunnel.maxcompute.source;

import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.seatunnel.maxcompute.source.event.MaxcomputeCompletedSplitsReportEvent;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/** Tests lazy MaxCompute split assignment and checkpoint-safe split completion recovery. */
class MaxcomputeSourceSplitLifecycleTest {

    @Test
    void testSplitIdentityUsesLogicalRangeInsteadOfReaderAssignment() {
        MaxcomputeSourceSplit first =
                new MaxcomputeSourceSplit(0, 100, TablePath.of("app_data", "orders"), 0);
        MaxcomputeSourceSplit second =
                new MaxcomputeSourceSplit(0, 100, TablePath.of("app_data", "orders"), 1);

        Assertions.assertEquals(first, second);
        Assertions.assertEquals(first.splitId(), second.splitId());
    }

    @Test
    void testReaderSnapshotStateKeepsCurrentSplit() throws Exception {
        SourceReader.Context context = Mockito.mock(SourceReader.Context.class);
        MaxcomputeSourceReader reader =
                new MaxcomputeSourceReader(
                        Mockito.mock(org.apache.seatunnel.api.configuration.ReadonlyConfig.class),
                        context,
                        Collections.emptyMap());
        MaxcomputeSourceSplit currentSplit =
                new MaxcomputeSourceSplit(0, 100, TablePath.of("app_data", "orders"), 0);
        MaxcomputeSourceSplit pendingSplit =
                new MaxcomputeSourceSplit(100, 100, TablePath.of("app_data", "orders"), 0);
        reader.addSplits(Collections.singletonList(pendingSplit));
        writeField(reader, "currentProcessingSplit", currentSplit);

        List<MaxcomputeSourceSplit> snapshot = reader.snapshotState(1L);

        Assertions.assertEquals(2, snapshot.size());
        Assertions.assertEquals(currentSplit, snapshot.get(0));
        Assertions.assertEquals(pendingSplit, snapshot.get(1));
    }

    @Test
    void testCompletedSplitsReleaseAssignedSplitReferences() {
        MaxcomputeSourceSplit split = split();
        MaxcomputeSourceSplitEnumerator enumerator = enumeratorWithAssignedSplit(split);

        enumerator.handleSourceEvent(
                0,
                new MaxcomputeCompletedSplitsReportEvent(
                        Collections.singletonList(split.splitId())));

        Assertions.assertTrue(enumerator.snapshotState(11L).getAssignedSplit().isEmpty());
    }

    @Test
    void testCompletedSplitsCanBeReturnedForRecovery() {
        MaxcomputeSourceSplit split = split();
        MaxcomputeSourceSplitEnumerator enumerator = enumeratorWithAssignedSplit(split);

        enumerator.handleSourceEvent(
                0,
                new MaxcomputeCompletedSplitsReportEvent(
                        Collections.singletonList(split.splitId())));
        enumerator.addSplitsBack(Collections.singletonList(split), 0);

        Assertions.assertEquals(1, enumerator.currentUnassignedSplitSize());
        Assertions.assertEquals(
                Collections.singleton(split), enumerator.snapshotState(11L).getAssignedSplit());
    }

    @Test
    void testStateKeepsLazySplitCursor() {
        MaxcomputeSourceState state = new MaxcomputeSourceState(Collections.emptySet(), 2, 300L);

        Assertions.assertEquals(2, state.getNextTableIndex());
        Assertions.assertEquals(300L, state.getNextRowStart());
        Assertions.assertTrue(state.isLazySplitAssignment());
    }

    @Test
    void testLazyAssignmentMaterializesOnlyRequestedBatch() throws Exception {
        TablePath tablePath = TablePath.of("app_data", "orders");
        TestingEnumeratorContext context = new TestingEnumeratorContext();
        MaxcomputeSourceSplitEnumerator enumerator =
                new MaxcomputeSourceSplitEnumerator(
                        context,
                        Mockito.mock(org.apache.seatunnel.api.configuration.ReadonlyConfig.class),
                        Collections.singletonMap(tablePath, sourceTableInfo(tablePath, 10000)));
        writeField(enumerator, "splitDiscoveryComplete", true);
        tableRecordCounts(enumerator).put(tablePath, 1010000L);

        enumerator.handleSplitRequest(0);

        Assertions.assertEquals(100, context.getAssignedSplits().size());
        Assertions.assertEquals(100, enumerator.snapshotState(1L).getAssignedSplit().size());
    }

    @Test
    void testCheckpointCompletionReportsOnlySnapshottedSplits() throws Exception {
        SourceReader.Context context = Mockito.mock(SourceReader.Context.class);
        Mockito.when(context.isCheckpointEnabled()).thenReturn(true);
        MaxcomputeSourceReader reader =
                new MaxcomputeSourceReader(
                        Mockito.mock(org.apache.seatunnel.api.configuration.ReadonlyConfig.class),
                        context,
                        Collections.emptyMap());
        MaxcomputeSourceSplit beforeCheckpoint = split();
        beforeCheckpoint.setFinished(true);
        MaxcomputeSourceSplit afterCheckpoint =
                new MaxcomputeSourceSplit(100, 100, TablePath.of("app_data", "orders"), 0);
        afterCheckpoint.setFinished(true);
        completedSplits(reader).add(beforeCheckpoint);

        reader.snapshotState(1L);
        completedSplits(reader).add(afterCheckpoint);
        reader.notifyCheckpointComplete(1L);

        Assertions.assertEquals(
                Collections.singletonList(beforeCheckpoint.splitId()), completedSplitIds(context));
        Mockito.clearInvocations(context);

        reader.snapshotState(2L);
        reader.notifyCheckpointComplete(2L);

        Assertions.assertEquals(
                Collections.singletonList(afterCheckpoint.splitId()), completedSplitIds(context));
    }

    @Test
    void testLazyStateRestoresAssignedSplits() throws Exception {
        MaxcomputeSourceSplit split = split();
        TestingEnumeratorContext context = new TestingEnumeratorContext();
        MaxcomputeSourceSplitEnumerator enumerator =
                new MaxcomputeSourceSplitEnumerator(
                        context,
                        Mockito.mock(org.apache.seatunnel.api.configuration.ReadonlyConfig.class),
                        Collections.emptyMap(),
                        new MaxcomputeSourceState(Collections.singleton(split), 0, 0L));

        enumerator.run();

        Assertions.assertEquals(Collections.singletonList(split), context.getAssignedSplits());
    }

    @Test
    void testLegacyStateRestoresOnlyReaderReturnedSplits() throws Exception {
        MaxcomputeSourceSplit completed = split();
        MaxcomputeSourceSplit remaining =
                new MaxcomputeSourceSplit(100, 100, TablePath.of("app_data", "orders"), 0);
        TestingEnumeratorContext context = new TestingEnumeratorContext();
        MaxcomputeSourceSplitEnumerator enumerator =
                new MaxcomputeSourceSplitEnumerator(
                        context,
                        Mockito.mock(org.apache.seatunnel.api.configuration.ReadonlyConfig.class),
                        Collections.emptyMap(),
                        new MaxcomputeSourceState(
                                new HashSet<>(Arrays.asList(completed, remaining))));

        enumerator.addSplitsBack(Collections.singletonList(remaining), 0);

        Assertions.assertTrue(enumerator.snapshotState(1L).getAssignedSplit().isEmpty());
        Assertions.assertEquals(1, enumerator.currentUnassignedSplitSize());
        enumerator.run();

        Assertions.assertEquals(Collections.singletonList(remaining), context.getAssignedSplits());
        Assertions.assertEquals(
                Collections.singleton(remaining), enumerator.snapshotState(1L).getAssignedSplit());
    }

    @Test
    void testFinishedReaderStateRemovesLazyAssignmentOnRestore() {
        MaxcomputeSourceSplit assigned = split();
        MaxcomputeSourceSplit finished = split();
        finished.setFinished(true);
        MaxcomputeSourceSplitEnumerator enumerator =
                new MaxcomputeSourceSplitEnumerator(
                        new TestingEnumeratorContext(),
                        Mockito.mock(org.apache.seatunnel.api.configuration.ReadonlyConfig.class),
                        Collections.emptyMap(),
                        new MaxcomputeSourceState(Collections.singleton(assigned), 0, 0L));

        enumerator.addSplitsBack(Collections.singletonList(finished), 0);

        Assertions.assertTrue(enumerator.snapshotState(1L).getAssignedSplit().isEmpty());
        Assertions.assertEquals(0, enumerator.currentUnassignedSplitSize());
    }

    private static MaxcomputeSourceSplit split() {
        return new MaxcomputeSourceSplit(0, 100, TablePath.of("app_data", "orders"), 0);
    }

    private static SourceTableInfo sourceTableInfo(TablePath tablePath, int splitRow) {
        CatalogTable catalogTable = Mockito.mock(CatalogTable.class);
        Mockito.when(catalogTable.getTablePath()).thenReturn(tablePath);
        return new SourceTableInfo(catalogTable, null, splitRow);
    }

    private static MaxcomputeSourceSplitEnumerator enumeratorWithAssignedSplit(
            MaxcomputeSourceSplit split) {
        return new MaxcomputeSourceSplitEnumerator(
                new TestingEnumeratorContext(),
                Mockito.mock(org.apache.seatunnel.api.configuration.ReadonlyConfig.class),
                Collections.emptyMap(),
                new MaxcomputeSourceState(Collections.singleton(split), 0, 0L));
    }

    private static void writeField(Object target, String fieldName, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }

    private static List<String> completedSplitIds(SourceReader.Context context) {
        ArgumentCaptor<SourceEvent> eventCaptor = ArgumentCaptor.forClass(SourceEvent.class);
        Mockito.verify(context).sendSourceEventToEnumerator(eventCaptor.capture());
        return ((MaxcomputeCompletedSplitsReportEvent) eventCaptor.getValue()).getCompletedSplits();
    }

    @SuppressWarnings("unchecked")
    private static List<MaxcomputeSourceSplit> completedSplits(MaxcomputeSourceReader reader)
            throws Exception {
        Field field = reader.getClass().getDeclaredField("completedSplits");
        field.setAccessible(true);
        return (List<MaxcomputeSourceSplit>) field.get(reader);
    }

    @SuppressWarnings("unchecked")
    private static Map<TablePath, Long> tableRecordCounts(
            MaxcomputeSourceSplitEnumerator enumerator) throws Exception {
        Field field = enumerator.getClass().getDeclaredField("tableRecordCounts");
        field.setAccessible(true);
        return (Map<TablePath, Long>) field.get(enumerator);
    }

    private static final class TestingEnumeratorContext
            implements SourceSplitEnumerator.Context<MaxcomputeSourceSplit> {
        private final List<MaxcomputeSourceSplit> assignedSplits = new ArrayList<>();

        @Override
        public int currentParallelism() {
            return 1;
        }

        @Override
        public Set<Integer> registeredReaders() {
            return Collections.singleton(0);
        }

        @Override
        public void assignSplit(int subtaskId, List<MaxcomputeSourceSplit> splits) {
            assignedSplits.addAll(splits);
        }

        private List<MaxcomputeSourceSplit> getAssignedSplits() {
            return assignedSplits;
        }

        @Override
        public void signalNoMoreSplits(int subtask) {}

        @Override
        public void sendEventToSourceReader(
                int subtaskId, org.apache.seatunnel.api.source.SourceEvent event) {}

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
