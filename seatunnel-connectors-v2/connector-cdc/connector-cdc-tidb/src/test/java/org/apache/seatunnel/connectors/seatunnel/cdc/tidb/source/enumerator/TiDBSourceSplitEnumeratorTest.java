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

package org.apache.seatunnel.connectors.seatunnel.cdc.tidb.source.enumerator;

import org.apache.seatunnel.api.common.metrics.AbstractMetricsContext;
import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.event.Event;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.common.utils.SerializationUtils;
import org.apache.seatunnel.connectors.seatunnel.cdc.tidb.source.config.TiDBSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.tidb.source.split.TiDBSourceSplit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.tikv.kvproto.Coprocessor;
import org.tikv.shade.com.google.protobuf.ByteString;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

class TiDBSourceSplitEnumeratorTest {

    @Test
    void addSplitsBackShouldNotOverwriteMultipleSplitsForSameReader() {
        TestingEnumeratorContext context =
                new TestingEnumeratorContext(1, Collections.singleton(0));
        TiDBSourceSplitEnumerator enumerator =
                new TiDBSourceSplitEnumerator(context, new TiDBSourceConfig());
        List<TiDBSourceSplit> splits = Arrays.asList(newSplit("a"), newSplit("b"), newSplit("c"));

        enumerator.addSplitsBack(splits, 0);

        Assertions.assertEquals(0, enumerator.currentUnassignedSplitSize());
        Assertions.assertEquals(splits, context.getAssignedSplits(0));
    }

    @Test
    void currentUnassignedSplitSizeShouldCountAllSplitsForSameReader() {
        TestingEnumeratorContext context = new TestingEnumeratorContext(1, Collections.emptySet());
        TiDBSourceSplitEnumerator enumerator =
                new TiDBSourceSplitEnumerator(context, new TiDBSourceConfig());

        enumerator.addSplitsBack(Arrays.asList(newSplit("a"), newSplit("b"), newSplit("c")), 0);

        Assertions.assertEquals(3, enumerator.currentUnassignedSplitSize());
    }

    @Test
    void addPendingSplitShouldAssignSplitsRoundRobin() throws Exception {
        TestingEnumeratorContext context =
                new TestingEnumeratorContext(2, new HashSet<>(Arrays.asList(0, 1)));
        TiDBSourceSplitEnumerator enumerator =
                new TiDBSourceSplitEnumerator(context, new TiDBSourceConfig());

        invokeAddPendingSplit(
                enumerator, Arrays.asList(newSplit("a"), newSplit("c"), newSplit("e")));
        enumerator.registerReader(0);

        Assertions.assertEquals(2, context.getAssignedSplits(0).size());
        Assertions.assertEquals(1, enumerator.currentUnassignedSplitSize());
    }

    @Test
    void addSplitsBackShouldReturnSplitsToOriginalReader() {
        TestingEnumeratorContext context =
                new TestingEnumeratorContext(2, new HashSet<>(Arrays.asList(0, 1)));
        TiDBSourceSplitEnumerator enumerator =
                new TiDBSourceSplitEnumerator(context, new TiDBSourceConfig());

        enumerator.addSplitsBack(
                Arrays.asList(newSplit("a"), newSplit("c"), newSplit("e"), newSplit("g")), 0);

        Assertions.assertEquals(4, context.getAssignedSplits(0).size());
        Assertions.assertEquals(0, enumerator.currentUnassignedSplitSize());
    }

    @Test
    void snapshotStateShouldKeepAssignCountAcrossRestore() throws Exception {
        TestingEnumeratorContext context =
                new TestingEnumeratorContext(2, new HashSet<>(Arrays.asList(0, 1)));
        TiDBSourceSplitEnumerator enumerator =
                new TiDBSourceSplitEnumerator(context, new TiDBSourceConfig());

        invokeAddPendingSplit(
                enumerator, Arrays.asList(newSplit("a"), newSplit("b"), newSplit("c")));
        TiDBSourceCheckpointState snapshot = enumerator.snapshotState(1L);

        Assertions.assertEquals(3, snapshot.getAssignCount());

        TiDBSourceSplitEnumerator restored =
                new TiDBSourceSplitEnumerator(context, new TiDBSourceConfig(), snapshot);
        invokeAddPendingSplit(restored, Collections.singletonList(newSplit("d")));
        TiDBSourceCheckpointState restoredSnapshot = restored.snapshotState(2L);

        Assertions.assertEquals(4, restoredSnapshot.getAssignCount());
    }

    @Test
    void checkpointStateShouldRestoreLegacySingleSplitPendingMap() throws Exception {
        TiDBSourceSplit split = newSplit("legacy");
        TiDBSourceCheckpointState legacyState =
                new TiDBSourceCheckpointState(false, Collections.emptyMap());
        Map<Integer, TiDBSourceSplit> legacyPendingSplit = new HashMap<>();
        legacyPendingSplit.put(0, split);
        setPendingSplitField(legacyState, legacyPendingSplit);

        TiDBSourceCheckpointState restoredState =
                SerializationUtils.deserialize(SerializationUtils.serialize(legacyState));

        Assertions.assertFalse(restoredState.isShouldEnumerate());
        Assertions.assertEquals(1, restoredState.getPendingSplit().size());
        Assertions.assertEquals(1, restoredState.getPendingSplit().get(0).size());
        assertSplitEquals(split, restoredState.getPendingSplit().get(0).get(0));

        TestingEnumeratorContext context =
                new TestingEnumeratorContext(1, Collections.singleton(0));
        TiDBSourceSplitEnumerator enumerator =
                new TiDBSourceSplitEnumerator(context, new TiDBSourceConfig(), restoredState);

        Assertions.assertEquals(1, enumerator.currentUnassignedSplitSize());
        enumerator.registerReader(0);
        Assertions.assertEquals(0, enumerator.currentUnassignedSplitSize());
        Assertions.assertEquals(1, context.getAssignedSplits(0).size());
        assertSplitEquals(split, context.getAssignedSplits(0).get(0));
    }

    @Test
    void getTiDBSourceSplitShouldGenerateSplitsForEachTable() throws Exception {
        TestingEnumeratorContext context = new TestingEnumeratorContext(2, Collections.emptySet());
        TiDBSourceSplitEnumerator enumerator =
                new TiDBSourceSplitEnumerator(context, new TiDBSourceConfig());
        setTableIds(enumerator, "db.table_one", 1L, "db.table_two", 2L);

        List<TiDBSourceSplit> splits =
                invokeGetTiDBSourceSplit(enumerator, Arrays.asList("db.table_one", "db.table_two"));

        Assertions.assertEquals(4, splits.size());
        Map<String, Long> splitsPerTable = new HashMap<>();
        for (TiDBSourceSplit split : splits) {
            splitsPerTable.merge(split.getDatabase() + "." + split.getTable(), 1L, Long::sum);
        }
        Assertions.assertEquals(2L, splitsPerTable.get("db.table_one"));
        Assertions.assertEquals(2L, splitsPerTable.get("db.table_two"));
    }

    @Test
    void restoreShouldDiscardStateOfTablesRemovedFromConfigAndReAddRunsFreshSnapshot()
            throws Exception {
        TiDBSourceConfig configWithOneTable =
                TiDBSourceConfig.builder()
                        .tableNames(Collections.singletonList("db.table_one"))
                        .build();
        TiDBSourceSplit removedTableSplit =
                new TiDBSourceSplit(
                        "db",
                        "table_two",
                        Coprocessor.KeyRange.newBuilder()
                                .setStart(ByteString.copyFromUtf8("start-removed"))
                                .setEnd(ByteString.copyFromUtf8("end-removed"))
                                .build(),
                        1234L,
                        null,
                        true);
        Map<Integer, List<TiDBSourceSplit>> pendingSplit = new HashMap<>();
        pendingSplit.put(0, new ArrayList<>(Collections.singletonList(removedTableSplit)));
        TiDBSourceCheckpointState restoreState =
                new TiDBSourceCheckpointState(
                        false,
                        pendingSplit,
                        0,
                        new HashSet<>(Arrays.asList("db.table_one", "db.table_two")));

        TestingEnumeratorContext context =
                new TestingEnumeratorContext(1, Collections.singleton(0));
        TiDBSourceSplitEnumerator restored =
                new TiDBSourceSplitEnumerator(context, configWithOneTable, restoreState);

        Assertions.assertEquals(0, restored.currentUnassignedSplitSize());
        Assertions.assertEquals(
                new HashSet<>(Collections.singletonList("db.table_one")),
                restored.snapshotState(1L).getEnumeratedTables());

        TiDBSourceConfig configWithBothTables =
                TiDBSourceConfig.builder()
                        .tableNames(Arrays.asList("db.table_one", "db.table_two"))
                        .build();
        TiDBSourceSplitEnumerator reAdded =
                new TiDBSourceSplitEnumerator(
                        context,
                        configWithBothTables,
                        new TiDBSourceCheckpointState(
                                false,
                                new HashMap<>(),
                                0,
                                new HashSet<>(Collections.singletonList("db.table_one"))));
        setTableIds(reAdded, "db.table_one", 1L, "db.table_two", 2L);

        reAdded.run();

        List<TiDBSourceSplit> assignedSplits = context.getAssignedSplits(0);
        Assertions.assertFalse(assignedSplits.isEmpty());
        for (TiDBSourceSplit split : assignedSplits) {
            Assertions.assertEquals("db.table_two", split.tableFullName());
        }
    }

    @Test
    void runShouldEnumerateOnlyTablesAddedAfterCheckpointOnRestore() throws Exception {
        TiDBSourceConfig config =
                TiDBSourceConfig.builder()
                        .tableNames(Arrays.asList("db.table_one", "db.table_two"))
                        .build();
        TiDBSourceCheckpointState restoreState =
                new TiDBSourceCheckpointState(
                        false,
                        Collections.emptyMap(),
                        0,
                        new HashSet<>(Collections.singletonList("db.table_one")));
        TestingEnumeratorContext context =
                new TestingEnumeratorContext(2, new HashSet<>(Arrays.asList(0, 1)));
        TiDBSourceSplitEnumerator enumerator =
                new TiDBSourceSplitEnumerator(context, config, restoreState);
        setTableIds(enumerator, "db.table_one", 1L, "db.table_two", 2L);

        enumerator.run();

        List<TiDBSourceSplit> assignedSplits = context.getAssignedSplits(0);
        assignedSplits.addAll(context.getAssignedSplits(1));
        Assertions.assertEquals(2, assignedSplits.size());
        for (TiDBSourceSplit split : assignedSplits) {
            Assertions.assertEquals("db.table_two", split.getDatabase() + "." + split.getTable());
        }
        TiDBSourceCheckpointState snapshot = enumerator.snapshotState(1L);
        Assertions.assertEquals(
                new HashSet<>(Arrays.asList("db.table_one", "db.table_two")),
                snapshot.getEnumeratedTables());
    }

    @Test
    void runShouldNotEnumerateAnythingForLegacyCheckpointWithoutEnumeratedTables()
            throws Exception {
        TiDBSourceConfig config =
                TiDBSourceConfig.builder()
                        .tableNames(Arrays.asList("db.table_one", "db.table_two"))
                        .build();
        TiDBSourceCheckpointState legacyState =
                new TiDBSourceCheckpointState(false, Collections.emptyMap());
        TestingEnumeratorContext context =
                new TestingEnumeratorContext(2, new HashSet<>(Arrays.asList(0, 1)));
        TiDBSourceSplitEnumerator enumerator =
                new TiDBSourceSplitEnumerator(context, config, legacyState);
        setTableIds(enumerator, "db.table_one", 1L, "db.table_two", 2L);

        enumerator.run();

        Assertions.assertTrue(context.getAssignedSplits(0).isEmpty());
        Assertions.assertTrue(context.getAssignedSplits(1).isEmpty());
        Assertions.assertEquals(0, enumerator.currentUnassignedSplitSize());
    }

    private static TiDBSourceSplit newSplit(String suffix) {
        Coprocessor.KeyRange keyRange =
                Coprocessor.KeyRange.newBuilder()
                        .setStart(ByteString.copyFromUtf8("start-" + suffix))
                        .setEnd(ByteString.copyFromUtf8("end-" + suffix))
                        .build();
        return new TiDBSourceSplit("db", "table", keyRange, -1L, keyRange.getStart(), false);
    }

    private static void setPendingSplitField(
            TiDBSourceCheckpointState state, Map<Integer, TiDBSourceSplit> pendingSplit)
            throws Exception {
        Field pendingSplitField = TiDBSourceCheckpointState.class.getDeclaredField("pendingSplit");
        pendingSplitField.setAccessible(true);
        pendingSplitField.set(state, pendingSplit);
    }

    @SuppressWarnings("unchecked")
    private static void setTableIds(TiDBSourceSplitEnumerator enumerator, Object... tableIdPairs)
            throws Exception {
        Field tableIdsField = TiDBSourceSplitEnumerator.class.getDeclaredField("tableIds");
        tableIdsField.setAccessible(true);
        Map<String, Long> tableIds = (Map<String, Long>) tableIdsField.get(enumerator);
        for (int i = 0; i < tableIdPairs.length; i += 2) {
            tableIds.put((String) tableIdPairs[i], (Long) tableIdPairs[i + 1]);
        }
    }

    private static List<TiDBSourceSplit> invokeGetTiDBSourceSplit(
            TiDBSourceSplitEnumerator enumerator, List<String> tables) throws Exception {
        Method getTiDBSourceSplit =
                TiDBSourceSplitEnumerator.class.getDeclaredMethod(
                        "getTiDBSourceSplit", Collection.class);
        getTiDBSourceSplit.setAccessible(true);
        return (List<TiDBSourceSplit>) getTiDBSourceSplit.invoke(enumerator, tables);
    }

    private static void invokeAddPendingSplit(
            TiDBSourceSplitEnumerator enumerator, List<TiDBSourceSplit> splits) throws Exception {
        Method addPendingSplit =
                TiDBSourceSplitEnumerator.class.getDeclaredMethod("addPendingSplit", List.class);
        addPendingSplit.setAccessible(true);
        addPendingSplit.invoke(enumerator, splits);
    }

    private static void assertSplitEquals(TiDBSourceSplit expected, TiDBSourceSplit actual) {
        Assertions.assertEquals(expected.getDatabase(), actual.getDatabase());
        Assertions.assertEquals(expected.getTable(), actual.getTable());
        Assertions.assertEquals(expected.getKeyRange(), actual.getKeyRange());
        Assertions.assertEquals(expected.getResolvedTs(), actual.getResolvedTs());
        Assertions.assertEquals(expected.getSnapshotStart(), actual.getSnapshotStart());
        Assertions.assertEquals(expected.isSnapshotCompleted(), actual.isSnapshotCompleted());
    }

    private static class TestingEnumeratorContext
            implements SourceSplitEnumerator.Context<TiDBSourceSplit> {
        private final int parallelism;
        private final Set<Integer> registeredReaders;
        private final Map<Integer, List<TiDBSourceSplit>> assignedSplitsByReader = new HashMap<>();
        private final MetricsContext metricsContext = new AbstractMetricsContext() {};
        private final EventListener eventListener =
                new EventListener() {
                    @Override
                    public void onEvent(Event event) {
                        // no-op
                    }
                };

        private TestingEnumeratorContext(int parallelism, Set<Integer> registeredReaders) {
            this.parallelism = parallelism;
            this.registeredReaders = new HashSet<>(registeredReaders);
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
        public void assignSplit(int subtaskId, List<TiDBSourceSplit> splits) {
            assignedSplitsByReader
                    .computeIfAbsent(subtaskId, ignored -> new ArrayList<>())
                    .addAll(splits);
        }

        @Override
        public void signalNoMoreSplits(int subtask) {}

        @Override
        public void sendEventToSourceReader(int subtaskId, SourceEvent event) {}

        @Override
        public MetricsContext getMetricsContext() {
            return metricsContext;
        }

        @Override
        public EventListener getEventListener() {
            return eventListener;
        }

        private List<TiDBSourceSplit> getAssignedSplits(int subtaskId) {
            return assignedSplitsByReader.getOrDefault(subtaskId, Collections.emptyList());
        }
    }
}
