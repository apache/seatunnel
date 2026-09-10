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

package org.apache.seatunnel.connectors.cdc.base.source;

import org.apache.seatunnel.connectors.cdc.base.source.enumerator.state.SnapshotPhaseState;
import org.apache.seatunnel.connectors.cdc.base.source.split.SnapshotSplit;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import io.debezium.relational.TableId;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Tests for {@link IncrementalSource#getCheckpointCapturedTables(SnapshotPhaseState)} via
 * reflection, verifying the table-set union derivation from checkpoint state.
 */
public class IncrementalSourceCheckpointCapturedTablesTest {

    private static final TableId TABLE_A = TableId.parse("db1.table_a");
    private static final TableId TABLE_B = TableId.parse("db1.table_b");
    private static final TableId TABLE_C = TableId.parse("db2.table_c");
    private static final TableId TABLE_D = TableId.parse("db3.table_d");

    @Test
    public void testGetCheckpointCapturedTablesEmptyState() throws Exception {
        SnapshotPhaseState state =
                new SnapshotPhaseState(
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        false,
                        Collections.emptyList(),
                        false,
                        true);

        @SuppressWarnings("unchecked")
        Set<TableId> result = (Set<TableId>) invokeGetCheckpointCapturedTables(state);

        Assertions.assertTrue(result.isEmpty());
    }

    @Test
    public void testGetCheckpointCapturedTablesFromAlreadyProcessedTables() throws Exception {
        SnapshotPhaseState state =
                new SnapshotPhaseState(
                        Arrays.asList(TABLE_A),
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        false,
                        Collections.emptyList(),
                        false,
                        true);

        @SuppressWarnings("unchecked")
        Set<TableId> result = (Set<TableId>) invokeGetCheckpointCapturedTables(state);

        Assertions.assertEquals(1, result.size());
        Assertions.assertTrue(result.contains(TABLE_A));
    }

    @Test
    public void testGetCheckpointCapturedTablesFromRemainingTables() throws Exception {
        SnapshotPhaseState state =
                new SnapshotPhaseState(
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        false,
                        Arrays.asList(TABLE_B),
                        false,
                        true);

        @SuppressWarnings("unchecked")
        Set<TableId> result = (Set<TableId>) invokeGetCheckpointCapturedTables(state);

        Assertions.assertEquals(1, result.size());
        Assertions.assertTrue(result.contains(TABLE_B));
    }

    @Test
    public void testGetCheckpointCapturedTablesFromRemainingSplits() throws Exception {
        SnapshotSplit split = createSnapshotSplit("split-1", TABLE_C);
        SnapshotPhaseState state =
                new SnapshotPhaseState(
                        Collections.emptyList(),
                        Arrays.asList(split),
                        Collections.emptyMap(),
                        Collections.emptyMap(),
                        false,
                        Collections.emptyList(),
                        false,
                        true);

        @SuppressWarnings("unchecked")
        Set<TableId> result = (Set<TableId>) invokeGetCheckpointCapturedTables(state);

        Assertions.assertEquals(1, result.size());
        Assertions.assertTrue(result.contains(TABLE_C));
    }

    @Test
    public void testGetCheckpointCapturedTablesFromAssignedSplits() throws Exception {
        SnapshotSplit split = createSnapshotSplit("split-2", TABLE_D);
        Map<String, SnapshotSplit> assignedSplits = new HashMap<>();
        assignedSplits.put(split.splitId(), split);

        SnapshotPhaseState state =
                new SnapshotPhaseState(
                        Collections.emptyList(),
                        Collections.emptyList(),
                        assignedSplits,
                        Collections.emptyMap(),
                        false,
                        Collections.emptyList(),
                        false,
                        true);

        @SuppressWarnings("unchecked")
        Set<TableId> result = (Set<TableId>) invokeGetCheckpointCapturedTables(state);

        Assertions.assertEquals(1, result.size());
        Assertions.assertTrue(result.contains(TABLE_D));
    }

    @Test
    public void testGetCheckpointCapturedTablesFullUnionAcrossAllSources() throws Exception {
        // TABLE_A: in alreadyProcessedTables only
        // TABLE_B: in remainingTables only
        // TABLE_C: in remainingSplits only
        // TABLE_D: in assignedSplits only
        SnapshotSplit splitC = createSnapshotSplit("split-c", TABLE_C);
        SnapshotSplit splitD = createSnapshotSplit("split-d", TABLE_D);
        Map<String, SnapshotSplit> assignedSplits = new HashMap<>();
        assignedSplits.put(splitD.splitId(), splitD);

        SnapshotPhaseState state =
                new SnapshotPhaseState(
                        Arrays.asList(TABLE_A),
                        Arrays.asList(splitC),
                        assignedSplits,
                        Collections.emptyMap(),
                        false,
                        Arrays.asList(TABLE_B),
                        false,
                        true);

        @SuppressWarnings("unchecked")
        Set<TableId> result = (Set<TableId>) invokeGetCheckpointCapturedTables(state);

        Assertions.assertEquals(4, result.size());
        Assertions.assertTrue(result.contains(TABLE_A));
        Assertions.assertTrue(result.contains(TABLE_B));
        Assertions.assertTrue(result.contains(TABLE_C));
        Assertions.assertTrue(result.contains(TABLE_D));
    }

    @Test
    public void testGetCheckpointCapturedTablesDeduplicatesAcrossSources() throws Exception {
        // TABLE_A appears in BOTH alreadyProcessedTables and remainingTables
        SnapshotSplit splitA = createSnapshotSplit("split-a", TABLE_A);
        Map<String, SnapshotSplit> assignedSplits = new HashMap<>();
        assignedSplits.put(splitA.splitId(), splitA);

        SnapshotPhaseState state =
                new SnapshotPhaseState(
                        Arrays.asList(TABLE_A),
                        Arrays.asList(splitA),
                        assignedSplits,
                        Collections.emptyMap(),
                        false,
                        Arrays.asList(TABLE_A),
                        false,
                        true);

        @SuppressWarnings("unchecked")
        Set<TableId> result = (Set<TableId>) invokeGetCheckpointCapturedTables(state);

        Assertions.assertEquals(1, result.size());
        Assertions.assertTrue(result.contains(TABLE_A));
    }

    private static SnapshotSplit createSnapshotSplit(String splitId, TableId tableId) {
        return new SnapshotSplit(splitId, tableId, null, null, null);
    }

    private static Object invokeGetCheckpointCapturedTables(SnapshotPhaseState state)
            throws Exception {
        Method method =
                IncrementalSource.class.getDeclaredMethod(
                        "getCheckpointCapturedTables", SnapshotPhaseState.class);
        method.setAccessible(true);
        return method.invoke(null, state);
    }
}
