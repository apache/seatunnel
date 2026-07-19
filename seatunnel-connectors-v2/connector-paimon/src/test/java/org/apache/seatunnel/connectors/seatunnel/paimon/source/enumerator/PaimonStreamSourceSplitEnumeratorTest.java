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

package org.apache.seatunnel.connectors.seatunnel.paimon.source.enumerator;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.connectors.seatunnel.paimon.source.PaimonSourceSplit;
import org.apache.seatunnel.connectors.seatunnel.paimon.source.PaimonSourceState;

import org.apache.paimon.table.source.ReadBuilder;
import org.apache.paimon.table.source.StreamTableScan;
import org.apache.paimon.table.source.TableScan;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PaimonStreamSourceSplitEnumeratorTest {

    @Test
    void shouldKeepIndependentSnapshotIdForEachTableWhenRestore() throws Exception {
        StreamTableScan firstScan = newStreamScan(10L);
        StreamTableScan secondScan = newStreamScan(20L);

        PaimonStreamSourceSplitEnumerator enumerator =
                new PaimonStreamSourceSplitEnumerator(
                        context(),
                        new LinkedList<>(),
                        null,
                        readBuilders(firstScan, secondScan),
                        1);

        enumerator.processDiscoveredSplits(enumerator.scanNextSnapshot(), null);
        PaimonSourceState state = enumerator.snapshotState(1L);
        StreamTableScan restoredFirstScan = newStreamScan(11L);
        StreamTableScan restoredSecondScan = newStreamScan(21L);

        PaimonStreamSourceSplitEnumerator restored =
                new PaimonStreamSourceSplitEnumerator(
                        context(),
                        state.getAssignedSplits(),
                        state.getCurrentSnapshotId(),
                        state.getCurrentSnapshotIds(),
                        readBuilders(restoredFirstScan, restoredSecondScan),
                        1);
        restored.close();
        enumerator.close();

        verify(firstScan).checkpoint();
        verify(secondScan).checkpoint();
        assertEquals(10L, state.getCurrentSnapshotIds().get("db.table_a"));
        assertEquals(20L, state.getCurrentSnapshotIds().get("db.table_b"));
        verify(restoredFirstScan).restore(10L);
        verify(restoredSecondScan).restore(20L);
    }

    @Test
    void shouldNotFallbackToAnotherTableSnapshotWhenTableSnapshotIsNull() throws Exception {
        StreamTableScan firstScan = newStreamScan(10L);
        StreamTableScan secondScan = newStreamScan(null);

        PaimonStreamSourceSplitEnumerator enumerator =
                new PaimonStreamSourceSplitEnumerator(
                        context(),
                        new LinkedList<>(),
                        null,
                        readBuilders(firstScan, secondScan),
                        1);

        enumerator.processDiscoveredSplits(enumerator.scanNextSnapshot(), null);
        PaimonSourceState state = enumerator.snapshotState(1L);
        StreamTableScan restoredFirstScan = newStreamScan(11L);
        StreamTableScan restoredSecondScan = newStreamScan(21L);

        PaimonStreamSourceSplitEnumerator restored =
                new PaimonStreamSourceSplitEnumerator(
                        context(),
                        state.getAssignedSplits(),
                        state.getCurrentSnapshotId(),
                        state.getCurrentSnapshotIds(),
                        readBuilders(restoredFirstScan, restoredSecondScan),
                        1);
        restored.close();
        enumerator.close();

        assertEquals(10L, state.getCurrentSnapshotIds().get("db.table_a"));
        assertTrue(state.getCurrentSnapshotIds().containsKey("db.table_b"));
        assertNull(state.getCurrentSnapshotIds().get("db.table_b"));
        verify(restoredFirstScan).restore(10L);
        verify(restoredSecondScan, never()).restore(10L);
    }

    @Test
    void shouldRestoreLegacySingleSnapshotId() throws Exception {
        StreamTableScan restoredScan = newStreamScan(11L);

        PaimonStreamSourceSplitEnumerator restored =
                new PaimonStreamSourceSplitEnumerator(
                        context(), new LinkedList<>(), 10L, readBuilders(restoredScan), 1);
        restored.close();

        verify(restoredScan).restore(10L);
    }

    private static Map<String, ReadBuilder> readBuilders(
            StreamTableScan firstScan, StreamTableScan secondScan) {
        Map<String, ReadBuilder> readBuilders = new LinkedHashMap<>();
        readBuilders.put("db.table_a", readBuilder(firstScan));
        readBuilders.put("db.table_b", readBuilder(secondScan));
        return readBuilders;
    }

    private static Map<String, ReadBuilder> readBuilders(StreamTableScan scan) {
        Map<String, ReadBuilder> readBuilders = new LinkedHashMap<>();
        readBuilders.put("db.table_a", readBuilder(scan));
        return readBuilders;
    }

    private static ReadBuilder readBuilder(StreamTableScan scan) {
        ReadBuilder readBuilder = mock(ReadBuilder.class);
        when(readBuilder.newStreamScan()).thenReturn(scan);
        return readBuilder;
    }

    private static StreamTableScan newStreamScan(Long nextSnapshotId) {
        StreamTableScan scan = mock(StreamTableScan.class);
        TableScan.Plan plan = mock(TableScan.Plan.class);
        when(plan.splits()).thenReturn(Collections.emptyList());
        when(scan.plan()).thenReturn(plan);
        when(scan.checkpoint()).thenReturn(nextSnapshotId);
        return scan;
    }

    private static SourceSplitEnumerator.Context<PaimonSourceSplit> context() {
        SourceSplitEnumerator.Context<PaimonSourceSplit> context =
                mock(SourceSplitEnumerator.Context.class);
        when(context.currentParallelism()).thenReturn(1);
        when(context.registeredReaders()).thenReturn(new HashSet<>());
        return context;
    }
}
