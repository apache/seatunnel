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
import org.apache.paimon.table.source.Split;
import org.apache.paimon.table.source.StreamTableScan;
import org.apache.paimon.table.source.TableScan;

import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
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

    @Test
    void shouldCheckpointSnapshotIdAndPendingSplitsAtomicallyDuringAsyncDiscovery()
            throws Exception {
        CountDownLatch discoveryBlockedAfterSnapshotIdUpdate = new CountDownLatch(1);
        CountDownLatch continueDiscovery = new CountDownLatch(1);
        CountDownLatch snapshotStateStarted = new CountDownLatch(1);
        AtomicInteger splitsCalls = new AtomicInteger();
        Split split =
                new Split() {
                    @Override
                    public long rowCount() {
                        return 1L;
                    }

                    @Override
                    public String toString() {
                        return "split-0";
                    }
                };
        TableScan.Plan plan =
                () -> {
                    if (splitsCalls.incrementAndGet() == 1) {
                        discoveryBlockedAfterSnapshotIdUpdate.countDown();
                        try {
                            assertTrue(continueDiscovery.await(30, TimeUnit.SECONDS));
                        } catch (InterruptedException e) {
                            Thread.currentThread().interrupt();
                            throw new AssertionError(e);
                        }
                    }
                    return Collections.singletonList(split);
                };

        PaimonStreamSourceSplitEnumerator enumerator =
                new PaimonStreamSourceSplitEnumerator(
                        context(), new LinkedList<>(), null, Collections.emptyMap(), 1) {
                    @Override
                    public PaimonSourceState snapshotState(long checkpointId) throws Exception {
                        snapshotStateStarted.countDown();
                        return super.snapshotState(checkpointId);
                    }
                };
        AtomicReference<Throwable> discoveryError = new AtomicReference<>();
        Thread discoveryThread =
                new Thread(
                        () -> {
                            try {
                                enumerator.processDiscoveredSplits(
                                        Collections.singletonList(
                                                new AbstractSplitEnumerator.PlanWithNextSnapshotId(
                                                        "db.table_a", plan, 10L)),
                                        null);
                            } catch (Throwable throwable) {
                                discoveryError.set(throwable);
                            }
                        });
        FutureTask<PaimonSourceState> snapshotState =
                new FutureTask<>(() -> enumerator.snapshotState(1L));
        Thread snapshotThread = new Thread(snapshotState);

        try {
            discoveryThread.start();
            assertTrue(discoveryBlockedAfterSnapshotIdUpdate.await(10, TimeUnit.SECONDS));

            snapshotThread.start();
            assertTrue(snapshotStateStarted.await(10, TimeUnit.SECONDS));
            assertThrows(
                    TimeoutException.class, () -> snapshotState.get(200, TimeUnit.MILLISECONDS));

            continueDiscovery.countDown();
            PaimonSourceState state = snapshotState.get(10, TimeUnit.SECONDS);
            discoveryThread.join(TimeUnit.SECONDS.toMillis(10));

            assertFalse(discoveryThread.isAlive());
            if (discoveryError.get() != null) {
                throw new AssertionError(discoveryError.get());
            }
            assertEquals(10L, state.getCurrentSnapshotIds().get("db.table_a"));
            assertEquals(1, state.getAssignedSplits().size());
        } finally {
            continueDiscovery.countDown();
            enumerator.close();
        }
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
