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

package org.apache.seatunnel.connectors.bigquery.source;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;

import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class BigQuerySourceSplitEnumeratorTest {

    private static final String TEST_QUERY = "SELECT id FROM `proj.ds.tbl`";

    @SuppressWarnings("unchecked")
    private SourceSplitEnumerator.Context<BigQuerySourceSplit> mockContext(Set<Integer> readers) {
        SourceSplitEnumerator.Context<BigQuerySourceSplit> context = mock();
        when(context.registeredReaders()).thenReturn(readers);
        return context;
    }

    @Test
    public void testRunAssignsSplitToFirstReader() {
        SourceSplitEnumerator.Context<BigQuerySourceSplit> context = mockContext(Set.of(0));

        BigQuerySourceSplitEnumerator enumerator =
                new BigQuerySourceSplitEnumerator(context, TEST_QUERY, null);
        enumerator.run();

        verify(context)
                .assignSplit(
                        eq(0),
                        ArgumentMatchers.<List<BigQuerySourceSplit>>argThat(
                                list -> list.size() == 1));
        verify(context).signalNoMoreSplits(0);
    }

    @Test
    public void testNoReadersRegisteredDoesNotAssign() {
        SourceSplitEnumerator.Context<BigQuerySourceSplit> context =
                mockContext(Collections.emptySet());

        BigQuerySourceSplitEnumerator enumerator =
                new BigQuerySourceSplitEnumerator(context, TEST_QUERY, null);
        enumerator.run();

        verify(context, never()).assignSplit(anyInt(), anyList());
    }

    @Test
    public void testSnapshotStateReturnsPendingSplits() {
        SourceSplitEnumerator.Context<BigQuerySourceSplit> context =
                mockContext(Collections.emptySet());

        BigQuerySourceSplitEnumerator enumerator =
                new BigQuerySourceSplitEnumerator(context, TEST_QUERY, null);
        BigQuerySourceState state = enumerator.snapshotState(1L);

        assertEquals(1, state.getPendingSplits().size());
        assertEquals(TEST_QUERY, state.getPendingSplits().get(0).getQuery());
    }

    @Test
    public void testRestoreEnumeratorWithState() {
        BigQuerySourceState restoreState =
                new BigQuerySourceState(
                        List.of(
                                new BigQuerySourceSplit("0", "SELECT 1"),
                                new BigQuerySourceSplit("1", "SELECT 2")));
        SourceSplitEnumerator.Context<BigQuerySourceSplit> context = mockContext(Set.of(0));

        BigQuerySourceSplitEnumerator enumerator =
                new BigQuerySourceSplitEnumerator(context, TEST_QUERY, restoreState);
        enumerator.run();

        verify(context)
                .assignSplit(
                        eq(0),
                        ArgumentMatchers.<List<BigQuerySourceSplit>>argThat(
                                list -> list.size() == 2));
        verify(context).signalNoMoreSplits(0);
    }

    @Test
    public void testAddSplitsBackReassigns() {
        SourceSplitEnumerator.Context<BigQuerySourceSplit> context = mockContext(Set.of(0));

        BigQuerySourceSplitEnumerator enumerator =
                new BigQuerySourceSplitEnumerator(context, TEST_QUERY, null);
        enumerator.run();

        enumerator.addSplitsBack(List.of(new BigQuerySourceSplit("0", TEST_QUERY)), 0);

        verify(context, times(2)).assignSplit(eq(0), anyList());
        verify(context, times(2)).signalNoMoreSplits(0);
    }

    @Test
    public void testCurrentUnassignedSplitSizeBeforeAssignment() {
        SourceSplitEnumerator.Context<BigQuerySourceSplit> context =
                mockContext(Collections.emptySet());

        BigQuerySourceSplitEnumerator enumerator =
                new BigQuerySourceSplitEnumerator(context, TEST_QUERY, null);
        assertEquals(1, enumerator.currentUnassignedSplitSize());
    }

    @Test
    public void testCurrentUnassignedSplitSizeAfterAssignment() {
        SourceSplitEnumerator.Context<BigQuerySourceSplit> context = mockContext(Set.of(0));

        BigQuerySourceSplitEnumerator enumerator =
                new BigQuerySourceSplitEnumerator(context, TEST_QUERY, null);
        enumerator.run();
        assertEquals(0, enumerator.currentUnassignedSplitSize());
    }

    @Test
    public void testRegisterReaderTriggersAssignment() {
        SourceSplitEnumerator.Context<BigQuerySourceSplit> context = mockContext(Set.of(1));

        BigQuerySourceSplitEnumerator enumerator =
                new BigQuerySourceSplitEnumerator(context, TEST_QUERY, null);
        enumerator.registerReader(1);

        verify(context)
                .assignSplit(
                        eq(1),
                        ArgumentMatchers.<List<BigQuerySourceSplit>>argThat(
                                list -> list.size() == 1));
        verify(context).signalNoMoreSplits(1);
    }

    @Test
    public void testHandleSplitRequestLogsWarning() {
        SourceSplitEnumerator.Context<BigQuerySourceSplit> context =
                mockContext(Collections.emptySet());

        BigQuerySourceSplitEnumerator enumerator =
                new BigQuerySourceSplitEnumerator(context, TEST_QUERY, null);
        // Should not throw or assign
        enumerator.handleSplitRequest(0);
        verify(context, never()).assignSplit(anyInt(), anyList());
    }

    @Test
    public void testRunSignalsAllReadersNoMoreSplits() {
        // With two readers, the split goes to the first reader; the second must also
        // receive signalNoMoreSplits so it can terminate instead of hanging.
        Set<Integer> readers = new LinkedHashSet<>(Arrays.asList(0, 1));
        SourceSplitEnumerator.Context<BigQuerySourceSplit> context = mockContext(readers);

        BigQuerySourceSplitEnumerator enumerator =
                new BigQuerySourceSplitEnumerator(context, TEST_QUERY, null);
        enumerator.run();

        // Reader 0 gets the split + no-more-splits signal
        verify(context)
                .assignSplit(
                        eq(0),
                        ArgumentMatchers.<List<BigQuerySourceSplit>>argThat(
                                list -> list.size() == 1));
        verify(context).signalNoMoreSplits(0);
        // Reader 1 gets only the no-more-splits signal (no split assigned)
        verify(context, never()).assignSplit(eq(1), anyList());
        verify(context).signalNoMoreSplits(1);
    }

    @Test
    public void testLateRegisteredReaderGetsNoMoreSplits() {
        // After run() completes with reader 0, a late-registered reader 1 must
        // receive signalNoMoreSplits so it does not wait for splits forever.
        SourceSplitEnumerator.Context<BigQuerySourceSplit> context = mockContext(Set.of(0));

        BigQuerySourceSplitEnumerator enumerator =
                new BigQuerySourceSplitEnumerator(context, TEST_QUERY, null);
        enumerator.run();

        // Now simulate late registration of reader 1
        when(context.registeredReaders()).thenReturn(new LinkedHashSet<>(Arrays.asList(0, 1)));
        enumerator.registerReader(1);

        verify(context).signalNoMoreSplits(1);
    }

    @Test
    public void testEmptyRestoredStateSignalsNoMoreSplits() {
        // Restoring from an empty pending-splits list must still signal completion
        // to all registered readers so the bounded source can finish.
        BigQuerySourceState emptyState = new BigQuerySourceState(Collections.emptyList());
        SourceSplitEnumerator.Context<BigQuerySourceSplit> context = mockContext(Set.of(0));

        BigQuerySourceSplitEnumerator enumerator =
                new BigQuerySourceSplitEnumerator(context, TEST_QUERY, emptyState);
        enumerator.run();

        // No split to assign, but reader 0 must still be signaled
        verify(context, never()).assignSplit(anyInt(), anyList());
        verify(context).signalNoMoreSplits(0);
    }
}
