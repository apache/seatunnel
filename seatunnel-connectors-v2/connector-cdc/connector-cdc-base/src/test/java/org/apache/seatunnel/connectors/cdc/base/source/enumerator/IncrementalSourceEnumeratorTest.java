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

package org.apache.seatunnel.connectors.cdc.base.source.enumerator;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;
import org.apache.seatunnel.connectors.cdc.base.source.split.SnapshotSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.debezium.relational.TableId;

import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class IncrementalSourceEnumeratorTest {

    private SourceSplitEnumerator.Context<SourceSplitBase> context;
    private SplitAssigner splitAssigner;

    @BeforeEach
    void setUp() {
        context = mock(SourceSplitEnumerator.Context.class);
        splitAssigner = mock(SplitAssigner.class);
    }

    @Test
    void shouldAssignRestoredSplitsToWaitingReader() throws Exception {
        // Given: a reader (subtask 0) is registered and waiting
        when(context.registeredReaders()).thenReturn(Collections.singleton(0));

        SnapshotSplit restoredSplit =
                new SnapshotSplit("test-split-1", TableId.parse("db1.table1"), null, null, null);

        when(splitAssigner.getNext())
                .thenReturn(Optional.empty(), Optional.of(restoredSplit));
        when(splitAssigner.waitingForCompletedSplits()).thenReturn(true, false);

        IncrementalSourceEnumerator enumerator =
                new IncrementalSourceEnumerator(context, splitAssigner);

        // Simulate the reader requesting a split (registers the reader as waiting)
        enumerator.handleSplitRequest(0);
        // Set running = true
        enumerator.run();

        // When: restored splits are added back
        List<SourceSplitBase> restoredSplits = Collections.singletonList(restoredSplit);
        enumerator.addSplitsBack(restoredSplits, 0);

        // Then: the restored split should be assigned to the waiting reader
        verify(splitAssigner).addSplits(restoredSplits);
        verify(context).assignSplit(0, restoredSplit);
    }

    @Test
    void shouldNotAssignSplitsBackWhenNotRunning() throws Exception {
        // Given: a reader is waiting but enumerator is not running yet
        when(context.registeredReaders()).thenReturn(Collections.singleton(0));

        SnapshotSplit restoredSplit =
                new SnapshotSplit("test-split-1", TableId.parse("db1.table1"), null, null, null);

        when(splitAssigner.getNext()).thenReturn(Optional.of(restoredSplit));
        when(splitAssigner.waitingForCompletedSplits()).thenReturn(false);

        IncrementalSourceEnumerator enumerator =
                new IncrementalSourceEnumerator(context, splitAssigner);

        // Simulate the reader requesting a split
        enumerator.handleSplitRequest(0);
        // Do NOT call run() — so running is still false

        // When: restored splits are added back
        List<SourceSplitBase> restoredSplits = Collections.singletonList(restoredSplit);
        enumerator.addSplitsBack(restoredSplits, 0);

        // Then: splits are added to the assigner but NOT assigned to the reader
        verify(splitAssigner).addSplits(restoredSplits);
        verify(context, never()).assignSplit(anyInt(), any(SourceSplitBase.class));
    }
}
