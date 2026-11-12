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

package org.apache.seatunnel.connectors.seatunnel.common.source;

import org.apache.seatunnel.api.source.SourceSplitEnumerator;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Drives the enumerator through its context the way Flink does when the reader subtask fails: the
 * reader is removed, the splits assigned since the last completed checkpoint are handed back
 * (possibly none), and a new reader registers.
 */
class SingleSplitEnumeratorTest {

    private static final int READER = 0;

    private final Set<Integer> readers = new HashSet<>();
    private final List<Integer> assignedReaders = new ArrayList<>();
    private final List<SingleSplit> assignedSplits = new ArrayList<>();
    private SingleSplitEnumerator enumerator;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() {
        SourceSplitEnumerator.Context<SingleSplit> context =
                Mockito.mock(SourceSplitEnumerator.Context.class);
        Mockito.when(context.registeredReaders()).thenAnswer(invocation -> new HashSet<>(readers));
        Mockito.doAnswer(
                        invocation -> {
                            assignedReaders.add(invocation.getArgument(0));
                            assignedSplits.add(invocation.getArgument(1));
                            return null;
                        })
                .when(context)
                .assignSplit(Mockito.anyInt(), Mockito.any(SingleSplit.class));
        enumerator = new SingleSplitEnumerator(context);
    }

    @Test
    void testFirstAssignmentWhenTheReaderRegistersBeforeRun() throws Exception {
        registerReader();
        enumerator.run();
        assertAssignments(1);

        enumerator.run();
        registerReader();
        assertAssignments(1);
    }

    @Test
    void testFirstAssignmentWhenRunBeforeTheReaderRegisters() throws Exception {
        enumerator.run();
        assertAssignments(0);

        registerReader();
        assertAssignments(1);

        enumerator.run();
        registerReader();
        assertAssignments(1);
    }

    @Test
    void testSplitGivenBackIsAssignedAgainWhenTheReaderRegisters() throws Exception {
        start();
        SingleSplit split = assignedSplits.get(0);

        readers.remove(READER);
        enumerator.addSplitsBack(Collections.singletonList(split), READER);
        assertAssignments(1);

        registerReader();
        assertAssignments(2);
        Assertions.assertSame(split, assignedSplits.get(1));
    }

    @Test
    void testEmptyListGivenBackIsHarmless() throws Exception {
        start();

        // The reader fails before a checkpoint completes: it gets the split back.
        restartReader(Collections.singletonList(assignedSplits.get(0)));
        assertAssignments(2);

        // It fails again: nothing was assigned since the last checkpoint, so the list is empty.
        Assertions.assertDoesNotThrow(() -> restartReader(Collections.emptyList()));
        Assertions.assertDoesNotThrow(
                () -> enumerator.addSplitsBack(Collections.emptyList(), READER));
        assertAssignments(2);
    }

    @Test
    void testEmptyListGivenBackKeepsAPendingSplit() throws Exception {
        start();
        SingleSplit split = assignedSplits.get(0);

        // The split comes back while no reader is registered, then an empty list follows.
        readers.remove(READER);
        enumerator.addSplitsBack(Collections.singletonList(split), READER);
        enumerator.addSplitsBack(Collections.emptyList(), READER);
        assertAssignments(1);

        registerReader();
        assertAssignments(2);
        Assertions.assertSame(split, assignedSplits.get(1));
    }

    private void start() throws Exception {
        registerReader();
        enumerator.run();
        assertAssignments(1);
    }

    private void registerReader() {
        readers.add(READER);
        enumerator.registerReader(READER);
    }

    private void restartReader(List<SingleSplit> splitsGivenBack) {
        readers.remove(READER);
        enumerator.addSplitsBack(splitsGivenBack, READER);
        registerReader();
    }

    private void assertAssignments(int expected) {
        Assertions.assertEquals(expected, assignedSplits.size(), "assignments");
        Assertions.assertEquals(Collections.nCopies(expected, READER), assignedReaders, "readers");
    }
}
