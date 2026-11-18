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

package org.apache.seatunnel.connectors.seatunnel.jms.source;

import org.apache.seatunnel.api.serialization.DefaultSerializer;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class JmsSourceSplitEnumeratorTest {

    @Test
    void eachReaderGetsOneConsumerSlot() {
        Map<Integer, List<Integer>> assigned = new HashMap<>();
        JmsSourceSplitEnumerator enumerator =
                new JmsSourceSplitEnumerator(context(2, assigned, 0, 1));

        enumerator.run();

        assertEquals(Collections.singletonList(0), assigned.get(0));
        assertEquals(Collections.singletonList(1), assigned.get(1));
        JmsSourceState state = enumerator.snapshotState(1);
        assertEquals(2, state.getConsumerSlots());
        assertEquals(Collections.emptyList(), state.getUnassigned());
    }

    @Test
    void restoreKeepsSlotsAndAddsSlotsWhenParallelismGrows() {
        Map<Integer, List<Integer>> assigned = new HashMap<>();
        JmsSourceState restored =
                new JmsSourceState(2, Collections.singletonList(new JmsSourceSplit(1)));
        JmsSourceSplitEnumerator enumerator =
                new JmsSourceSplitEnumerator(context(3, assigned, 0, 1, 2), restored);

        enumerator.run();

        // Slot 0 was assigned before the restore and comes back with its reader's state.
        assertEquals(null, assigned.get(0));
        assertEquals(Collections.singletonList(1), assigned.get(1));
        assertEquals(Collections.singletonList(2), assigned.get(2));
        assertEquals(3, enumerator.snapshotState(2).getConsumerSlots());
    }

    @Test
    void splitsAddedBackAreAssignedAgain() {
        Map<Integer, List<Integer>> assigned = new HashMap<>();
        JmsSourceSplitEnumerator enumerator = new JmsSourceSplitEnumerator(context(1, assigned, 0));
        enumerator.run();
        assigned.clear();

        enumerator.addSplitsBack(Collections.singletonList(new JmsSourceSplit(0)), 0);

        assertEquals(Collections.singletonList(0), assigned.get(0));
    }

    /** The engine stores splits and enumerator state with Java serialization. */
    @Test
    void splitAndStateSurviveTheDefaultSerializer() throws Exception {
        JmsSourceSplit split =
                new DefaultSerializer<JmsSourceSplit>()
                        .deserialize(
                                new DefaultSerializer<JmsSourceSplit>()
                                        .serialize(new JmsSourceSplit(3)));
        assertEquals("consumer-3", split.splitId());

        JmsSourceState state =
                new DefaultSerializer<JmsSourceState>()
                        .deserialize(
                                new DefaultSerializer<JmsSourceState>()
                                        .serialize(
                                                new JmsSourceState(
                                                        4,
                                                        Arrays.asList(
                                                                new JmsSourceSplit(2),
                                                                new JmsSourceSplit(3)))));
        assertEquals(4, state.getConsumerSlots());
        assertEquals(
                Arrays.asList(2, 3),
                state.getUnassigned().stream()
                        .map(JmsSourceSplit::getId)
                        .collect(Collectors.toList()));
    }

    @SuppressWarnings("unchecked")
    private static SourceSplitEnumerator.Context<JmsSourceSplit> context(
            int parallelism, Map<Integer, List<Integer>> assigned, Integer... readers) {
        SourceSplitEnumerator.Context<JmsSourceSplit> context =
                mock(SourceSplitEnumerator.Context.class);
        Set<Integer> registered = new HashSet<>(Arrays.asList(readers));
        when(context.currentParallelism()).thenReturn(parallelism);
        when(context.registeredReaders()).thenReturn(registered);
        doAnswer(
                        call -> {
                            List<JmsSourceSplit> splits = call.getArgument(1);
                            assigned.computeIfAbsent(call.getArgument(0), k -> new ArrayList<>())
                                    .addAll(
                                            splits.stream()
                                                    .map(JmsSourceSplit::getId)
                                                    .collect(Collectors.toList()));
                            return null;
                        })
                .when(context)
                .assignSplit(anyInt(), anyList());
        return context;
    }
}
