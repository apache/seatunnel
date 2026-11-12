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
 * Replays what Flink does to the enumerator when the reader subtask fails: it hands back the splits
 * assigned since the last completed checkpoint, and a new reader registers.
 */
class SingleSplitEnumeratorTest {

    private final Set<Integer> readers = new HashSet<>();
    private final List<SingleSplit> assigned = new ArrayList<>();
    private SingleSplitEnumerator enumerator;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() throws Exception {
        SourceSplitEnumerator.Context<SingleSplit> context =
                Mockito.mock(SourceSplitEnumerator.Context.class);
        Mockito.when(context.registeredReaders()).thenAnswer(invocation -> new HashSet<>(readers));
        Mockito.doAnswer(
                        invocation -> {
                            assigned.add(invocation.getArgument(1));
                            return null;
                        })
                .when(context)
                .assignSplit(Mockito.anyInt(), Mockito.any(SingleSplit.class));
        enumerator = new SingleSplitEnumerator(context);
        readers.add(0);
        enumerator.registerReader(0);
        enumerator.run();
        Assertions.assertEquals(1, assigned.size());
    }

    @Test
    void testSplitGivenBackIsAssignedAgainWhenTheReaderRegisters() {
        SingleSplit split = assigned.get(0);

        restartReader(Collections.singletonList(split));

        Assertions.assertEquals(2, assigned.size(), "the restarted reader must get the split back");
        Assertions.assertSame(split, assigned.get(1));
    }

    @Test
    void testEmptyListGivenBackIsIgnored() {
        restartReader(Collections.singletonList(assigned.get(0)));

        // Nothing was assigned since the first restart, so Flink gives back an empty list.
        Assertions.assertDoesNotThrow(() -> restartReader(Collections.emptyList()));
        Assertions.assertEquals(2, assigned.size(), "an empty list must not assign anything");
    }

    private void restartReader(List<SingleSplit> splitsGivenBack) {
        readers.remove(0);
        enumerator.addSplitsBack(splitsGivenBack, 0);
        readers.add(0);
        enumerator.registerReader(0);
    }
}
