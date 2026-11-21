/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.azurecosmosdb.source;

import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.source.SourceEvent;
import org.apache.seatunnel.api.source.SourceSplitEnumerator;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class AzureCosmosDBSourceSplitEnumeratorTest {

    @Test
    public void testEnumeratorAssignsSingleSplit() throws Exception {
        Set<Integer> readers = new HashSet<>(Arrays.asList(0, 1));
        RecordingContext context = new RecordingContext(2, readers);
        AzureCosmosDBSourceSplitEnumerator enumerator =
                new AzureCosmosDBSourceSplitEnumerator(context, null);

        try {
            enumerator.run();
        } finally {
            enumerator.close();
        }

        Assertions.assertEquals(1, context.assignedSplits.get(0).size());
        Assertions.assertEquals("0", context.assignedSplits.get(0).get(0).splitId());
        Assertions.assertTrue(
                context.assignedSplits.getOrDefault(1, Collections.emptyList()).isEmpty());
        Assertions.assertEquals(readers, context.noMoreSplitReaders);
    }

    @Test
    public void testAddSplitsBackPreservesContinuationToken() throws Exception {
        RecordingContext context = new RecordingContext(1, Collections.singleton(0));
        AzureCosmosDBSourceSplitEnumerator enumerator =
                new AzureCosmosDBSourceSplitEnumerator(context, null);
        AzureCosmosDBSourceSplit restoredSplit = new AzureCosmosDBSourceSplit(0, "token-1");

        try {
            enumerator.addSplitsBack(Collections.singletonList(restoredSplit), 0);
        } finally {
            enumerator.close();
        }

        Assertions.assertEquals(
                "token-1", context.assignedSplits.get(0).get(0).getContinuationToken());
    }

    @Test
    public void testSplitOwnerRoutesNonZeroSplitIdsByBucketIndex() throws Exception {
        RecordingContext context = new RecordingContext(3, new HashSet<>(Arrays.asList(0, 1, 2)));
        AzureCosmosDBSourceSplitEnumerator enumerator =
                new AzureCosmosDBSourceSplitEnumerator(context, null);
        // bucketIndex(4, 3) is 1 and bucketIndex(5, 3) is 2, so the two splits are owned by
        // different readers even though they are handed back to the enumerator together.
        AzureCosmosDBSourceSplit ownedByReaderOne = new AzureCosmosDBSourceSplit(4);
        AzureCosmosDBSourceSplit ownedByReaderTwo = new AzureCosmosDBSourceSplit(5);

        try {
            enumerator.addSplitsBack(Arrays.asList(ownedByReaderOne, ownedByReaderTwo), 1);
            enumerator.registerReader(2);
        } finally {
            enumerator.close();
        }

        Assertions.assertEquals(1, context.assignedSplits.get(1).size());
        Assertions.assertEquals("4", context.assignedSplits.get(1).get(0).splitId());
        Assertions.assertEquals(1, context.assignedSplits.get(2).size());
        Assertions.assertEquals("5", context.assignedSplits.get(2).get(0).splitId());
    }

    @Test
    public void testSplitOwnerKeepsIntegerMinValueSplitIdInRange() throws Exception {
        RecordingContext context = new RecordingContext(3, new HashSet<>(Arrays.asList(0, 1, 2)));
        AzureCosmosDBSourceSplitEnumerator enumerator =
                new AzureCosmosDBSourceSplitEnumerator(context, null);
        // Math.abs(Integer.MIN_VALUE) is itself negative, so a split id at the minimum value is
        // the input that would yield a negative owner index without the masking in HashUtils.
        AzureCosmosDBSourceSplit split = new AzureCosmosDBSourceSplit(Integer.MIN_VALUE);

        try {
            enumerator.addSplitsBack(Collections.singletonList(split), 0);
        } finally {
            enumerator.close();
        }

        context.assignedSplits.keySet().forEach(reader -> Assertions.assertTrue(reader >= 0));
        Assertions.assertEquals(1, context.assignedSplits.get(0).size());
        Assertions.assertEquals(
                String.valueOf(Integer.MIN_VALUE), context.assignedSplits.get(0).get(0).splitId());
    }

    private static class RecordingContext
            implements SourceSplitEnumerator.Context<AzureCosmosDBSourceSplit> {

        private final int parallelism;
        private final Set<Integer> registeredReaders;
        private final Map<Integer, List<AzureCosmosDBSourceSplit>> assignedSplits = new HashMap<>();
        private final Set<Integer> noMoreSplitReaders = new HashSet<>();

        private RecordingContext(int parallelism, Set<Integer> registeredReaders) {
            this.parallelism = parallelism;
            this.registeredReaders = registeredReaders;
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
        public void assignSplit(int subtaskId, List<AzureCosmosDBSourceSplit> splits) {
            assignedSplits.put(subtaskId, splits);
        }

        @Override
        public void signalNoMoreSplits(int subtask) {
            noMoreSplitReaders.add(subtask);
        }

        @Override
        public void sendEventToSourceReader(int subtaskId, SourceEvent event) {}

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
