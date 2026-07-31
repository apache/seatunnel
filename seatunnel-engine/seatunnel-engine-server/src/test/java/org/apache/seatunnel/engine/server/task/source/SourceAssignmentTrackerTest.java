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

package org.apache.seatunnel.engine.server.task.source;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

class SourceAssignmentTrackerTest {

    @Test
    void shouldRetainOwnershipUntilReaderCheckpointCompletes() throws Exception {
        SourceAssignmentTracker tracker = new SourceAssignmentTracker(10, 16_384L);
        recordAssignment(tracker, "command-1", 1L, 0, "attempt-1");

        tracker.markAdmitted("command-1", "attempt-1");
        tracker.markApplied("command-1", "attempt-1", Arrays.asList("split-1", "split-2"));
        tracker.markReaderCheckpointIncluded(
                0, "attempt-1", 11L, 1L, new HashSet<>(Arrays.asList("split-1", "split-2")));

        Assertions.assertEquals(1, tracker.stateCount(SourceAssignmentState.CHECKPOINT_INCLUDED));
        byte[] serialized = SourceAssignmentTrackerSerializer.serialize(tracker.entries());

        SourceAssignmentTracker restored = new SourceAssignmentTracker(10, 16_384L);
        restored.restore(SourceAssignmentTrackerSerializer.deserialize(serialized));
        Assertions.assertEquals(1, restored.size());
        restored.checkpointCompleted(11L);

        Assertions.assertEquals(0, restored.size());
        Assertions.assertEquals(1L, restored.compactedEntries());
        Assertions.assertEquals(0L, restored.trackedBytes());
    }

    @Test
    void shouldKeepAcceptedAssignmentUntilReaderCheckpointProofArrives() {
        SourceAssignmentTracker tracker = new SourceAssignmentTracker(10, 16_384L);
        recordAssignment(tracker, "command-1", 1L, 0, "attempt-1");

        tracker.markAdmitted("command-1", "attempt-1");
        tracker.markApplied("command-1", "attempt-1", Arrays.asList("split-1", "split-2"));
        tracker.checkpointCompleted(11L);

        Assertions.assertTrue(tracker.contains("command-1"));
        Assertions.assertEquals(1, tracker.stateCount(SourceAssignmentState.APPLIED));
        Assertions.assertEquals(0L, tracker.compactedEntries());
    }

    @Test
    void shouldFenceAttemptProofAndReturnAssignmentsFromMissingReader() {
        SourceAssignmentTracker tracker = new SourceAssignmentTracker(10, 16_384L);
        recordAssignment(tracker, "command-1", 1L, 3, "attempt-1");

        tracker.markApplied("command-1", "stale-attempt", Arrays.asList("split-1", "split-2"));
        Assertions.assertEquals(1, tracker.stateCount(SourceAssignmentState.DISPATCHED));

        List<SourceAssignmentTracker.Entry> orphaned =
                tracker.takeAssignmentsForMissingReaders(Collections.singleton(0));
        Assertions.assertEquals(1, orphaned.size());
        Assertions.assertEquals("command-1", orphaned.get(0).getCommandId());
        Assertions.assertEquals(0, tracker.size());
    }

    @Test
    void shouldRejectUnalignedAssignmentPayloads() {
        SourceAssignmentTracker tracker = new SourceAssignmentTracker(10, 16_384L);

        Assertions.assertThrows(
                IllegalArgumentException.class,
                () ->
                        tracker.recordDispatched(
                                "command-1",
                                "group-1",
                                1L,
                                0,
                                "attempt-1",
                                0,
                                1,
                                Arrays.asList("split-1", "split-2"),
                                Collections.singletonList(new byte[] {1})));
    }

    private static void recordAssignment(
            SourceAssignmentTracker tracker,
            String commandId,
            long sequence,
            int subtask,
            String attemptId) {
        tracker.recordDispatched(
                commandId,
                "group-1",
                sequence,
                subtask,
                attemptId,
                0,
                1,
                Arrays.asList("split-1", "split-2"),
                Arrays.asList(new byte[] {1}, new byte[] {2}));
    }
}
