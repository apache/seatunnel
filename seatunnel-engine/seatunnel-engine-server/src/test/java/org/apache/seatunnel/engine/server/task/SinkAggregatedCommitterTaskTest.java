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

package org.apache.seatunnel.engine.server.task;

import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.engine.core.dag.actions.SinkAction;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.execution.TaskLocation;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

public class SinkAggregatedCommitterTaskTest {

    private SinkAggregatedCommitterTask<String, String> task;
    private SinkAction<SeaTunnelRow, ?, String, String> mockSinkAction;
    private SinkAggregatedCommitter<String, String> mockAggregatedCommitter;

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() throws Exception {
        mockSinkAction = Mockito.mock(SinkAction.class);
        mockAggregatedCommitter = Mockito.mock(SinkAggregatedCommitter.class);

        Mockito.when(mockSinkAction.getParallelism()).thenReturn(1);
        Mockito.when(mockAggregatedCommitter.commit(Mockito.anyList()))
                .thenReturn(Collections.emptyList());
        Mockito.when(mockAggregatedCommitter.combine(Mockito.anyList())).thenReturn("combined");

        TaskLocation taskLocation = new TaskLocation(new TaskGroupLocation(1L, 1, 1L), 1L, 1);

        task =
                new SinkAggregatedCommitterTask<>(
                        1L, taskLocation, mockSinkAction, mockAggregatedCommitter);

        // Initialize internal maps via reflection since init() requires more setup
        Field commitInfoCacheField =
                SinkAggregatedCommitterTask.class.getDeclaredField("commitInfoCache");
        commitInfoCacheField.setAccessible(true);
        commitInfoCacheField.set(task, new java.util.concurrent.ConcurrentHashMap<>());

        Field checkpointBarrierCounterField =
                SinkAggregatedCommitterTask.class.getDeclaredField("checkpointBarrierCounter");
        checkpointBarrierCounterField.setAccessible(true);
        checkpointBarrierCounterField.set(task, new java.util.concurrent.ConcurrentHashMap<>());

        Field checkpointCommitInfoMapField =
                SinkAggregatedCommitterTask.class.getDeclaredField("checkpointCommitInfoMap");
        checkpointCommitInfoMapField.setAccessible(true);
        checkpointCommitInfoMapField.set(task, new java.util.concurrent.ConcurrentHashMap<>());
    }

    @Test
    void testCheckpointCacheCleanupAfterNotifyCheckpointComplete() throws Exception {
        // Simulate receiving commit info for multiple checkpoints
        task.receivedWriterCommitInfo(1L, "commitInfo1");
        task.receivedWriterCommitInfo(2L, "commitInfo2");
        task.receivedWriterCommitInfo(3L, "commitInfo3");

        // Simulate barrier counter entries
        Map<Long, Integer> checkpointBarrierCounter = getCheckpointBarrierCounter();
        checkpointBarrierCounter.put(1L, 1);
        checkpointBarrierCounter.put(2L, 1);
        checkpointBarrierCounter.put(3L, 1);

        // Simulate checkpointCommitInfoMap entries
        ConcurrentMap<Long, List<String>> checkpointCommitInfoMap = getCheckpointCommitInfoMap();
        checkpointCommitInfoMap.put(1L, Collections.singletonList("aggregated1"));
        checkpointCommitInfoMap.put(2L, Collections.singletonList("aggregated2"));
        checkpointCommitInfoMap.put(3L, Collections.singletonList("aggregated3"));

        // Verify initial state - all caches have data
        ConcurrentMap<Long, List<String>> commitInfoCache = getCommitInfoCache();
        Assertions.assertEquals(3, commitInfoCache.size());
        Assertions.assertEquals(3, checkpointBarrierCounter.size());
        Assertions.assertEquals(3, checkpointCommitInfoMap.size());

        // Notify checkpoint 2 complete - should clean up checkpoints 1 and 2
        task.notifyCheckpointComplete(2L);

        // Verify that checkpoints 1 and 2 are cleaned from all caches
        Assertions.assertFalse(
                commitInfoCache.containsKey(1L),
                "commitInfoCache should not contain checkpoint 1 after completion");
        Assertions.assertFalse(
                commitInfoCache.containsKey(2L),
                "commitInfoCache should not contain checkpoint 2 after completion");
        Assertions.assertTrue(
                commitInfoCache.containsKey(3L),
                "commitInfoCache should still contain checkpoint 3");

        Assertions.assertFalse(
                checkpointBarrierCounter.containsKey(1L),
                "checkpointBarrierCounter should not contain checkpoint 1 after completion");
        Assertions.assertFalse(
                checkpointBarrierCounter.containsKey(2L),
                "checkpointBarrierCounter should not contain checkpoint 2 after completion");
        Assertions.assertTrue(
                checkpointBarrierCounter.containsKey(3L),
                "checkpointBarrierCounter should still contain checkpoint 3");

        Assertions.assertFalse(
                checkpointCommitInfoMap.containsKey(1L),
                "checkpointCommitInfoMap should not contain checkpoint 1 after completion");
        Assertions.assertFalse(
                checkpointCommitInfoMap.containsKey(2L),
                "checkpointCommitInfoMap should not contain checkpoint 2 after completion");
        Assertions.assertTrue(
                checkpointCommitInfoMap.containsKey(3L),
                "checkpointCommitInfoMap should still contain checkpoint 3");
    }

    @Test
    void testCheckpointCacheCleanupAfterNotifyCheckpointAborted() throws Exception {
        // Simulate receiving commit info for a checkpoint
        task.receivedWriterCommitInfo(5L, "commitInfo5");

        // Simulate barrier counter entry
        Map<Long, Integer> checkpointBarrierCounter = getCheckpointBarrierCounter();
        checkpointBarrierCounter.put(5L, 1);

        // Simulate checkpointCommitInfoMap entry
        ConcurrentMap<Long, List<String>> checkpointCommitInfoMap = getCheckpointCommitInfoMap();
        checkpointCommitInfoMap.put(5L, Collections.singletonList("aggregated5"));

        // Verify initial state
        ConcurrentMap<Long, List<String>> commitInfoCache = getCommitInfoCache();
        Assertions.assertTrue(commitInfoCache.containsKey(5L));
        Assertions.assertTrue(checkpointBarrierCounter.containsKey(5L));
        Assertions.assertTrue(checkpointCommitInfoMap.containsKey(5L));

        // Notify checkpoint 5 aborted
        task.notifyCheckpointAborted(5L);

        // Verify that checkpoint 5 is cleaned from all caches
        Assertions.assertFalse(
                commitInfoCache.containsKey(5L),
                "commitInfoCache should not contain checkpoint 5 after abort");
        Assertions.assertFalse(
                checkpointBarrierCounter.containsKey(5L),
                "checkpointBarrierCounter should not contain checkpoint 5 after abort");
        Assertions.assertFalse(
                checkpointCommitInfoMap.containsKey(5L),
                "checkpointCommitInfoMap should not contain checkpoint 5 after abort");
    }

    @Test
    void testMultipleCheckpointCompletionsCleansUpCorrectly() throws Exception {
        // Simulate a long-running job with many checkpoints
        for (long i = 1; i <= 100; i++) {
            task.receivedWriterCommitInfo(i, "commitInfo" + i);
            getCheckpointBarrierCounter().put(i, 1);
            getCheckpointCommitInfoMap().put(i, Collections.singletonList("aggregated" + i));
        }

        ConcurrentMap<Long, List<String>> commitInfoCache = getCommitInfoCache();
        Map<Long, Integer> checkpointBarrierCounter = getCheckpointBarrierCounter();
        ConcurrentMap<Long, List<String>> checkpointCommitInfoMap = getCheckpointCommitInfoMap();

        // Verify initial state
        Assertions.assertEquals(100, commitInfoCache.size());
        Assertions.assertEquals(100, checkpointBarrierCounter.size());
        Assertions.assertEquals(100, checkpointCommitInfoMap.size());

        // Complete checkpoint 50 - should clean up checkpoints 1-50
        task.notifyCheckpointComplete(50L);

        // Verify cleanup
        Assertions.assertEquals(
                50,
                commitInfoCache.size(),
                "commitInfoCache should have 50 entries after completing checkpoint 50");
        Assertions.assertEquals(
                50,
                checkpointBarrierCounter.size(),
                "checkpointBarrierCounter should have 50 entries after completing checkpoint 50");
        Assertions.assertEquals(
                50,
                checkpointCommitInfoMap.size(),
                "checkpointCommitInfoMap should have 50 entries after completing checkpoint 50");

        // Verify that checkpoints 1-50 are removed and 51-100 remain
        for (long i = 1; i <= 50; i++) {
            Assertions.assertFalse(
                    commitInfoCache.containsKey(i),
                    "commitInfoCache should not contain checkpoint " + i);
            Assertions.assertFalse(
                    checkpointBarrierCounter.containsKey(i),
                    "checkpointBarrierCounter should not contain checkpoint " + i);
            Assertions.assertFalse(
                    checkpointCommitInfoMap.containsKey(i),
                    "checkpointCommitInfoMap should not contain checkpoint " + i);
        }

        for (long i = 51; i <= 100; i++) {
            Assertions.assertTrue(
                    commitInfoCache.containsKey(i),
                    "commitInfoCache should contain checkpoint " + i);
            Assertions.assertTrue(
                    checkpointBarrierCounter.containsKey(i),
                    "checkpointBarrierCounter should contain checkpoint " + i);
            Assertions.assertTrue(
                    checkpointCommitInfoMap.containsKey(i),
                    "checkpointCommitInfoMap should contain checkpoint " + i);
        }

        // Complete checkpoint 100 - should clean up all remaining checkpoints
        task.notifyCheckpointComplete(100L);

        Assertions.assertEquals(
                0,
                commitInfoCache.size(),
                "commitInfoCache should be empty after completing all checkpoints");
        Assertions.assertEquals(
                0,
                checkpointBarrierCounter.size(),
                "checkpointBarrierCounter should be empty after completing all checkpoints");
        Assertions.assertEquals(
                0,
                checkpointCommitInfoMap.size(),
                "checkpointCommitInfoMap should be empty after completing all checkpoints");
    }

    @SuppressWarnings("unchecked")
    private ConcurrentMap<Long, List<String>> getCommitInfoCache() throws Exception {
        Field field = SinkAggregatedCommitterTask.class.getDeclaredField("commitInfoCache");
        field.setAccessible(true);
        return (ConcurrentMap<Long, List<String>>) field.get(task);
    }

    @SuppressWarnings("unchecked")
    private Map<Long, Integer> getCheckpointBarrierCounter() throws Exception {
        Field field =
                SinkAggregatedCommitterTask.class.getDeclaredField("checkpointBarrierCounter");
        field.setAccessible(true);
        return (Map<Long, Integer>) field.get(task);
    }

    @SuppressWarnings("unchecked")
    private ConcurrentMap<Long, List<String>> getCheckpointCommitInfoMap() throws Exception {
        Field field = SinkAggregatedCommitterTask.class.getDeclaredField("checkpointCommitInfoMap");
        field.setAccessible(true);
        return (ConcurrentMap<Long, List<String>>) field.get(task);
    }

    @Test
    void testCleanupWithEmptyCaches() throws Exception {
        // Verify that cleanup works correctly when caches are empty
        ConcurrentMap<Long, List<String>> commitInfoCache = getCommitInfoCache();
        Map<Long, Integer> checkpointBarrierCounter = getCheckpointBarrierCounter();
        ConcurrentMap<Long, List<String>> checkpointCommitInfoMap = getCheckpointCommitInfoMap();

        // All caches are empty
        Assertions.assertEquals(0, commitInfoCache.size());
        Assertions.assertEquals(0, checkpointBarrierCounter.size());
        Assertions.assertEquals(0, checkpointCommitInfoMap.size());

        // Should not throw any exception when cleaning up empty caches
        Assertions.assertDoesNotThrow(() -> task.notifyCheckpointComplete(1L));
        Assertions.assertDoesNotThrow(() -> task.notifyCheckpointAborted(2L));

        // Caches should still be empty
        Assertions.assertEquals(0, commitInfoCache.size());
        Assertions.assertEquals(0, checkpointBarrierCounter.size());
        Assertions.assertEquals(0, checkpointCommitInfoMap.size());
    }

    @Test
    void testCleanupWithPartialCacheData() throws Exception {
        // Test when only some caches have data for a checkpoint
        // This simulates edge cases where data might be partially written

        // Only commitInfoCache has data
        task.receivedWriterCommitInfo(1L, "commitInfo1");

        ConcurrentMap<Long, List<String>> commitInfoCache = getCommitInfoCache();
        Map<Long, Integer> checkpointBarrierCounter = getCheckpointBarrierCounter();
        ConcurrentMap<Long, List<String>> checkpointCommitInfoMap = getCheckpointCommitInfoMap();

        Assertions.assertEquals(1, commitInfoCache.size());
        Assertions.assertEquals(0, checkpointBarrierCounter.size());
        Assertions.assertEquals(0, checkpointCommitInfoMap.size());

        // Should not throw and should clean up what exists
        Assertions.assertDoesNotThrow(() -> task.notifyCheckpointAborted(1L));

        Assertions.assertEquals(0, commitInfoCache.size());
        Assertions.assertEquals(0, checkpointBarrierCounter.size());
        Assertions.assertEquals(0, checkpointCommitInfoMap.size());
    }

    @Test
    void testCleanupDoesNotAffectFutureCheckpoints() throws Exception {
        // Verify that cleaning up checkpoint N does not affect checkpoint N+1 data
        // This is critical for ensuring the fix doesn't break normal operation

        // Setup checkpoints 1, 2, 3
        task.receivedWriterCommitInfo(1L, "commitInfo1");
        task.receivedWriterCommitInfo(2L, "commitInfo2");
        task.receivedWriterCommitInfo(3L, "commitInfo3");

        Map<Long, Integer> checkpointBarrierCounter = getCheckpointBarrierCounter();
        checkpointBarrierCounter.put(1L, 1);
        checkpointBarrierCounter.put(2L, 1);
        checkpointBarrierCounter.put(3L, 1);

        ConcurrentMap<Long, List<String>> checkpointCommitInfoMap = getCheckpointCommitInfoMap();
        checkpointCommitInfoMap.put(1L, Collections.singletonList("aggregated1"));
        checkpointCommitInfoMap.put(2L, Collections.singletonList("aggregated2"));
        checkpointCommitInfoMap.put(3L, Collections.singletonList("aggregated3"));

        // Complete checkpoint 1
        task.notifyCheckpointComplete(1L);

        // Verify checkpoint 1 is cleaned
        ConcurrentMap<Long, List<String>> commitInfoCache = getCommitInfoCache();
        Assertions.assertFalse(commitInfoCache.containsKey(1L));
        Assertions.assertFalse(checkpointBarrierCounter.containsKey(1L));
        Assertions.assertFalse(checkpointCommitInfoMap.containsKey(1L));

        // Verify checkpoints 2 and 3 are intact with correct data
        Assertions.assertTrue(commitInfoCache.containsKey(2L));
        Assertions.assertTrue(commitInfoCache.containsKey(3L));
        Assertions.assertEquals(1, commitInfoCache.get(2L).size());
        Assertions.assertEquals("commitInfo2", commitInfoCache.get(2L).get(0));
        Assertions.assertEquals(1, commitInfoCache.get(3L).size());
        Assertions.assertEquals("commitInfo3", commitInfoCache.get(3L).get(0));

        Assertions.assertTrue(checkpointBarrierCounter.containsKey(2L));
        Assertions.assertTrue(checkpointBarrierCounter.containsKey(3L));
        Assertions.assertEquals(1, checkpointBarrierCounter.get(2L));
        Assertions.assertEquals(1, checkpointBarrierCounter.get(3L));

        Assertions.assertTrue(checkpointCommitInfoMap.containsKey(2L));
        Assertions.assertTrue(checkpointCommitInfoMap.containsKey(3L));
    }

    @Test
    void testCommitIsCalledWithCorrectDataBeforeCleanup() throws Exception {
        // Verify that aggregatedCommitter.commit() is called with correct data
        // before the cleanup happens

        task.receivedWriterCommitInfo(1L, "commitInfo1");

        ConcurrentMap<Long, List<String>> checkpointCommitInfoMap = getCheckpointCommitInfoMap();
        checkpointCommitInfoMap.put(1L, Collections.singletonList("aggregated1"));

        // Complete checkpoint 1
        task.notifyCheckpointComplete(1L);

        // Verify commit was called with the correct aggregated data
        Mockito.verify(mockAggregatedCommitter, Mockito.times(1))
                .commit(
                        Mockito.argThat(
                                list -> list.size() == 1 && list.get(0).equals("aggregated1")));
    }

    @Test
    void testAbortIsCalledWithCorrectDataBeforeCleanup() throws Exception {
        // Verify that aggregatedCommitter.abort() is called with correct data
        // before the cleanup happens

        task.receivedWriterCommitInfo(1L, "commitInfo1");

        ConcurrentMap<Long, List<String>> checkpointCommitInfoMap = getCheckpointCommitInfoMap();
        checkpointCommitInfoMap.put(1L, Collections.singletonList("aggregated1"));

        // Abort checkpoint 1
        task.notifyCheckpointAborted(1L);

        // Verify abort was called with the correct data
        Mockito.verify(mockAggregatedCommitter, Mockito.times(1))
                .abort(
                        Mockito.argThat(
                                list ->
                                        list != null
                                                && list.size() == 1
                                                && list.get(0).equals("aggregated1")));
    }

    @Test
    void testReceivedWriterCommitInfoStillWorksAfterCleanup() throws Exception {
        // Verify that receivedWriterCommitInfo still works correctly after cleanup
        // This ensures the fix doesn't break the normal data flow

        // Add data for checkpoint 1
        task.receivedWriterCommitInfo(1L, "commitInfo1");

        ConcurrentMap<Long, List<String>> checkpointCommitInfoMap = getCheckpointCommitInfoMap();
        checkpointCommitInfoMap.put(1L, Collections.singletonList("aggregated1"));

        // Complete checkpoint 1
        task.notifyCheckpointComplete(1L);

        // Now add data for checkpoint 2 - should work normally
        task.receivedWriterCommitInfo(2L, "commitInfo2");

        ConcurrentMap<Long, List<String>> commitInfoCache = getCommitInfoCache();
        Assertions.assertTrue(commitInfoCache.containsKey(2L));
        Assertions.assertEquals(1, commitInfoCache.get(2L).size());
        Assertions.assertEquals("commitInfo2", commitInfoCache.get(2L).get(0));

        // Add more data to checkpoint 2
        task.receivedWriterCommitInfo(2L, "commitInfo2_additional");
        Assertions.assertEquals(2, commitInfoCache.get(2L).size());
    }

    @Test
    void testCleanupOnlyAffectsCheckpointsLessThanOrEqualToCompleted() throws Exception {
        // Explicitly test the <= condition in notifyCheckpointComplete

        // Setup checkpoints 5, 10, 15, 20
        for (long id : new long[] {5L, 10L, 15L, 20L}) {
            task.receivedWriterCommitInfo(id, "commitInfo" + id);
            getCheckpointBarrierCounter().put(id, 1);
            getCheckpointCommitInfoMap().put(id, Collections.singletonList("aggregated" + id));
        }

        // Complete checkpoint 10 - should clean 5 and 10, keep 15 and 20
        task.notifyCheckpointComplete(10L);

        ConcurrentMap<Long, List<String>> commitInfoCache = getCommitInfoCache();
        Map<Long, Integer> checkpointBarrierCounter = getCheckpointBarrierCounter();
        ConcurrentMap<Long, List<String>> checkpointCommitInfoMap = getCheckpointCommitInfoMap();

        // Checkpoints <= 10 should be cleaned
        Assertions.assertFalse(commitInfoCache.containsKey(5L));
        Assertions.assertFalse(commitInfoCache.containsKey(10L));
        Assertions.assertFalse(checkpointBarrierCounter.containsKey(5L));
        Assertions.assertFalse(checkpointBarrierCounter.containsKey(10L));
        Assertions.assertFalse(checkpointCommitInfoMap.containsKey(5L));
        Assertions.assertFalse(checkpointCommitInfoMap.containsKey(10L));

        // Checkpoints > 10 should remain
        Assertions.assertTrue(commitInfoCache.containsKey(15L));
        Assertions.assertTrue(commitInfoCache.containsKey(20L));
        Assertions.assertTrue(checkpointBarrierCounter.containsKey(15L));
        Assertions.assertTrue(checkpointBarrierCounter.containsKey(20L));
        Assertions.assertTrue(checkpointCommitInfoMap.containsKey(15L));
        Assertions.assertTrue(checkpointCommitInfoMap.containsKey(20L));
    }
}
