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

package org.apache.seatunnel.engine.server.checkpoint;

import org.apache.seatunnel.engine.core.checkpoint.CheckpointType;
import org.apache.seatunnel.engine.server.checkpoint.monitor.CheckpointMonitorService;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.execution.TaskLocation;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

public class PendingCheckpointTest {

    private static final int STATE_BYTES = 100;

    @Test
    void testCompletesOnlyAfterEveryAcknowledgedStateIsReported() throws Exception {
        CountDownLatch subtask1Reporting = new CountDownLatch(1);
        CountDownLatch completed = new CountDownLatch(1);
        // Holds the ack of subtask 1 between leaving the not-yet-acknowledged set and writing its
        // state, which is where a concurrent ack of subtask 0 used to complete the checkpoint
        // without it.
        Context context =
                new Context(
                        key ->
                                new ActionState(key, 2) {
                                    @Override
                                    public void reportState(int index, ActionSubtaskState state) {
                                        if (index == 1) {
                                            subtask1Reporting.countDown();
                                            awaitQuietly(completed, 500);
                                        }
                                        super.reportState(index, state);
                                    }
                                });
        AtomicInteger missingAtCompletion = new AtomicInteger(-1);
        context.pendingCheckpoint
                .getCompletableFuture()
                .thenAccept(
                        checkpoint -> {
                            missingAtCompletion.set(missingSubtaskStates(checkpoint, context.key));
                            completed.countDown();
                        });

        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<?> ack1 = executor.submit(() -> context.ack(context.subtask1));
            Assertions.assertTrue(subtask1Reporting.await(10, TimeUnit.SECONDS));
            context.ack(context.subtask0);
            ack1.get(10, TimeUnit.SECONDS);
        } finally {
            executor.shutdownNow();
        }

        context.pendingCheckpoint.getCompletableFuture().get(10, TimeUnit.SECONDS);
        Assertions.assertEquals(
                0,
                missingAtCompletion.get(),
                "the checkpoint completed before every acknowledged subtask state was reported");
    }

    @Test
    void testConcurrentAcknowledgementsNeverCompleteWithMissingState() {
        Assertions.assertTimeout(
                Duration.ofMinutes(2),
                () -> {
                    int iterations = 20_000;
                    ExecutorService ackThreads = Executors.newFixedThreadPool(2);
                    ExecutorService coordinatorThread = Executors.newSingleThreadExecutor();
                    AtomicInteger missingAtCompletion = new AtomicInteger();
                    AtomicInteger missingOnCoordinatorThread = new AtomicInteger();
                    try {
                        for (int i = 0; i < iterations; i++) {
                            Context context = new Context(key -> new ActionState(key, 2));
                            CountDownLatch observed = new CountDownLatch(1);
                            context.pendingCheckpoint
                                    .getCompletableFuture()
                                    .thenAccept(
                                            checkpoint -> {
                                                if (missingSubtaskStates(checkpoint, context.key)
                                                        > 0) {
                                                    missingAtCompletion.incrementAndGet();
                                                }
                                            });
                            // CheckpointCoordinator persists the completed checkpoint from its
                            // own executor.
                            context.pendingCheckpoint
                                    .getCompletableFuture()
                                    .whenCompleteAsync(
                                            (checkpoint, error) -> {
                                                if (checkpoint != null
                                                        && missingSubtaskStates(
                                                                        checkpoint, context.key)
                                                                > 0) {
                                                    missingOnCoordinatorThread.incrementAndGet();
                                                }
                                                observed.countDown();
                                            },
                                            coordinatorThread);
                            CyclicBarrier start = new CyclicBarrier(2);
                            Future<?> ack0 =
                                    ackThreads.submit(
                                            () -> {
                                                start.await();
                                                context.ack(context.subtask0);
                                                return null;
                                            });
                            Future<?> ack1 =
                                    ackThreads.submit(
                                            () -> {
                                                start.await();
                                                context.ack(context.subtask1);
                                                return null;
                                            });
                            ack0.get(10, TimeUnit.SECONDS);
                            ack1.get(10, TimeUnit.SECONDS);
                            Assertions.assertNotNull(
                                    context.pendingCheckpoint
                                            .getCompletableFuture()
                                            .get(10, TimeUnit.SECONDS));
                            Assertions.assertTrue(observed.await(10, TimeUnit.SECONDS));
                        }
                    } finally {
                        ackThreads.shutdownNow();
                        coordinatorThread.shutdownNow();
                    }
                    Assertions.assertEquals(
                            0,
                            missingAtCompletion.get(),
                            "checkpoints completed with a missing subtask state");
                    Assertions.assertEquals(
                            0,
                            missingOnCoordinatorThread.get(),
                            "checkpoints handed to the coordinator with a missing subtask state");
                });
    }

    @Test
    void testStatisticsCountEveryAcknowledgedSubtaskAndStateByte() throws Exception {
        Context context = new Context(key -> new ActionState(key, 2));
        Assertions.assertEquals(2, context.pendingCheckpoint.getTotalSubtasks());

        context.ack(context.subtask0);
        context.ack(context.subtask1);

        CompletedCheckpoint checkpoint =
                context.pendingCheckpoint.getCompletableFuture().get(10, TimeUnit.SECONDS);
        Assertions.assertEquals(0, missingSubtaskStates(checkpoint, context.key));
        Assertions.assertEquals(2, context.pendingCheckpoint.getAcknowledgedSubtasks());
        Assertions.assertEquals(
                2L * STATE_BYTES, CheckpointMonitorService.calculateStateSize(checkpoint));
    }

    private static int missingSubtaskStates(CompletedCheckpoint checkpoint, ActionStateKey key) {
        return (int)
                checkpoint.getTaskStates().get(key).getSubtaskStates().stream()
                        .filter(Objects::isNull)
                        .count();
    }

    private static void awaitQuietly(CountDownLatch latch, long millis) {
        try {
            latch.await(millis, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * One action with parallelism 2, laid out like {@code PhysicalPlanGenerator} does: every
     * parallel subtask runs in its own task group. The statistics, action states and pending tasks
     * are built the same way {@link CheckpointCoordinator} builds them.
     */
    private static final class Context {
        private final ActionStateKey key = new ActionStateKey("ActionStateKey - sink");
        private final TaskLocation subtask0 =
                new TaskLocation(new TaskGroupLocation(1L, 1, 1L), 0, 0);
        private final TaskLocation subtask1 =
                new TaskLocation(new TaskGroupLocation(1L, 1, 2L), 0, 1);
        private final PendingCheckpoint pendingCheckpoint;

        private Context(Function<ActionStateKey, ActionState> actionStateFactory) {
            Set<TaskLocation> subtasks = new HashSet<>(Arrays.asList(subtask0, subtask1));
            Map<Long, TaskStatistics> taskStatistics =
                    CheckpointCoordinator.getPipelineTasks(subtasks).entrySet().stream()
                            .collect(
                                    Collectors.toMap(
                                            Map.Entry::getKey,
                                            entry ->
                                                    new TaskStatistics(
                                                            entry.getKey(), entry.getValue())));
            Map<ActionStateKey, ActionState> actionStates = new HashMap<>();
            actionStates.put(key, actionStateFactory.apply(key));
            Set<Long> notYetAcknowledged = new CopyOnWriteArraySet<>();
            subtasks.forEach(subtask -> notYetAcknowledged.add(subtask.getTaskID()));
            pendingCheckpoint =
                    new PendingCheckpoint(
                            1L,
                            1,
                            1L,
                            System.currentTimeMillis(),
                            CheckpointType.CHECKPOINT_TYPE,
                            notYetAcknowledged,
                            taskStatistics,
                            actionStates);
        }

        private void ack(TaskLocation subtask) {
            List<ActionSubtaskState> states =
                    Collections.singletonList(
                            new ActionSubtaskState(
                                    key,
                                    subtask.getTaskIndex(),
                                    Collections.singletonList(new byte[STATE_BYTES])));
            pendingCheckpoint.acknowledgeTask(subtask, states, SubtaskStatus.RUNNING);
        }
    }
}
