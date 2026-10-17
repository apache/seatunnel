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

package org.apache.seatunnel.engine.server.checkpoint.scheduler;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

class SharedCheckpointSchedulerTest {

    private static final long AWAIT_SECONDS = 10L;

    private SharedCheckpointScheduler scheduler;

    @BeforeEach
    void setUp() {
        scheduler = new SharedCheckpointScheduler();
    }

    @AfterEach
    void tearDown() {
        scheduler.close();
    }

    @Test
    void testScheduledBodyRunsOnDispatchThread() throws InterruptedException {
        PipelineCheckpointScheduler lease = scheduler.lease(1L, 0);
        CountDownLatch ran = new CountDownLatch(1);
        List<String> threadNames = new ArrayList<>();

        lease.schedule(
                () -> {
                    threadNames.add(Thread.currentThread().getName());
                    ran.countDown();
                },
                0,
                TimeUnit.MILLISECONDS);

        Assertions.assertTrue(ran.await(AWAIT_SECONDS, TimeUnit.SECONDS), "body did not run");
        Assertions.assertTrue(
                threadNames.get(0).startsWith("checkpoint-dispatcher-"),
                "body must run on the dispatch pool, not the timer thread, but ran on "
                        + threadNames.get(0));
    }

    @Test
    void testCancelAllOnOneLeaseLeavesSiblingPipelineRunning() throws InterruptedException {
        PipelineCheckpointScheduler cancelled = scheduler.lease(1L, 0);
        PipelineCheckpointScheduler kept = scheduler.lease(1L, 1);

        AtomicBoolean cancelledRan = new AtomicBoolean(false);
        CountDownLatch keptRan = new CountDownLatch(1);

        cancelled.schedule(() -> cancelledRan.set(true), 500, TimeUnit.MILLISECONDS);
        kept.schedule(keptRan::countDown, 500, TimeUnit.MILLISECONDS);

        cancelled.cancelAll();

        Assertions.assertTrue(
                keptRan.await(AWAIT_SECONDS, TimeUnit.SECONDS),
                "cancelling one pipeline must not cancel another pipeline's timer");
        Assertions.assertFalse(cancelledRan.get(), "cancelled pipeline's body must not run");
        Assertions.assertEquals(0, cancelled.outstandingCount());
    }

    /**
     * A task cancelled between the timer firing and the dispatch pool picking it up must not run.
     * The timer future is already complete at that point, so only the task's own cancellation flag
     * can stop it.
     */
    @Test
    void testCancelAfterTimerFiredStillStopsTheBody() {
        AtomicBoolean ran = new AtomicBoolean(false);
        PipelineCheckpointTask task =
                new PipelineCheckpointTask(() -> ran.set(true), settled -> {});

        Assertions.assertTrue(task.cancel(false));
        task.run();

        Assertions.assertFalse(ran.get(), "a cancelled task must not run its body");
        Assertions.assertTrue(task.isCancelled());
        Assertions.assertTrue(task.isDone());
        Assertions.assertFalse(task.cancel(false), "cancel must be idempotent");
    }

    /**
     * A pending task cancelled before its timer fires must never reach the dispatch pool.
     *
     * <p>This is a negative assertion, so it has to hold for a window that outlasts the task's own
     * delay rather than being checked once. {@code during} keeps re-checking across that window, so
     * a body that runs at any point inside it fails the test immediately.
     */
    @Test
    void testCancelBeforeTimerFiresStopsTheBody() {
        PipelineCheckpointScheduler lease = scheduler.lease(1L, 0);
        AtomicBoolean ran = new AtomicBoolean(false);

        ScheduledFuture<?> future = lease.schedule(() -> ran.set(true), 500, TimeUnit.MILLISECONDS);
        Assertions.assertTrue(future.cancel(false));

        Awaitility.await()
                .during(Duration.ofMillis(800))
                .atMost(Duration.ofSeconds(AWAIT_SECONDS))
                .untilAsserted(
                        () ->
                                Assertions.assertFalse(
                                        ran.get(), "a cancelled task must not run its body"));
        Assertions.assertEquals(0, lease.outstandingCount());
    }

    @Test
    void testTimerThreadCountStaysFlatAsPipelinesGrow() throws InterruptedException {
        int pipelines = 500;
        CountDownLatch allRan = new CountDownLatch(pipelines);
        List<PipelineCheckpointScheduler> leases = new ArrayList<>(pipelines);

        for (int pipelineId = 0; pipelineId < pipelines; pipelineId++) {
            PipelineCheckpointScheduler lease = scheduler.lease(1L, pipelineId);
            leases.add(lease);
            lease.schedule(allRan::countDown, 0, TimeUnit.MILLISECONDS);
        }

        Assertions.assertTrue(allRan.await(AWAIT_SECONDS, TimeUnit.SECONDS));
        Assertions.assertEquals(pipelines, leases.size());
        Assertions.assertTrue(
                scheduler.getTimerPoolSize() <= 2,
                "timer threads must not grow with pipeline count, but pool size was "
                        + scheduler.getTimerPoolSize());
    }

    @Test
    void testLeaseIsReusableAfterCancelAll() throws InterruptedException {
        PipelineCheckpointScheduler lease = scheduler.lease(1L, 0);
        lease.schedule(() -> {}, 1, TimeUnit.HOURS);

        lease.cancelAll();
        lease.cancelAll();

        CountDownLatch ranAfterReset = new CountDownLatch(1);
        lease.schedule(ranAfterReset::countDown, 0, TimeUnit.MILLISECONDS);

        Assertions.assertTrue(
                ranAfterReset.await(AWAIT_SECONDS, TimeUnit.SECONDS),
                "a lease must stay usable after a reset cancels its tasks");
    }

    @Test
    void testCompletedTasksDoNotAccumulate() throws InterruptedException {
        PipelineCheckpointScheduler lease = scheduler.lease(1L, 0);
        int rounds = 50;
        CountDownLatch allRan = new CountDownLatch(rounds);

        for (int i = 0; i < rounds; i++) {
            lease.schedule(allRan::countDown, 0, TimeUnit.MILLISECONDS);
        }

        Assertions.assertTrue(allRan.await(AWAIT_SECONDS, TimeUnit.SECONDS));
        // A body's side effect becomes visible before the task reaches the finally block that
        // unregisters it, so the last countDown() does not mean the last task has settled yet.
        // Poll instead of asserting once, or the main thread can observe a task that is finished
        // but not yet removed.
        Awaitility.await()
                .atMost(Duration.ofSeconds(AWAIT_SECONDS))
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        0,
                                        lease.outstandingCount(),
                                        "finished tasks must remove themselves"));
    }

    @Test
    void testScheduleAfterCloseReturnsCancelledFuture() {
        PipelineCheckpointScheduler lease = scheduler.lease(1L, 0);
        scheduler.close();

        AtomicInteger runs = new AtomicInteger();
        ScheduledFuture<?> future = lease.schedule(runs::incrementAndGet, 0, TimeUnit.MILLISECONDS);

        Assertions.assertTrue(future.isCancelled());
        Assertions.assertTrue(lease.isShutdown());
        Assertions.assertEquals(0, runs.get());
    }

    /** More blocked bodies than the bound allows must not create more threads than the bound. */
    @Test
    void testDispatchPoolRespectsItsBound() throws InterruptedException {
        int maxThreads = SharedCheckpointScheduler.getMaxDispatchThreadNum();
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch blockedBodiesStarted = new CountDownLatch(maxThreads);

        try {
            for (int pipelineId = 0; pipelineId < maxThreads * 2; pipelineId++) {
                scheduler
                        .lease(1L, pipelineId)
                        .schedule(
                                blockUntilReleased(blockedBodiesStarted, release),
                                0,
                                TimeUnit.MILLISECONDS);
            }

            Assertions.assertTrue(
                    blockedBodiesStarted.await(AWAIT_SECONDS, TimeUnit.SECONDS),
                    "the dispatch pool should grow to its bound under load");
            Assertions.assertTrue(
                    scheduler.getDispatcherPoolSize() <= maxThreads,
                    "dispatch threads must not exceed the bound, but pool size was "
                            + scheduler.getDispatcherPoolSize());
        } finally {
            release.countDown();
        }
    }

    /**
     * The point of splitting timing from execution: a pipeline whose checkpoint body is stuck must
     * not hold up another pipeline's trigger.
     */
    @Test
    void testBlockedPipelineDoesNotDelayOtherPipelines() throws InterruptedException {
        int blockedPipelines = SharedCheckpointScheduler.getMaxDispatchThreadNum() - 1;
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch blockedBodiesStarted = new CountDownLatch(blockedPipelines);

        try {
            for (int pipelineId = 0; pipelineId < blockedPipelines; pipelineId++) {
                scheduler
                        .lease(1L, pipelineId)
                        .schedule(
                                blockUntilReleased(blockedBodiesStarted, release),
                                0,
                                TimeUnit.MILLISECONDS);
            }
            Assertions.assertTrue(blockedBodiesStarted.await(AWAIT_SECONDS, TimeUnit.SECONDS));

            CountDownLatch healthyPipelineRan = new CountDownLatch(1);
            scheduler
                    .lease(2L, 0)
                    .schedule(healthyPipelineRan::countDown, 0, TimeUnit.MILLISECONDS);

            Assertions.assertTrue(
                    healthyPipelineRan.await(AWAIT_SECONDS, TimeUnit.SECONDS),
                    "a pipeline blocked in its checkpoint body must not delay another pipeline");
        } finally {
            release.countDown();
        }
    }

    private static Runnable blockUntilReleased(CountDownLatch started, CountDownLatch release) {
        return () -> {
            started.countDown();
            try {
                release.await();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        };
    }
}
