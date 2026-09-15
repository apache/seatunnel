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

package org.apache.seatunnel.engine.server;

import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.server.execution.CooperativeProbeTask;
import org.apache.seatunnel.engine.server.execution.CooperativeWorkerBudget;
import org.apache.seatunnel.engine.server.execution.Task;
import org.apache.seatunnel.engine.server.execution.TaskExecutionState;
import org.apache.seatunnel.engine.server.execution.TaskGroup;
import org.apache.seatunnel.engine.server.execution.TaskGroupDefaultImpl;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import lombok.NonNull;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.seatunnel.engine.server.execution.ExecutionState.FINISHED;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Covers the promotion budget of cooperative workers, configured by {@code
 * seatunnel_cooperative_worker_budget.yaml} with a node limit of 2 and a per job limit of 1.
 *
 * <p>Every cooperative task here needs much longer than the call timer allows, so without a budget
 * each one of them would promote its own worker thread. The two tests assert the two halves of the
 * contract: promotions stay within the budget, and denying a promotion never stops queued tasks
 * from starting.
 */
public class TaskExecutionServiceCooperativeBudgetTest
        extends AbstractSeaTunnelServerTest<TaskExecutionServiceCooperativeBudgetTest> {

    private static final int MAX_PROMOTED_WORKERS = 2;
    private static final int MAX_PROMOTED_WORKERS_PER_JOB = 1;
    private static final long SLOW_CALL_TIME_MILLIS = 300;
    private static final int SLOW_TASK_COUNT = 8;
    private static final int BLOCKED_TASK_COUNT = 3;

    private static String previousConfigFile;

    @BeforeAll
    public void before() {
        previousConfigFile = System.getProperty("seatunnel.config");
        System.setProperty("seatunnel.config", configFilePath());
        super.before();
    }

    @AfterAll
    public void after() {
        try {
            super.after();
        } finally {
            if (previousConfigFile == null) {
                System.clearProperty("seatunnel.config");
            } else {
                System.setProperty("seatunnel.config", previousConfigFile);
            }
        }
    }

    @Test
    public void testSlowCooperativeTasksCannotPromoteMoreWorkersThanTheBudget()
            throws InterruptedException {
        TaskExecutionService taskExecutionService = server.getTaskExecutionService();
        CooperativeWorkerBudget budget = taskExecutionService.getCooperativeWorkerBudget();
        assertEquals(MAX_PROMOTED_WORKERS, budget.getMaxPromotedWorkers());
        assertEquals(MAX_PROMOTED_WORKERS_PER_JOB, budget.getMaxPromotedWorkersPerJob());

        long testJobId = System.currentTimeMillis();
        AtomicBoolean stop = new AtomicBoolean(false);
        List<CooperativeProbeTask> probes = new ArrayList<>();
        List<Task> tasks = new ArrayList<>();
        for (int i = 0; i < SLOW_TASK_COUNT; i++) {
            CooperativeProbeTask task =
                    CooperativeProbeTask.slowTask(i + 1L, SLOW_CALL_TIME_MILLIS, stop);
            probes.add(task);
            tasks.add(task);
        }

        PassiveCompletableFuture<TaskExecutionState> future =
                deployLocalTask(taskExecutionService, taskGroup(testJobId, tasks));

        // The budget denies promotions once the job holds its single promoted worker.
        await().atMost(30, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            assertTrue(budget.getDeniedPromotions() > 0);
                            assertEquals(
                                    MAX_PROMOTED_WORKERS_PER_JOB,
                                    budget.getPromotedWorkers(testJobId));
                        });
        assertTrue(
                budget.getPromotedWorkers() <= budget.getMaxPromotedWorkers(),
                "promoted workers must never exceed the configured budget");

        // Denied tasks are not parked behind the promoted one: every single task runs, and each of
        // them is called again after its first call.
        for (CooperativeProbeTask probe : probes) {
            assertTrue(
                    probe.awaitStarted(60, TimeUnit.SECONDS),
                    "task " + probe.getTaskID() + " never started");
        }
        await().atMost(60, TimeUnit.SECONDS)
                .untilAsserted(
                        () ->
                                probes.forEach(
                                        probe ->
                                                assertTrue(
                                                        probe.getCallCount() >= 2,
                                                        "task "
                                                                + probe.getTaskID()
                                                                + " stopped making progress after "
                                                                + probe.getCallCount()
                                                                + " calls")));

        stop.set(true);
        await().atMost(60, TimeUnit.SECONDS)
                .untilAsserted(() -> assertEquals(FINISHED, future.get().getExecutionState()));

        // The promotion budget is returned once the promoted worker is done with its task.
        await().atMost(30, TimeUnit.SECONDS)
                .untilAsserted(() -> assertEquals(0, budget.getPromotedWorkers(testJobId)));
    }

    /**
     * Regression for the case where every worker is blocked inside a task call while promotions are
     * denied. The blocked calls only return once a task that is still queued behind them starts, so
     * the shared queue has to keep being served or the job cannot finish at all.
     */
    @Test
    public void testQueuedTaskStartsWhileDeniedPromotionsBlockEveryWorker()
            throws InterruptedException {
        TaskExecutionService taskExecutionService = server.getTaskExecutionService();
        CooperativeWorkerBudget budget = taskExecutionService.getCooperativeWorkerBudget();

        long testJobId = System.currentTimeMillis() + 1;
        AtomicBoolean stop = new AtomicBoolean(false);
        CountDownLatch gate = new CountDownLatch(1);
        List<Task> tasks = new ArrayList<>();
        List<CooperativeProbeTask> blockedTasks = new ArrayList<>();
        for (int i = 0; i < BLOCKED_TASK_COUNT; i++) {
            CooperativeProbeTask blocked = CooperativeProbeTask.gatedTask(i + 1L, stop, gate);
            blockedTasks.add(blocked);
            tasks.add(blocked);
        }
        // Queued last, so it only runs if the shared queue is still served while the blocked calls
        // hold their workers. Starting it is what releases those calls.
        CooperativeProbeTask queuedTask =
                CooperativeProbeTask.gateOpeningTask(
                        BLOCKED_TASK_COUNT + 1L, SLOW_CALL_TIME_MILLIS, stop, gate);
        tasks.add(queuedTask);

        PassiveCompletableFuture<TaskExecutionState> future =
                deployLocalTask(taskExecutionService, taskGroup(testJobId, tasks));

        assertTrue(
                queuedTask.awaitStarted(90, TimeUnit.SECONDS),
                "the queued task never started, so denied promotions starved the shared queue");
        for (CooperativeProbeTask blocked : blockedTasks) {
            assertTrue(blocked.isStarted(), "task " + blocked.getTaskID() + " never started");
        }
        assertTrue(
                budget.getPromotedWorkers() <= budget.getMaxPromotedWorkers(),
                "promoted workers must never exceed the configured budget");
        // One worker per blocked call, plus the workers that keep the queue served.
        assertTrue(
                taskExecutionService.getCooperativeWorkers() <= tasks.size() + 2,
                "cooperative workers must stay bounded by the blocked calls, but were "
                        + taskExecutionService.getCooperativeWorkers());

        stop.set(true);
        await().atMost(60, TimeUnit.SECONDS)
                .untilAsserted(() -> assertEquals(FINISHED, future.get().getExecutionState()));
    }

    private TaskGroupDefaultImpl taskGroup(long jobId, List<Task> tasks) {
        return new TaskGroupDefaultImpl(
                new TaskGroupLocation(jobId, 1, 1), "cooperative-budget", tasks);
    }

    private PassiveCompletableFuture<TaskExecutionState> deployLocalTask(
            TaskExecutionService taskExecutionService, @NonNull TaskGroup taskGroup) {
        ConcurrentHashMap<Long, ClassLoader> classLoaders = new ConcurrentHashMap<>();
        taskGroup
                .getTasks()
                .forEach(
                        task ->
                                classLoaders.put(
                                        task.getTaskID(),
                                        Thread.currentThread().getContextClassLoader()));
        return taskExecutionService.deployLocalTask(
                taskGroup, classLoaders, new ConcurrentHashMap<>());
    }

    private static String configFilePath() {
        String rootModuleDir = "seatunnel-engine";
        Path path = Paths.get(System.getProperty("user.dir"));
        while (!path.endsWith(Paths.get(rootModuleDir))) {
            path = path.getParent();
        }
        return path.getParent()
                + "/seatunnel-engine/seatunnel-engine-server/src/test/resources/seatunnel_cooperative_worker_budget.yaml";
    }
}
