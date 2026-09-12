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
import org.apache.seatunnel.engine.server.execution.CooperativeWorkerBudget;
import org.apache.seatunnel.engine.server.execution.FixedCallTestTimeTask;
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
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.seatunnel.engine.server.execution.ExecutionState.FINISHED;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Covers the promotion budget of cooperative workers, configured by {@code
 * seatunnel_cooperative_worker_budget.yaml} with a global limit of 2 and a per job limit of 1.
 *
 * <p>Every cooperative task here needs much longer than the call timer allows, so without a budget
 * each one of them would promote its own worker thread.
 */
public class TaskExecutionServiceCooperativeBudgetTest
        extends AbstractSeaTunnelServerTest<TaskExecutionServiceCooperativeBudgetTest> {

    private static final int MAX_PROMOTED_WORKERS_PER_JOB = 1;
    private static final long SLOW_CALL_TIME_MILLIS = 300;
    private static final int SLOW_TASK_COUNT = 8;

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
        assertEquals(2, budget.getMaxPromotedWorkers());
        assertEquals(MAX_PROMOTED_WORKERS_PER_JOB, budget.getMaxPromotedWorkersPerJob());

        long testJobId = System.currentTimeMillis();
        AtomicBoolean stop = new AtomicBoolean(false);
        CopyOnWriteArrayList<Long> lagList = new CopyOnWriteArrayList<>();
        List<Task> tasks = new ArrayList<>();
        for (int i = 0; i < SLOW_TASK_COUNT; i++) {
            tasks.add(
                    new FixedCallTestTimeTask(
                            SLOW_CALL_TIME_MILLIS, "slow-task-" + i, stop, lagList));
        }

        TaskGroupDefaultImpl taskGroup =
                new TaskGroupDefaultImpl(
                        new TaskGroupLocation(testJobId, 1, 1), "cooperative-budget", tasks);

        PassiveCompletableFuture<TaskExecutionState> future =
                deployLocalTask(taskExecutionService, taskGroup);

        // The budget must deny promotions once the job holds its single promoted worker, and the
        // tasks that were denied must keep running instead of being dropped.
        await().atMost(30, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            assertTrue(budget.getDeniedPromotions() > 0);
                            assertEquals(
                                    MAX_PROMOTED_WORKERS_PER_JOB,
                                    budget.getPromotedWorkers(testJobId));
                        });

        // Every task keeps making progress while the budget is exhausted, so the shared queue is
        // still served: each task must be called more than once.
        await().atMost(30, TimeUnit.SECONDS)
                .untilAsserted(() -> assertTrue(lagList.size() >= SLOW_TASK_COUNT));

        assertTrue(
                budget.getPromotedWorkers() <= budget.getMaxPromotedWorkers(),
                "promoted workers must never exceed the configured budget");
        // Denied promotions add a worker only while a single worker is left on the shared queue,
        // so the cooperative thread count stays bounded instead of growing per slow task.
        assertTrue(
                taskExecutionService.getSharedCooperativeWorkers() <= 3,
                "workers serving the shared queue must stay bounded, but were "
                        + taskExecutionService.getSharedCooperativeWorkers());

        stop.set(true);
        await().atMost(60, TimeUnit.SECONDS)
                .untilAsserted(() -> assertEquals(FINISHED, future.get().getExecutionState()));

        // The promotion budget is returned once the promoted worker is done with its task.
        await().atMost(30, TimeUnit.SECONDS)
                .untilAsserted(() -> assertEquals(0, budget.getPromotedWorkers(testJobId)));
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
