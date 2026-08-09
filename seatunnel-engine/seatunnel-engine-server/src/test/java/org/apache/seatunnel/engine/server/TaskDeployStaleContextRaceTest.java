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

import org.apache.seatunnel.engine.server.execution.TaskDeployState;
import org.apache.seatunnel.engine.server.execution.TaskGroupContext;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.execution.TaskGroupType;
import org.apache.seatunnel.engine.server.execution.TestTask;
import org.apache.seatunnel.engine.server.task.TaskGroupImmutableInformation;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.internal.serialization.Data;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Collections.emptySet;

/**
 * Regression test for <a href="https://github.com/apache/seatunnel/issues/11679">#11679</a>.
 *
 * <p>{@code TaskGroupLocation} is {jobId, pipelineId, taskGroupId} and is reused verbatim across
 * pipeline restore generations. {@code TaskGroupExecutionTracker.taskDone()} removes the entry for
 * that location from {@code executionContexts}, so a late {@code taskDone()} belonging to the
 * previous generation can delete the context that the current generation's {@code
 * deployLocalTask()} has just installed.
 *
 * <p>When that happens, {@code BlockingWorker.run()} resolves its class loader through {@code
 * executionContexts.get(location)} <em>before</em> its {@code try} block, and {@code
 * startedLatch.countDown()} sits <em>inside</em> it. A missing context therefore throws before the
 * latch is ever counted down, the exception is swallowed by the submitting {@code Future}, and
 * {@code submitBlockingTask()} waits on {@code startedLatch.await()} forever — while holding the
 * {@code SubPlan} monitor, which in turn blocks checkpoint-error handling from ever moving the
 * pipeline to a terminal state.
 *
 * <p>This test asserts the narrow contract that prevents the hang: <b>deploying a task group must
 * return, even if the execution context for that location disappears while the deployment is in
 * flight.</b> Whether it returns successfully or throws is not asserted — only that it does not
 * block indefinitely.
 *
 * <p>The race window is small, so the test repeats the deployment and races a remover thread
 * against it. It can therefore fail to <em>detect</em> a regression on an unlucky run, but it
 * cannot report a failure that is not real: the only way it fails is a deployment that never
 * returns.
 */
public class TaskDeployStaleContextRaceTest extends AbstractSeaTunnelServerTest {

    /** Deployment is a local, in-memory operation; anything beyond this is the hang. */
    private static final long DEPLOY_TIMEOUT_SECONDS = 30;

    private static final int ITERATIONS = 30;

    @Test
    public void deployMustNotHangWhenExecutionContextDisappearsDuringDeploy() throws Exception {
        TaskExecutionService taskExecutionService = server.getTaskExecutionService();
        ConcurrentMap<TaskGroupLocation, TaskGroupContext> executionContexts =
                executionContextsOf(taskExecutionService);

        ExecutorService deployer = Executors.newSingleThreadExecutor();
        try {
            for (int iteration = 0; iteration < ITERATIONS; iteration++) {
                long jobId = System.nanoTime();
                TaskGroupLocation location = new TaskGroupLocation(jobId, 1, 1);
                AtomicBoolean stopTask = new AtomicBoolean(false);

                // isThreadsShare() == false routes the task through submitBlockingTask(),
                // which is the path that waits on startedLatch.
                TestTask blockingTask = new TestTask(stopTask, 300, false);

                TaskGroupImmutableInformation information =
                        new TaskGroupImmutableInformation(
                                jobId,
                                1,
                                TaskGroupType.INTERMEDIATE_BLOCKING_QUEUE,
                                location,
                                "staleContextRace",
                                Collections.singletonList(
                                        nodeEngine.getSerializationService().toData(blockingTask)),
                                Collections.singletonList(emptySet()),
                                Arrays.asList(emptySet()));
                Data data = nodeEngine.getSerializationService().toData(information);

                // Stand in for the previous generation's late taskDone(): clear the entry for
                // this location while the deployment is in flight.
                AtomicBoolean stopRemover = new AtomicBoolean(false);
                Thread remover =
                        new Thread(
                                () -> {
                                    while (!stopRemover.get()) {
                                        executionContexts.remove(location);
                                    }
                                },
                                "stale-taskDone-simulator");
                remover.setDaemon(true);
                remover.start();

                Future<TaskDeployState> deployment =
                        deployer.submit(() -> taskExecutionService.deployTask(data));
                try {
                    // Returning normally and throwing both satisfy the contract. Only a
                    // deployment that never returns is the defect under test.
                    deployment.get(DEPLOY_TIMEOUT_SECONDS, TimeUnit.SECONDS);
                } catch (ExecutionException expectedWhenContextIsGone) {
                    // deployTask surfaced the missing context as an error - acceptable.
                } catch (TimeoutException e) {
                    deployment.cancel(true);
                    Assertions.fail(
                            "deployTask did not return within "
                                    + DEPLOY_TIMEOUT_SECONDS
                                    + "s on iteration "
                                    + iteration
                                    + ". A BlockingWorker that fails before"
                                    + " startedLatch.countDown() leaves submitBlockingTask"
                                    + " waiting forever while holding the SubPlan monitor."
                                    + " See https://github.com/apache/seatunnel/issues/11679");
                } finally {
                    stopRemover.set(true);
                    remover.join(TimeUnit.SECONDS.toMillis(5));
                    stopTask.set(true);
                    try {
                        taskExecutionService.cancelTaskGroup(location);
                    } catch (RuntimeException ignored) {
                        // the group may already be gone; irrelevant to this assertion
                    }
                }
            }
        } finally {
            deployer.shutdownNow();
        }
    }

    @SuppressWarnings("unchecked")
    private static ConcurrentMap<TaskGroupLocation, TaskGroupContext> executionContextsOf(
            TaskExecutionService taskExecutionService) throws Exception {
        Field field = TaskExecutionService.class.getDeclaredField("executionContexts");
        field.setAccessible(true);
        return (ConcurrentMap<TaskGroupLocation, TaskGroupContext>) field.get(taskExecutionService);
    }
}
