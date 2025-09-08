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

import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.server.execution.BlockTask;
import org.apache.seatunnel.engine.server.execution.ExceptionTestTask;
import org.apache.seatunnel.engine.server.execution.FixedCallTestTimeTask;
import org.apache.seatunnel.engine.server.execution.StopTimeTestTask;
import org.apache.seatunnel.engine.server.execution.Task;
import org.apache.seatunnel.engine.server.execution.TaskDeployState;
import org.apache.seatunnel.engine.server.execution.TaskExecutionState;
import org.apache.seatunnel.engine.server.execution.TaskGroup;
import org.apache.seatunnel.engine.server.execution.TaskGroupContext;
import org.apache.seatunnel.engine.server.execution.TaskGroupDefaultImpl;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.execution.TaskGroupType;
import org.apache.seatunnel.engine.server.execution.TestTask;
import org.apache.seatunnel.engine.server.task.TaskGroupImmutableInformation;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;

import com.hazelcast.flakeidgen.FlakeIdGenerator;
import com.hazelcast.internal.serialization.Data;
import lombok.NonNull;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static java.util.Collections.emptySet;
import static org.apache.seatunnel.engine.server.execution.ExecutionState.CANCELED;
import static org.apache.seatunnel.engine.server.execution.ExecutionState.FAILED;
import static org.apache.seatunnel.engine.server.execution.ExecutionState.FINISHED;
import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TaskExecutionServiceTest extends AbstractSeaTunnelServerTest {

    static FlakeIdGenerator FLAKE_ID_GENERATOR;
    long taskRunTime = 2000;
    long jobId = 10001;
    int pipeLineId = 100001;

    @BeforeAll
    public void before() {
        super.before();
        FLAKE_ID_GENERATOR = instance.getFlakeIdGenerator("test");
    }

    private PassiveCompletableFuture<TaskExecutionState> deployLocalTask(
            TaskExecutionService taskExecutionService, @NonNull TaskGroup taskGroup) {
        Long taskId = taskGroup.getTasks().iterator().next().getTaskID();
        ConcurrentHashMap<Long, ClassLoader> classLoaders = new ConcurrentHashMap<>();
        classLoaders.put(taskId, Thread.currentThread().getContextClassLoader());
        return taskExecutionService.deployLocalTask(
                taskGroup, classLoaders, new ConcurrentHashMap<>());
    }

    @Test
    public void testCancel() {
        TaskExecutionService taskExecutionService = server.getTaskExecutionService();

        long sleepTime = 300;

        AtomicBoolean stop = new AtomicBoolean(false);
        TestTask testTask1 = new TestTask(stop, sleepTime, true);
        TestTask testTask2 = new TestTask(stop, sleepTime, false);

        TaskGroupDefaultImpl ts =
                new TaskGroupDefaultImpl(
                        new TaskGroupLocation(jobId, pipeLineId, FLAKE_ID_GENERATOR.newId()),
                        "ts",
                        Lists.newArrayList(testTask1, testTask2));
        CompletableFuture<TaskExecutionState> completableFuture =
                deployLocalTask(taskExecutionService, ts);

        taskExecutionService.cancelTaskGroup(ts.getTaskGroupLocation());

        await().atMost(sleepTime + 10000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> assertEquals(CANCELED, completableFuture.get().getExecutionState()));
    }

    @Test
    public void testCancelBlockTask() throws InterruptedException {
        TaskExecutionService taskExecutionService = server.getTaskExecutionService();

        BlockTask testTask1 = new BlockTask();
        BlockTask testTask2 = new BlockTask();

        TaskGroupDefaultImpl ts =
                new TaskGroupDefaultImpl(
                        new TaskGroupLocation(jobId, pipeLineId, FLAKE_ID_GENERATOR.newId()),
                        "ts",
                        Lists.newArrayList(testTask1, testTask2));
        CompletableFuture<TaskExecutionState> completableFuture =
                deployLocalTask(taskExecutionService, ts);

        Thread.sleep(5000);

        taskExecutionService.cancelTaskGroup(ts.getTaskGroupLocation());

        await().atMost(10, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> assertEquals(CANCELED, completableFuture.get().getExecutionState()));
    }

    @Test
    public void testFinish() {
        TaskExecutionService taskExecutionService = server.getTaskExecutionService();

        long sleepTime = 300;

        AtomicBoolean stop = new AtomicBoolean(false);
        AtomicBoolean futureMark = new AtomicBoolean(false);
        TestTask testTask1 = new TestTask(stop, sleepTime, true);
        TestTask testTask2 = new TestTask(stop, sleepTime, false);

        final CompletableFuture<TaskExecutionState> completableFuture =
                deployLocalTask(
                        taskExecutionService,
                        new TaskGroupDefaultImpl(
                                new TaskGroupLocation(
                                        jobId, pipeLineId, FLAKE_ID_GENERATOR.newId()),
                                "ts",
                                Lists.newArrayList(testTask1, testTask2)));
        completableFuture.whenComplete((unused, throwable) -> futureMark.set(true));
        stop.set(true);

        await().atMost(sleepTime + 10000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            assertEquals(FINISHED, completableFuture.get().getExecutionState());
                        });
        assertTrue(futureMark.get());
    }

    @Test
    public void testClassloaderSplit() throws IOException {
        File console = File.createTempFile("console", ".jar");
        File fake = File.createTempFile("fake", ".jar");
        String consoleFile = console.toURI().toURL().toString();
        String fakeFile = fake.toURI().toURL().toString();

        TaskExecutionService taskExecutionService = server.getTaskExecutionService();

        long sleepTime = 300;

        AtomicBoolean stop = new AtomicBoolean(false);
        TestTask testTask1 = new TestTask(stop, sleepTime, true);
        TestTask testTask2 = new TestTask(stop, sleepTime, false);

        long jobId = System.currentTimeMillis();

        TaskGroupLocation location = new TaskGroupLocation(jobId, 1, 1);
        TaskGroupImmutableInformation taskGroupImmutableInformation =
                new TaskGroupImmutableInformation(
                        jobId,
                        1,
                        TaskGroupType.INTERMEDIATE_BLOCKING_QUEUE,
                        location,
                        "testClassloaderSplit",
                        Arrays.asList(
                                nodeEngine.getSerializationService().toData(testTask1),
                                nodeEngine.getSerializationService().toData(testTask2)),
                        Arrays.asList(
                                Collections.singleton(new URL(fakeFile)),
                                Collections.singleton(new URL(consoleFile))),
                        Arrays.asList(emptySet(), emptySet()));

        Data data = nodeEngine.getSerializationService().toData(taskGroupImmutableInformation);

        final TaskDeployState taskDeployState = taskExecutionService.deployTask(data);

        Assertions.assertEquals(TaskDeployState.success(), taskDeployState);

        TaskGroupContext taskGroupContext =
                taskExecutionService.getActiveExecutionContext(location);
        Assertions.assertIterableEquals(
                Collections.singleton(new URL(fakeFile)),
                taskGroupContext.getJars().get(testTask1.getTaskID()));
        Assertions.assertIterableEquals(
                Collections.singleton(new URL(consoleFile)),
                taskGroupContext.getJars().get(testTask2.getTaskID()));

        Assertions.assertIterableEquals(
                Collections.singletonList(new URL(fakeFile)),
                Arrays.asList(
                        ((URLClassLoader) taskGroupContext.getClassLoader(testTask1.getTaskID()))
                                .getURLs()));
        Assertions.assertIterableEquals(
                Collections.singletonList(new URL(consoleFile)),
                Arrays.asList(
                        ((URLClassLoader) taskGroupContext.getClassLoader(testTask2.getTaskID()))
                                .getURLs()));

        taskExecutionService.cancelTaskGroup(location);

        fake.delete();
        console.delete();
    }

    /** Test task execution time is the same as the timer timeout */
    @Test
    public void testCriticalCallTime() throws InterruptedException {
        AtomicBoolean stopMark = new AtomicBoolean(false);
        CopyOnWriteArrayList<Long> stopTime = new CopyOnWriteArrayList<>();

        int count = 100;

        // Must be the same as the timer timeout
        int callTime = 50;

        // Create tasks with critical delays
        List<Task> criticalTask = buildStopTestTask(callTime, count, stopMark, stopTime);

        TaskExecutionService taskExecutionService = server.getTaskExecutionService();

        CompletableFuture<TaskExecutionState> taskCts =
                deployLocalTask(
                        taskExecutionService,
                        new TaskGroupDefaultImpl(
                                new TaskGroupLocation(
                                        jobId, pipeLineId, FLAKE_ID_GENERATOR.newId()),
                                "t1",
                                Lists.newArrayList(criticalTask)));

        // Run it for a while
        Thread.sleep(taskRunTime);

        // stop task
        stopMark.set(true);

        // Check all task ends right
        await().atMost(count * callTime, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> assertEquals(FINISHED, taskCts.get().getExecutionState()));

        // Check that each Task is only Done once
        assertEquals(count, stopTime.size());
    }

    @Test
    public void testThrowException() throws InterruptedException {
        TaskExecutionService taskExecutionService = server.getTaskExecutionService();

        AtomicBoolean stopMark = new AtomicBoolean(false);

        long t1Sleep = 100;
        long t2Sleep = 50;

        long lowLagSleep = 50;
        long highLagSleep = 300;

        List<Throwable> t1throwable = new ArrayList<>();
        ExceptionTestTask t1 = new ExceptionTestTask(t1Sleep, "t1", t1throwable);

        List<Throwable> t2throwable = new ArrayList<>();
        ExceptionTestTask t2 = new ExceptionTestTask(t2Sleep, "t2", t2throwable);

        // Create low lat tasks
        List<Task> lowLagTask =
                buildFixedTestTask(lowLagSleep, 10, stopMark, new CopyOnWriteArrayList<>());

        // Create high lat tasks
        List<Task> highLagTask =
                buildFixedTestTask(highLagSleep, 5, stopMark, new CopyOnWriteArrayList<>());

        List<Task> tasks = new ArrayList<>();
        tasks.addAll(highLagTask);
        tasks.addAll(lowLagTask);
        Collections.shuffle(tasks);

        CompletableFuture<TaskExecutionState> taskCts =
                deployLocalTask(
                        taskExecutionService,
                        new TaskGroupDefaultImpl(
                                new TaskGroupLocation(
                                        jobId, pipeLineId, FLAKE_ID_GENERATOR.newId()),
                                "ts",
                                Lists.newArrayList(tasks)));

        CompletableFuture<TaskExecutionState> t1c =
                deployLocalTask(
                        taskExecutionService,
                        new TaskGroupDefaultImpl(
                                new TaskGroupLocation(
                                        jobId, pipeLineId, FLAKE_ID_GENERATOR.newId()),
                                "t1",
                                Lists.newArrayList(t1)));

        CompletableFuture<TaskExecutionState> t2c =
                deployLocalTask(
                        taskExecutionService,
                        new TaskGroupDefaultImpl(
                                new TaskGroupLocation(
                                        jobId, pipeLineId, FLAKE_ID_GENERATOR.newId()),
                                "t2",
                                Lists.newArrayList(t2)));

        Thread.sleep(taskRunTime);

        t1throwable.add(new IOException());
        t2throwable.add(new IOException());

        await().atMost(t1Sleep + t2Sleep + 1000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            assertEquals(FAILED, t1c.get().getExecutionState());
                            assertEquals(FAILED, t2c.get().getExecutionState());
                        });

        stopMark.set(true);

        await().atMost(lowLagSleep * 10 + highLagSleep + 1000, TimeUnit.MILLISECONDS)
                .untilAsserted(() -> assertEquals(FINISHED, taskCts.get().getExecutionState()));
    }

    @RepeatedTest(2)
    public void testDelay() throws InterruptedException {

        long lowLagSleep = 10;
        long highLagSleep = 300;

        AtomicBoolean stopMark = new AtomicBoolean(false);

        CopyOnWriteArrayList<Long> lowLagList = new CopyOnWriteArrayList<>();
        CopyOnWriteArrayList<Long> highLagList = new CopyOnWriteArrayList<>();

        // Create low lat tasks
        List<Task> lowLagTask = buildFixedTestTask(lowLagSleep, 10, stopMark, lowLagList);

        // Create high lat tasks
        List<Task> highLagTask = buildFixedTestTask(highLagSleep, 5, stopMark, highLagList);

        List<Task> tasks = new ArrayList<>();
        tasks.addAll(highLagTask);
        tasks.addAll(lowLagTask);
        Collections.shuffle(tasks);

        TaskGroupDefaultImpl taskGroup =
                new TaskGroupDefaultImpl(
                        new TaskGroupLocation(jobId, pipeLineId, FLAKE_ID_GENERATOR.newId()),
                        "ts",
                        Lists.newArrayList(tasks));

        LOGGER.info("task size is : " + taskGroup.getTasks().size());

        TaskExecutionService taskExecutionService = server.getTaskExecutionService();

        CompletableFuture<TaskExecutionState> completableFuture =
                deployLocalTask(taskExecutionService, taskGroup);

        // stop tasks
        Thread.sleep(taskRunTime);
        stopMark.set(true);

        // Check all task ends right
        await().atMost(lowLagSleep * 100 + highLagSleep * 50, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> assertEquals(FINISHED, completableFuture.get().getExecutionState()));

        // Computation Delay
        double lowAvg = lowLagList.stream().mapToLong(x -> x).average().getAsDouble();
        double highAvg = highLagList.stream().mapToLong(x -> x).average().getAsDouble();

        assertTrue(lowAvg < highLagSleep * 5);

        LOGGER.info("lowAvg : " + lowAvg);
        LOGGER.info("highAvg : " + highAvg);
    }

    public List<Task> buildFixedTestTask(
            long callTime, long count, AtomicBoolean stopMart, CopyOnWriteArrayList<Long> lagList) {
        List<Task> taskQueue = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            taskQueue.add(
                    new FixedCallTestTimeTask(callTime, callTime + "t" + i, stopMart, lagList));
        }
        return taskQueue;
    }

    public List<Task> buildStopTestTask(
            long callTime,
            long count,
            AtomicBoolean stopMart,
            CopyOnWriteArrayList<Long> stopList) {
        List<Task> taskQueue = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            taskQueue.add(new StopTimeTestTask(callTime, stopList, stopMart));
        }
        return taskQueue;
    }
}
