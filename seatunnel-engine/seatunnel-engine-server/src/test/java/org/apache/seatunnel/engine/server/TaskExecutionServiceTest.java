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

import static org.apache.seatunnel.engine.server.execution.ExecutionState.CANCELED;
import static org.apache.seatunnel.engine.server.execution.ExecutionState.FAILED;
import static org.apache.seatunnel.engine.server.execution.ExecutionState.FINISHED;
import static org.awaitility.Awaitility.await;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.server.execution.ExceptionTestTask;
import org.apache.seatunnel.engine.server.execution.FixedCallTestTimeTask;
import org.apache.seatunnel.engine.server.execution.StopTimeTestTask;
import org.apache.seatunnel.engine.server.execution.Task;
import org.apache.seatunnel.engine.server.execution.TaskExecutionState;
import org.apache.seatunnel.engine.server.execution.TaskGroupDefaultImpl;
import org.apache.seatunnel.engine.server.execution.TestTask;

import com.google.common.collect.Lists;
import com.hazelcast.config.Config;
import com.hazelcast.flakeidgen.FlakeIdGenerator;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.instance.impl.HazelcastInstanceProxy;
import com.hazelcast.logging.ILogger;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;

public class TaskExecutionServiceTest {

    HazelcastInstanceImpl instance = ((HazelcastInstanceProxy) HazelcastInstanceFactory.newHazelcastInstance(new Config(), Thread.currentThread().getName(), new SeaTunnelNodeContext(new SeaTunnelConfig()))).getOriginal();
    SeaTunnelServer service = instance.node.nodeEngine.getService(SeaTunnelServer.SERVICE_NAME);
    ILogger logger = instance.node.nodeEngine.getLogger(TaskExecutionServiceTest.class);
    FlakeIdGenerator flakeIdGenerator = instance.getFlakeIdGenerator("test");
    long taskRunTime = 2000;

    @Test
    public void testAll() throws InterruptedException, ExecutionException {
        logger.info("----------start Cancel test----------");
        testCancel();

        logger.info("----------start Finish test----------");
        testFinish();

        logger.info("----------start Delay test----------");
        testDelay();
        testDelay();

        logger.info("----------start ThrowException test----------");
        testThrowException();

        logger.info("----------start CriticalCallTime test----------");
        testCriticalCallTime();

    }

    public void testCancel() {
        TaskExecutionService taskExecutionService = service.getTaskExecutionService();

        long sleepTime = 300;

        AtomicBoolean stop = new AtomicBoolean(false);
        TestTask testTask1 = new TestTask(stop, logger, sleepTime, true);
        TestTask testTask2 = new TestTask(stop, logger, sleepTime, false);

        long taskGroupId = flakeIdGenerator.newId();
        CompletableFuture<TaskExecutionState> completableFuture = taskExecutionService.deployLocalTask(new TaskGroupDefaultImpl(taskGroupId, "ts", Lists.newArrayList(testTask1, testTask2)), new CompletableFuture<>());

        taskExecutionService.cancelTaskGroup(taskGroupId);

        await().atMost(sleepTime + 300, TimeUnit.MILLISECONDS).untilAsserted(() -> {
            assertEquals(CANCELED, completableFuture.get().getExecutionState());
        });
    }

    public void testFinish() {
        TaskExecutionService taskExecutionService = service.getTaskExecutionService();

        long sleepTime = 300;

        AtomicBoolean stop = new AtomicBoolean(false);
        AtomicBoolean futureMark = new AtomicBoolean(false);
        TestTask testTask1 = new TestTask(stop, logger, sleepTime, true);
        TestTask testTask2 = new TestTask(stop, logger, sleepTime, false);

        CompletableFuture<TaskExecutionState> completableFuture = taskExecutionService.deployLocalTask(new TaskGroupDefaultImpl(flakeIdGenerator.newId(), "ts", Lists.newArrayList(testTask1, testTask2)), new CompletableFuture<>());
        completableFuture.whenComplete(new BiConsumer<TaskExecutionState, Throwable>() {
            @Override
            public void accept(TaskExecutionState unused, Throwable throwable) {
                futureMark.set(true);
            }
        });

        stop.set(true);

        await().atMost(sleepTime + 100, TimeUnit.MILLISECONDS).untilAsserted(() -> {
            assertEquals(FINISHED, completableFuture.get().getExecutionState());
        });

        assertTrue(futureMark.get());
    }

    /**
     * Test task execution time is the same as the timer timeout
     */
    public void testCriticalCallTime() throws InterruptedException {
        AtomicBoolean stopMark = new AtomicBoolean(false);
        CopyOnWriteArrayList<Long> stopTime = new CopyOnWriteArrayList<>();

        int count = 100;

        //Must be the same as the timer timeout
        int callTime = 50;

        //Create tasks with critical delays
        List<Task> criticalTask = buildStopTestTask(callTime, count, stopMark, stopTime);

        TaskExecutionService taskExecutionService = service.getTaskExecutionService();

        CompletableFuture<TaskExecutionState> taskCts = taskExecutionService.deployLocalTask(new TaskGroupDefaultImpl(flakeIdGenerator.newId(), "t1", Lists.newArrayList(criticalTask)), new CompletableFuture<>());

        // Run it for a while
        Thread.sleep(taskRunTime);

        //stop task
        stopMark.set(true);

        // Check all task ends right
        await().atMost(count * callTime, TimeUnit.MILLISECONDS).untilAsserted(() -> {
            assertEquals(FINISHED, taskCts.get().getExecutionState());
        });

        //Check that each Task is only Done once
        assertEquals(count, stopTime.size());

    }

    public void testThrowException() throws InterruptedException {
        TaskExecutionService taskExecutionService = service.getTaskExecutionService();

        AtomicBoolean stopMark = new AtomicBoolean(false);

        long t1Sleep = 100;
        long t2Sleep = 50;

        long lowLagSleep = 50;
        long highLagSleep = 300;

        List<Throwable> t1throwable = new ArrayList<>();
        ExceptionTestTask t1 = new ExceptionTestTask(t1Sleep, "t1", t1throwable);

        List<Throwable> t2throwable = new ArrayList<>();
        ExceptionTestTask t2 = new ExceptionTestTask(t2Sleep, "t2", t2throwable);

        //Create low lat tasks
        List<Task> lowLagTask = buildFixedTestTask(lowLagSleep, 10, stopMark, new CopyOnWriteArrayList<>());

        //Create high lat tasks
        List<Task> highLagTask = buildFixedTestTask(highLagSleep, 5, stopMark, new CopyOnWriteArrayList<>());

        List<Task> tasks = new ArrayList<>();
        tasks.addAll(highLagTask);
        tasks.addAll(lowLagTask);
        Collections.shuffle(tasks);

        CompletableFuture<TaskExecutionState> taskCts = taskExecutionService.deployLocalTask(new TaskGroupDefaultImpl(flakeIdGenerator.newId(), "ts", Lists.newArrayList(tasks)), new CompletableFuture<>());

        CompletableFuture<TaskExecutionState> t1c = taskExecutionService.deployLocalTask(new TaskGroupDefaultImpl(flakeIdGenerator.newId(), "t1", Lists.newArrayList(t1)), new CompletableFuture<>());

        CompletableFuture<TaskExecutionState> t2c = taskExecutionService.deployLocalTask(new TaskGroupDefaultImpl(flakeIdGenerator.newId(), "t2", Lists.newArrayList(t2)), new CompletableFuture<>());

        Thread.sleep(taskRunTime);

        t1throwable.add(new IOException());
        t2throwable.add(new IOException());

        await().atMost(t1Sleep + t2Sleep, TimeUnit.MILLISECONDS).untilAsserted(() -> {
            assertEquals(FAILED, t1c.get().getExecutionState());
            assertEquals(FAILED, t2c.get().getExecutionState());
        });

        stopMark.set(true);

        await().atMost(lowLagSleep * 10 + highLagSleep, TimeUnit.MILLISECONDS).untilAsserted(() -> {
            assertEquals(FINISHED, taskCts.get().getExecutionState());
        });
    }

    public void testDelay() throws InterruptedException {

        long lowLagSleep = 10;
        long highLagSleep = 300;

        AtomicBoolean stopMark = new AtomicBoolean(false);

        CopyOnWriteArrayList<Long> lowLagList = new CopyOnWriteArrayList<>();
        CopyOnWriteArrayList<Long> highLagList = new CopyOnWriteArrayList<>();

        //Create low lat tasks
        List<Task> lowLagTask = buildFixedTestTask(lowLagSleep, 10, stopMark, lowLagList);

        //Create high lat tasks
        List<Task> highLagTask = buildFixedTestTask(highLagSleep, 5, stopMark, highLagList);

        List<Task> tasks = new ArrayList<>();
        tasks.addAll(highLagTask);
        tasks.addAll(lowLagTask);
        Collections.shuffle(tasks);

        TaskGroupDefaultImpl taskGroup = new TaskGroupDefaultImpl(flakeIdGenerator.newId(), "ts", Lists.newArrayList(tasks));

        logger.info("task size is : " + taskGroup.getTasks().size());

        TaskExecutionService taskExecutionService = service.getTaskExecutionService();

        CompletableFuture<TaskExecutionState> completableFuture = taskExecutionService.deployLocalTask(taskGroup, new CompletableFuture<>());

        //stop tasks
        Thread.sleep(taskRunTime);
        stopMark.set(true);

        //Check all task ends right
        await().atMost(lowLagSleep * 10 + highLagSleep, TimeUnit.MILLISECONDS).untilAsserted(() -> {
            assertEquals(FINISHED, completableFuture.get().getExecutionState());
        });

        //Computation Delay
        double lowAvg = lowLagList.stream().mapToLong(x -> x).average().getAsDouble();
        double highAvg = highLagList.stream().mapToLong(x -> x).average().getAsDouble();

        assertTrue(lowAvg < 400);

        logger.info("lowAvg : " + lowAvg);
        logger.info("highAvg : " + highAvg);

    }

    public List<Task> buildFixedTestTask(long callTime, long count, AtomicBoolean stopMart, CopyOnWriteArrayList<Long> lagList) {
        List<Task> taskQueue = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            taskQueue.add(new FixedCallTestTimeTask(callTime, callTime + "t" + i, stopMart, lagList));
        }
        return taskQueue;
    }

    public List<Task> buildStopTestTask(long callTime, long count, AtomicBoolean stopMart, CopyOnWriteArrayList<Long> stopList) {
        List<Task> taskQueue = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            taskQueue.add(new StopTimeTestTask(callTime, stopList, stopMart));
        }
        return taskQueue;
    }

}
