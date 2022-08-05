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

package execution;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import org.apache.seatunnel.engine.server.SeaTunnelNodeContext;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.execution.Task;
import org.apache.seatunnel.engine.server.execution.TaskExecutionContext;
import org.apache.seatunnel.engine.server.TaskExecutionService;

import com.hazelcast.config.Config;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.instance.impl.HazelcastInstanceProxy;
import com.hazelcast.logging.ILogger;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;


public class TaskExecutionServiceTest {

    HazelcastInstanceImpl instance = ((HazelcastInstanceProxy) HazelcastInstanceFactory.newHazelcastInstance(new Config(), Thread.currentThread().getName(), new SeaTunnelNodeContext())).getOriginal();
    SeaTunnelServer service = instance.node.nodeEngine.getService(SeaTunnelServer.SERVICE_NAME);
    ILogger logger = instance.node.nodeEngine.getLogger(TaskExecutionServiceTest.class);

    long taskRunTime = 30000;

    @Test
    public void testAll() throws InterruptedException {
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

        List<TaskExecutionContext> collect = criticalTask.stream().map(taskExecutionService::submitThreadShareTask).collect(Collectors.toList());

        // Run it for a while
        Thread.sleep(taskRunTime);

        //stop task
        stopMark.set(true);

        Thread.sleep(count * callTime);

        // Check all task ends right
        collect.forEach(xt -> {
            assertTrue(xt.executionFuture.isDone());
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
        long highLagSleep = 1500;

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

        List<TaskExecutionContext> taskCts = tasks.stream().map(task -> task.getTaskID() % 2 == 0 ?
            taskExecutionService.submitBlockingTask(task) :
            taskExecutionService.submitThreadShareTask(task)).collect(Collectors.toList());

        TaskExecutionContext t1c = taskExecutionService.submitThreadShareTask(t1);

        TaskExecutionContext t2c = taskExecutionService.submitThreadShareTask(t2);

        Thread.sleep(taskRunTime);

        t1throwable.add(new IOException());
        t2throwable.add(new IOException());
        Thread.sleep(t1Sleep + t2Sleep);

        assertTrue(t1c.executionFuture.isCompletedExceptionally());
        assertTrue(t2c.executionFuture.isCompletedExceptionally());

        stopMark.set(true);
        Thread.sleep(lowLagSleep * 10 + highLagSleep);
        taskCts.forEach(c -> {
            assertTrue(c.executionFuture.isDone());
        });


    }

    public void testCancel() throws InterruptedException {
        TaskExecutionService taskExecutionService = service.getTaskExecutionService();

        long sleepTime = 2000;

        AtomicBoolean stop = new AtomicBoolean(false);
        TestTask testTask = new TestTask(stop, logger, sleepTime);

        TaskExecutionContext taskExecutionContext = taskExecutionService.submitTask(testTask);

        Thread.sleep(taskRunTime);

        taskExecutionContext.cancel();

        Thread.sleep(sleepTime + 100);
        assertTrue(taskExecutionContext.executionFuture.isCompletedExceptionally());
    }

    public void testFinish() throws InterruptedException {
        TaskExecutionService taskExecutionService = service.getTaskExecutionService();

        long sleepTime = 2000;

        AtomicBoolean stop = new AtomicBoolean(false);
        AtomicBoolean futureMark = new AtomicBoolean(false);
        TestTask testTasklet = new TestTask(stop, logger, sleepTime);

        TaskExecutionContext taskExecutionContext = taskExecutionService.submitTask(testTasklet);
        taskExecutionContext.executionFuture.whenComplete(new BiConsumer<Void, Throwable>() {
            @Override
            public void accept(Void unused, Throwable throwable) {
                futureMark.set(true);
            }
        });

        Thread.sleep(taskRunTime);

        stop.set(true);

        Thread.sleep(sleepTime + 100);


        assertTrue(taskExecutionContext.executionFuture.isDone());
        assertTrue(futureMark.get());
    }

    public void testDelay() throws InterruptedException {

        long lowLagSleep = 10;
        long highLagSleep = 1500;

        AtomicBoolean stopMark = new AtomicBoolean(false);

        CopyOnWriteArrayList<Long> lowLagList = new CopyOnWriteArrayList<>();
        CopyOnWriteArrayList<Long> highLagList = new CopyOnWriteArrayList<>();

        //Create low lat tasks
        List<Task> lowLagTask = buildFixedTestTask(lowLagSleep, 10, stopMark, lowLagList);

        //Create high lat tasks
        List<Task> highLagTask = buildFixedTestTask(highLagSleep, 5, stopMark, highLagList);

        List<Task> taskTrackers = new ArrayList<>();
        taskTrackers.addAll(highLagTask);
        taskTrackers.addAll(lowLagTask);
        Collections.shuffle(taskTrackers);

        LinkedBlockingDeque<Task> taskQueue = new LinkedBlockingDeque<>(taskTrackers);


        logger.info("task size is : " + taskTrackers.size());

        TaskExecutionService taskExecutionService = service.getTaskExecutionService();

        List<TaskExecutionContext> collect = taskQueue.stream().map(taskExecutionService::submitThreadShareTask).collect(Collectors.toList());


        //stop tasks
        Thread.sleep(taskRunTime);
        stopMark.set(true);

        Thread.sleep(lowLagSleep*10 + highLagSleep);
        //Check all task ends right
        collect.forEach(xt -> {
            assertTrue(xt.executionFuture.isDone());
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