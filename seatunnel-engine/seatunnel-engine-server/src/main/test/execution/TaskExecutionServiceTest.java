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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;


public class TaskExecutionServiceTest {

    HazelcastInstanceImpl instance = ((HazelcastInstanceProxy) HazelcastInstanceFactory.newHazelcastInstance(new Config(), Thread.currentThread().getName(), new SeaTunnelNodeContext())).getOriginal();
    SeaTunnelServer service = instance.node.nodeEngine.getService(SeaTunnelServer.SERVICE_NAME);
    ILogger logger = instance.node.nodeEngine.getLogger(TaskExecutionServiceTest.class);


    @Test
    public void testAll() throws InterruptedException {
        System.out.println("----------start Cancel test----------");
        testCancel();

        System.out.println("----------start Finish test----------");
        testFinish();

        System.out.println("----------start Delay test----------");
        testDelay();
        testDelay();

        System.out.println("----------start ThrowException test----------");
        testThrowException();

        System.out.println("----------start CriticalCallTime test----------");
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
        List<Task> criticalTask = buildStopTestTask(callTime,count, stopMark, stopTime);

        TaskExecutionService taskExecutionService = service.getTaskExecutionService();

        List<TaskExecutionContext> collect = criticalTask.stream().map(taskExecutionService::submitThreadShareTask).collect(Collectors.toList());

        //stop task
        Thread.sleep(10000);
        stopMark.set(true);

        Thread.sleep(count*callTime);

        // Check all task ends right
        collect.forEach(xt -> {
            assertTrue(xt.executionFuture.isDone());
        });

        //Check that each Task is only Done once
        assertEquals(count,stopTime.size());

    }

    public void testThrowException() throws InterruptedException {
        TaskExecutionService taskExecutionService = service.getTaskExecutionService();

        AtomicBoolean stopMark = new AtomicBoolean(false);

        List<Throwable> t1throwable = new ArrayList<>();
        ExceptionTestTask t1 = new ExceptionTestTask(100, "t1", t1throwable);

        List<Throwable> t2throwable = new ArrayList<>();
        ExceptionTestTask t2 = new ExceptionTestTask(50, "t2", t2throwable);

        //Create low lat tasks
        List<Task> lowLagTask = buildFixedTestTask(50, 10, stopMark, new CopyOnWriteArrayList<>());

        //Create high lat tasks
        List<Task> highLagTask = buildFixedTestTask(1500, 5, stopMark, new CopyOnWriteArrayList<>());

        List<Task> tasks = new ArrayList<>();
        tasks.addAll(highLagTask);
        tasks.addAll(lowLagTask);
        Collections.shuffle(tasks);

        List<TaskExecutionContext> taskCts = tasks.stream().map(task -> task.getTaskID() % 2 == 0 ?
            taskExecutionService.submitBlockingTask(task) :
            taskExecutionService.submitThreadShareTask(task)).collect(Collectors.toList());

        TaskExecutionContext t1c = taskExecutionService.submitThreadShareTask(t1);

        TaskExecutionContext t2c = taskExecutionService.submitThreadShareTask(t2);

        Thread.sleep(2000);

        t1throwable.add(new IOException());
        t2throwable.add(new IOException());
        Thread.sleep(200);

        assertTrue(t1c.executionFuture.isCompletedExceptionally());
        assertTrue(t2c.executionFuture.isCompletedExceptionally());

        stopMark.set(true);

        Thread.sleep(3000);
        taskCts.forEach(c-> {
            assertTrue(c.executionFuture.isDone());
        });


    }

    public void testCancel() throws InterruptedException {
        TaskExecutionService taskExecutionService = service.getTaskExecutionService();

        AtomicBoolean stop = new AtomicBoolean(false);
        TestTask testTask = new TestTask(stop, logger);

        TaskExecutionContext taskExecutionContext = taskExecutionService.submitTask(testTask);

        Thread.sleep(10000);

        taskExecutionContext.cancel();

        Thread.sleep(1000);
        assertTrue(taskExecutionContext.executionFuture.isCompletedExceptionally());
    }

    public void testFinish() throws InterruptedException {
        TaskExecutionService taskExecutionService = service.getTaskExecutionService();

        AtomicBoolean stop = new AtomicBoolean(false);
        AtomicBoolean futureMark = new AtomicBoolean(false);
        TestTask testTasklet = new TestTask(stop, logger);

        TaskExecutionContext taskExecutionContext = taskExecutionService.submitTask(testTasklet);
        taskExecutionContext.executionFuture.whenComplete(new BiConsumer<Void, Throwable>() {
            @Override
            public void accept(Void unused, Throwable throwable) {
                futureMark.set(true);
            }
        });

        Thread.sleep(3000);

        stop.set(true);

        Thread.sleep(3000);


        assertTrue(taskExecutionContext.executionFuture.isDone());
        assertTrue(futureMark.get());
    }

    public void testDelay() throws InterruptedException {

        AtomicBoolean stopMark = new AtomicBoolean(false);

        CopyOnWriteArrayList<Long> lowLagList = new CopyOnWriteArrayList<>();
        CopyOnWriteArrayList<Long> highLagList = new CopyOnWriteArrayList<>();

        //Create low lat tasks
        List<Task> lowLagTask = buildFixedTestTask(10,10, stopMark, lowLagList);

        //Create high lat tasks
        List<Task> highLagTask = buildFixedTestTask(1500, 5, stopMark, highLagList);

        List<Task> taskTrackers = new ArrayList<>();
        taskTrackers.addAll(highLagTask);
        taskTrackers.addAll(lowLagTask);
        Collections.shuffle(taskTrackers);

        LinkedBlockingDeque<Task> taskQueue = new LinkedBlockingDeque<>(taskTrackers);


        System.out.println("task size is : " + taskTrackers.size());

        TaskExecutionService taskExecutionService = service.getTaskExecutionService();

        List<TaskExecutionContext> collect = taskQueue.stream().map(taskExecutionService::submitThreadShareTask).collect(Collectors.toList());


        //stop tasks
        Thread.sleep(60000);
        stopMark.set(true);

        Thread.sleep(1000);
        //Check all task ends right
        collect.forEach(xt -> {
            assertTrue(xt.executionFuture.isDone());
        });

        Thread.sleep(1000);

        //Computation Delay
        double lowAvg = lowLagList.stream().mapToLong(x -> x).average().getAsDouble();
        double highAvg = highLagList.stream().mapToLong(x -> x).average().getAsDouble();

        assertTrue(lowAvg < 400);

        System.out.println(lowAvg);
        System.out.println(highAvg);

    }

    public List<Task> buildFixedTestTask(long callTime, long count, AtomicBoolean stopMart, CopyOnWriteArrayList<Long> lagList) {
        List<Task> taskQueue = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            taskQueue.add(new FixedCallTimeTask(callTime, callTime + "t" + i, stopMart, lagList));
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