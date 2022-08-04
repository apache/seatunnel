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

import static java.util.concurrent.Executors.newCachedThreadPool;

import org.apache.seatunnel.engine.server.execution.ProgressState;
import org.apache.seatunnel.engine.server.execution.Task;
import org.apache.seatunnel.engine.server.execution.TaskCallTimer;
import org.apache.seatunnel.engine.server.execution.TaskExecutionContext;
import org.apache.seatunnel.engine.server.execution.TaskGroup;
import org.apache.seatunnel.engine.server.execution.TaskTracker;

import com.hazelcast.jet.impl.util.NonCompletableFuture;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.properties.HazelcastProperties;
import lombok.NonNull;
import lombok.SneakyThrows;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * This class is responsible for the execution of the Task
 */
public class TaskExecutionService {

    private final String hzInstanceName;
    private final NodeEngine nodeEngine;
    private final ILogger logger;
    private volatile boolean isShutdown;
    private final LinkedBlockingDeque<TaskTracker> threadShareTaskQueue = new LinkedBlockingDeque<>();
    private final ExecutorService executorService = newCachedThreadPool(new BlockingTaskThreadFactory());
    private final RunBusWorkSupplier runBusWorkSupplier = new RunBusWorkSupplier(executorService, threadShareTaskQueue);
    // key: TaskID
    private final ConcurrentMap<Long, TaskExecutionContext> executionContexts = new ConcurrentHashMap<>();

    public TaskExecutionService(NodeEngineImpl nodeEngine, HazelcastProperties properties) {
        this.hzInstanceName = nodeEngine.getHazelcastInstance().getName();
        this.nodeEngine = nodeEngine;
        this.logger = nodeEngine.getLoggingService().getLogger(TaskExecutionService.class);
    }

    public void start() {
        runBusWorkSupplier.runNewBusWork(false);
    }

    public void shutdown() {
        isShutdown = true;
        executorService.shutdownNow();
    }

    public TaskExecutionContext getExecutionContext(long taskId) {
        return executionContexts.get(taskId);
    }

    /**
     * Submit a TaskGroup and run the Task in it
     */
    public Map<Long, TaskExecutionContext> submitTask(
        TaskGroup taskGroup
    ) {
        Map<Long, TaskExecutionContext> contextMap = new HashMap<>(taskGroup.getTasks().size());
        taskGroup.getTasks().forEach(task -> {
            contextMap.put(task.getTaskID(), submitTask(task));
        });
        return contextMap;
    }

    public TaskExecutionContext submitTask(Task task) {
        return task.isThreadsShare() ? submitThreadShareTask(task) : submitBlockingTask(task);
    }

    /**
     * Submit a Task that exclusively use the thread
     */
    public TaskExecutionContext submitBlockingTask(Task task) {
        CompletableFuture<Void> cancellationFuture = new CompletableFuture<Void>();
        TaskTracker taskTracker = new TaskTracker(task, cancellationFuture, logger);
        taskTracker.taskRuntimeFutures =
            executorService.submit(new BlockingWorker(taskTracker));

        TaskExecutionContext taskExecutionContext = new TaskExecutionContext(
            taskTracker.taskFuture,
            cancellationFuture
        );

        executionContexts.put(task.getTaskID(), taskExecutionContext);
        return taskExecutionContext;
    }

    /**
     * Submit a Task that can share threads
     */
    public TaskExecutionContext submitThreadShareTask(Task task) {

        CompletableFuture<Void> cancellationFuture = new CompletableFuture<Void>();

        TaskTracker taskTracker = new TaskTracker(task, cancellationFuture, logger);
        taskTracker.taskRuntimeFutures = new CompletableFuture<Void>();

        TaskExecutionContext taskExecutionContext = new TaskExecutionContext(
            taskTracker.taskFuture,
            cancellationFuture
        );

        threadShareTaskQueue.add(taskTracker);

        executionContexts.put(task.getTaskID(), taskExecutionContext);
        return taskExecutionContext;
    }

    private final class BlockingWorker implements Runnable {

        private final TaskTracker tracker;

        private BlockingWorker(TaskTracker tracker) {
            this.tracker = tracker;
        }

        @Override
        public void run() {
            final Task t = tracker.task;
            try {
                t.init();
                ProgressState result;
                do {
                    result = t.call();
                } while (!result.isDone() && !isShutdown && !tracker.taskRuntimeFutures.isCancelled());

            } catch (Throwable e) {
                logger.warning("Exception in " + t, e);
                tracker.taskFuture.internalCompleteExceptionally(e);
            } finally {
                tracker.taskFuture.internalComplete();
            }
        }
    }

    private final class BlockingTaskThreadFactory implements ThreadFactory {
        private final AtomicInteger seq = new AtomicInteger();

        @Override
        public Thread newThread(@NonNull Runnable r) {
            return new Thread(r,
                String.format("hz.%s.seaTunnel.task.thread-%d", hzInstanceName, seq.getAndIncrement()));
        }
    }

    /**
     * BusWork is used to poll the task call method,
     * When a task times out, a new BusWork will be created to take over the execution of the task
     */

    public final class BusWork implements Runnable {

        AtomicBoolean keep = new AtomicBoolean(true);
        public AtomicReference<TaskTracker> exclusiveTaskTracker = new AtomicReference<>();
        final TaskCallTimer timer;
        public LinkedBlockingDeque<TaskTracker> taskqueue;

        @SuppressWarnings("checkstyle:MagicNumber")
        public BusWork(LinkedBlockingDeque<TaskTracker> taskqueue, RunBusWorkSupplier runBusWorkSupplier) {
            logger.info(String.format("Created new BusWork : %s", this.hashCode()));
            this.taskqueue = taskqueue;
            this.timer = new TaskCallTimer(50, keep, runBusWorkSupplier, this);
        }

        @SneakyThrows
        @Override
        public void run() {
            while (keep.get()) {
                TaskTracker taskTracker = null != exclusiveTaskTracker.get() ?
                    exclusiveTaskTracker.get() :
                    taskqueue.takeFirst();
                NonCompletableFuture future = taskTracker.taskFuture;
                if (taskTracker.taskRuntimeFutures.isCancelled()) {
                    if (null != exclusiveTaskTracker.get()) {
                        // If it's exclusive need to end the work
                        break;
                    } else {
                        // No action required and don't put back
                        continue;
                    }
                }
                //start timer, if it's exclusive, don't need to start
                if (null == exclusiveTaskTracker.get()) {
                    timer.timerStart(taskTracker);
                }
                ProgressState call = null;
                try {
                    //run task
                    call = taskTracker.task.call();
                    synchronized (timer) {
                        timer.timerStop();
                    }
                } catch (Throwable e) {
                    //task Failure and complete
                    future.internalCompleteExceptionally(e);
                    //If it's exclusive need to end the work
                    logger.warning("Exception in " + taskTracker.task, e);
                    if (null != exclusiveTaskTracker.get()) {
                        break;
                    }
                } finally {
                    //stop timer
                    timer.timerStop();
                }
                //task call finished
                if (null != call) {
                    if (call.isDone()) {
                        //If it's exclusive, you need to end the work
                        future.internalComplete();
                        if (null != exclusiveTaskTracker.get()) {
                            break;
                        }
                    } else {
                        //Task is not completed. Put task to the end of the queue
                        //If the current work has an exclusive tracker, it will not be put back
                        if (null == exclusiveTaskTracker.get()) {
                            taskqueue.offer(taskTracker);
                        }
                    }
                }
            }
        }
    }

    /**
     * Used to create a new BusWork and run
     */
    public final class RunBusWorkSupplier {

        ExecutorService executorService;
        LinkedBlockingDeque<TaskTracker> taskQueue;

        public RunBusWorkSupplier(ExecutorService executorService, LinkedBlockingDeque<TaskTracker> taskqueue) {
            this.executorService = executorService;
            this.taskQueue = taskqueue;
        }

        public boolean runNewBusWork(boolean checkTaskQueue) {
            if (!checkTaskQueue || taskQueue.size() > 0) {
                executorService.submit(new BusWork(taskQueue, this));
                return true;
            }
            return false;
        }
    }

    /**
     * The action to be performed when the task call method execution times out
     */
    private final class TimeoutAction implements Runnable {
        AtomicBoolean keep;
        RunBusWorkSupplier runBusWorkSupplier;

        public TimeoutAction(AtomicBoolean keep, RunBusWorkSupplier runBusWorkSupplier) {
            this.keep = keep;
            this.runBusWorkSupplier = runBusWorkSupplier;
        }

        @Override
        public void run() {
            // 1 Stop the current busWork from continuing to execute the new Task
            keep.set(false);
            // 2 Submit a new BusWork to execute other tasks
            runBusWorkSupplier.runNewBusWork(false);
        }
    }

}
