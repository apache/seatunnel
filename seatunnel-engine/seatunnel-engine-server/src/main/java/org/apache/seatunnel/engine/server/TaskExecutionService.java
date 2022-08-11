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

import static com.hazelcast.jet.impl.util.ExceptionUtil.withTryCatch;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static java.util.Collections.emptyList;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.stream.Collectors.partitioningBy;
import static java.util.stream.Collectors.toList;

import org.apache.seatunnel.common.utils.ExceptionUtils;
import org.apache.seatunnel.engine.common.utils.NonCompletableFuture;
import org.apache.seatunnel.engine.server.execution.ExecutionState;
import org.apache.seatunnel.engine.server.execution.ProgressState;
import org.apache.seatunnel.engine.server.execution.Task;
import org.apache.seatunnel.engine.server.execution.TaskCallTimer;
import org.apache.seatunnel.engine.server.execution.TaskExecutionContext;
import org.apache.seatunnel.engine.server.execution.TaskExecutionState;
import org.apache.seatunnel.engine.server.execution.TaskGroup;
import org.apache.seatunnel.engine.server.execution.TaskGroupContext;
import org.apache.seatunnel.engine.server.execution.TaskTracker;
import org.apache.seatunnel.engine.server.task.TaskGroupImmutableInformation;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.properties.HazelcastProperties;
import lombok.NonNull;
import lombok.SneakyThrows;

import java.net.URL;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
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
    private final NodeEngineImpl nodeEngine;
    private final ILogger logger;
    private volatile boolean isShutdown;
    private final LinkedBlockingDeque<TaskTracker> threadShareTaskQueue = new LinkedBlockingDeque<>();
    private final ExecutorService executorService = newCachedThreadPool(new BlockingTaskThreadFactory());
    private final RunBusWorkSupplier runBusWorkSupplier = new RunBusWorkSupplier(executorService, threadShareTaskQueue);
    // key: TaskID
    private final ConcurrentMap<Long, TaskGroupContext> executionContexts = new ConcurrentHashMap<>();

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

    public TaskGroupContext getExecutionContext(long taskGroupId) {
        return executionContexts.get(taskGroupId);
    }

    private void submitThreadShareTask(TaskGroupExecutionTracker taskGroupExecutionTracker, List<Task> tasks) {
        tasks.stream()
            .map(t -> new TaskTracker(t, taskGroupExecutionTracker))
            .forEach(threadShareTaskQueue::add);
    }

    private void submitBlockingTask(TaskGroupExecutionTracker taskGroupExecutionTracker, List<Task> tasks) {

        CountDownLatch startedLatch = new CountDownLatch(tasks.size());
        taskGroupExecutionTracker.blockingFutures = tasks
            .stream()
            .map(t -> new BlockingWorker(new TaskTracker(t, taskGroupExecutionTracker), startedLatch))
            .map(executorService::submit)
            .collect(toList());

        // Do not return from this method until all workers have started. Otherwise
        // on cancellation there is a race where the executor might not have started
        // the worker yet. This would result in taskletDone() never being called for
        // a worker.
        uncheckRun(startedLatch::await);
    }

    public NonCompletableFuture<TaskExecutionState> deployTask(
        @NonNull Data taskImmutableInformation
    ) {
        CompletableFuture<TaskExecutionState> resultFuture = new CompletableFuture<>();
        TaskGroup taskGroup = null;
        try {
            TaskGroupImmutableInformation taskImmutableInfo =
                nodeEngine.getSerializationService().toObject(taskImmutableInformation);
            Set<URL> jars = taskImmutableInfo.getJars();

            // TODO Use classloader load the connector jars and deserialize Task
            taskGroup = nodeEngine.getSerializationService().toData(taskImmutableInfo.getGroup());
            taskGroup.init();
            Collection<Task> tasks = taskGroup.getTasks();

            // TODO We need add a method to cancel task
            CompletableFuture<Void> cancellationFuture = new CompletableFuture<>();
            TaskGroupExecutionTracker executionTracker = new TaskGroupExecutionTracker(cancellationFuture, taskGroup, resultFuture);
            ConcurrentMap<Long, TaskExecutionContext> taskExecutionContextMap = new ConcurrentHashMap<>();
            final Map<Boolean, List<Task>> byCooperation =
                tasks.stream()
                    .peek(x -> {
                        TaskExecutionContext taskExecutionContext = new TaskExecutionContext(x, nodeEngine);
                        x.setTaskExecutionContext(taskExecutionContext);
                        taskExecutionContextMap.put(x.getTaskID(), taskExecutionContext);
                    })
                    .collect(partitioningBy(Task::isThreadsShare));
            submitThreadShareTask(executionTracker, byCooperation.get(true));
            submitBlockingTask(executionTracker, byCooperation.get(false));
            taskGroup.setTasksContext(taskExecutionContextMap);
            executionContexts.put(taskGroup.getId(), new TaskGroupContext(taskGroup));
        } catch (Throwable t) {
            logger.severe(ExceptionUtils.getMessage(t));
            resultFuture.complete(new TaskExecutionState(taskGroup.getId(), ExecutionState.FAILED, t));
        }
        return new NonCompletableFuture<>(resultFuture);
    }

    private final class BlockingWorker implements Runnable {

        private final TaskTracker tracker;
        private final CountDownLatch startedLatch;

        private BlockingWorker(TaskTracker tracker, CountDownLatch startedLatch) {
            this.tracker = tracker;
            this.startedLatch = startedLatch;
        }

        @Override
        public void run() {
            final Task t = tracker.task;
            try {
                startedLatch.countDown();
                t.init();
                ProgressState result;
                do {
                    result = t.call();
                } while (!result.isDone() && !isShutdown &&
                    !tracker.taskGroupExecutionTracker.executionCompletedExceptionally());
            } catch (Throwable e) {
                logger.warning("Exception in " + t, e);
                tracker.taskGroupExecutionTracker.exception(e);
            } finally {
                tracker.taskGroupExecutionTracker.taskDone();
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
     * CooperativeTaskWorker is used to poll the task call method,
     * When a task times out, a new BusWork will be created to take over the execution of the task
     */
    public final class CooperativeTaskWorker implements Runnable {

        AtomicBoolean keep = new AtomicBoolean(true);
        public AtomicReference<TaskTracker> exclusiveTaskTracker = new AtomicReference<>();
        final TaskCallTimer timer;
        public LinkedBlockingDeque<TaskTracker> taskqueue;

        @SuppressWarnings("checkstyle:MagicNumber")
        public CooperativeTaskWorker(LinkedBlockingDeque<TaskTracker> taskqueue,
                                     RunBusWorkSupplier runBusWorkSupplier) {
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
                TaskGroupExecutionTracker taskGroupExecutionTracker = taskTracker.taskGroupExecutionTracker;
                if (taskGroupExecutionTracker.executionCompletedExceptionally()) {
                    taskGroupExecutionTracker.taskDone();
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
                    taskGroupExecutionTracker.exception(e);
                    taskGroupExecutionTracker.taskDone();
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
                        taskGroupExecutionTracker.taskDone();
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
                executorService.submit(new CooperativeTaskWorker(taskQueue, this));
                return true;
            }
            return false;
        }
    }

    /**
     * Internal utility class to track the overall state of tasklet execution.
     * There's one instance of this class per job.
     */
    public final class TaskGroupExecutionTracker {

        private final TaskGroup taskGroup;
        final CompletableFuture<TaskExecutionState> future;
        volatile List<Future<?>> blockingFutures = emptyList();

        private final AtomicInteger completionLatch;
        private final AtomicReference<Throwable> executionException = new AtomicReference<>();

        private final AtomicBoolean isCancel = new AtomicBoolean(false);

        TaskGroupExecutionTracker(@NonNull CompletableFuture<Void> cancellationFuture, @NonNull TaskGroup taskGroup,
                                  @NonNull CompletableFuture<TaskExecutionState> future) {
            this.future = future;
            this.completionLatch = new AtomicInteger(taskGroup.getTasks().size());
            this.taskGroup = taskGroup;
            cancellationFuture.whenComplete(withTryCatch(logger, (r, e) -> {
                isCancel.set(true);
                if (e == null) {
                    e = new IllegalStateException("cancellationFuture should be completed exceptionally");
                }
                exception(e);
                // Don't interrupt the threads. We require that they do not block for too long,
                // interrupting them might make the termination faster, but can also cause troubles.
                blockingFutures.forEach(f -> f.cancel(false));
            }));
        }

        void exception(Throwable t) {
            executionException.compareAndSet(null, t);
        }

        void taskDone() {
            if (completionLatch.decrementAndGet() == 0) {
                executionContexts.remove(taskGroup.getId());
                Throwable ex = executionException.get();
                if (ex == null) {
                    future.complete(new TaskExecutionState(taskGroup.getId(), ExecutionState.FINISHED, null));
                } else if (isCancel.get()) {
                    future.complete(new TaskExecutionState(taskGroup.getId(), ExecutionState.CANCELED, ex));
                } else {
                    future.complete(new TaskExecutionState(taskGroup.getId(), ExecutionState.FAILED, ex));
                }
            }
        }

        boolean executionCompletedExceptionally() {
            return executionException.get() != null;
        }
    }

}
