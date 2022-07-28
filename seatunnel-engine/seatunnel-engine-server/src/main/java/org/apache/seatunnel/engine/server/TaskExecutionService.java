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
import static java.util.concurrent.Executors.newCachedThreadPool;

import org.apache.seatunnel.engine.server.execution.ProgressState;
import org.apache.seatunnel.engine.server.execution.Task;
import org.apache.seatunnel.engine.server.execution.TaskExecutionContext;
import org.apache.seatunnel.engine.server.execution.TaskGroup;

import com.hazelcast.jet.impl.util.NonCompletableFuture;
import com.hazelcast.logging.ILogger;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.properties.HazelcastProperties;
import lombok.NonNull;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * This class is responsible for the execution of the Task
 */
public class TaskExecutionService {

    private final String hzInstanceName;
    private final NodeEngine nodeEngine;
    private final ILogger logger;
    private volatile boolean isShutdown;
    private final ExecutorService blockingTaskletExecutor = newCachedThreadPool(new BlockingTaskThreadFactory());
    // key: TaskID
    private final ConcurrentMap<Long, TaskExecutionContext> executionContexts = new ConcurrentHashMap<>();

    public TaskExecutionService(NodeEngineImpl nodeEngine, HazelcastProperties properties) {
        this.hzInstanceName = nodeEngine.getHazelcastInstance().getName();
        this.nodeEngine = nodeEngine;
        this.logger = nodeEngine.getLoggingService().getLogger(TaskExecutionService.class);
    }

    public void shutdown() {
        isShutdown = true;
        blockingTaskletExecutor.shutdownNow();
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
        CompletableFuture<Void> cancellationFuture = new CompletableFuture<Void>();
        TaskletTracker taskletTracker = new TaskletTracker(task, cancellationFuture);
        taskletTracker.taskletFutures =
            blockingTaskletExecutor.submit(new BlockingWorker(taskletTracker));

        TaskExecutionContext taskExecutionContext = new TaskExecutionContext(
            taskletTracker.future,
            cancellationFuture,
            this
        );

        executionContexts.put(task.getTaskID(), taskExecutionContext);
        return taskExecutionContext;

    }

    private final class TaskletTracker {
        final NonCompletableFuture future = new NonCompletableFuture();
        final Task task;
        volatile Future<?> taskletFutures;

        TaskletTracker(Task task, CompletableFuture<Void> cancellationFuture) {
            this.task = task;

            cancellationFuture.whenComplete(withTryCatch(logger, (r, e) -> {
                if (e == null) {
                    e = new IllegalStateException("cancellationFuture should be completed exceptionally");
                }
                future.internalCompleteExceptionally(e);
                taskletFutures.cancel(true);
            }));
        }

        @Override
        public String toString() {
            return "Tracking " + task;
        }
    }

    private final class BlockingWorker implements Runnable {

        private final TaskletTracker tracker;

        private BlockingWorker(TaskletTracker tracker) {
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
                } while (!result.isDone() && !isShutdown && !tracker.taskletFutures.isCancelled());

            } catch (Throwable e) {
                logger.warning("Exception in " + t, e);
                tracker.future.internalCompleteExceptionally(e);
            } finally {
                tracker.future.internalComplete();
            }
        }
    }

    private final class BlockingTaskThreadFactory implements ThreadFactory {
        private final AtomicInteger seq = new AtomicInteger();

        @Override
        public Thread newThread(@NonNull Runnable r) {
            return new Thread(r,
                String.format("hz.%s.seaTunnel.blocking.thread-%d", hzInstanceName, seq.getAndIncrement()));
        }
    }

}
