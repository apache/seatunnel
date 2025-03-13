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

import org.apache.seatunnel.api.common.metrics.MetricTags;
import org.apache.seatunnel.api.event.Event;
import org.apache.seatunnel.api.tracing.MDCExecutorService;
import org.apache.seatunnel.api.tracing.MDCTracer;
import org.apache.seatunnel.common.utils.ExceptionUtils;
import org.apache.seatunnel.common.utils.StringFormatUtils;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.config.server.ThreadShareMode;
import org.apache.seatunnel.engine.common.exception.JobNotFoundException;
import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.core.classloader.ClassLoaderService;
import org.apache.seatunnel.engine.core.job.ConnectorJarIdentifier;
import org.apache.seatunnel.engine.server.exception.TaskGroupContextNotFoundException;
import org.apache.seatunnel.engine.server.execution.ExecutionState;
import org.apache.seatunnel.engine.server.execution.ProgressState;
import org.apache.seatunnel.engine.server.execution.Task;
import org.apache.seatunnel.engine.server.execution.TaskCallTimer;
import org.apache.seatunnel.engine.server.execution.TaskDeployState;
import org.apache.seatunnel.engine.server.execution.TaskExecutionContext;
import org.apache.seatunnel.engine.server.execution.TaskExecutionState;
import org.apache.seatunnel.engine.server.execution.TaskGroup;
import org.apache.seatunnel.engine.server.execution.TaskGroupContext;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.execution.TaskGroupUtils;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.execution.TaskTracker;
import org.apache.seatunnel.engine.server.metrics.SeaTunnelMetricsContext;
import org.apache.seatunnel.engine.server.service.jar.ServerConnectorPackageClient;
import org.apache.seatunnel.engine.server.task.SeaTunnelTask;
import org.apache.seatunnel.engine.server.task.TaskGroupImmutableInformation;
import org.apache.seatunnel.engine.server.task.operation.NotifyTaskStatusOperation;

import org.apache.commons.collections4.CollectionUtils;

import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.instance.impl.NodeState;
import com.hazelcast.internal.metrics.DynamicMetricsProvider;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsCollectionContext;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.jet.impl.execution.init.CustomClassLoadedObject;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.impl.InvocationFuture;
import lombok.NonNull;
import lombok.SneakyThrows;

import java.io.IOException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

import static com.hazelcast.jet.impl.util.ExceptionUtil.withTryCatch;
import static com.hazelcast.jet.impl.util.Util.uncheckRun;
import static java.lang.Thread.currentThread;
import static java.util.Collections.emptyList;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.stream.Collectors.partitioningBy;
import static java.util.stream.Collectors.toList;
import static org.apache.seatunnel.api.common.metrics.MetricTags.JOB_ID;
import static org.apache.seatunnel.api.common.metrics.MetricTags.PIPELINE_ID;
import static org.apache.seatunnel.api.common.metrics.MetricTags.TASK_GROUP_ID;
import static org.apache.seatunnel.api.common.metrics.MetricTags.TASK_GROUP_LOCATION;
import static org.apache.seatunnel.api.common.metrics.MetricTags.TASK_ID;

/** This class is responsible for the execution of the Task */
public class TaskExecutionService implements DynamicMetricsProvider {

    private final String hzInstanceName;
    private final NodeEngineImpl nodeEngine;
    private final ClassLoaderService classLoaderService;
    private final ILogger logger;
    private volatile boolean isRunning = true;
    private final LinkedBlockingDeque<TaskTracker> threadShareTaskQueue =
            new LinkedBlockingDeque<>();
    private final ExecutorService executorService =
            newCachedThreadPool(new BlockingTaskThreadFactory());
    private final RunBusWorkSupplier runBusWorkSupplier =
            new RunBusWorkSupplier(executorService, threadShareTaskQueue);
    // key: TaskID
    private final ConcurrentMap<TaskGroupLocation, TaskGroupContext> executionContexts =
            new ConcurrentHashMap<>();
    private final ConcurrentMap<TaskGroupLocation, TaskGroupContext> finishedExecutionContexts =
            new ConcurrentHashMap<>();

    private final ConcurrentMap<TaskGroupLocation, Map<String, CompletableFuture<?>>>
            taskAsyncFunctionFuture = new ConcurrentHashMap<>();

    private final ConcurrentMap<TaskGroupLocation, CompletableFuture<Void>> cancellationFutures =
            new ConcurrentHashMap<>();
    private final SeaTunnelConfig seaTunnelConfig;

    private final ScheduledExecutorService scheduledExecutorService;

    private final ServerConnectorPackageClient serverConnectorPackageClient;

    private final EventService eventService;

    public TaskExecutionService(
            ClassLoaderService classLoaderService,
            NodeEngineImpl nodeEngine,
            EventService eventService) {
        seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        this.hzInstanceName = nodeEngine.getHazelcastInstance().getName();
        this.nodeEngine = nodeEngine;
        this.classLoaderService = classLoaderService;
        this.logger = nodeEngine.getLoggingService().getLogger(TaskExecutionService.class);

        MetricsRegistry registry = nodeEngine.getMetricsRegistry();
        MetricDescriptor descriptor =
                registry.newMetricDescriptor()
                        .withTag(MetricTags.SERVICE, this.getClass().getSimpleName());
        registry.registerStaticMetrics(descriptor, this);

        scheduledExecutorService = Executors.newSingleThreadScheduledExecutor();
        scheduledExecutorService.scheduleAtFixedRate(
                this::updateMetricsContextInImap,
                0,
                seaTunnelConfig.getEngineConfig().getJobMetricsBackupInterval(),
                TimeUnit.SECONDS);

        serverConnectorPackageClient =
                new ServerConnectorPackageClient(nodeEngine, seaTunnelConfig);

        this.eventService = eventService;
    }

    public void start() {
        runBusWorkSupplier.runNewBusWork(false);
    }

    public void shutdown() {
        isRunning = false;
        executorService.shutdownNow();
        scheduledExecutorService.shutdown();
    }

    public TaskGroupContext getExecutionContext(TaskGroupLocation taskGroupLocation) {
        TaskGroupContext taskGroupContext = executionContexts.get(taskGroupLocation);

        if (taskGroupContext == null) {
            taskGroupContext = finishedExecutionContexts.get(taskGroupLocation);
        }
        if (taskGroupContext == null) {
            throw new TaskGroupContextNotFoundException(
                    String.format("task group %s not found.", taskGroupLocation));
        }
        return taskGroupContext;
    }

    public TaskGroupContext getActiveExecutionContext(TaskGroupLocation taskGroupLocation) {
        TaskGroupContext taskGroupContext = executionContexts.get(taskGroupLocation);

        if (taskGroupContext == null) {
            throw new TaskGroupContextNotFoundException(
                    String.format("task group %s not found.", taskGroupLocation));
        }
        return taskGroupContext;
    }

    private void submitThreadShareTask(
            TaskGroupExecutionTracker taskGroupExecutionTracker, List<Task> tasks) {
        Stream<TaskTracker> taskTrackerStream =
                tasks.stream()
                        .map(
                                t -> {
                                    if (!taskGroupExecutionTracker
                                            .executionCompletedExceptionally()) {
                                        try {
                                            TaskTracker taskTracker =
                                                    new TaskTracker(t, taskGroupExecutionTracker);
                                            taskTracker.task.init();
                                            return taskTracker;
                                        } catch (Exception e) {
                                            taskGroupExecutionTracker.exception(e);
                                            taskGroupExecutionTracker.taskDone(t);
                                        }
                                    }
                                    return null;
                                });
        if (!taskGroupExecutionTracker.executionCompletedExceptionally()) {
            taskTrackerStream.forEach(threadShareTaskQueue::add);
        }
    }

    private void submitBlockingTask(
            TaskGroupExecutionTracker taskGroupExecutionTracker, List<Task> tasks) {
        MDCExecutorService mdcExecutorService = MDCTracer.tracing(executorService);

        CountDownLatch startedLatch = new CountDownLatch(tasks.size());
        taskGroupExecutionTracker.blockingFutures =
                tasks.stream()
                        .map(
                                t ->
                                        new BlockingWorker(
                                                new TaskTracker(t, taskGroupExecutionTracker),
                                                startedLatch))
                        .map(
                                r ->
                                        new NamedTaskWrapper(
                                                r,
                                                "BlockingWorker-"
                                                        + taskGroupExecutionTracker.taskGroup
                                                                .getTaskGroupLocation()))
                        .map(mdcExecutorService::submit)
                        .collect(toList());

        // Do not return from this method until all workers have started. Otherwise,
        // on cancellation there is a race where the executor might not have started
        // the worker yet. This would result in taskletDone() never being called for
        // a worker.
        uncheckRun(startedLatch::await);
    }

    public TaskDeployState deployTask(@NonNull Data taskImmutableInformation) {
        TaskGroupImmutableInformation taskImmutableInfo =
                nodeEngine.getSerializationService().toObject(taskImmutableInformation);
        return deployTask(taskImmutableInfo);
    }

    public <T extends Task> T getTask(@NonNull TaskLocation taskLocation) {
        TaskGroupContext executionContext =
                this.getActiveExecutionContext(taskLocation.getTaskGroupLocation());
        return executionContext.getTaskGroup().getTask(taskLocation.getTaskID());
    }

    public TaskDeployState deployTask(@NonNull TaskGroupImmutableInformation taskImmutableInfo) {
        logger.info(
                String.format(
                        "received deploying task executionId [%s]",
                        taskImmutableInfo.getExecutionId()));
        TaskGroup taskGroup = null;
        try {
            List<Set<ConnectorJarIdentifier>> connectorJarIdentifiersList =
                    taskImmutableInfo.getConnectorJarIdentifiers();
            List<Data> taskData = taskImmutableInfo.getTasksData();
            ConcurrentHashMap<Long, ClassLoader> classLoaders = new ConcurrentHashMap<>();
            List<Task> tasks = new ArrayList<>();
            ConcurrentHashMap<Long, Collection<URL>> taskJars = new ConcurrentHashMap<>();
            for (int i = 0; i < taskData.size(); i++) {
                Set<URL> jars = new HashSet<>();
                Set<ConnectorJarIdentifier> connectorJarIdentifiers =
                        connectorJarIdentifiersList.get(i);
                if (!CollectionUtils.isEmpty(connectorJarIdentifiers)) {
                    // Prioritize obtaining the jar package file required for the current task
                    // execution
                    // from the local, if it does not exist locally, it will be downloaded from the
                    // master node.
                    jars =
                            serverConnectorPackageClient.getConnectorJarFromLocal(
                                    connectorJarIdentifiers);
                } else if (!CollectionUtils.isEmpty(taskImmutableInfo.getJars().get(i))) {
                    jars = taskImmutableInfo.getJars().get(i);
                }
                ClassLoader classLoader =
                        classLoaderService.getClassLoader(
                                taskImmutableInfo.getJobId(), Lists.newArrayList(jars));
                Task task;
                if (jars.isEmpty()) {
                    task = nodeEngine.getSerializationService().toObject(taskData.get(i));
                } else {
                    task =
                            CustomClassLoadedObject.deserializeWithCustomClassLoader(
                                    nodeEngine.getSerializationService(),
                                    classLoader,
                                    taskData.get(i));
                }
                tasks.add(task);
                classLoaders.put(task.getTaskID(), classLoader);
                taskJars.put(task.getTaskID(), jars);
            }
            taskGroup =
                    TaskGroupUtils.createTaskGroup(
                            taskImmutableInfo.getTaskGroupType(),
                            taskImmutableInfo.getTaskGroupLocation(),
                            taskImmutableInfo.getTaskGroupName(),
                            tasks);

            logger.info(
                    String.format(
                            "deploying task %s, executionId [%s]",
                            taskGroup.getTaskGroupLocation(), taskImmutableInfo.getExecutionId()));

            synchronized (this) {
                if (executionContexts.containsKey(taskGroup.getTaskGroupLocation())) {
                    throw new RuntimeException(
                            String.format(
                                    "TaskGroupLocation: %s already exists",
                                    taskGroup.getTaskGroupLocation()));
                }
                deployLocalTask(taskGroup, classLoaders, taskJars);
                return TaskDeployState.success();
            }
        } catch (Throwable t) {
            logger.severe(
                    String.format(
                            "TaskGroupID : %s  deploy error with Exception: %s",
                            taskGroup != null && taskGroup.getTaskGroupLocation() != null
                                    ? taskGroup.getTaskGroupLocation().toString()
                                    : "taskGroupLocation is null",
                            ExceptionUtils.getMessage(t)));
            return TaskDeployState.failed(t);
        }
    }

    public PassiveCompletableFuture<TaskExecutionState> deployLocalTask(
            @NonNull TaskGroup taskGroup,
            @NonNull ConcurrentHashMap<Long, ClassLoader> classLoaders,
            ConcurrentHashMap<Long, Collection<URL>> jars) {
        CompletableFuture<TaskExecutionState> resultFuture = new CompletableFuture<>();
        try {
            taskGroup.init();
            logger.info(
                    String.format(
                            "deploying TaskGroup %s init success",
                            taskGroup.getTaskGroupLocation()));
            Collection<Task> tasks = taskGroup.getTasks();
            CompletableFuture<Void> cancellationFuture = new CompletableFuture<>();
            TaskGroupExecutionTracker executionTracker =
                    new TaskGroupExecutionTracker(cancellationFuture, taskGroup, resultFuture);
            ConcurrentMap<Long, TaskExecutionContext> taskExecutionContextMap =
                    new ConcurrentHashMap<>();
            final Map<Boolean, List<Task>> byCooperation =
                    tasks.stream()
                            .peek(
                                    task -> {
                                        TaskExecutionContext taskExecutionContext =
                                                new TaskExecutionContext(task, nodeEngine, this);
                                        task.setTaskExecutionContext(taskExecutionContext);
                                        taskExecutionContextMap.put(
                                                task.getTaskID(), taskExecutionContext);
                                    })
                            .collect(
                                    partitioningBy(
                                            t -> {
                                                ThreadShareMode mode =
                                                        seaTunnelConfig
                                                                .getEngineConfig()
                                                                .getTaskExecutionThreadShareMode();
                                                if (mode.equals(ThreadShareMode.ALL)) {
                                                    return true;
                                                }
                                                if (mode.equals(ThreadShareMode.OFF)) {
                                                    return false;
                                                }
                                                if (mode.equals(ThreadShareMode.PART)) {
                                                    return t.isThreadsShare();
                                                }
                                                return true;
                                            }));
            executionContexts.put(
                    taskGroup.getTaskGroupLocation(),
                    new TaskGroupContext(taskGroup, classLoaders, jars));
            cancellationFutures.put(taskGroup.getTaskGroupLocation(), cancellationFuture);
            submitThreadShareTask(executionTracker, byCooperation.get(true));
            submitBlockingTask(executionTracker, byCooperation.get(false));
            taskGroup.setTasksContext(taskExecutionContextMap);
            logger.info(
                    String.format(
                            "deploying TaskGroup %s success", taskGroup.getTaskGroupLocation()));
        } catch (Throwable t) {
            logger.severe(ExceptionUtils.getMessage(t));
            resultFuture.completeExceptionally(t);
        }
        resultFuture.whenCompleteAsync(
                withTryCatch(
                        logger,
                        (r, s) -> {
                            if (s != null) {
                                logger.severe(
                                        String.format(
                                                "Task %s complete with error %s",
                                                taskGroup.getTaskGroupLocation(),
                                                ExceptionUtils.getMessage(s)));
                            }
                            if (r == null) {
                                r =
                                        new TaskExecutionState(
                                                taskGroup.getTaskGroupLocation(),
                                                ExecutionState.FAILED,
                                                s);
                            }
                            logger.info(
                                    String.format(
                                            "Task %s complete with state %s",
                                            r.getTaskGroupLocation(), r.getExecutionState()));
                            notifyTaskStatusToMaster(taskGroup.getTaskGroupLocation(), r);
                        }),
                MDCTracer.tracing(executorService));
        return new PassiveCompletableFuture<>(resultFuture);
    }

    private void notifyTaskStatusToMaster(
            TaskGroupLocation taskGroupLocation, TaskExecutionState taskExecutionState) {
        long sleepTime = 1000;
        boolean notifyStateSuccess = false;
        while (isRunning && !notifyStateSuccess) {
            InvocationFuture<Object> invoke =
                    nodeEngine
                            .getOperationService()
                            .createInvocationBuilder(
                                    SeaTunnelServer.SERVICE_NAME,
                                    new NotifyTaskStatusOperation(
                                            taskGroupLocation, taskExecutionState),
                                    nodeEngine.getMasterAddress())
                            .invoke();
            try {
                invoke.get();
                notifyStateSuccess = true;
            } catch (InterruptedException e) {
                logger.severe("send notify task status failed", e);
            } catch (JobNotFoundException e) {
                logger.warning("send notify task status failed because can't find job", e);
                notifyStateSuccess = true;
            } catch (ExecutionException e) {
                if (e.getCause() instanceof JobNotFoundException) {
                    logger.warning("send notify task status failed because can't find job", e);
                    notifyStateSuccess = true;
                } else {
                    logger.warning(ExceptionUtils.getMessage(e));
                    logger.warning(
                            String.format(
                                    "notify the job of the task(%s) status failed, retry in %s millis",
                                    taskGroupLocation, sleepTime));
                    try {
                        Thread.sleep(sleepTime);
                    } catch (InterruptedException ex) {
                        logger.severe(e);
                    }
                }
            }
        }
    }

    /**
     * JobMaster call this method to cancel a task, and then {@link TaskExecutionService} cancel
     * this task and send the {@link TaskExecutionState} to JobMaster.
     *
     * @param taskGroupLocation TaskGroup.getTaskGroupLocation()
     */
    public void cancelTaskGroup(TaskGroupLocation taskGroupLocation) {
        logger.info(String.format("Task (%s) need cancel.", taskGroupLocation));
        if (cancellationFutures.containsKey(taskGroupLocation)) {
            try {
                cancellationFutures.get(taskGroupLocation).cancel(false);
            } catch (CancellationException ignore) {
                // ignore
            }
        } else {
            logger.warning(
                    String.format("need cancel taskId : %s is not exist", taskGroupLocation));
        }
    }

    public void asyncExecuteFunction(TaskGroupLocation taskGroupLocation, Runnable task) {
        String id = UUID.randomUUID().toString();
        logger.fine("accept async execute function from " + taskGroupLocation + " with id " + id);
        if (!taskAsyncFunctionFuture.containsKey(taskGroupLocation)) {
            taskAsyncFunctionFuture.put(taskGroupLocation, new ConcurrentHashMap<>());
        }
        CompletableFuture<?> future =
                CompletableFuture.runAsync(task, MDCTracer.tracing(executorService));
        taskAsyncFunctionFuture.get(taskGroupLocation).put(id, future);
        future.whenComplete(
                (r, e) -> {
                    taskAsyncFunctionFuture.get(taskGroupLocation).remove(id);
                    logger.fine(
                            "remove async execute function from "
                                    + taskGroupLocation
                                    + " with id "
                                    + id);
                });
    }

    public void notifyCleanTaskGroupContext(TaskGroupLocation taskGroupLocation) {
        finishedExecutionContexts.remove(taskGroupLocation);
    }

    @Override
    public void provideDynamicMetrics(
            MetricDescriptor descriptor, MetricsCollectionContext context) {
        try {
            MetricDescriptor copy1 =
                    descriptor.copy().withTag(MetricTags.SERVICE, this.getClass().getSimpleName());
            Map<TaskGroupLocation, TaskGroupContext> contextMap = new HashMap<>();
            contextMap.putAll(finishedExecutionContexts);
            contextMap.putAll(executionContexts);
            contextMap.forEach(
                    (taskGroupLocation, taskGroupContext) -> {
                        MetricDescriptor copy2 =
                                copy1.copy()
                                        .withTag(TASK_GROUP_LOCATION, taskGroupLocation.toString())
                                        .withTag(
                                                JOB_ID,
                                                String.valueOf(taskGroupLocation.getJobId()))
                                        .withTag(
                                                PIPELINE_ID,
                                                String.valueOf(taskGroupLocation.getPipelineId()))
                                        .withTag(
                                                TASK_GROUP_ID,
                                                String.valueOf(taskGroupLocation.getTaskGroupId()));
                        taskGroupContext
                                .getTaskGroup()
                                .getTasks()
                                .forEach(
                                        task -> {
                                            Long taskID = task.getTaskID();
                                            MetricDescriptor copy3 =
                                                    copy2.copy()
                                                            .withTag(
                                                                    TASK_ID,
                                                                    String.valueOf(taskID));
                                            task.provideDynamicMetrics(copy3, context);
                                        });
                    });
        } catch (Throwable t) {
            logger.warning("Dynamic metric collection failed", t);
            throw t;
        }
    }

    private void updateMetricsContextInImap() {
        if (!nodeEngine.getNode().getState().equals(NodeState.ACTIVE)) {
            logger.warning(
                    String.format(
                            "The Node is not ready yet, Node state %s,looking forward to the next "
                                    + "scheduling",
                            nodeEngine.getNode().getState()));
            return;
        }
        IMap<Long, HashMap<TaskLocation, SeaTunnelMetricsContext>> metricsImap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_METRICS);
        Map<TaskGroupLocation, TaskGroupContext> contextMap = new HashMap<>();
        contextMap.putAll(finishedExecutionContexts);
        contextMap.putAll(executionContexts);
        HashMap<TaskLocation, SeaTunnelMetricsContext> localMap = new HashMap<>();
        contextMap.forEach(
                (taskGroupLocation, taskGroupContext) -> {
                    taskGroupContext
                            .getTaskGroup()
                            .getTasks()
                            .forEach(
                                    task -> {
                                        // MetricsContext only exists in SeaTunnelTask
                                        if (task instanceof SeaTunnelTask) {
                                            SeaTunnelTask seaTunnelTask = (SeaTunnelTask) task;
                                            if (null != seaTunnelTask.getMetricsContext()) {
                                                localMap.put(
                                                        seaTunnelTask.getTaskLocation(),
                                                        seaTunnelTask.getMetricsContext());
                                            }
                                        }
                                    });
                });
        if (!localMap.isEmpty()) {
            boolean lockedIMap = false;
            try {
                lockedIMap =
                        metricsImap.tryLock(
                                Constant.IMAP_RUNNING_JOB_METRICS_KEY, 5, TimeUnit.SECONDS);
                if (!lockedIMap) {
                    logger.warning("try lock failed in update metrics");
                    return;
                }
                HashMap<TaskLocation, SeaTunnelMetricsContext> centralMap =
                        metricsImap.computeIfAbsent(
                                Constant.IMAP_RUNNING_JOB_METRICS_KEY, k -> new HashMap<>());
                centralMap.putAll(localMap);
                metricsImap.put(Constant.IMAP_RUNNING_JOB_METRICS_KEY, centralMap);
            } catch (Exception e) {
                logger.warning(
                        "The Imap acquisition failed due to the hazelcast node being offline or restarted, and will be retried next time",
                        e);
            } finally {
                if (lockedIMap) {
                    boolean unLockedIMap = false;
                    while (!unLockedIMap) {
                        try {
                            metricsImap.unlock(Constant.IMAP_RUNNING_JOB_METRICS_KEY);
                            unLockedIMap = true;
                        } catch (OperationTimeoutException e) {
                            logger.warning("unlock imap failed in update metrics", e);
                        }
                    }
                }
            }
        }
        this.printTaskExecutionRuntimeInfo();
    }

    public void printTaskExecutionRuntimeInfo() {
        if (logger.isFineEnabled()) {
            ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) executorService;
            int activeCount = threadPoolExecutor.getActiveCount();
            int taskQueueSize = threadShareTaskQueue.size();
            long completedTaskCount = threadPoolExecutor.getCompletedTaskCount();
            long taskCount = threadPoolExecutor.getTaskCount();
            logger.fine(
                    StringFormatUtils.formatTable(
                            "TaskExecutionServer Thread Pool Status",
                            "activeCount",
                            activeCount,
                            "threadShareTaskQueueSize",
                            taskQueueSize,
                            "completedTaskCount",
                            completedTaskCount,
                            "taskCount",
                            taskCount));
        }
    }

    public void reportEvent(Event e) {
        eventService.reportEvent(e);
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
            TaskExecutionService.TaskGroupExecutionTracker taskGroupExecutionTracker =
                    tracker.taskGroupExecutionTracker;
            ClassLoader classLoader =
                    executionContexts
                            .get(taskGroupExecutionTracker.taskGroup.getTaskGroupLocation())
                            .getClassLoaders()
                            .get(tracker.task.getTaskID());
            ClassLoader oldClassLoader = Thread.currentThread().getContextClassLoader();
            Thread.currentThread().setContextClassLoader(classLoader);
            final Task t = tracker.task;
            ProgressState result = null;
            try {
                startedLatch.countDown();
                t.init();
                do {
                    result = t.call();
                } while (!result.isDone()
                        && isRunning
                        && !taskGroupExecutionTracker.executionCompletedExceptionally());
            } catch (InterruptedException e) {
                logger.warning(String.format("Interrupted task %d - %s", t.getTaskID(), t));
                if (taskGroupExecutionTracker.executionException.get() == null
                        && !taskGroupExecutionTracker.isCancel.get()) {
                    taskGroupExecutionTracker.exception(e);
                }
            } catch (Throwable e) {
                logger.warning("Exception in " + t, e);
                taskGroupExecutionTracker.exception(e);
            } finally {
                taskGroupExecutionTracker.taskDone(t);
                if (result == null || !result.isDone()) {
                    try {
                        tracker.task.close();
                    } catch (IOException e) {
                        logger.severe("Close task error", e);
                    }
                }
            }
            Thread.currentThread().setContextClassLoader(oldClassLoader);
        }
    }

    private final class BlockingTaskThreadFactory implements ThreadFactory {
        private final AtomicInteger seq = new AtomicInteger();

        @Override
        public Thread newThread(@NonNull Runnable r) {
            return new Thread(
                    r,
                    String.format(
                            "hz.%s.seaTunnel.task.thread-%d",
                            hzInstanceName, seq.getAndIncrement()));
        }
    }

    /**
     * CooperativeTaskWorker is used to poll the task call method, When a task times out, a new
     * BusWork will be created to take over the execution of the task
     */
    public final class CooperativeTaskWorker implements Runnable {

        AtomicBoolean keep = new AtomicBoolean(true);
        public AtomicReference<TaskTracker> exclusiveTaskTracker = new AtomicReference<>();
        final TaskCallTimer timer;
        private Thread myThread;
        public LinkedBlockingDeque<TaskTracker> taskQueue;
        private Future<?> thisTaskFuture;
        private BlockingQueue<Future<?>> futureBlockingQueue;

        public CooperativeTaskWorker(
                LinkedBlockingDeque<TaskTracker> taskQueue,
                RunBusWorkSupplier runBusWorkSupplier,
                BlockingQueue<Future<?>> futureBlockingQueue) {
            logger.info(String.format("Created new BusWork : %s", this.hashCode()));
            this.taskQueue = taskQueue;
            this.timer = new TaskCallTimer(50, keep, runBusWorkSupplier, this);
            this.futureBlockingQueue = futureBlockingQueue;
        }

        @SneakyThrows
        @Override
        public void run() {
            thisTaskFuture = futureBlockingQueue.take();
            futureBlockingQueue = null;
            myThread = currentThread();
            while (keep.get() && isRunning) {
                TaskTracker taskTracker =
                        null != exclusiveTaskTracker.get()
                                ? exclusiveTaskTracker.get()
                                : taskQueue.takeFirst();
                TaskGroupExecutionTracker taskGroupExecutionTracker =
                        taskTracker.taskGroupExecutionTracker;
                if (taskGroupExecutionTracker.executionCompletedExceptionally()) {
                    taskGroupExecutionTracker.taskDone(taskTracker.task);
                    if (null != exclusiveTaskTracker.get()) {
                        // If it's exclusive need to end the work
                        break;
                    } else {
                        // No action required and don't put back
                        continue;
                    }
                }
                taskGroupExecutionTracker.currRunningTaskFuture.put(
                        taskTracker.task.getTaskID(), thisTaskFuture);
                // start timer, if it's exclusive, don't need to start
                if (null == exclusiveTaskTracker.get()) {
                    timer.timerStart(taskTracker);
                }
                ProgressState call = null;
                try {
                    // run task
                    myThread.setContextClassLoader(
                            executionContexts
                                    .get(taskGroupExecutionTracker.taskGroup.getTaskGroupLocation())
                                    .getClassLoaders()
                                    .get(taskTracker.task.getTaskID()));
                    call = taskTracker.task.call();
                    synchronized (timer) {
                        timer.timerStop();
                    }
                } catch (InterruptedException e) {
                    if (taskGroupExecutionTracker.executionException.get() == null
                            && !taskGroupExecutionTracker.isCancel.get()) {
                        taskGroupExecutionTracker.exception(e);
                    }
                    taskGroupExecutionTracker.taskDone(taskTracker.task);
                    logger.warning("Exception in " + taskTracker.task, e);
                    if (null != exclusiveTaskTracker.get()) {
                        break;
                    }
                } catch (Throwable e) {
                    // task Failure and complete
                    taskGroupExecutionTracker.exception(e);
                    taskGroupExecutionTracker.taskDone(taskTracker.task);
                    // If it's exclusive need to end the work
                    logger.warning("Exception in " + taskTracker.task, e);
                    if (null != exclusiveTaskTracker.get()) {
                        break;
                    }
                } finally {
                    // stop timer
                    timer.timerStop();
                    taskGroupExecutionTracker.currRunningTaskFuture.remove(
                            taskTracker.task.getTaskID());
                }
                // task call finished
                if (null != call) {
                    if (call.isDone()) {
                        // If it's exclusive, you need to end the work
                        taskGroupExecutionTracker.taskDone(taskTracker.task);
                        if (null != exclusiveTaskTracker.get()) {
                            break;
                        }
                    } else {
                        // Task is not completed. Put task to the end of the queue
                        // If the current work has an exclusive tracker, it will not be put back
                        if (null == exclusiveTaskTracker.get()) {
                            taskQueue.offer(taskTracker);
                        }
                    }
                }
            }
        }
    }

    /** Used to create a new BusWork and run */
    public final class RunBusWorkSupplier {

        ExecutorService executorService;
        LinkedBlockingDeque<TaskTracker> taskQueue;

        public RunBusWorkSupplier(
                ExecutorService executorService, LinkedBlockingDeque<TaskTracker> taskqueue) {
            this.executorService = executorService;
            this.taskQueue = taskqueue;
        }

        public boolean runNewBusWork(boolean checkTaskQueue) {
            if (!checkTaskQueue || !taskQueue.isEmpty()) {
                BlockingQueue<Future<?>> futureBlockingQueue = new LinkedBlockingQueue<>();
                CooperativeTaskWorker cooperativeTaskWorker =
                        new CooperativeTaskWorker(taskQueue, this, futureBlockingQueue);
                Future<?> submit = executorService.submit(cooperativeTaskWorker);
                futureBlockingQueue.add(submit);
                return true;
            }
            return false;
        }
    }

    /**
     * Internal utility class to track the overall state of tasklet execution. There's one instance
     * of this class per job.
     */
    public final class TaskGroupExecutionTracker {

        private final TaskGroup taskGroup;
        final CompletableFuture<TaskExecutionState> future;
        volatile List<Future<?>> blockingFutures = emptyList();

        private final AtomicInteger completionLatch;
        private final AtomicReference<Throwable> executionException = new AtomicReference<>();

        private final AtomicBoolean isCancel = new AtomicBoolean(false);

        private final Map<Long, Future<?>> currRunningTaskFuture = new ConcurrentHashMap<>();

        TaskGroupExecutionTracker(
                @NonNull CompletableFuture<Void> cancellationFuture,
                @NonNull TaskGroup taskGroup,
                @NonNull CompletableFuture<TaskExecutionState> future) {
            this.future = future;
            this.completionLatch = new AtomicInteger(taskGroup.getTasks().size());
            this.taskGroup = taskGroup;
            cancellationFuture.whenComplete(
                    withTryCatch(
                            logger,
                            (r, e) -> {
                                isCancel.set(true);
                                if (e == null) {
                                    e =
                                            new IllegalStateException(
                                                    "cancellationFuture should be completed exceptionally");
                                }
                                exception(e);
                                cancelAllTask(taskGroup.getTaskGroupLocation());
                            }));
        }

        void exception(Throwable t) {
            executionException.compareAndSet(null, t);
        }

        private void cancelAllTask(TaskGroupLocation taskGroupLocation) {
            try {
                blockingFutures.forEach(f -> f.cancel(true));
                currRunningTaskFuture.values().forEach(f -> f.cancel(true));
            } catch (CancellationException ignore) {
                // ignore
            }
            cancelAsyncFunction(taskGroupLocation);
        }

        private void cancelAsyncFunction(TaskGroupLocation taskGroupLocation) {
            try {
                if (taskAsyncFunctionFuture.containsKey(taskGroupLocation)) {
                    taskAsyncFunctionFuture.remove(taskGroupLocation).values().stream()
                            .filter(f -> !f.isDone())
                            .filter(f -> !f.isCancelled())
                            .forEach(f -> f.cancel(true));
                }
            } catch (CancellationException ignore) {
                logger.warning(ExceptionUtils.getMessage(ignore));
            }
        }

        void taskDone(Task task) {
            TaskGroupLocation taskGroupLocation = taskGroup.getTaskGroupLocation();
            logger.info(
                    String.format(
                            "taskDone, taskId = %d, taskGroup = %s",
                            task.getTaskID(), taskGroupLocation));
            Throwable ex = executionException.get();
            if (completionLatch.decrementAndGet() == 0) {
                recycleClassLoader(taskGroupLocation);
                finishedExecutionContexts.put(
                        taskGroupLocation, executionContexts.remove(taskGroupLocation));
                cancellationFutures.remove(taskGroupLocation);
                try {
                    cancelAsyncFunction(taskGroupLocation);
                } catch (Throwable t) {
                    logger.severe("cancel async function failed", t);
                }
                try {
                    updateMetricsContextInImap();
                } catch (Throwable t) {
                    logger.severe("update metrics context in imap failed", t);
                }
                if (ex == null) {
                    logger.info(
                            String.format(
                                    "taskGroup %s complete with FINISHED", taskGroupLocation));
                    future.complete(
                            new TaskExecutionState(taskGroupLocation, ExecutionState.FINISHED));
                    return;
                } else if (isCancel.get()) {
                    logger.info(
                            String.format(
                                    "taskGroup %s complete with CANCELED", taskGroupLocation));
                    future.complete(
                            new TaskExecutionState(taskGroupLocation, ExecutionState.CANCELED));
                    return;
                } else {
                    logger.info(
                            String.format("taskGroup %s complete with FAILED", taskGroupLocation));
                    future.complete(
                            new TaskExecutionState(taskGroupLocation, ExecutionState.FAILED, ex));
                }
            }
            if (!isCancel.get() && ex != null) {
                logger.info(
                        String.format(
                                "task %s error with exception: [%s], cancel other task in taskGroup %s.",
                                task.getTaskID(), ex, taskGroupLocation));
                cancelAllTask(taskGroupLocation);
            }
        }

        private void recycleClassLoader(TaskGroupLocation taskGroupLocation) {
            TaskGroupContext context = executionContexts.get(taskGroupLocation);
            executionContexts.get(taskGroupLocation).setClassLoaders(null);
            for (Collection<URL> jars : context.getJars().values()) {
                classLoaderService.releaseClassLoader(taskGroupLocation.getJobId(), jars);
            }
        }

        boolean executionCompletedExceptionally() {
            return executionException.get() != null;
        }
    }

    public ServerConnectorPackageClient getServerConnectorPackageClient() {
        return serverConnectorPackageClient;
    }

    public static class NamedTaskWrapper implements Runnable {
        private final Runnable task;
        private final String threadName;

        public NamedTaskWrapper(Runnable task, String threadName) {
            this.task = task;
            this.threadName = threadName;
        }

        @Override
        public void run() {
            Thread currentThread = Thread.currentThread();
            String originalName = currentThread.getName();
            try {
                currentThread.setName(threadName);
                task.run();
            } finally {
                currentThread.setName(originalName);
            }
        }
    }
}
