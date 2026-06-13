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
import org.apache.seatunnel.engine.server.task.operation.ReportMetricsOperation;

import org.apache.commons.collections4.CollectionUtils;

import com.hazelcast.instance.impl.NodeState;
import com.hazelcast.internal.metrics.DynamicMetricsProvider;
import com.hazelcast.internal.metrics.MetricDescriptor;
import com.hazelcast.internal.metrics.MetricsCollectionContext;
import com.hazelcast.internal.metrics.MetricsRegistry;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.jet.impl.execution.init.CustomClassLoadedObject;
import com.hazelcast.logging.ILogger;
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

/**
 * This class is responsible for the execution of the Task.
 *
 * <p>TaskExecutionService manages the lifecycle of task execution in the SeaTunnel engine. It
 * handles:
 *
 * <ul>
 *   <li>Task deployment and deserialization.
 *   <li>Task execution using cooperative multitasking (CooperativeTaskWorker) and blocking workers
 *       (BlockingWorker).
 *   <li>Class loader management for connector jars.
 *   <li>Task cancellation and cleanup.
 *   <li>Metrics collection and reporting.
 * </ul>
 *
 * <p>The service supports two execution modes:
 *
 * <ul>
 *   <li>Thread-share mode: Tasks share a common thread pool and are executed cooperatively.
 *   <li>Blocking mode: Tasks run in dedicated threads for blocking operations.
 * </ul>
 *
 * <p>Tasks are organized into TaskGroups, each tracked by a TaskGroupExecutionTracker that monitors
 * execution state and handles completion/cancellation.
 */
public class TaskExecutionService implements DynamicMetricsProvider {

    /** The name of the Hazelcast instance this service runs on. */
    private final String hzInstanceName;

    /** The NodeEngine implementation for this Hazelcast node. */
    private final NodeEngineImpl nodeEngine;

    /** Service for managing class loaders for connector jars. */
    private final ClassLoaderService classLoaderService;

    /** Logger for this service. */
    private final ILogger logger;

    /** Flag indicating whether the service is running. */
    private volatile boolean isRunning = true;

    /** Queue for tasks that can share threads (cooperative multitasking). */
    private final LinkedBlockingDeque<TaskTracker> threadShareTaskQueue =
            new LinkedBlockingDeque<>();

    /** Executor service for running task workers. */
    private final ExecutorService executorService =
            newCachedThreadPool(new BlockingTaskThreadFactory());

    /** Supplier for creating and running new BusWork threads. */
    private final RunBusWorkSupplier runBusWorkSupplier =
            new RunBusWorkSupplier(executorService, threadShareTaskQueue);

    /**
     * Cache of active execution contexts, keyed by TaskGroupLocation. Contains context for tasks
     * currently being executed.
     */
    private final ConcurrentMap<TaskGroupLocation, TaskGroupContext> executionContexts =
            new ConcurrentHashMap<>();

    /**
     * Cache of finished execution contexts, keyed by TaskGroupLocation. Contains context for tasks
     * that have completed but have not been cleaned up yet.
     */
    private final ConcurrentMap<TaskGroupLocation, TaskGroupContext> finishedExecutionContexts =
            new ConcurrentHashMap<>();

    /**
     * Map of async function futures for each task group. Used to track and cancel async functions
     * associated with a task group.
     */
    private final ConcurrentMap<TaskGroupLocation, Map<String, CompletableFuture<?>>>
            taskAsyncFunctionFuture = new ConcurrentHashMap<>();

    /**
     * Map of cancellation futures for each task group. Used to cancel task group execution on
     * request.
     */
    private final ConcurrentMap<TaskGroupLocation, CompletableFuture<Void>> cancellationFutures =
            new ConcurrentHashMap<>();

    /** SeaTunnel configuration for this engine. */
    private final SeaTunnelConfig seaTunnelConfig;

    /** Scheduled executor for periodic tasks like metrics backup. */
    private final ScheduledExecutorService scheduledExecutorService;

    /** Client for managing connector packages on the server. */
    private final ServerConnectorPackageClient serverConnectorPackageClient;

    /** Service for reporting events. */
    private final EventService eventService;

    /**
     * Creates a new TaskExecutionService.
     *
     * @param classLoaderService service for managing class loaders
     * @param nodeEngine the Hazelcast node engine
     * @param eventService service for reporting events
     */
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

    /**
     * Gets the Hazelcast node engine backing this task execution service.
     *
     * @return the Hazelcast node engine
     */
    public NodeEngineImpl getNodeEngine() {
        return nodeEngine;
    }

    /** Starts the task execution service by creating initial cooperative task worker threads. */
    public void start() {
        runBusWorkSupplier.runNewBusWork(false);
    }

    /**
     * Shuts down the task execution service. This method stops accepting new tasks and interrupts
     * all running tasks.
     */
    public void shutdown() {
        isRunning = false;
        executorService.shutdownNow();
        scheduledExecutorService.shutdown();
    }

    /**
     * Gets the execution context for a task group. First checks active execution contexts, then
     * falls back to finished execution contexts.
     *
     * @param taskGroupLocation the location of the task group
     * @return the TaskGroupContext for the task group
     * @throws TaskGroupContextNotFoundException if the task group is not found
     */
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

    /**
     * Gets the active execution context for a task group. Only checks active execution contexts,
     * does not check finished contexts.
     *
     * @param taskGroupLocation the location of the task group
     * @return the TaskGroupContext for the task group
     * @throws TaskGroupContextNotFoundException if the task group is not found or not active
     */
    public TaskGroupContext getActiveExecutionContext(TaskGroupLocation taskGroupLocation) {
        TaskGroupContext taskGroupContext = executionContexts.get(taskGroupLocation);

        if (taskGroupContext == null) {
            throw new TaskGroupContextNotFoundException(
                    String.format("task group %s not found.", taskGroupLocation));
        }
        return taskGroupContext;
    }

    /**
     * Submits tasks to the thread-share queue for cooperative execution. Each task is wrapped in a
     * TaskTracker and initialized before being added to the queue.
     *
     * @param taskGroupExecutionTracker the tracker for the task group execution
     * @param tasks the list of tasks to submit
     */
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

    /**
     * Submits tasks to the executor service for blocking execution. Each task runs in a dedicated
     * thread using BlockingWorker. A CountDownLatch is used to ensure all workers have started
     * before returning.
     *
     * @param taskGroupExecutionTracker the tracker for the task group execution
     * @param tasks the list of tasks to submit
     */
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

    /**
     * Deploys a task from serialized data.
     *
     * @param taskImmutableInformation serialized task information
     * @return the deployment state indicating success or failure
     */
    public TaskDeployState deployTask(@NonNull Data taskImmutableInformation) {
        TaskGroupImmutableInformation taskImmutableInfo =
                nodeEngine.getSerializationService().toObject(taskImmutableInformation);
        return deployTask(taskImmutableInfo);
    }

    /**
     * Gets a task by its location.
     *
     * @param taskLocation the location of the task
     * @param <T> the task type
     * @return the task
     */
    public <T extends Task> T getTask(@NonNull TaskLocation taskLocation) {
        TaskGroupContext executionContext =
                this.getActiveExecutionContext(taskLocation.getTaskGroupLocation());
        return executionContext.getTaskGroup().getTask(taskLocation.getTaskID());
    }

    /**
     * Deploys a task group from TaskGroupImmutableInformation. This method handles task
     * deserialization, class loader setup, and task group creation.
     *
     * @param taskImmutableInfo the task group information
     * @return the deployment state indicating success or failure
     */
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
                    // Task is actively running (present in executionContexts, not
                    // finishedExecutionContexts). This happens during master failover: the new
                    // master restores state and tries to re-deploy tasks that never stopped on
                    // the worker. Return success so the master reconnects without interrupting
                    // the running task. The worker will notify the master of the terminal state
                    // via NotifyTaskStatusOperation when the task eventually completes.
                    logger.warning(
                            String.format(
                                    "TaskGroupLocation %s already exists and is active, "
                                            + "skipping redeploy for master failover recovery",
                                    taskGroup.getTaskGroupLocation()));
                    // Release classloaders acquired during deserialization
                    for (Map.Entry<Long, Collection<URL>> entry : taskJars.entrySet()) {
                        classLoaderService.releaseClassLoader(
                                taskImmutableInfo.getJobId(), entry.getValue());
                    }
                    return TaskDeployState.success();
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

    /**
     * Deploys a task group locally. This method initializes the task group, creates execution
     * contexts, and submits tasks for execution based on the configured thread share mode.
     *
     * @param taskGroup the task group to deploy
     * @param classLoaders map of task IDs to class loaders
     * @param jars map of task IDs to connector jars
     * @return a future that completes with the task execution state
     */
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

    /**
     * Notifies the master node of the task execution state. This method retries indefinitely until
     * successful or the service is shutdown.
     *
     * @param taskGroupLocation the location of the task group
     * @param taskExecutionState the execution state to report
     */
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

    /**
     * Executes a function asynchronously in the context of a task group. The function is tracked
     * and can be cancelled when the task group is cancelled.
     *
     * @param taskGroupLocation the task group location
     * @param task the Runnable to execute
     */
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

    /**
     * Notifies the service to clean up the execution context for a finished task group. This is
     * called when the task group context is no longer needed.
     *
     * @param taskGroupLocation the task group location to clean up
     */
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

        InvocationFuture<Object> invoke =
                nodeEngine
                        .getOperationService()
                        .createInvocationBuilder(
                                SeaTunnelServer.SERVICE_NAME,
                                new ReportMetricsOperation(collectLocalMetricsMap()),
                                nodeEngine.getMasterAddress())
                        .invoke();

        try {
            invoke.get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            logger.severe("update metrics context stopped due to thread interruption.", e);
        } catch (Exception e) {
            logger.severe("failed to update metrics", e);
        }
        this.printTaskExecutionRuntimeInfo();
    }

    private HashMap<TaskLocation, SeaTunnelMetricsContext> collectLocalMetricsMap() {
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
        return localMap;
    }

    /**
     * Prints task execution runtime information to the log. This includes thread pool status like
     * active count, queue size, and task counts. Only logs when fine level logging is enabled.
     */
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

    /**
     * Reports an event to the event service.
     *
     * @param e the event to report
     */
    public void reportEvent(Event e) {
        eventService.reportEvent(e);
    }

    /**
     * Gets the SeaTunnel configuration.
     *
     * @return the SeaTunnel configuration
     */
    public SeaTunnelConfig getSeaTunnelConfig() {
        return seaTunnelConfig;
    }

    /**
     * Worker that executes blocking tasks in a dedicated thread. Each BlockingWorker runs a single
     * task to completion, suitable for I/O-bound operations that may block.
     */
    private final class BlockingWorker implements Runnable {

        private final TaskTracker tracker;
        private final CountDownLatch startedLatch;

        private BlockingWorker(TaskTracker tracker, CountDownLatch startedLatch) {
            this.tracker = tracker;
            this.startedLatch = startedLatch;
        }

        /**
         * Executes the blocking task in a dedicated thread. The task runs to completion (or
         * failure/cancellation) without preemption.
         *
         * <p>Execution flow:
         *
         * <ol>
         *   <li>Set up the class loader for the task
         *   <li>Signal that the worker has started via CountDownLatch
         *   <li>Initialize the task via {@link Task#init()}
         *   <li>Execute the task repeatedly via {@link Task#call()} until done
         *   <li>Handle interrupts and exceptions, notifying the execution tracker
         *   <li>Clean up by calling {@link Task#close()} if not completed
         * </ol>
         */
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
                if (taskGroupExecutionTracker.isCancel.get()) {
                    logger.warning(String.format("Interrupted task %d - %s", t.getTaskID(), t));
                } else {
                    logger.warning("Exception in " + t, e);
                }
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

    /**
     * ThreadFactory for creating named threads used for SeaTunnel task execution. The shared
     * executor service created with this factory may run blocking workers, cooperative workers, and
     * asynchronous tasks. Threads are named with the pattern {@code
     * hz.{instance}.seaTunnel.task.thread-{n}}.
     */
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
     * Cooperative task worker that polls tasks from the queue and executes them cooperatively. Uses
     * a TaskCallTimer to detect stuck tasks. When a task times out, a new BusWork will be created
     * to take over the execution.
     *
     * <p>In cooperative mode, multiple tasks share a single worker thread. Each task yields control
     * by returning {@link ProgressState#isDone()} == false, allowing other tasks to run. This is
     * efficient for CPU-bound tasks that don't block.
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

        /**
         * Main execution loop for the cooperative task worker. Continuously polls tasks from the
         * queue and executes them.
         *
         * <p>The execution flow:
         *
         * <ol>
         *   <li>Wait for a task from the queue or exclusive tracker
         *   <li>Check if execution completed exceptionally, handle accordingly
         *   <li>Start the task call timer for timeout detection
         *   <li>Execute the task via {@link Task#call()}
         *   <li>Stop the timer and check the result
         *   <li>If task is done, mark it complete; otherwise, re-queue for next iteration
         * </ol>
         */
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

    /**
     * Supplier that creates and runs new CooperativeTaskWorker instances (BusWork) when needed. New
     * workers are created either unconditionally or, when requested by the caller, only if the task
     * queue currently contains pending tasks.
     */
    public final class RunBusWorkSupplier {

        ExecutorService executorService;
        LinkedBlockingDeque<TaskTracker> taskQueue;

        public RunBusWorkSupplier(
                ExecutorService executorService, LinkedBlockingDeque<TaskTracker> taskqueue) {
            this.executorService = executorService;
            this.taskQueue = taskqueue;
        }

        /**
         * Creates and submits a new CooperativeTaskWorker if conditions are met.
         *
         * @param checkTaskQueue if true, only creates a new worker if the task queue is not empty
         * @return true if a new worker was created and submitted, false otherwise
         */
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
     * Internal utility class to track the overall state of a TaskGroup execution. There's one
     * instance of this class per TaskGroup.
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

        /**
         * Records an exception that occurred during task execution. Uses compareAndSet to ensure
         * only the first exception is recorded.
         *
         * @param t the exception that occurred
         */
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

        /**
         * Marks a task as done and handles completion logic for the task group.
         *
         * <p>When the last task completes (completionLatch reaches zero):
         *
         * <ol>
         *   <li>Recycle the class loader
         *   <li>Move execution context from active to finished
         *   <li>Cancel async functions and update metrics
         *   <li>Complete the future with final state (FINISHED, CANCELED, or FAILED)
         * </ol>
         *
         * <p>If an exception occurred and the task group is not cancelled, cancels all remaining
         * tasks in the group.
         *
         * @param task the task that completed
         */
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

    /**
     * Gets the server connector package client for managing connector jars.
     *
     * @return the server connector package client
     */
    public ServerConnectorPackageClient getServerConnectorPackageClient() {
        return serverConnectorPackageClient;
    }

    /**
     * A Runnable wrapper that sets a custom thread name before executing the task and restores the
     * original name afterward.
     */
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
