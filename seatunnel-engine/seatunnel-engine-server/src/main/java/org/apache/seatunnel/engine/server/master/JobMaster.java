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

package org.apache.seatunnel.engine.server.master;

import org.apache.seatunnel.api.common.metrics.JobMetrics;
import org.apache.seatunnel.api.common.metrics.RawJobMetrics;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.options.EnvCommonOptions;
import org.apache.seatunnel.api.sink.SaveModeExecuteLocation;
import org.apache.seatunnel.api.sink.SaveModeExecuteWrapper;
import org.apache.seatunnel.api.sink.SaveModeHandler;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SupportSaveMode;
import org.apache.seatunnel.api.sink.multitablesink.MultiTableSink;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.common.utils.ExceptionUtils;
import org.apache.seatunnel.common.utils.RetryUtils;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.engine.checkpoint.storage.exception.CheckpointStorageException;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.config.EngineConfig;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.server.CheckpointConfig;
import org.apache.seatunnel.engine.common.config.server.CheckpointStorageConfig;
import org.apache.seatunnel.engine.common.exception.SeaTunnelEngineException;
import org.apache.seatunnel.engine.common.utils.ExceptionUtil;
import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.core.dag.actions.SinkAction;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.dag.logical.LogicalVertex;
import org.apache.seatunnel.engine.core.job.ConnectorJarIdentifier;
import org.apache.seatunnel.engine.core.job.ExecutionAddress;
import org.apache.seatunnel.engine.core.job.JobDAGInfo;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.core.job.JobInfo;
import org.apache.seatunnel.engine.core.job.JobResult;
import org.apache.seatunnel.engine.core.job.JobStatus;
import org.apache.seatunnel.engine.core.job.PipelineStatus;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointManager;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointPlan;
import org.apache.seatunnel.engine.server.checkpoint.CompletedCheckpoint;
import org.apache.seatunnel.engine.server.dag.DAGUtils;
import org.apache.seatunnel.engine.server.dag.physical.PhysicalPlan;
import org.apache.seatunnel.engine.server.dag.physical.PipelineLocation;
import org.apache.seatunnel.engine.server.dag.physical.PlanUtils;
import org.apache.seatunnel.engine.server.dag.physical.ResourceUtils;
import org.apache.seatunnel.engine.server.dag.physical.SubPlan;
import org.apache.seatunnel.engine.server.execution.TaskExecutionState;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.metrics.JobMetricsUtil;
import org.apache.seatunnel.engine.server.metrics.SeaTunnelMetricsContext;
import org.apache.seatunnel.engine.server.resourcemanager.AbstractResourceManager;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManager;
import org.apache.seatunnel.engine.server.resourcemanager.allocation.strategy.SlotAllocationStrategy;
import org.apache.seatunnel.engine.server.resourcemanager.allocation.strategy.SlotRatioStrategy;
import org.apache.seatunnel.engine.server.resourcemanager.allocation.strategy.SystemLoadStrategy;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;
import org.apache.seatunnel.engine.server.task.operation.CleanTaskGroupContextOperation;
import org.apache.seatunnel.engine.server.task.operation.GetTaskGroupMetricsOperation;
import org.apache.seatunnel.engine.server.utils.NodeEngineUtil;

import com.hazelcast.cluster.Address;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.core.OperationTimeoutException;
import com.hazelcast.flakeidgen.FlakeIdGenerator;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.impl.execution.init.CustomClassLoadedObject;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.NodeEngine;
import lombok.Getter;
import lombok.NonNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.hazelcast.jet.impl.util.ExceptionUtil.withTryCatch;
import static org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode.HANDLE_SAVE_MODE_FAILED;
import static org.apache.seatunnel.common.constants.JobMode.BATCH;

public class JobMaster {
    private static final ILogger LOGGER = Logger.getLogger(JobMaster.class);

    private PhysicalPlan physicalPlan;

    private final Data jobImmutableInformationData;

    private final NodeEngine nodeEngine;

    private final ExecutorService executorService;

    private final FlakeIdGenerator flakeIdGenerator;

    private final ResourceManager resourceManager;

    private final JobHistoryService jobHistoryService;

    private CheckpointManager checkpointManager;

    private CompletableFuture<JobResult> jobMasterCompleteFuture;

    private JobImmutableInformation jobImmutableInformation;

    private LogicalDag logicalDag;

    private JobDAGInfo jobDAGInfo;

    private SeaTunnelServer seaTunnelServer;

    /**
     * we need store slot used by task in Hazelcast IMap and release or reuse it when a new master
     * node active.
     */
    private final IMap<PipelineLocation, Map<TaskGroupLocation, SlotProfile>> ownedSlotProfilesIMap;

    private final IMap<Object, Object> runningJobStateIMap;

    private final IMap<Object, Object> runningJobStateTimestampsIMap;

    // TODO add config to change value
    private boolean isPhysicalDAGIInfo = true;

    private final EngineConfig engineConfig;

    private boolean isRunning = true;

    private Map<Integer, CheckpointPlan> checkpointPlanMap;

    private final Map<Integer, List<SlotProfile>> releasedSlotWhenTaskGroupFinished;

    private final IMap<Long, JobInfo> runningJobInfoIMap;

    @Getter private final Set<ExecutionAddress> historyExecutionAddress = new HashSet<>();

    private final IMap<Long, HashMap<TaskLocation, SeaTunnelMetricsContext>> metricsImap;

    /** If the job or pipeline cancel by user, needRestore will be false */
    @Getter private volatile boolean needRestore = true;

    private CheckpointConfig jobCheckpointConfig;

    @Getter private Long jobId;

    public String getErrorMessage() {
        return errorMessage;
    }

    private String errorMessage;

    public JobMaster(
            @NonNull Long jobId,
            @NonNull Data jobImmutableInformationData,
            @NonNull NodeEngine nodeEngine,
            @NonNull ExecutorService executorService,
            @NonNull ResourceManager resourceManager,
            @NonNull JobHistoryService jobHistoryService,
            @NonNull IMap runningJobStateIMap,
            @NonNull IMap runningJobStateTimestampsIMap,
            @NonNull IMap ownedSlotProfilesIMap,
            @NonNull IMap<Long, JobInfo> runningJobInfoIMap,
            @NonNull IMap<Long, HashMap<TaskLocation, SeaTunnelMetricsContext>> metricsImap,
            EngineConfig engineConfig,
            SeaTunnelServer seaTunnelServer) {
        this.jobId = jobId;
        this.jobImmutableInformationData = jobImmutableInformationData;
        this.nodeEngine = nodeEngine;
        this.executorService = executorService;
        flakeIdGenerator =
                this.nodeEngine
                        .getHazelcastInstance()
                        .getFlakeIdGenerator(Constant.SEATUNNEL_ID_GENERATOR_NAME);
        this.ownedSlotProfilesIMap = ownedSlotProfilesIMap;
        this.resourceManager = resourceManager;
        this.jobHistoryService = jobHistoryService;
        this.runningJobStateIMap = runningJobStateIMap;
        this.runningJobStateTimestampsIMap = runningJobStateTimestampsIMap;
        this.runningJobInfoIMap = runningJobInfoIMap;
        this.engineConfig = engineConfig;
        this.metricsImap = metricsImap;
        this.seaTunnelServer = seaTunnelServer;
        this.releasedSlotWhenTaskGroupFinished = new ConcurrentHashMap<>();
    }

    public synchronized void init(long initializationTimestamp, boolean restart) throws Exception {
        jobImmutableInformation =
                nodeEngine.getSerializationService().toObject(jobImmutableInformationData);
        jobCheckpointConfig =
                createJobCheckpointConfig(
                        engineConfig.getCheckpointConfig(), jobImmutableInformation.getJobConfig());

        LOGGER.info(
                String.format(
                        "Init JobMaster for Job %s (%s) ",
                        jobImmutableInformation.getJobConfig().getName(),
                        jobImmutableInformation.getJobId()));
        LOGGER.info(
                String.format(
                        "Job %s (%s) needed jar urls %s",
                        jobImmutableInformation.getJobConfig().getName(),
                        jobImmutableInformation.getJobId(),
                        jobImmutableInformation.getPluginJarsUrls()));
        ClassLoader appClassLoader = Thread.currentThread().getContextClassLoader();
        ClassLoader classLoader =
                seaTunnelServer
                        .getClassLoaderService()
                        .getClassLoader(
                                jobImmutableInformation.getJobId(),
                                jobImmutableInformation.getPluginJarsUrls());
        logicalDag =
                CustomClassLoadedObject.deserializeWithCustomClassLoader(
                        nodeEngine.getSerializationService(),
                        classLoader,
                        jobImmutableInformation.getLogicalDag());
        try {
            Thread.currentThread().setContextClassLoader(classLoader);
            if (!restart
                    && !logicalDag.isStartWithSavePoint()
                    && ReadonlyConfig.fromMap(logicalDag.getJobConfig().getEnvOptions())
                            .get(EnvCommonOptions.SAVEMODE_EXECUTE_LOCATION)
                            .equals(SaveModeExecuteLocation.CLUSTER)) {
                logicalDag.getLogicalVertexMap().values().stream()
                        .map(LogicalVertex::getAction)
                        .filter(action -> action instanceof SinkAction)
                        .map(sink -> ((SinkAction<?, ?, ?, ?>) sink).getSink())
                        .forEach(JobMaster::handleSaveMode);
            }

            final Tuple2<PhysicalPlan, Map<Integer, CheckpointPlan>> planTuple =
                    PlanUtils.fromLogicalDAG(
                            logicalDag,
                            nodeEngine,
                            jobImmutableInformation,
                            initializationTimestamp,
                            executorService,
                            flakeIdGenerator,
                            runningJobStateIMap,
                            runningJobStateTimestampsIMap,
                            engineConfig.getQueueType(),
                            engineConfig);
            this.physicalPlan = planTuple.f0();
            this.physicalPlan.setJobMaster(this);
            this.checkpointPlanMap = planTuple.f1();
        } finally {
            // revert to app class loader, it may be changed by PlanUtils.fromLogicalDAG
            Thread.currentThread().setContextClassLoader(appClassLoader);
            seaTunnelServer
                    .getClassLoaderService()
                    .releaseClassLoader(
                            jobImmutableInformation.getJobId(),
                            jobImmutableInformation.getPluginJarsUrls());
        }
        Exception initException = null;
        try {
            this.initCheckPointManager(restart);
        } catch (Exception e) {
            initException = e;
        }
        this.initStateFuture();
        if (initException != null) {
            if (restart) {
                cancelJob();
            }
            throw initException;
        }
    }

    public void initCheckPointManager(boolean restart) throws CheckpointStorageException {
        this.checkpointManager =
                new CheckpointManager(
                        jobImmutableInformation.getJobId(),
                        jobImmutableInformation.isStartWithSavePoint() || restart,
                        nodeEngine,
                        this,
                        checkpointPlanMap,
                        jobCheckpointConfig,
                        executorService,
                        runningJobStateIMap);
    }

    // TODO replace it after ReadableConfig Support parse yaml format, then use only one config to
    // read engine and env config.
    private CheckpointConfig createJobCheckpointConfig(
            CheckpointConfig defaultCheckpointConfig, JobConfig jobConfig) {
        Map<String, Object> jobEnv = jobConfig.getEnvOptions();
        CheckpointConfig jobCheckpointConfig = new CheckpointConfig();
        jobCheckpointConfig.setCheckpointTimeout(defaultCheckpointConfig.getCheckpointTimeout());
        jobCheckpointConfig.setCheckpointInterval(defaultCheckpointConfig.getCheckpointInterval());

        CheckpointStorageConfig jobCheckpointStorageConfig = new CheckpointStorageConfig();
        jobCheckpointStorageConfig.setStorage(defaultCheckpointConfig.getStorage().getStorage());
        jobCheckpointStorageConfig.setStoragePluginConfig(
                defaultCheckpointConfig.getStorage().getStoragePluginConfig());
        jobCheckpointStorageConfig.setMaxRetainedCheckpoints(
                defaultCheckpointConfig.getStorage().getMaxRetainedCheckpoints());
        jobCheckpointConfig.setStorage(jobCheckpointStorageConfig);

        if (jobEnv.containsKey(EnvCommonOptions.CHECKPOINT_INTERVAL.key())) {
            jobCheckpointConfig.setCheckpointInterval(
                    Long.parseLong(
                            jobEnv.get(EnvCommonOptions.CHECKPOINT_INTERVAL.key()).toString()));
        } else if (jobConfig.getJobContext().getJobMode() == BATCH) {
            LOGGER.info(
                    "in batch mode, the 'checkpoint.interval' configuration of env is missing, so checkpoint will be disabled");
            jobCheckpointConfig.setCheckpointEnable(false);
        }
        if (jobEnv.containsKey(EnvCommonOptions.CHECKPOINT_TIMEOUT.key())) {
            jobCheckpointConfig.setCheckpointTimeout(
                    Long.parseLong(
                            jobEnv.get(EnvCommonOptions.CHECKPOINT_TIMEOUT.key()).toString()));
        }
        return jobCheckpointConfig;
    }

    public void initStateFuture() {
        jobMasterCompleteFuture = new CompletableFuture<>();
        PassiveCompletableFuture<JobResult> jobStatusFuture = physicalPlan.initStateFuture();
        jobStatusFuture.whenComplete(
                withTryCatch(
                        LOGGER,
                        (v, t) -> {
                            JobMaster.this.errorMessage = v.getError();
                            JobResult jobResult =
                                    new JobResult(physicalPlan.getJobStatus(), v.getError());
                            cleanJob();
                            jobMasterCompleteFuture.complete(jobResult);
                        }));
    }

    /**
     * Apply for all resources
     *
     * @return true if apply resources successfully, otherwise false
     */
    public boolean preApplyResources() {
        return preApplyResources(null);
    }

    /**
     * Apply for resources
     *
     * @return true if apply resources successfully, otherwise false
     */
    public boolean preApplyResources(SubPlan subPlan) {

        // When starting to apply for task resources, reset the worker's slot allocation information
        // Mainly used in two scenarios:
        // 1. When based on the SYSTEM_LOAD strategy, the system load cannot change dynamically, and
        // the resources used by each slot need to be calculated and inferred
        // 2. When based on the SLOT_RATIO strategy, registerWorker is not updated in real time, and
        // is used to record the slot application status
        //        ((AbstractResourceManager) resourceManager)
        //                .setWorkerAssignedSlots(new ConcurrentHashMap<>());
        SlotAllocationStrategy slotAllocationStrategy =
                ((AbstractResourceManager) resourceManager).getSlotAllocationStrategy();
        if (slotAllocationStrategy instanceof SlotRatioStrategy) {
            ((SlotRatioStrategy) slotAllocationStrategy)
                    .setWorkerAssignedSlots(new ConcurrentHashMap<>());
        } else if (slotAllocationStrategy instanceof SystemLoadStrategy) {
            ((SystemLoadStrategy) slotAllocationStrategy)
                    .setWorkerAssignedSlots(new ConcurrentHashMap<>());
        }

        Map<TaskGroupLocation, CompletableFuture<SlotProfile>> preApplyResourceFutures =
                new HashMap<>();

        boolean isSubPlan = Objects.nonNull(subPlan);

        if (isSubPlan) {
            preApplyResourcesForSubPlan(subPlan, preApplyResourceFutures);
        } else {
            preApplyResourcesForAll(preApplyResourceFutures);
        }

        boolean enoughResource =
                preApplyResourceFutures.values().stream()
                                .filter(
                                        value -> {
                                            try {
                                                return value != null && value.join() != null;
                                            } catch (CompletionException e) {
                                                LOGGER.warning(
                                                        "Pre resource application failed, resources may be not enough");
                                                return false;
                                            }
                                        })
                                .count()
                        == preApplyResourceFutures.size();

        if (enoughResource) {
            for (Map.Entry<TaskGroupLocation, CompletableFuture<SlotProfile>> entry :
                    preApplyResourceFutures.entrySet()) {
                try {
                    Address worker = entry.getValue().get().getWorker();
                    historyExecutionAddress.add(
                            new ExecutionAddress(worker.getHost(), worker.getPort()));

                } catch (Exception e) {
                    LOGGER.warning("history execution plan add worker failed", e);
                }
            }
            if (isSubPlan) {
                // SubPlan applies for resources separately and needs to be merged into the entire
                // job's resources
                physicalPlan.getPreApplyResourceFutures().putAll(preApplyResourceFutures);
            } else {
                // Adequate resources, pass on resources to the plan
                physicalPlan.setPreApplyResourceFutures(preApplyResourceFutures);
            }
        } else {
            // Release the resource that has been applied
            try {
                RetryUtils.retryWithException(
                        () -> {
                            resourceManager
                                    .releaseResources(
                                            jobImmutableInformation.getJobId(),
                                            preApplyResourceFutures.values().stream()
                                                    .filter(
                                                            value -> {
                                                                try {
                                                                    return value != null
                                                                            && value.join() != null;
                                                                } catch (CompletionException e) {
                                                                    LOGGER.warning(
                                                                            "Pre resource application failed, resources may be not enough");
                                                                    return false;
                                                                }
                                                            })
                                                    .map(CompletableFuture::join)
                                                    .collect(Collectors.toList()))
                                    .join();
                            return null;
                        },
                        new RetryUtils.RetryMaterial(
                                Constant.OPERATION_RETRY_TIME,
                                true,
                                ExceptionUtil::isOperationNeedRetryException,
                                Constant.OPERATION_RETRY_SLEEP));
            } catch (Exception e) {
                LOGGER.warning(
                        String.format(
                                "Pre resource application failed %s",
                                ExceptionUtils.getMessage(e)));
            }
        }
        return enoughResource;
    }

    private Map<TaskGroupLocation, CompletableFuture<SlotProfile>> preApplyResourcesForAll(
            Map<TaskGroupLocation, CompletableFuture<SlotProfile>> preApplyResourceFutures) {
        for (SubPlan subPlan : physicalPlan.getPipelineList()) {
            preApplyResourcesForSubPlan(subPlan, preApplyResourceFutures);
        }
        return preApplyResourceFutures;
    }

    private void preApplyResourcesForSubPlan(
            SubPlan subPlan,
            Map<TaskGroupLocation, CompletableFuture<SlotProfile>> preApplyResourceFutures) {

        Map<TaskGroupLocation, CompletableFuture<SlotProfile>> coordinatorFutures = new HashMap<>();
        subPlan.getCoordinatorVertexList()
                .forEach(
                        coordinator ->
                                coordinatorFutures.put(
                                        coordinator.getTaskGroupLocation(),
                                        ResourceUtils.applyResourceForTask(
                                                resourceManager, coordinator, subPlan.getTags())));

        Map<TaskGroupLocation, CompletableFuture<SlotProfile>> taskFutures = new HashMap<>();
        subPlan.getPhysicalVertexList()
                .forEach(
                        task ->
                                taskFutures.put(
                                        task.getTaskGroupLocation(),
                                        ResourceUtils.applyResourceForTask(
                                                resourceManager, task, subPlan.getTags())));

        preApplyResourceFutures.putAll(coordinatorFutures);
        preApplyResourceFutures.putAll(taskFutures);
        LOGGER.fine("preApplyResourceFutures size: " + preApplyResourceFutures.size());
    }

    public void run() {
        try {
            physicalPlan.startJob();
        } catch (Throwable e) {
            LOGGER.severe(
                    String.format(
                            "Job %s (%s) run error with: %s",
                            physicalPlan.getJobImmutableInformation().getJobConfig().getName(),
                            physicalPlan.getJobImmutableInformation().getJobId(),
                            ExceptionUtils.getMessage(e)));
        } finally {
            jobMasterCompleteFuture.join();
            if (engineConfig.getConnectorJarStorageConfig().getEnable()) {
                List<ConnectorJarIdentifier> pluginJarIdentifiers =
                        jobImmutableInformation.getPluginJarIdentifiers();
                seaTunnelServer
                        .getConnectorPackageService()
                        .cleanUpWhenJobFinished(
                                jobImmutableInformation.getJobId(), pluginJarIdentifiers);
            }
        }
    }

    public static void handleSaveMode(SeaTunnelSink sink) {
        if (sink instanceof SupportSaveMode) {
            Optional<SaveModeHandler> saveModeHandler =
                    ((SupportSaveMode) sink).getSaveModeHandler();
            if (saveModeHandler.isPresent()) {
                try (SaveModeHandler handler = saveModeHandler.get()) {
                    handler.open();
                    new SaveModeExecuteWrapper(handler).execute();
                } catch (Exception e) {
                    throw new SeaTunnelRuntimeException(HANDLE_SAVE_MODE_FAILED, e);
                }
            }
        } else if (sink instanceof MultiTableSink) {
            Map<TablePath, SeaTunnelSink> sinks = ((MultiTableSink) sink).getSinks();
            for (SeaTunnelSink seaTunnelSink : sinks.values()) {
                handleSaveMode(seaTunnelSink);
            }
        }
    }

    public void handleCheckpointError(long pipelineId, boolean neverRestore) {
        if (neverRestore) {
            this.neverNeedRestore();
        }
        this.physicalPlan
                .getPipelineList()
                .forEach(
                        pipeline -> {
                            if (pipeline.getPipelineLocation().getPipelineId() == pipelineId) {
                                pipeline.handleCheckpointError();
                            }
                        });
    }

    private void removeJobIMap() {
        Long jobId = getJobImmutableInformation().getJobId();
        runningJobStateTimestampsIMap.remove(jobId);

        getPhysicalPlan()
                .getPipelineList()
                .forEach(
                        pipeline -> {
                            runningJobStateIMap.remove(pipeline.getPipelineLocation());
                            runningJobStateTimestampsIMap.remove(pipeline.getPipelineLocation());
                            pipeline.getCoordinatorVertexList()
                                    .forEach(
                                            coordinator -> {
                                                runningJobStateIMap.remove(
                                                        coordinator.getTaskGroupLocation());
                                                runningJobStateTimestampsIMap.remove(
                                                        coordinator.getTaskGroupLocation());
                                            });

                            pipeline.getPhysicalVertexList()
                                    .forEach(
                                            task -> {
                                                runningJobStateIMap.remove(
                                                        task.getTaskGroupLocation());
                                                runningJobStateTimestampsIMap.remove(
                                                        task.getTaskGroupLocation());
                                            });
                        });

        runningJobStateIMap.remove(jobId);
        runningJobInfoIMap.remove(jobId);
    }

    public JobDAGInfo getJobDAGInfo() {
        if (jobDAGInfo == null) {
            jobDAGInfo =
                    DAGUtils.getJobDAGInfo(
                            logicalDag,
                            jobImmutableInformation,
                            engineConfig,
                            isPhysicalDAGIInfo,
                            new ExecutionAddress(
                                    this.nodeEngine.getThisAddress().getHost(),
                                    this.nodeEngine.getThisAddress().getPort()),
                            historyExecutionAddress);
        }
        return jobDAGInfo;
    }

    public void releaseTaskGroupResource(
            PipelineLocation pipelineLocation, TaskGroupLocation taskGroupLocation) {
        Map<TaskGroupLocation, SlotProfile> taskGroupLocationSlotProfileMap =
                ownedSlotProfilesIMap.get(pipelineLocation);
        if (taskGroupLocationSlotProfileMap == null) {
            return;
        }
        SlotProfile taskGroupSlotProfile = taskGroupLocationSlotProfileMap.get(taskGroupLocation);
        if (taskGroupSlotProfile == null) {
            return;
        }

        try {
            RetryUtils.retryWithException(
                    () -> {
                        LOGGER.info(
                                String.format(
                                        "release the task group resource %s", taskGroupLocation));

                        resourceManager
                                .releaseResources(
                                        jobImmutableInformation.getJobId(),
                                        Collections.singletonList(taskGroupSlotProfile))
                                .join();
                        releasedSlotWhenTaskGroupFinished
                                .computeIfAbsent(
                                        pipelineLocation.getPipelineId(),
                                        k -> new CopyOnWriteArrayList<>())
                                .add(taskGroupSlotProfile);
                        return null;
                    },
                    new RetryUtils.RetryMaterial(
                            Constant.OPERATION_RETRY_TIME,
                            true,
                            ExceptionUtil::isOperationNeedRetryException,
                            Constant.OPERATION_RETRY_SLEEP));
        } catch (Exception e) {
            LOGGER.warning(
                    String.format(
                            "release the task group resource failed %s, with exception: %s ",
                            taskGroupLocation, ExceptionUtils.getMessage(e)));
        }
    }

    public void releasePipelineResource(SubPlan subPlan) {
        try {
            Map<TaskGroupLocation, SlotProfile> taskGroupLocationSlotProfileMap =
                    ownedSlotProfilesIMap.get(subPlan.getPipelineLocation());
            if (taskGroupLocationSlotProfileMap == null) {
                return;
            }
            List<SlotProfile> alreadyReleased = new ArrayList<>();
            if (releasedSlotWhenTaskGroupFinished.containsKey(subPlan.getPipelineId())) {
                alreadyReleased.addAll(
                        releasedSlotWhenTaskGroupFinished.get(subPlan.getPipelineId()));
            }

            RetryUtils.retryWithException(
                    () -> {
                        LOGGER.info(
                                String.format(
                                        "release the pipeline %s resource",
                                        subPlan.getPipelineFullName()));
                        resourceManager
                                .releaseResources(
                                        jobImmutableInformation.getJobId(),
                                        taskGroupLocationSlotProfileMap.values().stream()
                                                .filter(p -> !alreadyReleased.contains(p))
                                                .collect(Collectors.toList()))
                                .join();
                        ownedSlotProfilesIMap.remove(subPlan.getPipelineLocation());
                        releasedSlotWhenTaskGroupFinished.remove(subPlan.getPipelineId());
                        return null;
                    },
                    new RetryUtils.RetryMaterial(
                            Constant.OPERATION_RETRY_TIME,
                            true,
                            exception -> ExceptionUtil.isOperationNeedRetryException(exception),
                            Constant.OPERATION_RETRY_SLEEP));
        } catch (Exception e) {
            LOGGER.warning(
                    String.format(
                            "release the pipeline %s resource failed, with exception: %s ",
                            subPlan.getPipelineFullName(), ExceptionUtils.getMessage(e)));
        }
    }

    public void cleanJob() {
        checkpointManager.clearCheckpointIfNeed(physicalPlan.getJobStatus());
        jobHistoryService.storeJobInfo(jobImmutableInformation.getJobId(), getJobDAGInfo());
        jobHistoryService.storeFinishedJobState(this);
        removeJobIMap();
    }

    public Address queryTaskGroupAddress(TaskGroupLocation taskGroupLocation) {

        PipelineLocation pipelineLocation =
                new PipelineLocation(
                        taskGroupLocation.getJobId(), taskGroupLocation.getPipelineId());

        Map<TaskGroupLocation, SlotProfile> taskGroupLocationSlotProfileMap =
                ownedSlotProfilesIMap.get(pipelineLocation);

        if (null != taskGroupLocationSlotProfileMap) {
            SlotProfile slotProfile = taskGroupLocationSlotProfileMap.get(taskGroupLocation);
            if (null != slotProfile) {
                return slotProfile.getWorker();
            }
        }
        throw new IllegalArgumentException(
                "can't find task group address from taskGroupLocation: " + taskGroupLocation);
    }

    public synchronized void cancelJob() {
        physicalPlan.cancelJob();
    }

    public ResourceManager getResourceManager() {
        return resourceManager;
    }

    public CheckpointManager getCheckpointManager() {
        return checkpointManager;
    }

    public PassiveCompletableFuture<JobResult> getJobMasterCompleteFuture() {
        return new PassiveCompletableFuture<>(jobMasterCompleteFuture);
    }

    public JobImmutableInformation getJobImmutableInformation() {
        return jobImmutableInformation;
    }

    public JobStatus getJobStatus() {
        return physicalPlan.getJobStatus();
    }

    public List<RawJobMetrics> getCurrJobMetrics() {

        Map<TaskGroupLocation, Address> taskGroupLocationSlotProfileMap = new HashMap<>();

        ownedSlotProfilesIMap.forEach(
                (pipelineLocation, map) -> {
                    if (pipelineLocation.getJobId()
                            == this.getJobImmutableInformation().getJobId()) {
                        map.forEach(
                                (taskGroupLocation, slotProfile) -> {
                                    if (taskGroupLocation.getJobId()
                                            == this.getJobImmutableInformation().getJobId()) {
                                        taskGroupLocationSlotProfileMap.put(
                                                taskGroupLocation, slotProfile.getWorker());
                                    }
                                });
                    }
                });
        return getCurrJobMetrics(taskGroupLocationSlotProfileMap);
    }

    public List<RawJobMetrics> getCurrJobMetrics(List<PipelineLocation> pipelineLocations) {
        Map<TaskGroupLocation, Address> taskGroupLocationSlotProfileMap = new HashMap<>();

        ownedSlotProfilesIMap.forEach(
                (pipelineLocation, map) -> {
                    if (pipelineLocations.contains(pipelineLocation)) {
                        map.forEach(
                                (taskGroupLocation, slotProfile) -> {
                                    if (taskGroupLocation.getJobId()
                                            == this.getJobImmutableInformation().getJobId()) {
                                        taskGroupLocationSlotProfileMap.put(
                                                taskGroupLocation, slotProfile.getWorker());
                                    }
                                });
                    }
                });
        return getCurrJobMetrics(taskGroupLocationSlotProfileMap);
    }

    public List<RawJobMetrics> getCurrJobMetrics(
            Map<TaskGroupLocation, Address> taskGroupLocationSlotProfileMap) {
        Map<Address, List<TaskGroupLocation>> taskGroupLocationMap = new HashMap<>();

        for (Map.Entry<TaskGroupLocation, Address> entry :
                taskGroupLocationSlotProfileMap.entrySet()) {
            taskGroupLocationMap
                    .computeIfAbsent(entry.getValue(), k -> new ArrayList<>())
                    .add(entry.getKey());
        }
        List<RawJobMetrics> metrics = new ArrayList<>();
        taskGroupLocationMap.forEach(
                (address, taskGroupLocations) -> {
                    try {
                        if (nodeEngine.getClusterService().getMember(address) != null) {
                            RawJobMetrics rawJobMetrics =
                                    (RawJobMetrics)
                                            NodeEngineUtil.sendOperationToMemberNode(
                                                            nodeEngine,
                                                            new GetTaskGroupMetricsOperation(
                                                                    taskGroupLocations),
                                                            address)
                                                    .get();
                            metrics.add(rawJobMetrics);
                        }
                    }
                    // HazelcastInstanceNotActiveException. It means that the node is
                    // offline, so waiting for the taskGroup to restore can be successful
                    catch (HazelcastInstanceNotActiveException e) {
                        LOGGER.warning(
                                String.format(
                                        "%s get current job metrics with exception: %s.",
                                        Arrays.toString(taskGroupLocations.toArray()),
                                        ExceptionUtils.getMessage(e)));
                    } catch (Exception e) {
                        throw new SeaTunnelEngineException(ExceptionUtils.getMessage(e));
                    }
                });
        return metrics;
    }

    public void savePipelineMetricsToHistory(PipelineLocation pipelineLocation) {
        List<RawJobMetrics> currJobMetrics =
                this.getCurrJobMetrics(Collections.singletonList(pipelineLocation));
        JobMetrics jobMetrics = JobMetricsUtil.toJobMetrics(currJobMetrics);
        long jobId = this.getJobImmutableInformation().getJobId();
        synchronized (this) {
            jobHistoryService.storeFinishedPipelineMetrics(jobId, jobMetrics);
        }
        // Clean TaskGroupContext for TaskExecutionServer
        this.cleanTaskGroupContext(pipelineLocation);
    }

    public void removeMetricsContext(
            PipelineLocation pipelineLocation, PipelineStatus pipelineStatus) {
        if ((pipelineStatus.equals(PipelineStatus.FINISHED)
                        && !checkpointManager.isPipelineSavePointEnd(pipelineLocation))
                || pipelineStatus.equals(PipelineStatus.CANCELED)) {

            boolean lockedIMap = false;
            try {
                lockedIMap =
                        metricsImap.tryLock(
                                Constant.IMAP_RUNNING_JOB_METRICS_KEY, 5, TimeUnit.SECONDS);
                if (!lockedIMap) {
                    LOGGER.severe("lock imap failed in update metrics");
                    return;
                }

                HashMap<TaskLocation, SeaTunnelMetricsContext> centralMap =
                        metricsImap.get(Constant.IMAP_RUNNING_JOB_METRICS_KEY);
                if (centralMap != null) {
                    List<TaskLocation> collect =
                            centralMap.keySet().stream()
                                    .filter(
                                            taskLocation -> {
                                                return taskLocation
                                                        .getTaskGroupLocation()
                                                        .getPipelineLocation()
                                                        .equals(pipelineLocation);
                                            })
                                    .collect(Collectors.toList());
                    collect.forEach(centralMap::remove);
                    metricsImap.put(Constant.IMAP_RUNNING_JOB_METRICS_KEY, centralMap);
                }
            } catch (Exception e) {
                LOGGER.warning("failed to remove metrics context", e);
            } finally {
                if (lockedIMap) {
                    boolean unLockedIMap = false;
                    while (!unLockedIMap) {
                        try {
                            metricsImap.unlock(Constant.IMAP_RUNNING_JOB_METRICS_KEY);
                            unLockedIMap = true;
                        } catch (OperationTimeoutException e) {
                            LOGGER.warning("unlock imap failed in update metrics", e);
                        }
                    }
                }
            }
        }
    }

    private void cleanTaskGroupContext(PipelineLocation pipelineLocation) {
        Map<TaskGroupLocation, SlotProfile> slotProfileMap =
                ownedSlotProfilesIMap.get(pipelineLocation);
        if (slotProfileMap == null) {
            return;
        }
        slotProfileMap.forEach(
                (taskGroupLocation, slotProfile) -> {
                    try {
                        if (nodeEngine.getClusterService().getMember(slotProfile.getWorker())
                                != null) {
                            NodeEngineUtil.sendOperationToMemberNode(
                                            nodeEngine,
                                            new CleanTaskGroupContextOperation(taskGroupLocation),
                                            slotProfile.getWorker())
                                    .get();
                        }
                    } catch (HazelcastInstanceNotActiveException e) {
                        LOGGER.warning(
                                String.format(
                                        "%s clean TaskGroupContext with exception: %s.",
                                        taskGroupLocation, ExceptionUtils.getMessage(e)));
                    } catch (Exception e) {
                        throw new SeaTunnelException(e.getMessage());
                    }
                });
    }

    public PhysicalPlan getPhysicalPlan() {
        return physicalPlan;
    }

    public void updateTaskExecutionState(TaskExecutionState taskExecutionState) {
        this.physicalPlan
                .getPipelineList()
                .forEach(
                        pipeline -> {
                            if (pipeline.getPipelineLocation().getPipelineId()
                                    != taskExecutionState.getTaskGroupLocation().getPipelineId()) {
                                return;
                            }

                            pipeline.getCoordinatorVertexList()
                                    .forEach(
                                            task -> {
                                                if (!task.getTaskGroupLocation()
                                                        .equals(
                                                                taskExecutionState
                                                                        .getTaskGroupLocation())) {
                                                    return;
                                                }

                                                task.updateStateByExecutionService(
                                                        taskExecutionState);
                                            });

                            pipeline.getPhysicalVertexList()
                                    .forEach(
                                            task -> {
                                                if (!task.getTaskGroupLocation()
                                                        .equals(
                                                                taskExecutionState
                                                                        .getTaskGroupLocation())) {
                                                    return;
                                                }

                                                task.updateStateByExecutionService(
                                                        taskExecutionState);
                                                if (taskExecutionState
                                                        .getExecutionState()
                                                        .isEndState()) {
                                                    releaseTaskGroupResource(
                                                            pipeline.getPipelineLocation(),
                                                            task.getTaskGroupLocation());
                                                }
                                            });
                        });
    }

    /** Execute savePoint, which will cause the job to end. */
    public CompletableFuture<Boolean> savePoint() {
        LOGGER.info(
                String.format(
                        "Begin do save point for Job %s (%s) ",
                        jobImmutableInformation.getJobConfig().getName(),
                        jobImmutableInformation.getJobId()));
        physicalPlan.savepointJob();
        PassiveCompletableFuture<CompletedCheckpoint>[] passiveCompletableFutures =
                checkpointManager.triggerSavePoints();
        return CompletableFuture.supplyAsync(
                () ->
                        Arrays.stream(passiveCompletableFutures)
                                .allMatch(
                                        future -> {
                                            try {
                                                return future.get() != null;
                                            } catch (Exception e) {
                                                throw new SeaTunnelEngineException(e);
                                            }
                                        }));
    }

    public void setOwnedSlotProfiles(
            @NonNull PipelineLocation pipelineLocation,
            @NonNull Map<TaskGroupLocation, SlotProfile> pipelineOwnedSlotProfiles) {
        ownedSlotProfilesIMap.put(pipelineLocation, pipelineOwnedSlotProfiles);
        try {
            RetryUtils.retryWithException(
                    () ->
                            pipelineOwnedSlotProfiles.equals(
                                    ownedSlotProfilesIMap.get(pipelineLocation)),
                    new RetryUtils.RetryMaterial(
                            Constant.OPERATION_RETRY_TIME,
                            true,
                            exception -> exception instanceof NullPointerException && isRunning,
                            Constant.OPERATION_RETRY_SLEEP));
        } catch (Exception e) {
            throw new SeaTunnelEngineException(
                    "Can not sync pipeline owned slot profiles with IMap", e);
        }
    }

    public SlotProfile getOwnedSlotProfiles(@NonNull TaskGroupLocation taskGroupLocation) {
        Map<TaskGroupLocation, SlotProfile> taskGroupLocationSlotProfileMap =
                ownedSlotProfilesIMap.get(
                        new PipelineLocation(
                                taskGroupLocation.getJobId(), taskGroupLocation.getPipelineId()));
        if (taskGroupLocationSlotProfileMap == null) {
            return null;
        }

        return taskGroupLocationSlotProfileMap.get(taskGroupLocation);
    }

    public ExecutorService getExecutorService() {
        return executorService;
    }

    public void interrupt() {
        isRunning = false;
        jobMasterCompleteFuture.completeExceptionally(new InterruptedException());
    }

    public void neverNeedRestore() {
        this.needRestore = false;
    }

    public EngineConfig getEngineConfig() {
        return this.engineConfig;
    }
}
