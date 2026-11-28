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

import org.apache.seatunnel.shade.com.google.common.annotations.VisibleForTesting;

import org.apache.seatunnel.api.common.metrics.JobMetrics;
import org.apache.seatunnel.api.common.metrics.RawJobMetrics;
import org.apache.seatunnel.api.common.multitable.MultiTableFailedTable;
import org.apache.seatunnel.api.common.multitable.MultiTableFailureHelper;
import org.apache.seatunnel.api.common.multitable.MultiTableFailurePhase;
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
import org.apache.seatunnel.engine.checkpoint.storage.PipelineState;
import org.apache.seatunnel.engine.checkpoint.storage.api.CheckpointStorage;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.config.EngineConfig;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.config.server.CheckpointConfig;
import org.apache.seatunnel.engine.common.config.server.CheckpointStorageConfig;
import org.apache.seatunnel.engine.common.exception.SeaTunnelEngineException;
import org.apache.seatunnel.engine.common.job.JobResult;
import org.apache.seatunnel.engine.common.job.JobStatus;
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
import org.apache.seatunnel.engine.core.job.PipelineStatus;
import org.apache.seatunnel.engine.server.CoordinatorService;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointCloseReason;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointCoordinator;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointCoordinatorState;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointCoordinatorStatus;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointException;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointManager;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointPlan;
import org.apache.seatunnel.engine.server.dag.DAGUtils;
import org.apache.seatunnel.engine.server.dag.physical.PhysicalPlan;
import org.apache.seatunnel.engine.server.dag.physical.PipelineLocation;
import org.apache.seatunnel.engine.server.dag.physical.PlanUtils;
import org.apache.seatunnel.engine.server.dag.physical.ResourceUtils;
import org.apache.seatunnel.engine.server.dag.physical.SubPlan;
import org.apache.seatunnel.engine.server.execution.TaskExecutionState;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.master.cleanup.JobCleanupRecord;
import org.apache.seatunnel.engine.server.master.cleanup.PipelineCleanupRecord;
import org.apache.seatunnel.engine.server.metrics.JobMetricsUtil;
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
import com.hazelcast.flakeidgen.FlakeIdGenerator;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.NodeEngine;
import lombok.Getter;
import lombok.NonNull;

import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.hazelcast.jet.impl.util.ExceptionUtil.withTryCatch;
import static org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode.HANDLE_SAVE_MODE_FAILED;
import static org.apache.seatunnel.common.constants.JobMode.BATCH;

public class JobMaster {
    private static final ILogger LOGGER = Logger.getLogger(JobMaster.class);

    /**
     * Bounded number of read-backs used to confirm a written slot ownership record. The write is
     * synchronous on the IMap owner, so a mismatch normally appears in the first read; the small
     * bound only covers a transient read from another member without blocking job admission for
     * long.
     */
    private static final int OWNED_SLOT_PROFILES_CONFIRM_RETRY_TIME = 3;

    private static final long OWNED_SLOT_PROFILES_CONFIRM_RETRY_SLEEP_MS = 100L;

    /**
     * Bounded number of attempts used to record the allocations a failed admission round retains.
     * The record of the round already failed once, so these attempts only have to survive a
     * transient outage of the ownership map; an unreachable map keeps the local ledger instead of
     * blocking the job thread.
     */
    private static final int RETAINED_SLOT_OWNERSHIP_PERSIST_ATTEMPT = 3;

    private static final long RETAINED_SLOT_OWNERSHIP_PERSIST_RETRY_SLEEP_MS = 200L;

    /**
     * Bounded number of times a terminal release of admission-owned slots is re-run. A finished job
     * has no later cancel that would trigger another drain, so without this chain a transient
     * release outage would hold the capacity of a finished job forever.
     */
    private static final int ADMISSION_SLOT_CLEANUP_RETRY_TIME = 10;

    private static final long ADMISSION_SLOT_CLEANUP_RETRY_DELAY_MS = 2000L;

    private final Object metricsLock = new Object();

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

    private long initializationTimestamp;

    private LogicalDag logicalDag;

    private volatile JobDAGInfo jobDAGInfo;

    private SeaTunnelServer seaTunnelServer;

    /**
     * we need store slot used by task in Hazelcast IMap and release or reuse it when a new master
     * node active.
     */
    private final IMap<PipelineLocation, Map<TaskGroupLocation, SlotProfile>> ownedSlotProfilesIMap;

    private final IMap<Object, Object> runningJobStateIMap;

    private final IMap<Object, Object> runningJobStateTimestampsIMap;

    // TODO add config to change value
    private boolean isPhysicalDAGInfo = true;

    private final EngineConfig engineConfig;

    private boolean isRunning = true;

    private Map<Integer, CheckpointPlan> checkpointPlanMap;

    private final Map<Integer, List<SlotProfile>> releasedSlotWhenTaskGroupFinished;

    private final IMap<Long, JobInfo> runningJobInfoIMap;

    @Getter private final Set<ExecutionAddress> historyExecutionAddress = new HashSet<>();

    /** If the job or pipeline cancel by user, needRestore will be false */
    @Getter private volatile boolean needRestore = true;

    /** Whether this JobMaster was recreated after an active-master switch. */
    private volatile boolean masterFailoverRestore;

    /**
     * Slot profiles retained by a failed admission attempt of this master, keyed by the task group
     * they were allocated for.
     *
     * <p>A pre-apply round keeps the ownership record and the slots of every pipeline whose
     * ownership write was confirmed when another pipeline of the same round fails, because a
     * successor master may already have adopted them (see {@link #retainFailedAdmissionSlots}).
     * Those slots stay allocated to this job, so the next admission attempt of the same master, for
     * example the {@code WAIT} strategy retrying this {@link JobMaster}, has to reuse them instead
     * of requesting new ones. Otherwise the retry asks the workers for capacity the job already
     * holds: with a fixed capacity equal to the job requirement no free slot is left and the job
     * stays pending after the fault recovered, and with spare capacity the new allocation
     * overwrites the retained ownership while the old allocations stay untracked. A successor
     * master does not need this ledger: it adopts the same slots through the persisted ownership
     * records.
     */
    private final Map<TaskGroupLocation, AttemptOwnedSlotProfile> attemptOwnedSlotProfiles =
            new ConcurrentHashMap<>();

    /**
     * Monotonic sequence of the admission attempts of this master.
     *
     * <p>Every pre-apply round takes a new sequence, and a retained profile is reusable only by a
     * later attempt than the one that retained it. This fences the ledger against an entry recorded
     * by a concurrent or re-entrant round of the same master.
     */
    private final AtomicLong preApplyAttemptSequence = new AtomicLong();

    /** Sequence of the admission attempt currently being evaluated. */
    private volatile long currentPreApplyAttemptSequence;

    /**
     * Guards the hand-off of the retained admission slots between a pre-apply round and the
     * terminal cleanup of the job, so a job that ends during an admission round cannot keep slots
     * that no later attempt will ever use.
     */
    private final Object admissionSlotCleanupLock = new Object();

    /**
     * Whether this master already drained the slots retained by failed admission attempts.
     *
     * <p>Set once the job reached a terminal state and its retained slots were released, so a
     * concurrent admission round stops retaining capacity for a job that will never retry.
     */
    private volatile boolean admissionSlotsDrained;

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
        this.seaTunnelServer = seaTunnelServer;
        this.releasedSlotWhenTaskGroupFinished = new ConcurrentHashMap<>();
    }

    public synchronized void init(long initializationTimestamp, boolean restart) throws Exception {
        this.initializationTimestamp = initializationTimestamp;
        this.masterFailoverRestore = restart;
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

        List<Set<URL>> logicalVertexJarsList = jobImmutableInformation.getLogicalVertexJarsList();
        List<ClassLoader> logicalVertexClassLoaders = new ArrayList<>();
        for (Set<URL> urls : logicalVertexJarsList) {
            logicalVertexClassLoaders.add(
                    seaTunnelServer
                            .getClassLoaderService()
                            .getClassLoader(jobImmutableInformation.getJobId(), urls));
        }
        logicalDag =
                DAGUtils.restoreLogicalDag(
                        jobImmutableInformation,
                        nodeEngine.getSerializationService(),
                        logicalVertexClassLoaders);

        Map<Long, ClassLoader> logicalVertexIdClassLoaderMap = new HashMap<>();
        int i = 0;
        for (Long id : logicalDag.getLogicalVertexMap().keySet()) {
            logicalVertexIdClassLoaderMap.put(id, logicalVertexClassLoaders.get(i++));
        }
        try {
            if (!restart
                    && !logicalDag.isStartWithSavePoint()
                    && ReadonlyConfig.fromMap(logicalDag.getJobConfig().getEnvOptions())
                            .get(EnvCommonOptions.SAVEMODE_EXECUTE_LOCATION)
                            .equals(SaveModeExecuteLocation.CLUSTER)) {
                logicalDag.getLogicalVertexMap().values().stream()
                        .map(LogicalVertex::getAction)
                        .filter(action -> action instanceof SinkAction)
                        .forEach(
                                sink -> {
                                    Thread.currentThread()
                                            .setContextClassLoader(
                                                    logicalVertexIdClassLoaderMap.get(
                                                            sink.getId()));
                                    JobMaster.handleSaveMode(
                                            ((SinkAction<?, ?, ?, ?>) sink).getSink(),
                                            logicalDag.isStartWithSavePoint());
                                });
                Thread.currentThread().setContextClassLoader(appClassLoader);
            }

            final Tuple2<PhysicalPlan, Map<Integer, CheckpointPlan>> planTuple =
                    PlanUtils.fromLogicalDAG(
                            logicalDag,
                            nodeEngine,
                            jobImmutableInformation,
                            initializationTimestamp,
                            executorService,
                            seaTunnelServer.getClassLoaderService(),
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
            for (Set<URL> urls : logicalVertexJarsList) {
                seaTunnelServer
                        .getClassLoaderService()
                        .releaseClassLoader(jobImmutableInformation.getJobId(), urls);
            }
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

    public void initCheckPointManager(boolean restart) {
        CheckpointStorage checkpointStorage = resolveCheckpointStorageOrFallback(restart);
        this.checkpointManager =
                new CheckpointManager(
                        jobImmutableInformation.getJobId(),
                        jobImmutableInformation.isRestoreJob() || restart,
                        jobImmutableInformation.getRestoreMode(),
                        jobImmutableInformation.getRestoreSourceJobId(),
                        restart,
                        nodeEngine,
                        this,
                        checkpointPlanMap,
                        jobCheckpointConfig,
                        checkpointStorage,
                        executorService,
                        runningJobStateIMap,
                        seaTunnelServer.getEngineContext(),
                        seaTunnelServer.getCheckpointMonitorService());
    }

    private CheckpointStorage resolveCheckpointStorageOrFallback(boolean restart) {
        if (seaTunnelServer != null && seaTunnelServer.getCheckpointService() != null) {
            CheckpointStorage storage =
                    seaTunnelServer.getCheckpointService().getCheckpointStorage();
            if (storage != null) {
                return storage;
            }
        }

        boolean checkpointEnabled =
                jobCheckpointConfig != null && jobCheckpointConfig.isCheckpointEnable();
        boolean startWithSavePoint =
                jobImmutableInformation != null
                        && (jobImmutableInformation.isRestoreJob() || restart);

        if (checkpointEnabled && startWithSavePoint) {
            throw new IllegalStateException(
                    "Checkpoint is enabled and job starts with savepoint, but checkpoint storage is not available");
        }

        // When checkpoint is disabled, CheckpointManager will not touch the storage. We still need
        // a non-null placeholder to avoid NPEs during job initialization (especially in local
        // example runs where SeaTunnelServer components may initialize asynchronously).
        return new UnsupportedCheckpointStorage();
    }

    private static final class UnsupportedCheckpointStorage implements CheckpointStorage {
        private static UnsupportedOperationException unavailable() {
            return new UnsupportedOperationException("Checkpoint storage is unavailable");
        }

        @Override
        public String storeCheckPoint(PipelineState state) {
            throw unavailable();
        }

        @Override
        public void asyncStoreCheckPoint(PipelineState state) {
            throw unavailable();
        }

        @Override
        public List<PipelineState> getAllCheckpoints(String jobId) {
            throw unavailable();
        }

        @Override
        public List<PipelineState> getLatestCheckpoint(String jobId) {
            throw unavailable();
        }

        @Override
        public PipelineState getLatestCheckpointByJobIdAndPipelineId(
                String jobId, String pipelineId) {
            throw unavailable();
        }

        @Override
        public List<PipelineState> getCheckpointsByJobIdAndPipelineId(
                String jobId, String pipelineId) {
            throw unavailable();
        }

        @Override
        public void deleteCheckpoint(String jobId) {
            throw unavailable();
        }

        @Override
        public PipelineState getCheckpoint(String jobId, String pipelineId, String checkpointId) {
            throw unavailable();
        }

        @Override
        public void deleteCheckpoint(String jobId, String pipelineId, String checkpointId) {
            throw unavailable();
        }

        @Override
        public void deleteCheckpoint(
                String jobId, String pipelineId, List<String> checkpointIdList) {
            throw unavailable();
        }
    }

    // TODO replace it after ReadableConfig Support parse yaml format, then use only one config to
    // read engine and env config.
    private CheckpointConfig createJobCheckpointConfig(
            CheckpointConfig defaultCheckpointConfig, JobConfig jobConfig) {
        Map<String, Object> jobEnv = jobConfig.getEnvOptions();
        CheckpointConfig jobCheckpointConfig = new CheckpointConfig();
        jobCheckpointConfig.setCheckpointTimeout(defaultCheckpointConfig.getCheckpointTimeout());
        jobCheckpointConfig.setCheckpointInterval(defaultCheckpointConfig.getCheckpointInterval());
        jobCheckpointConfig.setCheckpointMinPause(defaultCheckpointConfig.getCheckpointMinPause());
        jobCheckpointConfig.setRetainAfterJobCancelled(
                defaultCheckpointConfig.isRetainAfterJobCancelled());

        CheckpointStorageConfig jobCheckpointStorageConfig = new CheckpointStorageConfig();
        jobCheckpointStorageConfig.setStorage(defaultCheckpointConfig.getStorage().getStorage());
        jobCheckpointStorageConfig.setStoragePluginConfig(
                defaultCheckpointConfig.getStorage().getStoragePluginConfig());
        jobCheckpointStorageConfig.setMaxRetainedCheckpoints(
                defaultCheckpointConfig.getStorage().getMaxRetainedCheckpoints());
        jobCheckpointConfig.setStorage(jobCheckpointStorageConfig);

        Optional<Object> checkpointIntervalOptional =
                Optional.ofNullable(jobEnv.get(EnvCommonOptions.CHECKPOINT_INTERVAL.key()));
        if (checkpointIntervalOptional.isPresent()) {
            jobCheckpointConfig.setCheckpointInterval(
                    Long.parseLong(checkpointIntervalOptional.get().toString()));
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
        if (jobEnv.containsKey(EnvCommonOptions.CHECKPOINT_MIN_PAUSE.key())) {
            jobCheckpointConfig.setCheckpointMinPause(
                    Long.parseLong(
                            jobEnv.get(EnvCommonOptions.CHECKPOINT_MIN_PAUSE.key()).toString()));
        }
        if (jobEnv.containsKey(EnvCommonOptions.CHECKPOINT_RETAIN_AFTER_JOB_CANCELLED.key())) {
            jobCheckpointConfig.setRetainAfterJobCancelled(
                    Boolean.parseBoolean(
                            jobEnv.get(EnvCommonOptions.CHECKPOINT_RETAIN_AFTER_JOB_CANCELLED.key())
                                    .toString()));
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

        long attemptSequence = preApplyAttemptSequence.incrementAndGet();
        currentPreApplyAttemptSequence = attemptSequence;

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
        // Value-based membership (SlotProfile#equals keys on worker+slotID+sequence) so the
        // cleanup filter below still excludes a reused slot even if a future code path re-reads
        // or copies the profile between pre-apply and cleanup, instead of relying on the exact
        // object instance surviving unchanged.
        Set<SlotProfile> reusedSlotProfiles = new HashSet<>();

        boolean isSubPlan = Objects.nonNull(subPlan);

        if (isSubPlan) {
            preApplyResourcesForSubPlan(subPlan, preApplyResourceFutures, reusedSlotProfiles);
        } else {
            preApplyResourcesForAll(preApplyResourceFutures, reusedSlotProfiles);
        }

        AtomicLong successCount = new AtomicLong(0);
        AtomicLong failedCount = new AtomicLong(0);

        boolean enoughResource =
                preApplyResourceFutures.values().stream()
                                .filter(
                                        value -> {
                                            try {
                                                if (value != null && value.join() != null) {
                                                    successCount.incrementAndGet();
                                                    return true;
                                                }
                                                failedCount.incrementAndGet();
                                                return false;
                                            } catch (CompletionException e) {
                                                long failed = failedCount.incrementAndGet();
                                                LOGGER.warning(
                                                        String.format(
                                                                "Pre resource application failed for job: %s, success: %d, failed: %d/%d, error: %s",
                                                                jobImmutableInformation.getJobId(),
                                                                successCount.get(),
                                                                failed,
                                                                preApplyResourceFutures.size(),
                                                                e.getCause() != null
                                                                        ? e.getCause().getMessage()
                                                                        : e.getMessage()));
                                                return false;
                                            }
                                        })
                                .count()
                        == preApplyResourceFutures.size();

        if (!enoughResource) {
            releaseUnusedPreAppliedResources(preApplyResourceFutures, reusedSlotProfiles);
            return false;
        }

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

        // Persist the ownership now, before any pipeline reaches SCHEDULED and starts deploying.
        // Otherwise a master failure in this window leaves the workers holding slots that the new
        // master cannot reuse or release, because it never recorded them.
        Map<PipelineLocation, Map<TaskGroupLocation, SlotProfile>> preAppliedSlotProfiles =
                collectPreAppliedSlotProfiles(preApplyResourceFutures);
        Set<PipelineLocation> confirmedPipelines =
                persistPreAppliedSlotProfiles(preAppliedSlotProfiles);
        if (confirmedPipelines.size() != preAppliedSlotProfiles.size()) {
            // A failed or unconfirmed ownership write proves neither that the slot is unused nor
            // that its record is absent: the write may have committed while its acknowledgement or
            // the confirmation read was lost, and a successor master may already have adopted that
            // exact allocation. Removing the record and releasing the slots on that assumption
            // would hand a slot in use to another job, so every pipeline of this round keeps its
            // record and its slots. The attempt ledger lets the next admission of this master reuse
            // them, and the terminal cleanup of this job releases them when it never retries.
            retainFailedAdmissionSlots(preAppliedSlotProfiles, attemptSequence);
            return false;
        }

        if (isSubPlan) {
            // SubPlan applies for resources separately and needs to be merged into the entire
            // job's resources
            physicalPlan.getPreApplyResourceFutures().putAll(preApplyResourceFutures);
        } else {
            // Adequate resources, pass on resources to the plan
            physicalPlan.setPreApplyResourceFutures(preApplyResourceFutures);
        }
        // The admitted slots are now owned by the plan, so they must not be handed out again as
        // retained slots of this master.
        clearAdmittedAttemptOwnedSlotProfiles(preApplyResourceFutures);
        // Retained slots are valid only for the first successful allocation after failover.
        // A later pipeline retry must obtain a fresh allocation instead of reusing them.
        masterFailoverRestore = false;
        return true;
    }

    /**
     * Groups the successfully pre-applied slots by pipeline, which is the grain of the ownership
     * IMap.
     *
     * @param preApplyResourceFutures allocation futures of this pre-apply round
     * @return the successfully allocated profiles per pipeline
     */
    private Map<PipelineLocation, Map<TaskGroupLocation, SlotProfile>>
            collectPreAppliedSlotProfiles(
                    Map<TaskGroupLocation, CompletableFuture<SlotProfile>>
                            preApplyResourceFutures) {
        Map<PipelineLocation, Map<TaskGroupLocation, SlotProfile>> profilesByPipeline =
                new HashMap<>();
        preApplyResourceFutures.forEach(
                (taskGroupLocation, future) -> {
                    if (future == null || future.isCompletedExceptionally()) {
                        return;
                    }
                    SlotProfile slotProfile = future.getNow(null);
                    if (slotProfile == null) {
                        return;
                    }
                    profilesByPipeline
                            .computeIfAbsent(
                                    taskGroupLocation.getPipelineLocation(), key -> new HashMap<>())
                            .put(taskGroupLocation, slotProfile);
                });
        return profilesByPipeline;
    }

    /**
     * Persists the task-group to slot mapping of a successful pre-apply before the pipelines are
     * deployed.
     *
     * <p>The mapping used to be written only when a pipeline reached {@code SCHEDULED}. A master
     * failure between the allocation and that transition left the workers holding slots that the
     * new master had no profile for, so it could neither reuse nor release them. Writing the exact
     * profiles here closes that window; the profiles keep their allocation sequence, so releasing
     * them later cannot affect a slot reassigned to another attempt.
     *
     * <p>A write that returns is not enough: {@link #setOwnedSlotProfiles} also reports whether the
     * written value could be read back unchanged. Both a thrown write and an unconfirmed read-back
     * mean the ownership is unknown rather than delayed, and the caller must not admit the job in
     * that case: the slots are already allocated on the workers, and a successor master could
     * neither reuse nor release them after an active-master switch.
     *
     * @param profilesByPipeline successfully allocated profiles of this pre-apply round, keyed by
     *     pipeline
     * @return the pipelines whose ownership was written and confirmed by a read-back
     */
    private Set<PipelineLocation> persistPreAppliedSlotProfiles(
            Map<PipelineLocation, Map<TaskGroupLocation, SlotProfile>> profilesByPipeline) {
        Set<PipelineLocation> confirmedPipelines = new HashSet<>();
        for (Map.Entry<PipelineLocation, Map<TaskGroupLocation, SlotProfile>> entry :
                profilesByPipeline.entrySet()) {
            try {
                if (setOwnedSlotProfiles(entry.getKey(), entry.getValue())) {
                    confirmedPipelines.add(entry.getKey());
                } else {
                    LOGGER.warning(
                            String.format(
                                    "Could not confirm pre-applied slot ownership for pipeline %s of job %d: the written mapping did not read back unchanged",
                                    entry.getKey(), jobImmutableInformation.getJobId()));
                }
            } catch (Exception e) {
                LOGGER.warning(
                        String.format(
                                "Could not persist pre-applied slot ownership for pipeline %s of job %d: %s",
                                entry.getKey(),
                                jobImmutableInformation.getJobId(),
                                ExceptionUtils.getMessage(e)));
            }
        }
        return confirmedPipelines;
    }

    /**
     * Keeps the slots a failed admission round allocated to this job so a later attempt of the same
     * master can reuse them, or releases them when the job is already over and will never retry.
     *
     * <p>An ownership write that threw or did not read back is not proof that the record is absent
     * or that the allocation is unused: the write may have committed before its acknowledgement or
     * the confirmation read was lost, and an active-master takeover may have adopted exactly those
     * profiles in the meantime. Removing the record or releasing the slots on that assumption would
     * free a slot that a successor master is already using, so this round keeps both. The attempt
     * ledger makes the allocations reusable for the next admission of this master, and {@link
     * #drainAdmissionOwnedSlots} releases them if the job reaches a terminal state instead.
     *
     * <p>The round keeps every profile it used, including the ones it reused from the previous
     * master. A reused profile is not re-recorded by this round, so its persisted ownership record
     * is the only trace of the allocation; skipping it here would let a cancelled or rejected
     * pending job keep a slot that no cleanup owns, because the terminal drain is the only release
     * path left for a job that never deployed.
     *
     * <p>Keeping the allocations in this instance's ledger is not enough on its own: the ledger
     * does not survive the death of the master. The retained allocations are therefore also written
     * back to the distributed ownership record with a bounded retry budget, so a successor master
     * can adopt the same allocations instead of stranding the capacity when this instance dies
     * before the job retries or ends.
     *
     * @param profilesByPipeline profiles of the pre-apply round that failed to persist
     * @param attemptSequence sequence of the attempt that kept the slots
     */
    private void retainFailedAdmissionSlots(
            Map<PipelineLocation, Map<TaskGroupLocation, SlotProfile>> profilesByPipeline,
            long attemptSequence) {
        synchronized (admissionSlotCleanupLock) {
            if (admissionSlotsDrained) {
                // The job ended while this round was still allocating, so no later attempt of this
                // master will reuse the slots: release what this round holds instead of keeping it
                // for a job that is already terminal.
                LOGGER.info(
                        String.format(
                                "Job %d ended while a pre-apply round was running, releasing the slots it holds",
                                jobImmutableInformation.getJobId()));
                releaseAdmissionOwnedSlots(profilesByPipeline);
                return;
            }
            profilesByPipeline.forEach(
                    (pipelineLocation, profiles) ->
                            profiles.forEach(
                                    (taskGroupLocation, slotProfile) ->
                                            attemptOwnedSlotProfiles.put(
                                                    taskGroupLocation,
                                                    new AttemptOwnedSlotProfile(
                                                            attemptSequence, slotProfile))));
        }
        persistRetainedSlotOwnership(profilesByPipeline);
    }

    /**
     * Writes the allocations retained by a failed admission round back to the distributed ownership
     * record so a successor master can adopt them.
     *
     * <p>The write of this round failed, but the failure may be transient and the retained
     * allocations are alive on their workers. A successor master can only reuse or release them
     * through this record, so a bounded retry here is what keeps the capacity recoverable after the
     * master that allocated it dies. When the record still rejects the write, the local ledger and
     * the live allocations are kept as evidence: releasing them on that path would hand a slot that
     * a successor may already have adopted to another job.
     *
     * @param profilesByPipeline retained allocations, keyed by pipeline
     */
    private void persistRetainedSlotOwnership(
            Map<PipelineLocation, Map<TaskGroupLocation, SlotProfile>> profilesByPipeline) {
        List<PipelineLocation> unrecordedPipelines = new ArrayList<>();
        profilesByPipeline.forEach(
                (pipelineLocation, slotProfiles) -> {
                    if (!persistRetainedPipelineOwnership(pipelineLocation, slotProfiles)) {
                        unrecordedPipelines.add(pipelineLocation);
                    }
                });
        if (!unrecordedPipelines.isEmpty()) {
            LOGGER.warning(
                    String.format(
                            "Could not record the retained allocations of pipelines %s of job %d after %d attempts: a successor master cannot adopt these slots until the ownership map accepts a write",
                            unrecordedPipelines,
                            jobImmutableInformation.getJobId(),
                            RETAINED_SLOT_OWNERSHIP_PERSIST_ATTEMPT));
        }
    }

    /**
     * Records the retained allocation of one pipeline in the distributed ownership map.
     *
     * @param pipelineLocation pipeline whose retained allocation is recorded
     * @param slotProfiles retained profile per task group of the pipeline
     * @return true when the record was written and read back unchanged
     */
    private boolean persistRetainedPipelineOwnership(
            PipelineLocation pipelineLocation, Map<TaskGroupLocation, SlotProfile> slotProfiles) {
        for (int attempt = 1; attempt <= RETAINED_SLOT_OWNERSHIP_PERSIST_ATTEMPT; attempt++) {
            try {
                if (setOwnedSlotProfiles(pipelineLocation, slotProfiles)) {
                    return true;
                }
            } catch (Exception e) {
                LOGGER.fine(
                        String.format(
                                "Attempt %d to record the retained allocation of pipeline %s failed: %s",
                                attempt, pipelineLocation, ExceptionUtils.getMessage(e)));
            }
            if (attempt < RETAINED_SLOT_OWNERSHIP_PERSIST_ATTEMPT) {
                try {
                    Thread.sleep(RETAINED_SLOT_OWNERSHIP_PERSIST_RETRY_SLEEP_MS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    return false;
                }
            }
        }
        return false;
    }

    /**
     * Drops the retained slots this round admitted into the plan, which must not be handed out to a
     * later round.
     *
     * @param preApplyResourceFutures allocation futures of the successful pre-apply round
     */
    private void clearAdmittedAttemptOwnedSlotProfiles(
            Map<TaskGroupLocation, CompletableFuture<SlotProfile>> preApplyResourceFutures) {
        preApplyResourceFutures.forEach(
                (taskGroupLocation, future) -> {
                    AttemptOwnedSlotProfile retained =
                            attemptOwnedSlotProfiles.get(taskGroupLocation);
                    if (retained == null || future == null) {
                        return;
                    }
                    SlotProfile admittedSlot = future.getNow(null);
                    if (retained.getSlotProfile().equals(admittedSlot)) {
                        attemptOwnedSlotProfiles.remove(taskGroupLocation, retained);
                    }
                });
    }

    /**
     * Drops the retained slots of a pipeline whose slots are being released, so a later round
     * cannot reuse a slot that no longer belongs to this job.
     *
     * @param pipelineLocation pipeline whose slots are released
     */
    private void discardAttemptOwnedSlotProfiles(PipelineLocation pipelineLocation) {
        attemptOwnedSlotProfiles
                .keySet()
                .removeIf(
                        taskGroupLocation ->
                                taskGroupLocation.getJobId() == pipelineLocation.getJobId()
                                        && taskGroupLocation.getPipelineId()
                                                == pipelineLocation.getPipelineId());
    }

    /**
     * Releases the fixed slots that failed admission rounds kept allocated to this job when the job
     * ends without ever deploying them.
     *
     * <p>A pending job that is cancelled or rejected completes its physical plan directly, so the
     * pipeline teardown that normally returns slots never runs. Without this drain the retained
     * slots stay owned by a job that will never use them, and repeated failed or cancelled
     * admissions can exhaust the fixed capacity of the cluster. Clearing the attempt ledger alone
     * is not a release, so the recorded profiles are released to their workers.
     *
     * <p>Only the master that retained the slots drains them: an active-master switch interrupts
     * this instance, and the persisted ownership records are then the successor's recovery path, so
     * nothing may be released here in that case. A profile is forgotten only after its worker
     * reports it inactive; an unconfirmed release keeps both the ledger entry and the ownership
     * record as evidence instead of dropping the last handle on the allocation.
     *
     * @return true when the job holds no retained admission slot any more, either because every
     *     profile was released and forgotten or because nothing is left to release; false when a
     *     release could not be confirmed and the job still owns capacity
     */
    private boolean drainAdmissionOwnedSlots() {
        synchronized (admissionSlotCleanupLock) {
            if (admissionSlotsDrained) {
                return true;
            }
            if (attemptOwnedSlotProfiles.isEmpty()) {
                admissionSlotsDrained = true;
                return true;
            }
            if (!isRunning) {
                LOGGER.info(
                        String.format(
                                "Job %d was interrupted before its retained admission slots could be released, keeping them for the successor master",
                                jobImmutableInformation.getJobId()));
                return false;
            }
            Map<PipelineLocation, Map<TaskGroupLocation, SlotProfile>> profilesByPipeline =
                    new HashMap<>();
            attemptOwnedSlotProfiles.forEach(
                    (taskGroupLocation, retained) ->
                            profilesByPipeline
                                    .computeIfAbsent(
                                            new PipelineLocation(
                                                    taskGroupLocation.getJobId(),
                                                    taskGroupLocation.getPipelineId()),
                                            key -> new HashMap<>())
                                    .put(taskGroupLocation, retained.getSlotProfile()));
            admissionSlotsDrained = releaseAdmissionOwnedSlots(profilesByPipeline);
            return admissionSlotsDrained;
        }
    }

    /**
     * Re-runs the terminal release of the admission-owned slots of a job whose release could not be
     * confirmed.
     *
     * <p>{@link #cleanJob()} is the only production caller of the drain and it runs once, so a
     * transient release outage would otherwise leave the capacity of a finished job held for good:
     * a later cancel of the same job only sees an end state and returns. This chain keeps the
     * ledger entry and the ownership record as evidence until a release is confirmed, and stops
     * after a bounded number of attempts instead of retrying an unreachable worker forever.
     *
     * @param attempt number of this retry, starting at one
     */
    private void scheduleAdmissionSlotCleanupRetry(int attempt) {
        ScheduledExecutorService cleanupScheduler =
                seaTunnelServer == null ? null : seaTunnelServer.getMonitorService();
        if (cleanupScheduler == null) {
            LOGGER.warning(
                    String.format(
                            "No scheduler is available to retry the retained admission slot release of job %d, keeping %d slot(s) retained",
                            jobImmutableInformation.getJobId(), attemptOwnedSlotProfiles.size()));
            return;
        }
        cleanupScheduler.schedule(
                () -> {
                    try {
                        if (drainAdmissionOwnedSlots()) {
                            LOGGER.info(
                                    String.format(
                                            "Retained admission slots of job %d were released by retry %d",
                                            jobImmutableInformation.getJobId(), attempt));
                            return;
                        }
                    } catch (Exception e) {
                        LOGGER.warning(
                                String.format(
                                        "Retry %d to release the retained admission slots of job %d failed: %s",
                                        attempt,
                                        jobImmutableInformation.getJobId(),
                                        ExceptionUtils.getMessage(e)));
                    }
                    if (!isRunning) {
                        LOGGER.info(
                                String.format(
                                        "Job %d no longer runs on this master, leaving the retained admission slots to the successor",
                                        jobImmutableInformation.getJobId()));
                        return;
                    }
                    if (attempt >= ADMISSION_SLOT_CLEANUP_RETRY_TIME) {
                        LOGGER.warning(
                                String.format(
                                        "Giving up after %d attempts to release the retained admission slots of job %d; the ownership record keeps them for a later cleanup",
                                        attempt, jobImmutableInformation.getJobId()));
                        return;
                    }
                    scheduleAdmissionSlotCleanupRetry(attempt + 1);
                },
                ADMISSION_SLOT_CLEANUP_RETRY_DELAY_MS,
                TimeUnit.MILLISECONDS);
    }

    /**
     * Releases admission-owned profiles and forgets them once the release is confirmed.
     *
     * @param profilesByPipeline profiles retained by failed admission rounds, grouped by pipeline
     * @return true when every profile was released and is no longer active on its worker
     */
    private boolean releaseAdmissionOwnedSlots(
            Map<PipelineLocation, Map<TaskGroupLocation, SlotProfile>> profilesByPipeline) {
        List<SlotProfile> profiles = new ArrayList<>();
        profilesByPipeline
                .values()
                .forEach(pipelineProfiles -> profiles.addAll(pipelineProfiles.values()));
        if (profiles.isEmpty()) {
            return true;
        }
        try {
            RetryUtils.retryWithException(
                    () -> {
                        resourceManager
                                .releaseResources(jobImmutableInformation.getJobId(), profiles)
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
                            "Could not release the retained admission slots %s of job %d, keeping them for a later cleanup: %s",
                            profiles,
                            jobImmutableInformation.getJobId(),
                            ExceptionUtils.getMessage(e)));
            return false;
        }

        boolean allReleased = true;
        for (Map.Entry<PipelineLocation, Map<TaskGroupLocation, SlotProfile>> entry :
                profilesByPipeline.entrySet()) {
            for (Map.Entry<TaskGroupLocation, SlotProfile> profileEntry :
                    entry.getValue().entrySet()) {
                SlotProfile slotProfile = profileEntry.getValue();
                if (resourceManager.slotActiveCheck(slotProfile)) {
                    allReleased = false;
                    LOGGER.warning(
                            String.format(
                                    "Slot %s of task group %s for job %d is still active after its release, keeping its ownership",
                                    slotProfile,
                                    profileEntry.getKey(),
                                    jobImmutableInformation.getJobId()));
                    continue;
                }
                attemptOwnedSlotProfiles.remove(profileEntry.getKey());
                removeOwnedSlotProfileRecord(entry.getKey(), profileEntry.getKey());
            }
        }
        return allReleased;
    }

    /**
     * Drops the ownership record of one released task group, so a later restore does not adopt a
     * slot that no longer belongs to the job.
     *
     * @param pipelineLocation pipeline owning the record
     * @param taskGroupLocation released task group to remove from the record
     */
    private void removeOwnedSlotProfileRecord(
            PipelineLocation pipelineLocation, TaskGroupLocation taskGroupLocation) {
        try {
            Map<TaskGroupLocation, SlotProfile> ownedSlotProfiles =
                    ownedSlotProfilesIMap.get(pipelineLocation);
            if (ownedSlotProfiles == null) {
                return;
            }
            Map<TaskGroupLocation, SlotProfile> remaining = new HashMap<>(ownedSlotProfiles);
            if (remaining.remove(taskGroupLocation) == null) {
                return;
            }
            if (remaining.isEmpty()) {
                ownedSlotProfilesIMap.remove(pipelineLocation);
            } else {
                ownedSlotProfilesIMap.put(pipelineLocation, remaining);
            }
        } catch (Exception e) {
            LOGGER.warning(
                    String.format(
                            "Could not remove the ownership record of released task group %s for job %d: %s",
                            taskGroupLocation,
                            jobImmutableInformation.getJobId(),
                            ExceptionUtils.getMessage(e)));
        }
    }

    /**
     * Returns the slot this master retained for the task group after an earlier failed admission
     * attempt, when that slot can still be reused.
     *
     * <p>The record is bound to the attempt that kept it: it is reusable only by a later attempt of
     * the same master, which owns the ledger, and only while the worker still reports the same
     * allocation sequence and owner job. A slot whose allocation sequence was reassigned, which
     * also covers a reassignment to another task group of the same job, is not reused, and its
     * record is dropped so it cannot be handed out again.
     *
     * @param taskGroupLocation task group whose retained slot is checked
     * @return the retained {@link SlotProfile} when it is still active for this job, otherwise
     *     {@code null}
     */
    private SlotProfile getAttemptOwnedSlotProfile(TaskGroupLocation taskGroupLocation) {
        AttemptOwnedSlotProfile retained = attemptOwnedSlotProfiles.get(taskGroupLocation);
        if (retained == null
                || admissionSlotsDrained
                || retained.getAttemptSequence() >= currentPreApplyAttemptSequence) {
            return null;
        }
        SlotProfile slotProfile = retained.getSlotProfile();
        if (!resourceManager.slotActiveCheck(slotProfile)) {
            attemptOwnedSlotProfiles.remove(taskGroupLocation, retained);
            LOGGER.warning(
                    String.format(
                            "Retained slot %s of task group %s for job %d is no longer active, dropping it",
                            slotProfile, taskGroupLocation, jobImmutableInformation.getJobId()));
            return null;
        }
        return slotProfile;
    }

    /**
     * Releases the slots a pre-apply acquired but will not use.
     *
     * <p>Slots retained from the previous master are skipped: they are already accounted for by the
     * failover restore path.
     *
     * @param preApplyResourceFutures allocation futures of this pre-apply round
     * @param reusedSlotProfiles slots retained from the previous master
     */
    private void releaseUnusedPreAppliedResources(
            Map<TaskGroupLocation, CompletableFuture<SlotProfile>> preApplyResourceFutures,
            Set<SlotProfile> reusedSlotProfiles) {
        try {
            RetryUtils.retryWithException(
                    () -> {
                        resourceManager
                                .releaseResources(
                                        jobImmutableInformation.getJobId(),
                                        preApplyResourceFutures.entrySet().stream()
                                                .filter(
                                                        entry -> {
                                                            try {
                                                                CompletableFuture<SlotProfile>
                                                                        value = entry.getValue();
                                                                return value != null
                                                                        && value.join() != null;
                                                            } catch (CompletionException e) {
                                                                LOGGER.warning(
                                                                        String.format(
                                                                                "Filtering failed resource for job %s during release: %s",
                                                                                jobImmutableInformation
                                                                                        .getJobId(),
                                                                                e.getCause() != null
                                                                                        ? e.getCause()
                                                                                                .getMessage()
                                                                                        : e
                                                                                                .getMessage()));
                                                                return false;
                                                            }
                                                        })
                                                .map(entry -> entry.getValue().join())
                                                .filter(
                                                        slotProfile ->
                                                                !reusedSlotProfiles.contains(
                                                                        slotProfile))
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
                            "Pre resource application failed %s", ExceptionUtils.getMessage(e)));
        }
    }

    /**
     * Pre-applies resources for every pipeline in this job.
     *
     * <p>Both maps are populated in place; there is nothing to return because the caller already
     * holds the references it passed in.
     *
     * @param preApplyResourceFutures target map for each task group's resource future, mutated in
     *     place
     * @param reusedSlotProfiles set collecting slots retained from the previous master; these slots
     *     must not be released when part of the pre-apply operation fails
     */
    private void preApplyResourcesForAll(
            Map<TaskGroupLocation, CompletableFuture<SlotProfile>> preApplyResourceFutures,
            Set<SlotProfile> reusedSlotProfiles) {
        for (SubPlan subPlan : physicalPlan.getPipelineList()) {
            preApplyResourcesForSubPlan(subPlan, preApplyResourceFutures, reusedSlotProfiles);
        }
    }

    /**
     * Pre-applies resources for one pipeline.
     *
     * @param subPlan pipeline whose task groups need resources
     * @param preApplyResourceFutures target map for each task group's resource future, mutated in
     *     place
     * @param reusedSlotProfiles set collecting slots retained from the previous master; these slots
     *     must not be released when part of the pre-apply operation fails
     */
    private void preApplyResourcesForSubPlan(
            SubPlan subPlan,
            Map<TaskGroupLocation, CompletableFuture<SlotProfile>> preApplyResourceFutures,
            Set<SlotProfile> reusedSlotProfiles) {

        Map<TaskGroupLocation, CompletableFuture<SlotProfile>> coordinatorFutures = new HashMap<>();
        subPlan.getCoordinatorVertexList()
                .forEach(
                        coordinator -> {
                            SlotProfile reusableSlot =
                                    getReusableSlot(coordinator.getTaskGroupLocation());
                            coordinatorFutures.put(
                                    coordinator.getTaskGroupLocation(),
                                    reusableSlot == null
                                            ? ResourceUtils.applyResourceForTask(
                                                    resourceManager, coordinator, subPlan.getTags())
                                            : CompletableFuture.completedFuture(reusableSlot));
                            if (reusableSlot != null) {
                                reusedSlotProfiles.add(reusableSlot);
                            }
                        });

        Map<TaskGroupLocation, CompletableFuture<SlotProfile>> taskFutures = new HashMap<>();
        subPlan.getPhysicalVertexList()
                .forEach(
                        task -> {
                            SlotProfile reusableSlot = getReusableSlot(task.getTaskGroupLocation());
                            taskFutures.put(
                                    task.getTaskGroupLocation(),
                                    reusableSlot == null
                                            ? ResourceUtils.applyResourceForTask(
                                                    resourceManager, task, subPlan.getTags())
                                            : CompletableFuture.completedFuture(reusableSlot));
                            if (reusableSlot != null) {
                                reusedSlotProfiles.add(reusableSlot);
                            }
                        });

        preApplyResourceFutures.putAll(coordinatorFutures);
        preApplyResourceFutures.putAll(taskFutures);
        LOGGER.fine("preApplyResourceFutures size: " + preApplyResourceFutures.size());
    }

    /**
     * Reuses an active slot this job already holds, so a resource application does not ask the
     * workers for capacity the job cannot obtain.
     *
     * <p>A slot retained by a failed admission attempt of this master is checked first: the
     * ownership record and the allocation survived the failed round, so the retry of the same
     * master must keep using them. This is not a cross-master or cross-job reuse: the ledger lives
     * in one {@link JobMaster}, is keyed by the task group, and the slot is only handed out after
     * {@link ResourceManager#slotActiveCheck(SlotProfile)} confirms the same worker allocation
     * sequence and owner job.
     *
     * <p>Otherwise the slot-to-task mapping stored in Hazelcast before task deployment is used, but
     * only after a master failover. Requesting those slots again cannot succeed with fixed slots
     * because Workers still own them for this job.
     *
     * @param taskGroupLocation task group whose previously retained or persisted slot assignment is
     *     checked
     * @return the retained or persisted {@link SlotProfile} if the Worker still actively holds it
     *     for this job, or {@code null} if no such slot can be reused
     */
    private SlotProfile getReusableSlot(TaskGroupLocation taskGroupLocation) {
        SlotProfile attemptOwnedSlot = getAttemptOwnedSlotProfile(taskGroupLocation);
        if (attemptOwnedSlot != null) {
            return attemptOwnedSlot;
        }
        if (!masterFailoverRestore) {
            return null;
        }
        SlotProfile slotProfile = getOwnedSlotProfiles(taskGroupLocation);
        return slotProfile != null && resourceManager.slotActiveCheck(slotProfile)
                ? slotProfile
                : null;
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

    public static void handleSaveMode(SeaTunnelSink sink, boolean isStartWithSavePoint) {
        if (sink instanceof SupportSaveMode) {
            Optional<SaveModeHandler> saveModeHandler =
                    ((SupportSaveMode) sink).getSaveModeHandler();
            if (saveModeHandler.isPresent()) {
                try (SaveModeHandler handler = saveModeHandler.get()) {
                    handler.open();
                    if (!isStartWithSavePoint) {
                        new SaveModeExecuteWrapper(handler).execute();
                    } else {
                        handler.handleSchemaSaveModeWithRestore();
                    }
                } catch (Exception e) {
                    throw new SeaTunnelRuntimeException(HANDLE_SAVE_MODE_FAILED, e);
                }
            }
        } else if (sink instanceof MultiTableSink) {
            MultiTableSink multiTableSink = (MultiTableSink) sink;
            Map<TablePath, SeaTunnelSink> sinks = multiTableSink.getSinks();
            if (!multiTableSink.getFailurePolicy().continueOtherTables()) {
                for (SeaTunnelSink seaTunnelSink : sinks.values()) {
                    handleSaveMode(seaTunnelSink, isStartWithSavePoint);
                }
                return;
            }

            List<MultiTableFailedTable> failedTables = new ArrayList<>();
            for (Map.Entry<TablePath, SeaTunnelSink> entry : new ArrayList<>(sinks.entrySet())) {
                try {
                    handleSaveMode(entry.getValue(), isStartWithSavePoint);
                } catch (RuntimeException error) {
                    MultiTableFailedTable failedTable =
                            MultiTableFailureHelper.buildFailedTable(
                                    entry.getKey().getFullName(),
                                    MultiTableFailurePhase.SAVE_MODE,
                                    entry.getValue().getPluginName(),
                                    error);
                    failedTables.add(failedTable);
                    LOGGER.warning(
                            "Skip failed sink table during cluster save mode: "
                                    + MultiTableFailureHelper.formatFailedTableLine(failedTable),
                            error);
                }
            }
            if (failedTables.isEmpty()) {
                return;
            }

            failedTables.forEach(
                    failedTable ->
                            multiTableSink.removeSink(TablePath.of(failedTable.getTablePath())));
            multiTableSink.registerInitialFailedTables(failedTables);
            if (multiTableSink.getSinks().isEmpty()) {
                throw new SeaTunnelRuntimeException(
                        HANDLE_SAVE_MODE_FAILED,
                        new IllegalStateException(
                                MultiTableFailureHelper.formatFailedTableSummary(
                                        "All candidate sink tables were skipped during cluster save mode.",
                                        failedTables)));
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

    private JobCleanupRecord createJobCleanupRecord() {
        Long jobId = getJobImmutableInformation().getJobId();
        Set<Object> stateKeys = new LinkedHashSet<>();
        Set<Object> timestampKeys = new LinkedHashSet<>();
        stateKeys.add(jobId);
        timestampKeys.add(jobId);

        getPhysicalPlan()
                .getPipelineList()
                .forEach(
                        pipeline -> {
                            stateKeys.add(pipeline.getPipelineLocation());
                            timestampKeys.add(pipeline.getPipelineLocation());
                            pipeline.getCoordinatorVertexList()
                                    .forEach(
                                            coordinator -> {
                                                stateKeys.add(coordinator.getTaskGroupLocation());
                                                timestampKeys.add(
                                                        coordinator.getTaskGroupLocation());
                                            });

                            pipeline.getPhysicalVertexList()
                                    .forEach(
                                            task -> {
                                                stateKeys.add(task.getTaskGroupLocation());
                                                timestampKeys.add(task.getTaskGroupLocation());
                                            });

                            if (checkpointManager != null) {
                                CheckpointCoordinator checkpointCoordinator =
                                        checkpointManager.getCheckpointCoordinator(
                                                pipeline.getPipelineId());
                                stateKeys.add(checkpointCoordinator.getCheckpointStateImapKey());
                                stateKeys.add(checkpointCoordinator.getReadyToCloseImapKey());
                            }
                        });
        return new JobCleanupRecord(
                initializationTimestamp,
                physicalPlan.getJobStatus(),
                stateKeys,
                timestampKeys,
                System.currentTimeMillis());
    }

    private void scheduleRemoveJobStateMaps() {
        Long jobId = getJobImmutableInformation().getJobId();
        JobCleanupRecord cleanupRecord = createJobCleanupRecord();
        IMap<Long, JobCleanupRecord> pendingJobCleanupIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_PENDING_JOB_CLEANUP);
        pendingJobCleanupIMap.put(jobId, cleanupRecord);

        CoordinatorService coordinatorService = seaTunnelServer.getCoordinatorService();
        if (coordinatorService == null) {
            LOGGER.warning(String.format("Skip delayed cleanup scheduling for job %s", jobId));
            return;
        }
        coordinatorService.schedulePendingJobCleanup(jobId, cleanupRecord);
    }

    /**
     * Lazily build and cache {@link JobDAGInfo} for REST/UI callers.
     *
     * <p>This uses the classic double-check locking pattern with a {@code volatile} field to avoid
     * repeated expensive DAG reconstruction while keeping the hot path lock-free after
     * initialization.
     *
     * <p>Thread-safety notes:
     *
     * <ul>
     *   <li>{@code jobDAGInfo} is {@code volatile}, so once assigned, all threads will observe the
     *       fully constructed reference.
     *   <li>The initialization is guarded by {@code synchronized (this)} to ensure at most one
     *       build happens.
     * </ul>
     */
    public JobDAGInfo getJobDAGInfo() {
        JobDAGInfo local = jobDAGInfo;
        if (local == null) {
            synchronized (this) {
                local = jobDAGInfo;
                if (local == null) {
                    local =
                            DAGUtils.getJobDAGInfo(
                                    logicalDag,
                                    jobImmutableInformation,
                                    engineConfig,
                                    isPhysicalDAGInfo,
                                    new ExecutionAddress(
                                            this.nodeEngine.getThisAddress().getHost(),
                                            this.nodeEngine.getThisAddress().getPort()),
                                    historyExecutionAddress);
                    jobDAGInfo = local;
                }
            }
        }
        return local;
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
        // The slot is being handed back, so the retry ledger must not offer it again.
        attemptOwnedSlotProfiles.remove(taskGroupLocation);

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
            // The slots of this pipeline are about to be released, so no later round may reuse
            // them as retained slots of this master.
            discardAttemptOwnedSlotProfiles(subPlan.getPipelineLocation());
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
        // A job cancelled or rejected before it ever ran completes its physical plan directly, so
        // the pipeline teardown that normally returns slots never runs for the slots retained by
        // failed admission attempts. Release them here instead of only dropping the ledger, and
        // keep retrying in the background when the release cannot be confirmed: this is the last
        // cleanup owner of the job.
        if (!drainAdmissionOwnedSlots() && isRunning) {
            scheduleAdmissionSlotCleanupRetry(1);
        }
        checkpointManager.clearCheckpointIfNeed(physicalPlan.getJobStatus());
        jobHistoryService.storeJobInfo(jobImmutableInformation.getJobId(), getJobDAGInfo());
        jobHistoryService.storeFinishedJobState(this);
        scheduleRemoveJobStateMaps();
    }

    public void storeJobEndState() {
        jobHistoryService.storeFinishedJobState(this);
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

    public synchronized void stopJob() {
        physicalPlan.stopJob();
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

    public Long getStateTimestamp(@NonNull JobStatus jobStatus) {
        return physicalPlan.getStateTimestamp(jobStatus);
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
        synchronized (metricsLock) {
            jobHistoryService.storeFinishedPipelineMetrics(jobId, jobMetrics);
        }
        // Clean TaskGroupContext for TaskExecutionServer
        this.cleanTaskGroupContext(pipelineLocation);
    }

    public void enqueuePipelineCleanupIfNeeded(
            PipelineLocation pipelineLocation, PipelineStatus pipelineStatus) {
        if (pipelineLocation == null || pipelineStatus == null) {
            return;
        }
        boolean savepointEnd =
                PipelineStatus.FINISHED.equals(pipelineStatus)
                        && checkpointManager != null
                        && checkpointManager.isPipelineSavePointEnd(pipelineLocation);
        // Failed pipelines also need cleanup so their distributed metrics do not leak into later
        // task recovery or re-submission flows.
        boolean shouldCleanup =
                PipelineStatus.FAILED.equals(pipelineStatus)
                        || PipelineStatus.CANCELED.equals(pipelineStatus)
                        || (PipelineStatus.FINISHED.equals(pipelineStatus) && !savepointEnd);
        if (!shouldCleanup) {
            return;
        }

        Map<TaskGroupLocation, SlotProfile> slotProfileMap =
                ownedSlotProfilesIMap.get(pipelineLocation);
        Map<TaskGroupLocation, Address> taskGroups = new HashMap<>();
        if (slotProfileMap != null) {
            slotProfileMap.forEach(
                    (taskGroupLocation, slotProfile) ->
                            taskGroups.put(taskGroupLocation, slotProfile.getWorker()));
        }

        IMap<PipelineLocation, PipelineCleanupRecord> pendingCleanupIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_PENDING_PIPELINE_CLEANUP);
        long now = System.currentTimeMillis();
        PipelineCleanupRecord newRecord =
                new PipelineCleanupRecord(
                        pipelineLocation,
                        pipelineStatus,
                        savepointEnd,
                        taskGroups,
                        Collections.emptySet(),
                        false,
                        now,
                        0,
                        0);

        while (true) {
            PipelineCleanupRecord existing = pendingCleanupIMap.get(pipelineLocation);
            if (existing == null) {
                PipelineCleanupRecord prev =
                        pendingCleanupIMap.putIfAbsent(pipelineLocation, newRecord);
                if (prev == null) {
                    return;
                }
                existing = prev;
            }
            PipelineCleanupRecord merged = existing.mergeFrom(newRecord);
            if (merged.equals(existing)) {
                return;
            }
            if (pendingCleanupIMap.replace(pipelineLocation, existing, merged)) {
                return;
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void removeMetricsContext(
            PipelineLocation pipelineLocation, PipelineStatus pipelineStatus) {
        if (pipelineStatus.equals(PipelineStatus.FAILED)
                || (pipelineStatus.equals(PipelineStatus.FINISHED)
                        && !checkpointManager.isPipelineSavePointEnd(pipelineLocation))
                || pipelineStatus.equals(PipelineStatus.CANCELED)) {

            removePipelineMetrics(pipelineLocation);
        }
    }

    /**
     * Clears runtime metrics from a failed pipeline attempt before it is restored.
     *
     * <p>Task locations are stable across retries, so keeping the old snapshots would make the
     * replacement attempt continue counting from the failed attempt's values.
     *
     * <p>The failed attempt's task-group contexts are cleaned first: a context that survives the
     * wipe can re-populate the metrics store afterwards, defeating the reset. The reset is best
     * effort: a worker report that was collected before its context was cleaned can still land
     * right after the wipe, so at most such a millisecond-scale window can carry stale values.
     *
     * @param pipelineLocation pipeline that is about to be restored
     */
    public void clearPipelineMetricsForRestore(PipelineLocation pipelineLocation) {
        try {
            cleanTaskGroupContext(pipelineLocation);
        } catch (Exception e) {
            LOGGER.warning(
                    "Failed to clean task-group contexts before restoring " + pipelineLocation, e);
        }
        removePipelineMetrics(pipelineLocation);
    }

    /**
     * Removes the pipeline's metrics from the metrics store. Best effort: a failure is logged and
     * never propagated, so neither the terminal cleanup nor a pipeline restore is blocked.
     */
    private void removePipelineMetrics(PipelineLocation pipelineLocation) {
        try {
            seaTunnelServer.removeMetrics(pipelineLocation);
        } catch (Exception e) {
            LOGGER.severe(
                    String.format("failed to remove metrics for pipeline %s", pipelineLocation), e);
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
                    } catch (Exception e) {
                        // Best effort per task group: one unreachable worker must not stop the
                        // remaining contexts from being cleaned, or those contexts would
                        // re-publish the failed attempt's metrics after the store was cleared.
                        if (e instanceof InterruptedException) {
                            Thread.currentThread().interrupt();
                        }
                        LOGGER.warning(
                                String.format(
                                        "%s clean TaskGroupContext with exception: %s.",
                                        taskGroupLocation, ExceptionUtils.getMessage(e)));
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
        return CompletableFuture.supplyAsync(
                () -> {
                    boolean savepointCompleted = false;
                    SavepointCompletionResult savepointCompletionResult = null;
                    try {
                        physicalPlan.savepointJob();
                        PassiveCompletableFuture<CheckpointCoordinatorState>[]
                                passiveCompletableFutures =
                                        checkpointManager.triggerSavePointsAndWaitComplete();
                        savepointCompletionResult =
                                waitSavepointCompleted(passiveCompletableFutures);
                        savepointCompleted = savepointCompletionResult.isCompleted();
                        Optional<Exception> failureException =
                                getSavepointFailureException(savepointCompletionResult);
                        if (failureException.isPresent()) {
                            throw new SeaTunnelEngineException(failureException.get());
                        }
                        return savepointCompleted;
                    } finally {
                        if (!savepointCompleted) {
                            if (isSavepointStartPreconditionFailure(savepointCompletionResult)) {
                                LOGGER.info(
                                        String.format(
                                                "Savepoint for Job %s (%s) could not start because the checkpoint coordinator is not ready; restore job status to RUNNING for retry.",
                                                jobImmutableInformation.getJobConfig().getName(),
                                                jobImmutableInformation.getJobId()));
                                restoreRunningAfterSavepointStartFailure();
                            } else {
                                // At least one pipeline failed its final checkpoint. Some other
                                // pipelines may already have completed the savepoint and stopped,
                                // so the job must leave DOING_SAVEPOINT through a deterministic
                                // stop path instead of being reported as RUNNING.
                                physicalPlan.savepointFailed();
                            }
                        }
                    }
                });
    }

    private Optional<Exception> getSavepointFailureException(
            SavepointCompletionResult savepointCompletionResult) {
        Optional<Exception> nonPreconditionFailure =
                savepointCompletionResult.getExceptions().stream()
                        .filter(exception -> !isSavepointStartPreconditionException(exception))
                        .findFirst();
        return nonPreconditionFailure.isPresent()
                ? nonPreconditionFailure
                : savepointCompletionResult.getFirstException();
    }

    private void restoreRunningAfterSavepointStartFailure() {
        if (physicalPlan.getJobStatus() == JobStatus.DOING_SAVEPOINT) {
            physicalPlan.updateJobState(JobStatus.RUNNING);
        }
    }

    /**
     * Waits for every pipeline savepoint future and preserves all failures for the job-level
     * decision.
     *
     * <p>A stop-with-savepoint request fans out to independent checkpoint coordinators. The cleanup
     * decision must therefore be based on the whole result set, not on whichever pipeline fails
     * first or appears first in the pipeline list.
     */
    private SavepointCompletionResult waitSavepointCompleted(
            PassiveCompletableFuture<CheckpointCoordinatorState>[] passiveCompletableFutures) {
        try {
            CompletableFuture.allOf(passiveCompletableFutures).join();
        } catch (Exception e) {
            // Inspect every pipeline future below so a fast failure does not short-circuit the
            // stop-with-savepoint decision while another pipeline is still completing.
            LOGGER.fine(
                    "Savepoint aggregate future completed exceptionally; inspect every pipeline future before deciding stop-with-savepoint result",
                    e);
        }

        boolean savepointCompleted = true;
        boolean anyPipelineSuspended = false;
        List<Exception> exceptions = new ArrayList<>();
        for (PassiveCompletableFuture<CheckpointCoordinatorState> future :
                passiveCompletableFutures) {
            try {
                CheckpointCoordinatorState state = future.get();
                if (state == null
                        || state.getCheckpointCoordinatorStatus()
                                != CheckpointCoordinatorStatus.SUSPEND) {
                    savepointCompleted = false;
                } else {
                    anyPipelineSuspended = true;
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                savepointCompleted = false;
                exceptions.add(e);
            } catch (Exception e) {
                savepointCompleted = false;
                exceptions.add(e);
            }
        }
        return new SavepointCompletionResult(savepointCompleted, exceptions, anyPipelineSuspended);
    }

    /**
     * Returns true only when the savepoint failed before any pipeline could start a checkpoint.
     *
     * <p>These pre-start failures are safe to retry: no pipeline has reached {@link
     * CheckpointCoordinatorStatus#SUSPEND}, and every reported failure reason comes from a
     * checkpoint coordinator that rejected the request before creating a pending checkpoint. Any
     * genuine checkpoint failure, partial suspension, or non-exceptional non-suspend state must use
     * the deterministic stop fallback instead.
     */
    private boolean isSavepointStartPreconditionFailure(
            SavepointCompletionResult savepointCompletionResult) {
        if (savepointCompletionResult == null
                || savepointCompletionResult.isAnyPipelineSuspended()
                || savepointCompletionResult.getExceptions().isEmpty()) {
            return false;
        }

        return savepointCompletionResult.getExceptions().stream()
                .allMatch(this::isSavepointStartPreconditionException);
    }

    /**
     * Returns true for checkpoint coordinator failures that happen before a savepoint checkpoint is
     * created.
     */
    private boolean isSavepointStartPreconditionException(Exception exception) {
        Throwable rootException = ExceptionUtils.getRootException(exception);
        if (!(rootException instanceof CheckpointException)) {
            return false;
        }

        CheckpointCloseReason failureReason =
                ((CheckpointException) rootException).getCheckpointFailureReason();
        return failureReason == CheckpointCloseReason.TASK_NOT_ALL_READY_WHEN_SAVEPOINT
                || failureReason == CheckpointCloseReason.CHECKPOINT_COORDINATOR_SHUTDOWN;
    }

    /**
     * Aggregated outcome of all pipeline savepoint futures.
     *
     * <p>The job can be restored to RUNNING only when all failures are retryable pre-start
     * rejections. A single genuine checkpoint failure or already-suspended pipeline means some
     * pipeline state may have changed, so the job must use the stop fallback.
     */
    private static class SavepointCompletionResult {

        private final boolean completed;
        private final List<Exception> exceptions;
        private final boolean anyPipelineSuspended;

        private SavepointCompletionResult(
                boolean completed, List<Exception> exceptions, boolean anyPipelineSuspended) {
            this.completed = completed;
            this.exceptions = exceptions;
            this.anyPipelineSuspended = anyPipelineSuspended;
        }

        private boolean isCompleted() {
            return completed;
        }

        private Optional<Exception> getFirstException() {
            return exceptions.stream().findFirst();
        }

        private List<Exception> getExceptions() {
            return exceptions;
        }

        private boolean isAnyPipelineSuspended() {
            return anyPipelineSuspended;
        }
    }

    /**
     * Writes the task-group to slot mapping of a pipeline and confirms it by reading it back.
     *
     * <p>A returned {@code put} only means the write was acknowledged, not that the mapping is now
     * stored: the value still has to read back unchanged. The read-back is retried a bounded number
     * of times, and this method reports whether one of those reads confirmed the written value.
     * Callers that decide whether a job may be admitted must inspect the return value, because an
     * unconfirmed write is not ownership they can rely on after a master failover.
     *
     * <p>Callers that only record the mapping for the later scheduling path may ignore the result;
     * the write and the confirmation attempt are still performed in that case.
     *
     * @param pipelineLocation pipeline whose ownership is written
     * @param pipelineOwnedSlotProfiles complete task-group to slot mapping of the pipeline
     * @return true only when the written mapping was read back unchanged
     */
    public boolean setOwnedSlotProfiles(
            @NonNull PipelineLocation pipelineLocation,
            @NonNull Map<TaskGroupLocation, SlotProfile> pipelineOwnedSlotProfiles) {
        ownedSlotProfilesIMap.put(pipelineLocation, pipelineOwnedSlotProfiles);
        try {
            Boolean confirmed =
                    RetryUtils.retryWithException(
                            () -> {
                                if (!pipelineOwnedSlotProfiles.equals(
                                        ownedSlotProfilesIMap.get(pipelineLocation))) {
                                    throw new UnconfirmedOwnedSlotProfilesException();
                                }
                                return Boolean.TRUE;
                            },
                            new RetryUtils.RetryMaterial(
                                    OWNED_SLOT_PROFILES_CONFIRM_RETRY_TIME,
                                    false,
                                    exception -> isRetriableOwnershipFailure(exception),
                                    OWNED_SLOT_PROFILES_CONFIRM_RETRY_SLEEP_MS));
            return Boolean.TRUE.equals(confirmed);
        } catch (Exception e) {
            throw new SeaTunnelEngineException(
                    "Can not sync pipeline owned slot profiles with IMap", e);
        }
    }

    private boolean isRetriableOwnershipFailure(Exception exception) {
        return (exception instanceof UnconfirmedOwnedSlotProfilesException
                        || exception instanceof NullPointerException)
                && isRunning;
    }

    /** Signals that a written ownership record did not read back as the written value. */
    private static final class UnconfirmedOwnedSlotProfilesException extends RuntimeException {
        private static final long serialVersionUID = 1L;
    }

    /**
     * A slot kept allocated to this job by a failed admission attempt of this master.
     *
     * <p>The attempt sequence identifies the round that kept the slot, so only a later attempt of
     * the same master can reuse it. The owning master is the instance holding the ledger.
     */
    private static final class AttemptOwnedSlotProfile {
        private final long attemptSequence;
        private final SlotProfile slotProfile;

        private AttemptOwnedSlotProfile(long attemptSequence, SlotProfile slotProfile) {
            this.attemptSequence = attemptSequence;
            this.slotProfile = slotProfile;
        }

        private long getAttemptSequence() {
            return attemptSequence;
        }

        private SlotProfile getSlotProfile() {
            return slotProfile;
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

    public CoordinatorService getCoordinatorService() {
        return this.seaTunnelServer.getCoordinatorService();
    }

    @VisibleForTesting
    public IMap<Object, Object> getRunningJobStateIMap() {
        return runningJobStateIMap;
    }
}
