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

import org.apache.seatunnel.api.common.metrics.JobMetrics;
import org.apache.seatunnel.api.common.metrics.RawJobMetrics;
import org.apache.seatunnel.common.utils.ExceptionUtils;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.common.utils.StringFormatUtils;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.config.EngineConfig;
import org.apache.seatunnel.engine.common.exception.JobException;
import org.apache.seatunnel.engine.common.exception.JobNotFoundException;
import org.apache.seatunnel.engine.common.exception.SeaTunnelEngineException;
import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.core.job.JobDAGInfo;
import org.apache.seatunnel.engine.core.job.JobInfo;
import org.apache.seatunnel.engine.core.job.JobResult;
import org.apache.seatunnel.engine.core.job.JobStatus;
import org.apache.seatunnel.engine.core.job.PipelineStatus;
import org.apache.seatunnel.engine.server.dag.physical.PhysicalVertex;
import org.apache.seatunnel.engine.server.dag.physical.PipelineLocation;
import org.apache.seatunnel.engine.server.dag.physical.SubPlan;
import org.apache.seatunnel.engine.server.execution.ExecutionState;
import org.apache.seatunnel.engine.server.execution.TaskExecutionState;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.master.JobHistoryService;
import org.apache.seatunnel.engine.server.master.JobMaster;
import org.apache.seatunnel.engine.server.metrics.JobMetricsUtil;
import org.apache.seatunnel.engine.server.metrics.SeaTunnelMetricsContext;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManager;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManagerFactory;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;
import org.apache.seatunnel.engine.server.task.operation.GetMetricsOperation;
import org.apache.seatunnel.engine.server.utils.NodeEngineUtil;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.hazelcast.cluster.Address;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.services.MembershipServiceEvent;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.NodeEngineImpl;
import lombok.NonNull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.apache.seatunnel.engine.server.metrics.JobMetricsUtil.toJobMetricsMap;

public class CoordinatorService {
    private final NodeEngineImpl nodeEngine;
    private final ILogger logger;

    private volatile ResourceManager resourceManager;

    private JobHistoryService jobHistoryService;

    /**
     * IMap key is jobId and value is {@link JobInfo}. Tuple2 key is JobMaster init timestamp and
     * value is the jobImmutableInformation which is sent by client when submit job
     *
     * <p>This IMap is used to recovery runningJobInfoIMap in JobMaster when a new master node
     * active
     */
    private IMap<Long, JobInfo> runningJobInfoIMap;

    /**
     * IMap key is one of jobId {@link
     * org.apache.seatunnel.engine.server.dag.physical.PipelineLocation} and {@link
     * org.apache.seatunnel.engine.server.execution.TaskGroupLocation}
     *
     * <p>The value of IMap is one of {@link JobStatus} {@link PipelineStatus} {@link
     * org.apache.seatunnel.engine.server.execution.ExecutionState}
     *
     * <p>This IMap is used to recovery runningJobStateIMap in JobMaster when a new master node
     * active
     */
    IMap<Object, Object> runningJobStateIMap;

    /**
     * IMap key is one of jobId {@link
     * org.apache.seatunnel.engine.server.dag.physical.PipelineLocation} and {@link
     * org.apache.seatunnel.engine.server.execution.TaskGroupLocation}
     *
     * <p>The value of IMap is one of {@link
     * org.apache.seatunnel.engine.server.dag.physical.PhysicalPlan} stateTimestamps {@link
     * org.apache.seatunnel.engine.server.dag.physical.SubPlan} stateTimestamps {@link
     * org.apache.seatunnel.engine.server.dag.physical.PhysicalVertex} stateTimestamps
     *
     * <p>This IMap is used to recovery runningJobStateTimestampsIMap in JobMaster when a new master
     * node active
     */
    IMap<Object, Long[]> runningJobStateTimestampsIMap;

    /**
     * key: job id; <br>
     * value: job master;
     */
    private Map<Long, JobMaster> runningJobMasterMap = new ConcurrentHashMap<>();

    /**
     * IMap key is {@link PipelineLocation}
     *
     * <p>The value of IMap is map of {@link TaskGroupLocation} and the {@link SlotProfile} it used.
     *
     * <p>This IMap is used to recovery ownedSlotProfilesIMap in JobMaster when a new master node
     * active
     */
    private IMap<PipelineLocation, Map<TaskGroupLocation, SlotProfile>> ownedSlotProfilesIMap;

    private IMap<Long, HashMap<TaskLocation, SeaTunnelMetricsContext>> metricsImap;

    /** If this node is a master node */
    private volatile boolean isActive = false;

    private ExecutorService executorService;

    private final SeaTunnelServer seaTunnelServer;

    private final ScheduledExecutorService masterActiveListener;

    private final EngineConfig engineConfig;

    public CoordinatorService(
            @NonNull NodeEngineImpl nodeEngine,
            @NonNull SeaTunnelServer seaTunnelServer,
            EngineConfig engineConfig) {
        this.nodeEngine = nodeEngine;
        this.logger = nodeEngine.getLogger(getClass());
        this.executorService =
                Executors.newCachedThreadPool(
                        new ThreadFactoryBuilder()
                                .setNameFormat("seatunnel-coordinator-service-%d")
                                .build());
        this.seaTunnelServer = seaTunnelServer;
        this.engineConfig = engineConfig;
        masterActiveListener = Executors.newSingleThreadScheduledExecutor();
        masterActiveListener.scheduleAtFixedRate(
                this::checkNewActiveMaster, 0, 100, TimeUnit.MILLISECONDS);
    }

    public JobHistoryService getJobHistoryService() {
        return jobHistoryService;
    }

    public JobMaster getJobMaster(Long jobId) {
        return runningJobMasterMap.get(jobId);
    }

    // On the new master node
    // 1. If runningJobStateIMap.get(jobId) == null and runningJobInfoIMap.get(jobId) != null. We
    // will do
    //    runningJobInfoIMap.remove(jobId)
    //
    // 2. If runningJobStateIMap.get(jobId) != null and the value equals JobStatus End State. We
    // need new a
    //    JobMaster and generate PhysicalPlan again and then try to remove all of PipelineLocation
    // and
    //    TaskGroupLocation key in the runningJobStateIMap.
    //
    // 3. If runningJobStateIMap.get(jobId) != null and the value equals JobStatus.SCHEDULED. We
    // need cancel the job
    //    and then call submitJob(long jobId, Data jobImmutableInformation) to resubmit it.
    //
    // 4. If runningJobStateIMap.get(jobId) != null and the value is CANCELING or RUNNING. We need
    // recover the JobMaster
    //    from runningJobStateIMap and then waiting for it complete.
    private void initCoordinatorService() {
        runningJobInfoIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_INFO);
        runningJobStateIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_STATE);
        runningJobStateTimestampsIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_STATE_TIMESTAMPS);
        ownedSlotProfilesIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_OWNED_SLOT_PROFILES);
        metricsImap = nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_METRICS);

        jobHistoryService =
                new JobHistoryService(
                        runningJobStateIMap,
                        logger,
                        runningJobMasterMap,
                        nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_FINISHED_JOB_STATE),
                        nodeEngine
                                .getHazelcastInstance()
                                .getMap(Constant.IMAP_FINISHED_JOB_METRICS),
                        nodeEngine
                                .getHazelcastInstance()
                                .getMap(Constant.IMAP_FINISHED_JOB_VERTEX_INFO),
                        engineConfig.getHistoryJobExpireMinutes());

        List<CompletableFuture<Void>> collect =
                runningJobInfoIMap.entrySet().stream()
                        .map(
                                entry ->
                                        CompletableFuture.runAsync(
                                                () -> {
                                                    logger.info(
                                                            String.format(
                                                                    "begin restore job (%s) from master active switch",
                                                                    entry.getKey()));
                                                    try {
                                                        restoreJobFromMasterActiveSwitch(
                                                                entry.getKey(), entry.getValue());
                                                    } catch (Exception e) {
                                                        logger.severe(e);
                                                    }
                                                    logger.info(
                                                            String.format(
                                                                    "restore job (%s) from master active switch finished",
                                                                    entry.getKey()));
                                                },
                                                executorService))
                        .collect(Collectors.toList());

        try {
            CompletableFuture<Void> voidCompletableFuture =
                    CompletableFuture.allOf(collect.toArray(new CompletableFuture[0]));
            voidCompletableFuture.get();
        } catch (Exception e) {
            throw new SeaTunnelEngineException(e);
        }
    }

    private void restoreJobFromMasterActiveSwitch(@NonNull Long jobId, @NonNull JobInfo jobInfo) {
        if (runningJobStateIMap.get(jobId) == null) {
            runningJobInfoIMap.remove(jobId);
            return;
        }

        JobStatus jobStatus = (JobStatus) runningJobStateIMap.get(jobId);
        JobMaster jobMaster =
                new JobMaster(
                        jobInfo.getJobImmutableInformation(),
                        nodeEngine,
                        executorService,
                        getResourceManager(),
                        getJobHistoryService(),
                        runningJobStateIMap,
                        runningJobStateTimestampsIMap,
                        ownedSlotProfilesIMap,
                        runningJobInfoIMap,
                        metricsImap,
                        engineConfig);

        try {
            jobMaster.init(runningJobInfoIMap.get(jobId).getInitializationTimestamp(), true);
        } catch (Exception e) {
            throw new SeaTunnelEngineException(String.format("Job id %s init failed", jobId), e);
        }

        String jobFullName = jobMaster.getPhysicalPlan().getJobFullName();
        runningJobMasterMap.put(jobId, jobMaster);

        logger.info(
                String.format(
                        "The restore %s is in %s state, restore pipeline and take over this job running",
                        jobFullName, jobStatus));
        CompletableFuture.runAsync(
                () -> {
                    try {
                        jobMaster
                                .getPhysicalPlan()
                                .getPipelineList()
                                .forEach(SubPlan::restorePipelineState);
                        jobMaster.run();
                    } finally {
                        // voidCompletableFuture will be cancelled when zeta master node
                        // shutdown to simulate master failure,
                        // don't update runningJobMasterMap is this case.
                        if (!jobMaster.getJobMasterCompleteFuture().isCompletedExceptionally()) {
                            runningJobMasterMap.remove(jobId);
                        }
                    }
                },
                executorService);
    }

    private void checkNewActiveMaster() {
        try {
            if (!isActive && this.seaTunnelServer.isMasterNode()) {
                logger.info(
                        "This node become a new active master node, begin init coordinator service");
                if (this.executorService.isShutdown()) {
                    this.executorService =
                            Executors.newCachedThreadPool(
                                    new ThreadFactoryBuilder()
                                            .setNameFormat("seatunnel-coordinator-service-%d")
                                            .build());
                }
                initCoordinatorService();
                isActive = true;
            } else if (isActive && !this.seaTunnelServer.isMasterNode()) {
                isActive = false;
                logger.info(
                        "This node become leave active master node, begin clear coordinator service");
                clearCoordinatorService();
            }
        } catch (Exception e) {
            isActive = false;
            logger.severe(ExceptionUtils.getMessage(e));
            throw new SeaTunnelEngineException("check new active master error, stop loop", e);
        }
    }

    public synchronized void clearCoordinatorService() {
        // interrupt all JobMaster
        runningJobMasterMap.values().forEach(JobMaster::interrupt);
        executorService.shutdownNow();
        runningJobMasterMap.clear();

        try {
            executorService.awaitTermination(20, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new SeaTunnelEngineException("wait clean executor service error", e);
        }

        if (resourceManager != null) {
            resourceManager.close();
        }
    }

    /** Lazy load for resource manager */
    public ResourceManager getResourceManager() {
        if (resourceManager == null) {
            synchronized (this) {
                if (resourceManager == null) {
                    ResourceManager manager =
                            new ResourceManagerFactory(nodeEngine).getResourceManager();
                    manager.init();
                    resourceManager = manager;
                }
            }
        }
        return resourceManager;
    }

    /** call by client to submit job */
    public PassiveCompletableFuture<Void> submitJob(long jobId, Data jobImmutableInformation) {
        CompletableFuture<Void> jobSubmitFuture = new CompletableFuture<>();

        // Check if the current jobID is already running. If so, complete the submission
        // successfully.
        // This avoids potential issues like redundant job restores or other anomalies.
        if (getJobMaster(jobId) != null) {
            logger.warning(
                    String.format(
                            "The job %s is currently running; no need to submit again.", jobId));
            jobSubmitFuture.complete(null);
            return new PassiveCompletableFuture<>(jobSubmitFuture);
        }

        JobMaster jobMaster =
                new JobMaster(
                        jobImmutableInformation,
                        this.nodeEngine,
                        executorService,
                        getResourceManager(),
                        getJobHistoryService(),
                        runningJobStateIMap,
                        runningJobStateTimestampsIMap,
                        ownedSlotProfilesIMap,
                        runningJobInfoIMap,
                        metricsImap,
                        engineConfig);
        executorService.submit(
                () -> {
                    try {
                        runningJobInfoIMap.put(
                                jobId,
                                new JobInfo(System.currentTimeMillis(), jobImmutableInformation));
                        runningJobMasterMap.put(jobId, jobMaster);
                        jobMaster.init(
                                runningJobInfoIMap.get(jobId).getInitializationTimestamp(), false);
                        // We specify that when init is complete, the submitJob is complete
                        jobSubmitFuture.complete(null);
                    } catch (Throwable e) {
                        logger.severe(
                                String.format(
                                        "submit job %s error %s ",
                                        jobId, ExceptionUtils.getMessage(e)));
                        jobSubmitFuture.completeExceptionally(e);
                    }
                    if (!jobSubmitFuture.isCompletedExceptionally()) {
                        try {
                            jobMaster.run();
                        } finally {
                            // voidCompletableFuture will be cancelled when zeta master node
                            // shutdown to simulate master failure,
                            // don't update runningJobMasterMap is this case.
                            if (!jobMaster.getJobMasterCompleteFuture().isCancelled()) {
                                runningJobMasterMap.remove(jobId);
                            }
                        }
                    } else {
                        runningJobInfoIMap.remove(jobId);
                        runningJobMasterMap.remove(jobId);
                    }
                });
        return new PassiveCompletableFuture<>(jobSubmitFuture);
    }

    public PassiveCompletableFuture<Void> savePoint(long jobId) {
        CompletableFuture<Void> voidCompletableFuture = new CompletableFuture<>();
        if (!runningJobMasterMap.containsKey(jobId)) {
            Throwable throwable =
                    new Throwable("The jobId: " + jobId + "of savePoint does not exist");
            logger.warning(throwable);
            voidCompletableFuture.completeExceptionally(throwable);
        } else {
            JobMaster jobMaster = runningJobMasterMap.get(jobId);
            voidCompletableFuture = jobMaster.savePoint();
        }
        return new PassiveCompletableFuture<>(voidCompletableFuture);
    }

    public PassiveCompletableFuture<JobResult> waitForJobComplete(long jobId) {
        JobMaster runningJobMaster = runningJobMasterMap.get(jobId);
        if (runningJobMaster == null) {
            // Because operations on Imap cannot be performed within Operation.
            CompletableFuture<JobHistoryService.JobState> jobStateFuture =
                    CompletableFuture.supplyAsync(
                            () -> {
                                return jobHistoryService.getJobDetailState(jobId);
                            },
                            executorService);
            JobHistoryService.JobState jobState = null;
            try {
                jobState = jobStateFuture.get();
            } catch (Exception e) {
                throw new SeaTunnelEngineException("get job state error", e);
            }

            CompletableFuture<JobResult> future = new CompletableFuture<>();
            if (jobState == null) future.complete(new JobResult(JobStatus.UNKNOWABLE, null));
            else
                future.complete(new JobResult(jobState.getJobStatus(), jobState.getErrorMessage()));
            return new PassiveCompletableFuture<>(future);
        } else {
            return new PassiveCompletableFuture<>(runningJobMaster.getJobMasterCompleteFuture());
        }
    }

    public PassiveCompletableFuture<Void> cancelJob(long jodId) {
        JobMaster runningJobMaster = runningJobMasterMap.get(jodId);
        if (runningJobMaster == null) {
            CompletableFuture<Void> future = new CompletableFuture<>();
            future.complete(null);
            return new PassiveCompletableFuture<>(future);
        } else {
            return new PassiveCompletableFuture<>(
                    CompletableFuture.supplyAsync(
                            () -> {
                                runningJobMaster.cancelJob();
                                return null;
                            },
                            executorService));
        }
    }

    public JobStatus getJobStatus(long jobId) {
        JobMaster runningJobMaster = runningJobMasterMap.get(jobId);
        if (runningJobMaster == null) {
            JobHistoryService.JobState jobDetailState = jobHistoryService.getJobDetailState(jobId);
            return null == jobDetailState ? JobStatus.UNKNOWABLE : jobDetailState.getJobStatus();
        }
        JobStatus jobStatus = runningJobMaster.getJobStatus();
        if (jobStatus == null) {
            return jobHistoryService.getFinishedJobStateImap().get(jobId).getJobStatus();
        }
        return jobStatus;
    }

    public JobMetrics getJobMetrics(long jobId) {
        JobMaster runningJobMaster = runningJobMasterMap.get(jobId);
        if (runningJobMaster == null) {
            return jobHistoryService.getJobMetrics(jobId);
        }
        JobMetrics jobMetrics = JobMetricsUtil.toJobMetrics(runningJobMaster.getCurrJobMetrics());
        JobMetrics jobMetricsImap = jobHistoryService.getJobMetrics(jobId);
        return jobMetricsImap != null ? jobMetricsImap.merge(jobMetrics) : jobMetrics;
    }

    public Map<Long, JobMetrics> getRunningJobMetrics() {
        final Set<Long> runningJobIds = runningJobMasterMap.keySet();

        Set<Address> addresses = new HashSet<>();
        ownedSlotProfilesIMap.forEach(
                (pipelineLocation, ownedSlotProfilesIMap) -> {
                    if (runningJobIds.contains(pipelineLocation.getJobId())) {
                        ownedSlotProfilesIMap
                                .values()
                                .forEach(
                                        ownedSlotProfile -> {
                                            addresses.add(ownedSlotProfile.getWorker());
                                        });
                    }
                });

        List<RawJobMetrics> metrics = new ArrayList<>();

        addresses.forEach(
                address -> {
                    try {
                        if (nodeEngine.getClusterService().getMember(address) != null) {
                            RawJobMetrics rawJobMetrics =
                                    (RawJobMetrics)
                                            NodeEngineUtil.sendOperationToMemberNode(
                                                            nodeEngine,
                                                            new GetMetricsOperation(runningJobIds),
                                                            address)
                                                    .get();
                            metrics.add(rawJobMetrics);
                        }
                    }
                    // HazelcastInstanceNotActiveException. It means that the node is
                    // offline, so waiting for the taskGroup to restore can be successful
                    catch (HazelcastInstanceNotActiveException e) {
                        logger.warning(
                                String.format(
                                        "get metrics with exception: %s.",
                                        ExceptionUtils.getMessage(e)));
                    } catch (Exception e) {
                        throw new SeaTunnelException(e.getMessage());
                    }
                });

        Map<Long, JobMetrics> longJobMetricsMap = toJobMetricsMap(metrics);

        longJobMetricsMap.forEach(
                (jobId, jobMetrics) -> {
                    JobMetrics jobMetricsImap = jobHistoryService.getJobMetrics(jobId);
                    if (jobMetricsImap != null) {
                        longJobMetricsMap.put(jobId, jobMetricsImap.merge(jobMetrics));
                    }
                });

        return longJobMetricsMap;
    }

    public JobDAGInfo getJobInfo(long jobId) {
        JobDAGInfo jobInfo = jobHistoryService.getJobDAGInfo(jobId);
        if (jobInfo != null) {
            return jobInfo;
        }
        return runningJobMasterMap.get(jobId).getJobDAGInfo();
    }

    /**
     * When TaskGroup ends, it is called by {@link TaskExecutionService} to notify JobMaster the
     * TaskGroup's state.
     */
    public void updateTaskExecutionState(TaskExecutionState taskExecutionState) {
        logger.info(
                String.format(
                        "Received task end from execution %s, state %s",
                        taskExecutionState.getTaskGroupLocation(),
                        taskExecutionState.getExecutionState()));
        TaskGroupLocation taskGroupLocation = taskExecutionState.getTaskGroupLocation();
        JobMaster runningJobMaster = runningJobMasterMap.get(taskGroupLocation.getJobId());
        if (runningJobMaster == null) {
            throw new JobNotFoundException(
                    String.format("Job %s not running", taskGroupLocation.getJobId()));
        }
        runningJobMaster.updateTaskExecutionState(taskExecutionState);
    }

    public void shutdown() {
        if (masterActiveListener != null) {
            masterActiveListener.shutdownNow();
        }
        clearCoordinatorService();
    }

    /** return true if this node is a master node and the coordinator service init finished. */
    public boolean isCoordinatorActive() {
        return isActive;
    }

    public void failedTaskOnMemberRemoved(MembershipServiceEvent event) {
        Address lostAddress = event.getMember().getAddress();
        runningJobMasterMap.forEach(
                (aLong, jobMaster) -> {
                    jobMaster
                            .getPhysicalPlan()
                            .getPipelineList()
                            .forEach(
                                    subPlan -> {
                                        makeTasksFailed(
                                                subPlan.getCoordinatorVertexList(), lostAddress);
                                        makeTasksFailed(
                                                subPlan.getPhysicalVertexList(), lostAddress);
                                    });
                });
    }

    private void makeTasksFailed(
            @NonNull List<PhysicalVertex> physicalVertexList, @NonNull Address lostAddress) {
        physicalVertexList.forEach(
                physicalVertex -> {
                    Address deployAddress = physicalVertex.getCurrentExecutionAddress();
                    ExecutionState executionState = physicalVertex.getExecutionState();
                    if (null != deployAddress
                            && deployAddress.equals(lostAddress)
                            && (executionState.equals(ExecutionState.DEPLOYING)
                                    || executionState.equals(ExecutionState.RUNNING)
                                    || executionState.equals(ExecutionState.CANCELING))) {
                        TaskGroupLocation taskGroupLocation = physicalVertex.getTaskGroupLocation();
                        physicalVertex.updateStateByExecutionService(
                                new TaskExecutionState(
                                        taskGroupLocation,
                                        ExecutionState.FAILED,
                                        new JobException(
                                                String.format(
                                                        "The taskGroup(%s) deployed node(%s) offline",
                                                        taskGroupLocation, lostAddress))));
                    }
                });
    }

    public void memberRemoved(MembershipServiceEvent event) {
        if (isCoordinatorActive()) {
            this.getResourceManager().memberRemoved(event);
        }
        this.failedTaskOnMemberRemoved(event);
    }

    public void printExecutionInfo() {
        ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) executorService;
        int activeCount = threadPoolExecutor.getActiveCount();
        int corePoolSize = threadPoolExecutor.getCorePoolSize();
        int maximumPoolSize = threadPoolExecutor.getMaximumPoolSize();
        int poolSize = threadPoolExecutor.getPoolSize();
        long completedTaskCount = threadPoolExecutor.getCompletedTaskCount();
        long taskCount = threadPoolExecutor.getTaskCount();
        logger.info(
                StringFormatUtils.formatTable(
                        "CoordinatorService Thread Pool Status",
                        "activeCount",
                        activeCount,
                        "corePoolSize",
                        corePoolSize,
                        "maximumPoolSize",
                        maximumPoolSize,
                        "poolSize",
                        poolSize,
                        "completedTaskCount",
                        completedTaskCount,
                        "taskCount",
                        taskCount));
    }

    public void printJobDetailInfo() {
        AtomicLong createdJobCount = new AtomicLong();
        AtomicLong scheduledJobCount = new AtomicLong();
        AtomicLong runningJobCount = new AtomicLong();
        AtomicLong failingJobCount = new AtomicLong();
        AtomicLong failedJobCount = new AtomicLong();
        AtomicLong cancellingJobCount = new AtomicLong();
        AtomicLong canceledJobCount = new AtomicLong();
        AtomicLong finishedJobCount = new AtomicLong();

        if (runningJobInfoIMap != null) {
            runningJobInfoIMap
                    .keySet()
                    .forEach(
                            jobId -> {
                                if (runningJobStateIMap.get(jobId) != null) {
                                    JobStatus jobStatus =
                                            (JobStatus) runningJobStateIMap.get(jobId);
                                    switch (jobStatus) {
                                        case CREATED:
                                            createdJobCount.addAndGet(1);
                                            break;
                                        case SCHEDULED:
                                            scheduledJobCount.addAndGet(1);
                                            break;
                                        case RUNNING:
                                            runningJobCount.addAndGet(1);
                                            break;
                                        case FAILING:
                                            failingJobCount.addAndGet(1);
                                            break;
                                        case FAILED:
                                            failedJobCount.addAndGet(1);
                                            break;
                                        case CANCELING:
                                            cancellingJobCount.addAndGet(1);
                                            break;
                                        case CANCELED:
                                            canceledJobCount.addAndGet(1);
                                            break;
                                        case FINISHED:
                                            finishedJobCount.addAndGet(1);
                                            break;
                                        default:
                                    }
                                }
                            });
        }

        logger.info(
                StringFormatUtils.formatTable(
                        "Job info detail",
                        "createdJobCount",
                        createdJobCount,
                        "scheduledJobCount",
                        scheduledJobCount,
                        "runningJobCount",
                        runningJobCount,
                        "failingJobCount",
                        failingJobCount,
                        "failedJobCount",
                        failedJobCount,
                        "cancellingJobCount",
                        cancellingJobCount,
                        "canceledJobCount",
                        canceledJobCount,
                        "finishedJobCount",
                        finishedJobCount));
    }
}
