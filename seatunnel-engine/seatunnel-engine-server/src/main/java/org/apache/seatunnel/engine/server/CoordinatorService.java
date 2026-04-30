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

import org.apache.seatunnel.shade.com.google.common.annotations.VisibleForTesting;
import org.apache.seatunnel.shade.com.google.common.util.concurrent.ThreadFactoryBuilder;

import org.apache.seatunnel.api.common.metrics.JobMetrics;
import org.apache.seatunnel.api.common.metrics.RawJobMetrics;
import org.apache.seatunnel.api.event.EventHandler;
import org.apache.seatunnel.api.event.EventProcessor;
import org.apache.seatunnel.api.tracing.MDCExecutorService;
import org.apache.seatunnel.api.tracing.MDCTracer;
import org.apache.seatunnel.common.utils.ExceptionUtils;
import org.apache.seatunnel.common.utils.RetryUtils;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.common.utils.StringFormatUtils;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.config.EngineConfig;
import org.apache.seatunnel.engine.common.config.server.ConnectorJarStorageConfig;
import org.apache.seatunnel.engine.common.config.server.ScheduleStrategy;
import org.apache.seatunnel.engine.common.exception.JobException;
import org.apache.seatunnel.engine.common.exception.JobNotFoundException;
import org.apache.seatunnel.engine.common.exception.SavePointFailedException;
import org.apache.seatunnel.engine.common.exception.SeaTunnelEngineException;
import org.apache.seatunnel.engine.common.job.JobResult;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.common.utils.ExceptionUtil;
import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.core.job.JobDAGInfo;
import org.apache.seatunnel.engine.core.job.JobInfo;
import org.apache.seatunnel.engine.core.job.PipelineStatus;
import org.apache.seatunnel.engine.server.dag.physical.PhysicalVertex;
import org.apache.seatunnel.engine.server.dag.physical.PipelineLocation;
import org.apache.seatunnel.engine.server.dag.physical.SubPlan;
import org.apache.seatunnel.engine.server.diagnostic.PendingDiagnosticsCollector;
import org.apache.seatunnel.engine.server.diagnostic.PendingJobDiagnostic;
import org.apache.seatunnel.engine.server.diagnostic.PendingJobsResponse;
import org.apache.seatunnel.engine.server.diagnostic.PendingQueueSummary;
import org.apache.seatunnel.engine.server.event.JobEventHttpReportHandler;
import org.apache.seatunnel.engine.server.event.JobEventProcessor;
import org.apache.seatunnel.engine.server.execution.ExecutionState;
import org.apache.seatunnel.engine.server.execution.PendingJobInfo;
import org.apache.seatunnel.engine.server.execution.PendingSourceState;
import org.apache.seatunnel.engine.server.execution.TaskExecutionState;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.execution.TaskLocation;
import org.apache.seatunnel.engine.server.master.JobHistoryService;
import org.apache.seatunnel.engine.server.master.JobMaster;
import org.apache.seatunnel.engine.server.master.cleanup.PipelineCleanupRecord;
import org.apache.seatunnel.engine.server.metrics.JobMetricsUtil;
import org.apache.seatunnel.engine.server.metrics.SeaTunnelMetricsContext;
import org.apache.seatunnel.engine.server.resourcemanager.NoEnoughResourceException;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManager;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManagerFactory;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;
import org.apache.seatunnel.engine.server.service.jar.ConnectorPackageService;
import org.apache.seatunnel.engine.server.task.operation.CleanTaskGroupContextOperation;
import org.apache.seatunnel.engine.server.task.operation.GetMetricsOperation;
import org.apache.seatunnel.engine.server.telemetry.metrics.entity.JobCounter;
import org.apache.seatunnel.engine.server.telemetry.metrics.entity.ThreadPoolStatus;
import org.apache.seatunnel.engine.server.utils.NodeEngineUtil;
import org.apache.seatunnel.engine.server.utils.PeekBlockingQueue;

import com.hazelcast.cluster.Address;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstanceNotActiveException;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.services.MembershipServiceEvent;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.IMap;
import com.hazelcast.ringbuffer.Ringbuffer;
import com.hazelcast.spi.impl.NodeEngineImpl;
import lombok.NonNull;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.apache.seatunnel.engine.server.metrics.JobMetricsUtil.toJobMetricsMap;

public class CoordinatorService {
    private static final int PIPELINE_CLEANUP_INTERVAL_SECONDS = 60;
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
    private IMap<Object, Object> runningJobStateIMap;

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
    private IMap<Object, Long[]> runningJobStateTimestampsIMap;

    /**
     * key: job id; <br>
     * value: job master;
     */
    private final Map<Long, JobMaster> runningJobMasterMap = new ConcurrentHashMap<>();

    private final PeekBlockingQueue<PendingJobInfo> pendingJobQueue =
            new PeekBlockingQueue<>(PendingJobInfo::getJobId);

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

    private IMap<PipelineLocation, PipelineCleanupRecord> pendingPipelineCleanupIMap;

    /** If this node is a master node */
    private volatile boolean isActive = false;

    private ExecutorService executorService;

    private final SeaTunnelServer seaTunnelServer;

    private final ScheduledExecutorService masterActiveListener;

    private final ScheduledExecutorService pipelineCleanupScheduler;

    private final EngineConfig engineConfig;

    private ConnectorPackageService connectorPackageService;

    private EventProcessor eventProcessor;

    private PassiveCompletableFuture restoreAllJobFromMasterNodeSwitchFuture;

    private final boolean isWaitStrategy;

    private final ScheduleStrategy scheduleStrategy;

    public CoordinatorService(
            @NonNull NodeEngineImpl nodeEngine,
            @NonNull SeaTunnelServer seaTunnelServer,
            EngineConfig engineConfig) {
        this.nodeEngine = nodeEngine;
        this.engineConfig = engineConfig;
        this.logger = nodeEngine.getLogger(getClass());
        this.executorService =
                new ThreadPoolExecutor(
                        engineConfig.getCoordinatorServiceConfig().getCoreThreadNum(),
                        engineConfig.getCoordinatorServiceConfig().getMaxThreadNum(),
                        60L,
                        TimeUnit.SECONDS,
                        new SynchronousQueue<>(),
                        new ThreadFactoryBuilder()
                                .setNameFormat("seatunnel-coordinator-service-%d")
                                .build(),
                        new ThreadPoolStatus.RejectionCountingHandler());

        this.seaTunnelServer = seaTunnelServer;
        masterActiveListener = Executors.newSingleThreadScheduledExecutor();
        masterActiveListener.scheduleAtFixedRate(
                this::checkNewActiveMaster, 0, 100, TimeUnit.MILLISECONDS);
        pipelineCleanupScheduler =
                Executors.newSingleThreadScheduledExecutor(
                        new ThreadFactoryBuilder()
                                .setNameFormat("seatunnel-pipeline-cleanup-%d")
                                .build());
        pipelineCleanupScheduler.scheduleAtFixedRate(
                this::cleanupPendingPipelines,
                PIPELINE_CLEANUP_INTERVAL_SECONDS,
                PIPELINE_CLEANUP_INTERVAL_SECONDS,
                TimeUnit.SECONDS);
        scheduleStrategy = engineConfig.getScheduleStrategy();
        isWaitStrategy = scheduleStrategy.equals(ScheduleStrategy.WAIT);
        logger.info("Start pending job schedule thread");
        // start pending job schedule thread
        startPendingJobScheduleThread();
    }

    /**
     * Starts the long-lived single-threaded pending job scheduler.
     *
     * <p>The scheduler keeps checking the queue head and only advances a job after resource
     * pre-checks succeed. Unexpected exceptions are logged and retried so one bad scheduling cycle
     * does not terminate future pending-job processing.
     */
    private void startPendingJobScheduleThread() {
        Runnable pendingJobScheduleTask =
                () -> {
                    Thread.currentThread().setName("pending-job-schedule-runner");
                    while (true) {
                        try {
                            pendingJobSchedule();
                        } catch (InterruptedException interrupted) {
                            throw new RuntimeException(interrupted);
                        } catch (Throwable e) {
                            logger.severe("Error in pending job schedule thread", e);
                            try {
                                Thread.sleep(3000L);
                            } catch (InterruptedException ex) {
                                logger.severe("Pending job schedule thread interrupted", ex);
                                Thread.currentThread().interrupt();
                            }
                        }
                    }
                };
        executorService.submit(pendingJobScheduleTask);
    }

    /**
     * Attempts to advance the head job in the pending queue into execution.
     *
     * <p>This method peeks the queue head before removing it so the current job keeps its position
     * while resource pre-allocation is evaluated. If resources are insufficient, the job either
     * stays queued for retry under {@link ScheduleStrategy#WAIT} or is failed before entering
     * {@link JobMaster#run()}. Restored jobs rehydrate pipeline execution state before the new
     * master takes over execution.
     *
     * @throws InterruptedException if queue waiting or retry sleep is interrupted
     */
    private void pendingJobSchedule() throws InterruptedException {
        PendingJobInfo pendingJobInfo = pendingJobQueue.peekBlocking();
        if (Objects.isNull(pendingJobInfo)) {
            // This situation almost never happens because pendingJobSchedule is single-threaded
            logger.warning("The peek job info is null");
            Thread.sleep(3000);
            return;
        }
        Long jobId = pendingJobInfo.getJobId();
        final JobMaster jobMaster = pendingJobInfo.getJobMaster();
        logger.fine(
                String.format(
                        "Start pending job schedule, pendingJob Size : %s",
                        pendingJobQueue.size()));
        logger.fine(
                String.format(
                        "Start calculating whether pending task resources are enough: %s", jobId));

        boolean preApplyResources = jobMaster.preApplyResources();
        if (!preApplyResources) {
            try {
                PendingJobDiagnostic diagnostic =
                        PendingDiagnosticsCollector.collectJobDiagnostic(
                                pendingJobInfo, Collections.emptyMap(), getResourceManager());
                pendingJobInfo.recordSnapshot(diagnostic);
            } catch (Exception e) {
                logger.warning(
                        String.format(
                                "Collect pending diagnostic for job %s failed: %s",
                                jobId, ExceptionUtils.getMessage(e)));
            }
            logger.info(
                    String.format(
                            "Current strategy is %s, and resources is not enough, skipping this schedule, JobID: %s",
                            scheduleStrategy, jobId));
            if (isWaitStrategy) {
                try {
                    Thread.sleep(3000);
                } catch (InterruptedException e) {
                    logger.severe(ExceptionUtils.getMessage(e));
                }
                return;
            } else {
                completeFailJob(jobMaster);
                queueRemove(jobMaster);
                return;
            }
        }
        logger.info(String.format("Resources enough, start running: %s", jobId));
        // When deleting jobmaster from pendingJobQueue, make sure that there is a corresponding
        // jobMaster in the runningJobMasterMap
        runningJobMasterMap.put(jobId, jobMaster);
        final PendingJobInfo finalPendingJobInfo = pendingJobQueue.take();
        final JobMaster finalJobMaster = finalPendingJobInfo.getJobMaster();
        PendingSourceState pendingSourceState = finalPendingJobInfo.getPendingSourceState();
        MDCExecutorService mdcExecutorService = MDCTracer.tracing(jobId, executorService);
        mdcExecutorService.submit(
                () -> {
                    try {
                        String jobFullName = finalJobMaster.getPhysicalPlan().getJobFullName();
                        JobStatus jobStatus = (JobStatus) runningJobStateIMap.get(jobId);
                        if (pendingSourceState == PendingSourceState.RESTORE
                                && !jobStatus.isEndState()) {
                            finalJobMaster
                                    .getPhysicalPlan()
                                    .getPipelineList()
                                    .forEach(SubPlan::restorePipelineState);
                        }
                        logger.info(
                                String.format(
                                        "The %s %s is in %s state, restore pipeline and take over this job running",
                                        pendingSourceState, jobFullName, jobStatus));
                        finalJobMaster.run();
                    } finally {
                        if (jobMasterCompletedSuccessfully(finalJobMaster, pendingSourceState)) {
                            runningJobMasterMap.remove(jobId);
                        }
                    }
                });
    }

    private void queueRemove(JobMaster jobMaster) {
        pendingJobQueue.removeById(jobMaster.getJobId());
    }

    /**
     * Completes a pending job as failed before it ever enters the running phase.
     *
     * <p>This path is used when the scheduling strategy does not permit waiting for resources. At
     * this point no task has been deployed to workers, so the coordinator only needs to mark the
     * physical plan failed and complete the job future rather than execute full runtime teardown.
     */
    private void completeFailJob(JobMaster jobMaster) {
        // If the pending queue is not enabled and resources are insufficient, stop the task from
        // running
        JobResult jobResult =
                new JobResult(
                        JobStatus.FAILED,
                        ExceptionUtils.getMessage(new NoEnoughResourceException()));
        jobMaster.getPhysicalPlan().updateJobState(JobStatus.FAILED);
        jobMaster.getPhysicalPlan().completeJobEndFuture(jobResult);
        // wait job complete
        jobMaster.getJobMasterCompleteFuture().join();
        logger.info(
                String.format(
                        "The job %s is not running because the resources is not enough insufficient",
                        jobMaster.getJobId()));
    }

    private boolean jobMasterCompletedSuccessfully(JobMaster jobMaster, PendingSourceState state) {
        return (!jobMaster.getJobMasterCompleteFuture().isCompletedExceptionally()
                        && state == PendingSourceState.RESTORE)
                || (!jobMaster.getJobMasterCompleteFuture().isCancelled()
                        && state == PendingSourceState.SUBMIT);
    }

    private JobEventProcessor createJobEventProcessor(
            String reportHttpEndpoint,
            Map<String, String> reportHttpHeaders,
            NodeEngineImpl nodeEngine) {
        List<EventHandler> handlers =
                EventProcessor.loadEventHandlers(Thread.currentThread().getContextClassLoader());

        if (reportHttpEndpoint != null) {
            String ringBufferName = "zeta-job-event";
            int maxBufferCapacity = 2000;
            nodeEngine
                    .getHazelcastInstance()
                    .getConfig()
                    .addRingBufferConfig(
                            new Config()
                                    .getRingbufferConfig(ringBufferName)
                                    .setCapacity(maxBufferCapacity)
                                    .setBackupCount(0)
                                    .setAsyncBackupCount(1)
                                    .setTimeToLiveSeconds(0));
            Ringbuffer ringbuffer = nodeEngine.getHazelcastInstance().getRingbuffer(ringBufferName);
            JobEventHttpReportHandler httpReportHandler =
                    new JobEventHttpReportHandler(
                            reportHttpEndpoint, reportHttpHeaders, ringbuffer);
            handlers.add(httpReportHandler);
        }
        logger.info("Loaded event handlers: " + handlers);
        return new JobEventProcessor(handlers);
    }

    public JobHistoryService getJobHistoryService() {
        return jobHistoryService;
    }

    public JobMaster getJobMaster(Long jobId) {
        PendingJobInfo pendingJobInfo = pendingJobQueue.getById(jobId);
        if (pendingJobInfo != null) {
            return pendingJobInfo.getJobMaster();
        }
        return runningJobMasterMap.get(jobId);
    }

    public EventProcessor getEventProcessor() {
        return eventProcessor;
    }

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
        pendingPipelineCleanupIMap =
                nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_PENDING_PIPELINE_CLEANUP);
        jobHistoryService =
                new JobHistoryService(
                        nodeEngine,
                        runningJobStateIMap,
                        logger,
                        pendingJobQueue.getJobIdMap(),
                        runningJobMasterMap,
                        nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_FINISHED_JOB_STATE),
                        nodeEngine
                                .getHazelcastInstance()
                                .getMap(Constant.IMAP_FINISHED_JOB_METRICS),
                        nodeEngine
                                .getHazelcastInstance()
                                .getMap(Constant.IMAP_FINISHED_JOB_VERTEX_INFO),
                        engineConfig.getHistoryJobExpireMinutes());
        eventProcessor =
                createJobEventProcessor(
                        engineConfig.getEventReportHttpApi(),
                        engineConfig.getEventReportHttpHeaders(),
                        nodeEngine);

        // If the user has configured the connector package service, create it  on the master node.
        ConnectorJarStorageConfig connectorJarStorageConfig =
                engineConfig.getConnectorJarStorageConfig();
        if (connectorJarStorageConfig.getEnable()) {
            connectorPackageService = new ConnectorPackageService(seaTunnelServer);
        }

        restoreAllJobFromMasterNodeSwitchFuture =
                new PassiveCompletableFuture(
                        CompletableFuture.runAsync(
                                this::restoreAllRunningJobFromMasterNodeSwitch, executorService));
    }

    private void cleanupPendingPipelines() {
        if (!isActive) {
            return;
        }
        IMap<PipelineLocation, PipelineCleanupRecord> pendingCleanupIMap =
                this.pendingPipelineCleanupIMap;
        if (pendingCleanupIMap == null || pendingCleanupIMap.isEmpty()) {
            return;
        }

        try {
            for (Map.Entry<PipelineLocation, PipelineCleanupRecord> entry :
                    pendingCleanupIMap.entrySet()) {
                processPendingPipelineCleanup(entry.getKey(), entry.getValue());
            }
        } catch (HazelcastInstanceNotActiveException e) {
            logger.warning(
                    String.format(
                            "Skip pending pipeline cleanup: hazelcast not active: %s",
                            ExceptionUtils.getMessage(e)));
        } catch (Throwable t) {
            logger.warning(
                    String.format(
                            "Unexpected exception in pending pipeline cleanup: %s",
                            ExceptionUtils.getMessage(t)),
                    t);
        }
    }

    private void processPendingPipelineCleanup(
            PipelineLocation pipelineLocation, PipelineCleanupRecord record) {
        if (pipelineLocation == null || record == null) {
            return;
        }
        if (!shouldCleanup(record)) {
            removePendingCleanupRecord(pipelineLocation, record);
            return;
        }

        PipelineStatus currentStatus = getPipelineStatusFromIMap(pipelineLocation);
        if (currentStatus != null && !currentStatus.isEndState()) {
            return;
        }

        long now = System.currentTimeMillis();
        PipelineCleanupRecord updated = copy(record);
        updated.setLastAttemptTimeMillis(now);
        updated.setAttemptCount(record.getAttemptCount() + 1);

        if (!updated.isMetricsImapCleaned() && cleanupPipelineMetrics(pipelineLocation)) {
            updated.setMetricsImapCleaned(true);
        }

        Map<TaskGroupLocation, Address> taskGroups = updated.getTaskGroups();
        if (taskGroups != null && !taskGroups.isEmpty()) {
            for (Map.Entry<TaskGroupLocation, Address> taskGroup : taskGroups.entrySet()) {
                TaskGroupLocation taskGroupLocation = taskGroup.getKey();
                if (updated.getCleanedTaskGroups() != null
                        && updated.getCleanedTaskGroups().contains(taskGroupLocation)) {
                    continue;
                }
                Address workerAddress = taskGroup.getValue();
                if (workerAddress == null
                        || nodeEngine.getClusterService().getMember(workerAddress) == null) {
                    continue;
                }
                try {
                    NodeEngineUtil.sendOperationToMemberNode(
                                    nodeEngine,
                                    new CleanTaskGroupContextOperation(taskGroupLocation),
                                    workerAddress)
                            .get();
                    updated.getCleanedTaskGroups().add(taskGroupLocation);
                } catch (HazelcastInstanceNotActiveException e) {
                    logger.warning(
                            String.format(
                                    "%s clean TaskGroupContext failed: %s",
                                    taskGroupLocation, ExceptionUtils.getMessage(e)));
                } catch (Exception e) {
                    logger.warning(
                            String.format(
                                    "%s clean TaskGroupContext failed: %s",
                                    taskGroupLocation, ExceptionUtils.getMessage(e)),
                            e);
                }
            }
        }

        boolean replaced = pendingPipelineCleanupIMap.replace(pipelineLocation, record, updated);
        if (!replaced) {
            return;
        }
        if (updated.isCleaned()) {
            pendingPipelineCleanupIMap.remove(pipelineLocation, updated);
        }
    }

    private void removePendingCleanupRecord(
            PipelineLocation pipelineLocation, PipelineCleanupRecord record) {
        try {
            pendingPipelineCleanupIMap.remove(pipelineLocation, record);
        } catch (Exception e) {
            logger.warning(
                    String.format(
                            "Remove pending pipeline cleanup record failed: %s",
                            ExceptionUtils.getMessage(e)),
                    e);
        }
    }

    private boolean shouldCleanup(PipelineCleanupRecord record) {
        if (record == null || record.getFinalStatus() == null) {
            return false;
        }
        if (record.isSavepointEnd()) {
            return false;
        }
        return PipelineStatus.CANCELED.equals(record.getFinalStatus())
                || PipelineStatus.FINISHED.equals(record.getFinalStatus());
    }

    private PipelineStatus getPipelineStatusFromIMap(PipelineLocation pipelineLocation) {
        Object state =
                runningJobStateIMap != null ? runningJobStateIMap.get(pipelineLocation) : null;
        return state instanceof PipelineStatus ? (PipelineStatus) state : null;
    }

    private PipelineCleanupRecord copy(PipelineCleanupRecord record) {
        Map<TaskGroupLocation, Address> taskGroups =
                record.getTaskGroups() == null
                        ? Collections.emptyMap()
                        : new HashMap<>(record.getTaskGroups());
        Set<TaskGroupLocation> cleanedTaskGroups =
                record.getCleanedTaskGroups() == null
                        ? new HashSet<>()
                        : new HashSet<>(record.getCleanedTaskGroups());
        return new PipelineCleanupRecord(
                record.getPipelineLocation(),
                record.getFinalStatus(),
                record.isSavepointEnd(),
                taskGroups,
                cleanedTaskGroups,
                record.isMetricsImapCleaned(),
                record.getCreateTimeMillis(),
                record.getLastAttemptTimeMillis(),
                record.getAttemptCount());
    }

    private boolean cleanupPipelineMetrics(PipelineLocation pipelineLocation) {
        try {
            seaTunnelServer.removeMetrics(pipelineLocation);
            return true;
        } catch (Exception e) {
            logger.warning(
                    String.format(
                            "Failed to remove metrics context for pipeline %s: %s",
                            pipelineLocation, ExceptionUtils.getMessage(e)),
                    e);
            return false;
        }
    }

    /**
     * Restores all distributed running jobs after this node becomes the active master.
     *
     * <p>The restore process scans shared job metadata, skips jobs that are already owned by the
     * local coordinator, waits for workers to register, and then recreates each missing {@link
     * JobMaster}. Each restored job is re-enqueued as a pending job so it can re-enter the normal
     * scheduling path on the new master.
     */
    private void restoreAllRunningJobFromMasterNodeSwitch() {
        List<Map.Entry<Long, JobInfo>> needRestoreFromMasterNodeSwitchJobs;
        try {
            needRestoreFromMasterNodeSwitchJobs =
                    RetryUtils.retryWithException(
                            () ->
                                    runningJobInfoIMap.entrySet().stream()
                                            .filter(
                                                    entry ->
                                                            !runningJobMasterMap.containsKey(
                                                                    entry.getKey()))
                                            .collect(Collectors.toList()),
                            new RetryUtils.RetryMaterial(
                                    Constant.OPERATION_RETRY_TIME,
                                    true,
                                    ExceptionUtil::isOperationNeedRetryException,
                                    Constant.OPERATION_RETRY_SLEEP));
        } catch (Exception e) {
            throw new SeaTunnelEngineException(
                    "Failed to fetch running jobs from IMap during master switch restore", e);
        }
        if (needRestoreFromMasterNodeSwitchJobs.isEmpty()) {
            return;
        }
        // Pre-filter: clean up terminal-state zombie jobs immediately before waiting for workers.
        // Zombies do not need a worker — they only need IMap cleanup. Processing them here avoids
        // blocking zombie cleanup behind the worker-wait loop.
        Iterator<Map.Entry<Long, JobInfo>> zombieIterator =
                needRestoreFromMasterNodeSwitchJobs.iterator();
        while (zombieIterator.hasNext()) {
            Map.Entry<Long, JobInfo> entry = zombieIterator.next();
            Object jobState;
            try {
                jobState =
                        RetryUtils.retryWithException(
                                () -> runningJobStateIMap.get(entry.getKey()),
                                new RetryUtils.RetryMaterial(
                                        Constant.OPERATION_RETRY_TIME,
                                        true,
                                        ExceptionUtil::isOperationNeedRetryException,
                                        Constant.OPERATION_RETRY_SLEEP));
            } catch (Exception e) {
                logger.warning(
                        String.format(
                                "Failed to read job state for job %s during zombie pre-filter,"
                                        + " skipping: %s",
                                entry.getKey(), e.getMessage()));
                continue;
            }
            if (jobState instanceof JobStatus && ((JobStatus) jobState).isEndState()) {
                restoreJobFromMasterActiveSwitch(entry.getKey(), entry.getValue());
                zombieIterator.remove();
            }
        }
        if (needRestoreFromMasterNodeSwitchJobs.isEmpty()) {
            return;
        }
        // waiting have worker registered
        while (getResourceManager().workerCount(Collections.emptyMap()) == 0) {
            try {
                logger.info("Waiting for worker registered");
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                logger.severe(ExceptionUtils.getMessage(e));
                throw new SeaTunnelEngineException("wait worker register error", e);
            }
        }
        List<CompletableFuture<Void>> collect =
                needRestoreFromMasterNodeSwitchJobs.stream()
                        .map(
                                entry ->
                                        CompletableFuture.runAsync(
                                                () -> {
                                                    logger.info(
                                                            String.format(
                                                                    "begin restore job (%s) from master active switch",
                                                                    entry.getKey()));
                                                    try {
                                                        // skip the job new submit
                                                        if (!runningJobMasterMap.containsKey(
                                                                entry.getKey())) {
                                                            restoreJobFromMasterActiveSwitch(
                                                                    entry.getKey(),
                                                                    entry.getValue());
                                                        }
                                                    } catch (Exception e) {
                                                        logger.severe(e);
                                                    }
                                                    logger.info(
                                                            String.format(
                                                                    "restore job (%s) from master active switch finished",
                                                                    entry.getKey()));
                                                },
                                                MDCTracer.tracing(entry.getKey(), executorService)))
                        .collect(Collectors.toList());

        try {
            CompletableFuture<Void> voidCompletableFuture =
                    CompletableFuture.allOf(collect.toArray(new CompletableFuture[0]));
            voidCompletableFuture.get();
        } catch (Exception e) {
            logger.severe(ExceptionUtils.getMessage(e));
            throw new SeaTunnelEngineException(e);
        }
    }

    /**
     * Recreates a single {@link JobMaster} from distributed metadata after an active-master switch.
     *
     * <p>If the shared runtime state for the job has already disappeared, the stale {@link JobInfo}
     * entry is removed and restore stops. Otherwise a new {@link JobMaster} is initialized in
     * restart mode and re-enqueued as {@link PendingSourceState#RESTORE}, allowing the job to reuse
     * the standard pending-job scheduling flow on the new master.
     *
     * @param jobId restored job identifier
     * @param jobInfo distributed immutable job metadata captured before the master switch
     */
    private void restoreJobFromMasterActiveSwitch(@NonNull Long jobId, @NonNull JobInfo jobInfo) {
        Object jobState;
        try {
            jobState =
                    RetryUtils.retryWithException(
                            () -> runningJobStateIMap.get(jobId),
                            new RetryUtils.RetryMaterial(
                                    Constant.OPERATION_RETRY_TIME,
                                    true,
                                    ExceptionUtil::isOperationNeedRetryException,
                                    Constant.OPERATION_RETRY_SLEEP));
        } catch (Exception e) {
            throw new SeaTunnelEngineException(
                    String.format("Job id %s restore failed, can not get job state", jobId), e);
        }
        if (jobState == null) {
            runningJobInfoIMap.remove(jobId);
            return;
        }

        JobMaster jobMaster =
                new JobMaster(
                        jobId,
                        jobInfo.getJobImmutableInformation(),
                        nodeEngine,
                        MDCTracer.tracing(jobId, executorService),
                        getResourceManager(),
                        getJobHistoryService(),
                        runningJobStateIMap,
                        runningJobStateTimestampsIMap,
                        ownedSlotProfilesIMap,
                        runningJobInfoIMap,
                        engineConfig,
                        seaTunnelServer);

        try {
            jobMaster.init(jobInfo.getInitializationTimestamp(), true);
        } catch (Exception e) {
            throw new SeaTunnelEngineException(String.format("Job id %s init failed", jobId), e);
        }

        PendingJobInfo pendingJobInfo = new PendingJobInfo(PendingSourceState.RESTORE, jobMaster);
        pendingJobQueue.put(pendingJobInfo);
        jobMaster.getPhysicalPlan().updateJobState(JobStatus.PENDING);
        logger.info(String.format("The restore job enter pending queue, JobId: %s", jobId));
    }

    /**
     * Polls master ownership and reconciles the local coordinator lifecycle with cluster state.
     *
     * <p>When this node becomes the active master, the coordinator initializes distributed services
     * and triggers job restore. When it loses master ownership, local coordinator state is torn
     * down. Initialization failures are cleaned up locally and retried by later polling cycles.
     */
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
            logger.severe("check new active master error, will retry later.", e);
            try {
                clearCoordinatorService();
            } catch (Exception ex) {
                logger.warning("clear coordinator service failed.", ex);
            }
        }
    }

    /**
     * Clears the local coordinator runtime after this node stops serving as the active master.
     *
     * <p>This method interrupts locally owned {@link JobMaster} instances, optionally clears queued
     * pending jobs for wait-based scheduling, and shuts down local services such as the executor,
     * resource manager, and event processor. It does not delete distributed job metadata that a new
     * active master needs for takeover recovery.
     */
    public synchronized void clearCoordinatorService() {
        // interrupt all JobMaster
        runningJobMasterMap.values().forEach(JobMaster::interrupt);
        if (isWaitStrategy) {
            pendingJobQueue
                    .getJobIdMap()
                    .values()
                    .forEach(
                            pendingJobInfo -> {
                                JobMaster jobMaster = pendingJobInfo.getJobMaster();
                                jobMaster.interrupt();
                            });
            pendingJobQueue.clear();
        }
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

        try {
            if (eventProcessor != null) {
                eventProcessor.close();
            }
        } catch (Exception e) {
            throw new SeaTunnelEngineException("close event processor error", e);
        }
    }

    /** Lazy load for resource manager */
    public ResourceManager getResourceManager() {
        if (resourceManager == null) {
            synchronized (this) {
                if (resourceManager == null) {
                    ResourceManager manager =
                            new ResourceManagerFactory(nodeEngine, engineConfig)
                                    .getResourceManager();
                    manager.init();
                    resourceManager = manager;
                }
            }
        }
        return resourceManager;
    }

    /** call by client to submit job */
    public PassiveCompletableFuture<Void> submitJob(
            long jobId, Data jobImmutableInformation, boolean isStartWithSavePoint) {
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

        MDCExecutorService mdcExecutorService = MDCTracer.tracing(jobId, executorService);
        JobMaster jobMaster =
                new JobMaster(
                        jobId,
                        jobImmutableInformation,
                        this.nodeEngine,
                        mdcExecutorService,
                        getResourceManager(),
                        getJobHistoryService(),
                        runningJobStateIMap,
                        runningJobStateTimestampsIMap,
                        ownedSlotProfilesIMap,
                        runningJobInfoIMap,
                        engineConfig,
                        seaTunnelServer);
        mdcExecutorService.submit(
                () -> {
                    try {
                        if (!isStartWithSavePoint
                                && getJobHistoryService().getJobMetrics(jobId)
                                        != JobMetrics.empty()) {
                            throw new JobException(
                                    String.format(
                                            "The job id %s has already been submitted and is not starting with a savepoint.",
                                            jobId));
                        }
                        runningJobInfoIMap.put(
                                jobId,
                                new JobInfo(System.currentTimeMillis(), jobImmutableInformation));
                        jobMaster.init(
                                runningJobInfoIMap.get(jobId).getInitializationTimestamp(), false);
                        // Initialize the JobMaster and add it to the pendingJobQueue, ensuring that
                        // calling the getJobMaster method does not return NULL when the
                        // jobSubmitFuture is still running.
                        PendingJobInfo pendingJobInfo =
                                new PendingJobInfo(PendingSourceState.SUBMIT, jobMaster);
                        pendingJobQueue.put(pendingJobInfo);
                        // We specify that when init is complete, the submitJob is complete.
                        jobSubmitFuture.complete(null);
                    } catch (Throwable e) {
                        String errorMsg = ExceptionUtils.getMessage(e);
                        logger.severe(String.format("submit job %s error %s ", jobId, errorMsg));
                        jobSubmitFuture.completeExceptionally(new JobException(errorMsg));
                    }
                    if (!jobSubmitFuture.isCompletedExceptionally()) {
                        jobMaster.getPhysicalPlan().updateJobState(JobStatus.PENDING);
                        logger.info(
                                String.format(
                                        "The submit job enter the pending queue , jobId: %s , jobName: %s",
                                        jobId,
                                        jobMaster.getJobImmutableInformation().getJobName()));
                    } else {
                        runningJobInfoIMap.remove(jobId);
                        runningJobMasterMap.remove(jobId);
                        pendingJobQueue.removeById(jobId);
                    }
                });
        return new PassiveCompletableFuture<>(jobSubmitFuture);
    }

    public PassiveCompletableFuture<Void> savePoint(long jobId) {
        CompletableFuture<Void> voidCompletableFuture = new CompletableFuture<>();
        if (!runningJobMasterMap.containsKey(jobId)) {
            SavePointFailedException exception =
                    new SavePointFailedException(
                            "The job with id '" + jobId + "' not running, save point failed");
            logger.warning(exception);
            voidCompletableFuture.completeExceptionally(exception);
        } else {
            voidCompletableFuture =
                    new PassiveCompletableFuture<>(
                            CompletableFuture.supplyAsync(
                                    () -> {
                                        JobMaster runningJobMaster = runningJobMasterMap.get(jobId);
                                        if (!runningJobMaster.savePoint().join()) {
                                            throw new SavePointFailedException(
                                                    "The job with id '"
                                                            + jobId
                                                            + "' save point failed");
                                        }
                                        try {
                                            waitForJobComplete(jobId).get();
                                        } catch (Throwable e) {
                                            logger.warning(
                                                    String.format(
                                                            "The job with id '%s' waiting state complete failed",
                                                            jobId));
                                        }
                                        return null;
                                    },
                                    executorService));
        }
        return new PassiveCompletableFuture<>(voidCompletableFuture);
    }

    public PassiveCompletableFuture<JobResult> waitForJobComplete(long jobId) {
        // must wait for all job restore complete
        restoreAllJobFromMasterNodeSwitchFuture.join();
        JobMaster runningJobMaster = getJobMaster(jobId);
        if (runningJobMaster == null) {
            // Because operations on Imap cannot be performed within Operation.
            CompletableFuture<JobHistoryService.JobState> jobStateFuture =
                    CompletableFuture.supplyAsync(
                            () -> jobHistoryService.getJobDetailState(jobId), executorService);
            JobHistoryService.JobState jobState = null;
            try {
                jobState = jobStateFuture.get();
            } catch (Exception e) {
                throw new SeaTunnelEngineException("get job state error", e);
            }

            CompletableFuture<JobResult> future = new CompletableFuture<>();
            if (jobState == null) {
                future.complete(new JobResult(JobStatus.UNKNOWABLE, null));
            } else {
                future.complete(new JobResult(jobState.getJobStatus(), jobState.getErrorMessage()));
            }
            return new PassiveCompletableFuture<>(future);
        } else {
            return new PassiveCompletableFuture<>(runningJobMaster.getJobMasterCompleteFuture());
        }
    }

    public PassiveCompletableFuture<Void> cancelJob(long jobId) {
        JobMaster runningJobMaster = getJobMaster(jobId);
        if (runningJobMaster == null) {
            CompletableFuture<Void> future = new CompletableFuture<>();
            future.complete(null);
            return new PassiveCompletableFuture<>(future);
        } else {
            boolean isPendingJob = pendingJobQueue.contains(jobId);
            // Cancel pending tasks
            if (isPendingJob) {
                pendingJobQueue.removeById(jobId);
                logger.fine(String.format("Cancel pending tasks : %s", jobId));
            }
            return new PassiveCompletableFuture<>(
                    CompletableFuture.supplyAsync(
                            () -> {
                                runningJobMaster.cancelJob();
                                return null;
                            },
                            executorService));
        }
    }

    public PassiveCompletableFuture<Void> stopJob(long jobId) {
        JobMaster runningJobMaster = getJobMaster(jobId);
        if (runningJobMaster == null) {
            CompletableFuture<Void> future = new CompletableFuture<>();
            future.complete(null);
            return new PassiveCompletableFuture<>(future);
        } else {
            boolean isPendingJob = pendingJobQueue.contains(jobId);
            if (isPendingJob) {
                pendingJobQueue.removeById(jobId);
                logger.fine(String.format("Stop pending tasks : %s", jobId));
            }
            return new PassiveCompletableFuture<>(
                    CompletableFuture.supplyAsync(
                            () -> {
                                runningJobMaster.stopJob();
                                return null;
                            },
                            executorService));
        }
    }

    public JobStatus getJobStatus(long jobId) {
        if (pendingJobQueue.contains(jobId)) {
            return JobStatus.PENDING;
        }
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
        if (pendingJobQueue.contains(jobId)) {
            // Tasks in pending, metric data is empty
            return JobMetrics.empty();
        }
        JobMaster runningJobMaster = runningJobMasterMap.get(jobId);
        if (runningJobMaster == null) {
            return jobHistoryService.getJobMetrics(jobId);
        }
        JobMetrics jobMetrics = JobMetricsUtil.toJobMetrics(runningJobMaster.getCurrJobMetrics());
        JobMetrics jobMetricsImap = jobHistoryService.getJobMetrics(jobId);
        return jobMetricsImap != JobMetrics.empty() ? jobMetricsImap.merge(jobMetrics) : jobMetrics;
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
                    if (jobMetricsImap != JobMetrics.empty()) {
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

        JobMaster runningJobMaster = runningJobMasterMap.get(jobId);
        if (runningJobMaster != null) {
            return runningJobMaster.getJobDAGInfo();
        }

        PendingJobInfo pendingJobInfo = pendingJobQueue.getById(jobId);
        if (pendingJobInfo != null) {
            return pendingJobInfo.getJobMaster().getJobDAGInfo();
        }

        throw new JobNotFoundException(String.format("Job %s not found", jobId));
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
        if (pipelineCleanupScheduler != null) {
            pipelineCleanupScheduler.shutdownNow();
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
        ThreadPoolStatus threadPoolStatus = getThreadPoolStatusMetrics();
        logger.info(
                StringFormatUtils.formatTable(
                        "CoordinatorService Thread Pool Status",
                        "activeCount",
                        threadPoolStatus.getActiveCount(),
                        "corePoolSize",
                        threadPoolStatus.getCorePoolSize(),
                        "maximumPoolSize",
                        threadPoolStatus.getMaximumPoolSize(),
                        "poolSize",
                        threadPoolStatus.getPoolSize(),
                        "completedTaskCount",
                        threadPoolStatus.getCompletedTaskCount(),
                        "taskCount",
                        threadPoolStatus.getTaskCount()));
    }

    public void printJobDetailInfo() {
        JobCounter jobCounter = getJobCountMetrics();
        logger.info(
                StringFormatUtils.formatTable(
                        "Job info detail",
                        "createdJobCount",
                        jobCounter.getCreatedJobCount(),
                        "pendingJobCount",
                        jobCounter.getPendingJobCount(),
                        "scheduledJobCount",
                        jobCounter.getScheduledJobCount(),
                        "runningJobCount",
                        jobCounter.getRunningJobCount(),
                        "failingJobCount",
                        jobCounter.getFailingJobCount(),
                        "failedJobCount",
                        jobCounter.getFailedJobCount(),
                        "cancellingJobCount",
                        jobCounter.getCancellingJobCount(),
                        "canceledJobCount",
                        jobCounter.getCanceledJobCount(),
                        "finishedJobCount",
                        jobCounter.getFinishedJobCount()));
    }

    public JobCounter getJobCountMetrics() {
        AtomicLong createdJobCount = new AtomicLong();
        AtomicLong scheduledJobCount = new AtomicLong();
        AtomicLong runningJobCount = new AtomicLong();
        AtomicLong pendingJobCount = new AtomicLong();
        AtomicLong failingJobCount = new AtomicLong();
        AtomicLong failedJobCount = new AtomicLong();
        AtomicLong cancellingJobCount = new AtomicLong();
        AtomicLong canceledJobCount = new AtomicLong();
        AtomicLong finishedJobCount = new AtomicLong();

        if (jobHistoryService != null) {
            jobHistoryService
                    .getJobStatusData()
                    .forEach(
                            jobStatusData -> {
                                JobStatus jobStatus = jobStatusData.getJobStatus();
                                switch (jobStatus) {
                                    case CREATED:
                                        createdJobCount.addAndGet(1);
                                        break;
                                    case PENDING:
                                        pendingJobCount.addAndGet(1);
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
                            });
        }

        return new JobCounter(
                createdJobCount.longValue(),
                pendingJobCount.longValue(),
                scheduledJobCount.longValue(),
                runningJobCount.longValue(),
                failingJobCount.longValue(),
                failedJobCount.longValue(),
                cancellingJobCount.longValue(),
                canceledJobCount.longValue(),
                finishedJobCount.longValue());
    }

    public ThreadPoolStatus getThreadPoolStatusMetrics() {
        ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) executorService;

        long rejectionCount =
                ((ThreadPoolStatus.RejectionCountingHandler)
                                threadPoolExecutor.getRejectedExecutionHandler())
                        .getRejectionCount();
        long queueTaskSize = threadPoolExecutor.getQueue().size();
        return new ThreadPoolStatus(
                threadPoolExecutor.getActiveCount(),
                threadPoolExecutor.getCorePoolSize(),
                threadPoolExecutor.getMaximumPoolSize(),
                threadPoolExecutor.getPoolSize(),
                threadPoolExecutor.getCompletedTaskCount(),
                threadPoolExecutor.getTaskCount(),
                queueTaskSize,
                rejectionCount);
    }

    public ConnectorPackageService getConnectorPackageService() {
        if (connectorPackageService == null) {
            throw new SeaTunnelEngineException(
                    "The user is not configured to enable connector package service, can not get connector package service service from master node.");
        }
        return connectorPackageService;
    }

    public PendingJobsResponse getPendingJobs(Map<String, String> tags, Long jobId, int limit) {
        Collection<PendingJobInfo> allPendingJobs =
                new ArrayList<>(pendingJobQueue.getJobIdMap().values());

        List<PendingJobInfo> selectedJobs = new ArrayList<>();
        if (jobId != null) {
            PendingJobInfo pendingJobInfo = pendingJobQueue.getById(jobId);
            if (pendingJobInfo != null) {
                selectedJobs.add(pendingJobInfo);
            }
        } else {
            selectedJobs.addAll(allPendingJobs);
            selectedJobs.sort(Comparator.comparingLong(PendingJobInfo::getEnqueueTimestamp));
            if (limit > 0 && selectedJobs.size() > limit) {
                selectedJobs = new ArrayList<>(selectedJobs.subList(0, limit));
            }
        }

        ResourceManager resourceManager = getResourceManager();
        List<PendingJobDiagnostic> diagnostics = new ArrayList<>();
        for (PendingJobInfo jobInfo : selectedJobs) {
            PendingJobDiagnostic diagnostic = jobInfo.getLastSnapshot();
            if (diagnostic == null) {
                diagnostic =
                        PendingDiagnosticsCollector.collectJobDiagnostic(
                                jobInfo, tags, resourceManager);
                if (diagnostic != null) {
                    diagnostic.setCheckCount(jobInfo.getCheckTimes());
                }
            }
            if (diagnostic != null) {
                diagnostics.add(diagnostic);
            }
        }

        PendingJobsResponse response = new PendingJobsResponse();
        response.setPendingJobs(diagnostics);
        response.setClusterSnapshot(
                PendingDiagnosticsCollector.collectClusterSnapshot(resourceManager, tags));
        response.setQueueSummary(buildQueueSummary(allPendingJobs, diagnostics));
        return response;
    }

    private PendingQueueSummary buildQueueSummary(
            Collection<PendingJobInfo> pendingJobs, List<PendingJobDiagnostic> diagnostics) {
        PendingQueueSummary summary = new PendingQueueSummary();
        summary.setSize(pendingJobQueue.size());
        summary.setScheduleStrategy(scheduleStrategy.name());
        summary.setLackingTaskGroups(
                diagnostics.stream().mapToInt(PendingJobDiagnostic::getLackingTaskGroups).sum());

        if (!pendingJobs.isEmpty()) {
            summary.setOldestEnqueueTimestamp(
                    pendingJobs.stream()
                            .mapToLong(PendingJobInfo::getEnqueueTimestamp)
                            .min()
                            .orElse(0L));
            summary.setNewestEnqueueTimestamp(
                    pendingJobs.stream()
                            .mapToLong(PendingJobInfo::getEnqueueTimestamp)
                            .max()
                            .orElse(0L));
        }
        return summary;
    }

    public int getPendingJobCount() {
        return pendingJobQueue.getJobIdMap().size();
    }

    @VisibleForTesting
    protected IMap<Long, HashMap<TaskLocation, SeaTunnelMetricsContext>> getMetricsImap() {
        return metricsImap;
    }

    @VisibleForTesting
    void runPendingPipelineCleanupOnce() {
        cleanupPendingPipelines();
    }

    @VisibleForTesting
    public PeekBlockingQueue<PendingJobInfo> getPendingJobQueue() {
        return pendingJobQueue;
    }
}
