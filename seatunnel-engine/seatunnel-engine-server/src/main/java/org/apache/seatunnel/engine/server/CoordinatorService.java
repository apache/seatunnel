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

import org.apache.seatunnel.common.utils.ExceptionUtils;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.exception.JobException;
import org.apache.seatunnel.engine.common.exception.SeaTunnelEngineException;
import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.core.job.JobStatus;
import org.apache.seatunnel.engine.core.job.RunningJobInfo;
import org.apache.seatunnel.engine.server.dag.physical.PhysicalVertex;
import org.apache.seatunnel.engine.server.dag.physical.PipelineLocation;
import org.apache.seatunnel.engine.server.dag.physical.SubPlan;
import org.apache.seatunnel.engine.server.execution.ExecutionState;
import org.apache.seatunnel.engine.server.execution.TaskExecutionState;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.master.JobMaster;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManager;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManagerFactory;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.services.MembershipServiceEvent;
import com.hazelcast.logging.ILogger;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.NodeEngineImpl;
import lombok.NonNull;

import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class CoordinatorService {
    private NodeEngineImpl nodeEngine;
    private final ILogger logger;

    private volatile ResourceManager resourceManager;

    /**
     * IMap key is jobId and value is a Tuple2
     * Tuple2 key is JobMaster init timestamp and value is the jobImmutableInformation which is sent by client when submit job
     * <p>
     * This IMap is used to recovery runningJobInfoIMap in JobMaster when a new master node active
     */
    private IMap<Long, RunningJobInfo> runningJobInfoIMap;

    /**
     * IMap key is one of jobId {@link org.apache.seatunnel.engine.server.dag.physical.PipelineLocation} and
     * {@link org.apache.seatunnel.engine.server.execution.TaskGroupLocation}
     * <p>
     * The value of IMap is one of {@link JobStatus} {@link org.apache.seatunnel.engine.core.job.PipelineState}
     * {@link org.apache.seatunnel.engine.server.execution.ExecutionState}
     * <p>
     * This IMap is used to recovery runningJobStateIMap in JobMaster when a new master node active
     */
    IMap<Object, Object> runningJobStateIMap;

    /**
     * IMap key is one of jobId {@link org.apache.seatunnel.engine.server.dag.physical.PipelineLocation} and
     * {@link org.apache.seatunnel.engine.server.execution.TaskGroupLocation}
     * <p>
     * The value of IMap is one of {@link org.apache.seatunnel.engine.server.dag.physical.PhysicalPlan} stateTimestamps
     * {@link org.apache.seatunnel.engine.server.dag.physical.SubPlan} stateTimestamps
     * {@link org.apache.seatunnel.engine.server.dag.physical.PhysicalVertex} stateTimestamps
     * <p>
     * This IMap is used to recovery runningJobStateTimestampsIMap in JobMaster when a new master node active
     */
    IMap<Object, Long[]> runningJobStateTimestampsIMap;

    /**
     * key: job id;
     * <br> value: job master;
     */
    private Map<Long, JobMaster> runningJobMasterMap = new ConcurrentHashMap<>();

    /**
     * IMap key is {@link PipelineLocation}
     * <p>
     * The value of IMap is map of {@link TaskGroupLocation} and the {@link SlotProfile} it used.
     * <p>
     * This IMap is used to recovery ownedSlotProfilesIMap in JobMaster when a new master node active
     */
    private IMap<PipelineLocation, Map<TaskGroupLocation, SlotProfile>> ownedSlotProfilesIMap;

    /**
     * If this node is a master node
     */
    private volatile boolean isActive = false;

    private final ExecutorService executorService;

    private final SeaTunnelServer seaTunnelServer;

    private ScheduledExecutorService masterActiveListener;

    @SuppressWarnings("checkstyle:MagicNumber")
    public CoordinatorService(@NonNull NodeEngineImpl nodeEngine, @NonNull ExecutorService executorService,
                              @NonNull SeaTunnelServer seaTunnelServer) {
        this.nodeEngine = nodeEngine;
        this.logger = nodeEngine.getLogger(getClass());
        this.executorService = executorService;
        this.seaTunnelServer = seaTunnelServer;
        masterActiveListener = Executors.newSingleThreadScheduledExecutor();
        CompletableFuture future = new CompletableFuture();
        future.whenComplete((r, t) -> {
            throw new SeaTunnelEngineException("check new active master thread failed", (Throwable) t);
        });
        masterActiveListener.scheduleAtFixedRate(() -> checkNewActiveMaster(future), 0, 100, TimeUnit.MILLISECONDS);
    }

    public JobMaster getJobMaster(Long jobId) {
        return runningJobMasterMap.get(jobId);
    }

    // On the new master node
    // 1. If runningJobStateIMap.get(jobId) == null and runningJobInfoIMap.get(jobId) != null. We will do
    //    runningJobInfoIMap.remove(jobId)
    //
    // 2. If runningJobStateIMap.get(jobId) != null and the value equals JobStatus End State. We need new a
    //    JobMaster and generate PhysicalPlan again and then try to remove all of PipelineLocation and
    //    TaskGroupLocation key in the runningJobStateIMap.
    //
    // 3. If runningJobStateIMap.get(jobId) != null and the value equals JobStatus.SCHEDULED. We need cancel the job
    //    and then call submitJob(long jobId, Data jobImmutableInformation) to resubmit it.
    //
    // 4. If runningJobStateIMap.get(jobId) != null and the value is CANCELING or RUNNING. We need recover the JobMaster
    //    from runningJobStateIMap and then waiting for it complete.
    private void initCoordinatorService() {
        runningJobInfoIMap = nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_INFO);
        runningJobStateIMap = nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_RUNNING_JOB_STATE);
        runningJobStateTimestampsIMap = nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_STATE_TIMESTAMPS);
        ownedSlotProfilesIMap = nodeEngine.getHazelcastInstance().getMap(Constant.IMAP_OWNED_SLOT_PROFILES);

        List<CompletableFuture<Void>> collect = runningJobInfoIMap.entrySet().stream().map(entry -> {
            return CompletableFuture.runAsync(() -> {
                logger.info(String.format("begin restore job (%s) from master active switch", entry.getKey()));
                restoreJobFromMasterActiveSwitch(entry.getKey(), entry.getValue());
                logger.info(String.format("restore job (%s) from master active switch finished", entry.getKey()));
            }, executorService);
        }).collect(Collectors.toList());

        try {
            CompletableFuture<Void> voidCompletableFuture = CompletableFuture.allOf(
                collect.toArray(new CompletableFuture[0]));
            voidCompletableFuture.get();
        } catch (Exception e) {
            throw new SeaTunnelEngineException(e);
        }
    }

    private void restoreJobFromMasterActiveSwitch(@NonNull Long jobId, @NonNull RunningJobInfo runningJobInfo) {
        if (runningJobStateIMap.get(jobId) == null) {
            runningJobInfoIMap.remove(jobId);
            return;
        }

        JobStatus jobStatus = (JobStatus) runningJobStateIMap.get(jobId);
        JobMaster jobMaster =
            new JobMaster(runningJobInfo.getJobImmutableInformation(),
                nodeEngine,
                executorService,
                getResourceManager(),
                runningJobStateIMap,
                runningJobStateTimestampsIMap,
                ownedSlotProfilesIMap);

        try {
            jobMaster.init(runningJobInfoIMap.get(jobId).getInitializationTimestamp());
        } catch (Exception e) {
            throw new SeaTunnelEngineException(String.format("Job id %s init JobMaster failed", jobId));
        }

        String jobFullName = jobMaster.getPhysicalPlan().getJobFullName();
        if (jobStatus.isEndState()) {
            logger.info(String.format(
                "The restore %s is in an end state %s, store the job info to JobHistory and clear the job running time info",
                jobFullName, jobStatus));
            removeJobIMap(jobMaster);
            return;
        }

        if (jobStatus.ordinal() < JobStatus.RUNNING.ordinal()) {
            logger.info(
                String.format("The restore %s is state %s, cancel job and submit it again.", jobFullName, jobStatus));
            jobMaster.cancelJob();
            jobMaster.getJobMasterCompleteFuture().join();
            submitJob(jobId, runningJobInfo.getJobImmutableInformation()).join();
            return;
        }

        runningJobMasterMap.put(jobId, jobMaster);
        jobMaster.markRestore();

        if (JobStatus.CANCELLING.equals(jobStatus)) {
            logger.info(String.format("The restore %s is in %s state, cancel the job", jobFullName, jobStatus));
            CompletableFuture.runAsync(() -> {
                try {
                    jobMaster.cancelJob();
                    jobMaster.run();
                } finally {
                    // storage job state info to HistoryStorage
                    removeJobIMap(jobMaster);
                    runningJobMasterMap.remove(jobId);
                }
            });
            return;
        }

        if (JobStatus.RUNNING.equals(jobStatus)) {
            logger.info(String.format("The restore %s is in %s state, restore pipeline and take over this job running", jobFullName, jobStatus));
            CompletableFuture.runAsync(() -> {
                try {
                    jobMaster.getPhysicalPlan().getPipelineList().forEach(SubPlan::restorePipelineState);
                    jobMaster.run();
                } finally {
                    // storage job state info to HistoryStorage
                    removeJobIMap(jobMaster);
                    runningJobMasterMap.remove(jobId);
                }
            });
            return;
        }
    }

    private void checkNewActiveMaster(@NonNull CompletableFuture future) {
        try {
            if (!isActive && this.seaTunnelServer.isMasterNode()) {
                logger.info("This node become a new active master node, begin init coordinator service");
                initCoordinatorService();
                isActive = true;
            } else if (isActive && !this.seaTunnelServer.isMasterNode()) {
                isActive = false;
                logger.info("This node become leave active master node, begin clear coordinator service");
                clearCoordinatorService();
            }
        } catch (Exception e) {
            logger.severe(ExceptionUtils.getMessage(e));
            future.completeExceptionally(e);
        }
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    private void clearCoordinatorService() {
        // interrupt all JobMaster
        runningJobMasterMap.values().forEach(JobMaster::interrupt);
        executorService.shutdownNow();

        try {
            executorService.awaitTermination(20, TimeUnit.SECONDS);
            runningJobMasterMap = new ConcurrentHashMap<>();
        } catch (InterruptedException e) {
            throw new SeaTunnelEngineException("wait clean executor service error");
        }

        if (resourceManager != null) {
            resourceManager.close();
        }
    }

    /**
     * Lazy load for resource manager
     */
    public ResourceManager getResourceManager() {
        if (resourceManager == null) {
            synchronized (this) {
                if (resourceManager == null) {
                    ResourceManager manager = new ResourceManagerFactory(nodeEngine).getResourceManager();
                    manager.init();
                    resourceManager = manager;
                }
            }
        }
        return resourceManager;
    }

    /**
     * call by client to submit job
     */
    public PassiveCompletableFuture<Void> submitJob(long jobId, Data jobImmutableInformation) {
        CompletableFuture<Void> voidCompletableFuture = new CompletableFuture<>();
        JobMaster jobMaster = new JobMaster(jobImmutableInformation,
            this.nodeEngine,
            executorService,
            getResourceManager(),
            runningJobStateIMap,
            runningJobStateTimestampsIMap,
            ownedSlotProfilesIMap);
        executorService.submit(() -> {
            try {
                runningJobInfoIMap.put(jobId, new RunningJobInfo(System.currentTimeMillis(), jobImmutableInformation));
                runningJobMasterMap.put(jobId, jobMaster);
                jobMaster.init(runningJobInfoIMap.get(jobId).getInitializationTimestamp());
            } catch (Throwable e) {
                logger.severe(String.format("submit job %s error %s ", jobId, ExceptionUtils.getMessage(e)));
                voidCompletableFuture.completeExceptionally(e);
            } finally {
                // We specify that when init is complete, the submitJob is complete
                voidCompletableFuture.complete(null);
            }

            try {
                jobMaster.run();
            } finally {
                // storage job state info to HistoryStorage
                removeJobIMap(jobMaster);
                runningJobMasterMap.remove(jobId);
            }
        });
        return new PassiveCompletableFuture(voidCompletableFuture);
    }

    private void removeJobIMap(JobMaster jobMaster) {
        Long jobId = jobMaster.getJobImmutableInformation().getJobId();
        runningJobStateTimestampsIMap.remove(jobId);

        jobMaster.getPhysicalPlan().getPipelineList().forEach(pipeline -> {
            runningJobStateIMap.remove(pipeline.getPipelineLocation());
            runningJobStateTimestampsIMap.remove(pipeline.getPipelineLocation());
            pipeline.getCoordinatorVertexList().forEach(coordinator -> {
                runningJobStateIMap.remove(coordinator.getTaskGroupLocation());
                runningJobStateTimestampsIMap.remove(coordinator.getTaskGroupLocation());
            });

            pipeline.getPhysicalVertexList().forEach(task -> {
                runningJobStateIMap.remove(task.getTaskGroupLocation());
                runningJobStateTimestampsIMap.remove(task.getTaskGroupLocation());
            });
        });

        // These should be deleted at the end.
        runningJobStateIMap.remove(jobId);
        runningJobInfoIMap.remove(jobId);
    }

    public PassiveCompletableFuture<JobStatus> waitForJobComplete(long jobId) {
        JobMaster runningJobMaster = runningJobMasterMap.get(jobId);
        if (runningJobMaster == null) {
            // TODO Get Job Status from JobHistoryStorage
            CompletableFuture<JobStatus> future = new CompletableFuture<>();
            future.complete(JobStatus.FINISHED);
            return new PassiveCompletableFuture<>(future);
        } else {
            return runningJobMaster.getJobMasterCompleteFuture();
        }
    }

    public PassiveCompletableFuture<Void> cancelJob(long jodId) {
        JobMaster runningJobMaster = runningJobMasterMap.get(jodId);
        if (runningJobMaster == null) {
            CompletableFuture<Void> future = new CompletableFuture<>();
            future.complete(null);
            return new PassiveCompletableFuture<>(future);
        } else {
            return new PassiveCompletableFuture<>(CompletableFuture.supplyAsync(() -> {
                runningJobMaster.cancelJob();
                return null;
            }, executorService));
        }
    }

    public JobStatus getJobStatus(long jobId) {
        JobMaster runningJobMaster = runningJobMasterMap.get(jobId);
        if (runningJobMaster == null) {
            // TODO Get Job Status from JobHistoryStorage
            return JobStatus.FINISHED;
        }
        return runningJobMaster.getJobStatus();
    }

    /**
     * When TaskGroup ends, it is called by {@link TaskExecutionService} to notify JobMaster the TaskGroup's state.
     */
    public void updateTaskExecutionState(TaskExecutionState taskExecutionState) {
        TaskGroupLocation taskGroupLocation = taskExecutionState.getTaskGroupLocation();
        JobMaster runningJobMaster = runningJobMasterMap.get(taskGroupLocation.getJobId());
        if (runningJobMaster == null) {
            throw new JobException(String.format("Job %s not running", taskGroupLocation.getJobId()));
        }
        runningJobMaster.updateTaskExecutionState(taskExecutionState);
    }

    public void shutdown() {
        if (masterActiveListener != null) {
            masterActiveListener.shutdown();
        }
        if (resourceManager != null) {
            resourceManager.close();
        }
    }

    /**
     * return true if this node is a master node and the coordinator service init finished.
     *
     * @return
     */
    public boolean isCoordinatorActive() {
        return isActive;
    }

    public void failedTaskOnMemberRemoved(MembershipServiceEvent event) {
        Address lostAddress = event.getMember().getAddress();
        runningJobMasterMap.forEach((aLong, jobMaster) -> {
            jobMaster.getPhysicalPlan().getPipelineList().forEach(subPlan -> {
                makeTasksFailed(subPlan.getCoordinatorVertexList(), lostAddress);
                makeTasksFailed(subPlan.getPhysicalVertexList(), lostAddress);
            });
        });
    }

    private void makeTasksFailed(@NonNull List<PhysicalVertex> physicalVertexList, @NonNull Address lostAddress) {
        physicalVertexList.forEach(physicalVertex -> {
            Address deployAddress = physicalVertex.getCurrentExecutionAddress();
            ExecutionState executionState = physicalVertex.getExecutionState();
            if (null != deployAddress && deployAddress.equals(lostAddress) &&
                (executionState.equals(ExecutionState.DEPLOYING) ||
                    executionState.equals(ExecutionState.RUNNING))) {
                TaskGroupLocation taskGroupLocation = physicalVertex.getTaskGroupLocation();
                physicalVertex.updateTaskExecutionState(
                    new TaskExecutionState(taskGroupLocation, ExecutionState.FAILED,
                        new JobException(
                            String.format("The taskGroup(%s) deployed node(%s) offline", taskGroupLocation,
                                lostAddress))));
            }
        });
    }

    public void memberRemoved(MembershipServiceEvent event) {
        this.getResourceManager().memberRemoved(event);
        this.failedTaskOnMemberRemoved(event);
    }
}
