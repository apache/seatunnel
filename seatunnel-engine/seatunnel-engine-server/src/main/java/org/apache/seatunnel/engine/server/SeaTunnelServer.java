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
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
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
import org.apache.seatunnel.engine.server.service.slot.DefaultSlotService;
import org.apache.seatunnel.engine.server.service.slot.SlotService;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.hazelcast.cluster.Address;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.services.ManagedService;
import com.hazelcast.internal.services.MembershipAwareService;
import com.hazelcast.internal.services.MembershipServiceEvent;
import com.hazelcast.jet.impl.LiveOperationRegistry;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.spi.impl.operationservice.LiveOperations;
import com.hazelcast.spi.impl.operationservice.LiveOperationsTracker;
import lombok.NonNull;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class SeaTunnelServer implements ManagedService, MembershipAwareService, LiveOperationsTracker {
    private static final ILogger LOGGER = Logger.getLogger(SeaTunnelServer.class);
    public static final String SERVICE_NAME = "st:impl:seaTunnelServer";

    private NodeEngineImpl nodeEngine;
    private final ILogger logger;
    private final LiveOperationRegistry liveOperationRegistry;

    private volatile SlotService slotService;
    private TaskExecutionService taskExecutionService;

    private final ExecutorService executorService;
    private volatile ResourceManager resourceManager;

    private final SeaTunnelConfig seaTunnelConfig;

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

    public SeaTunnelServer(@NonNull Node node, @NonNull SeaTunnelConfig seaTunnelConfig) {
        this.logger = node.getLogger(getClass());
        this.liveOperationRegistry = new LiveOperationRegistry();
        this.seaTunnelConfig = seaTunnelConfig;
        this.executorService =
            Executors.newCachedThreadPool(new ThreadFactoryBuilder()
                .setNameFormat("seatunnel-server-executor-%d").build());
        logger.info("SeaTunnel server start...");
    }

    /**
     * Lazy load for Slot Service
     */
    public SlotService getSlotService() {
        if (slotService == null) {
            synchronized (this) {
                if (slotService == null) {
                    SlotService service = new DefaultSlotService(nodeEngine, taskExecutionService, true, 2);
                    service.init();
                    slotService = service;
                }
            }
        }
        return slotService;
    }

    public JobMaster getJobMaster(Long jobId) {
        return runningJobMasterMap.get(jobId);
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    @Override
    public void init(NodeEngine engine, Properties hzProperties) {
        this.nodeEngine = (NodeEngineImpl) engine;
        // TODO Determine whether to execute there method on the master node according to the deploy type
        taskExecutionService = new TaskExecutionService(
            nodeEngine, nodeEngine.getProperties()
        );
        taskExecutionService.start();
        getSlotService();

        runningJobInfoIMap = nodeEngine.getHazelcastInstance().getMap("runningJobInfo");
        runningJobStateIMap = nodeEngine.getHazelcastInstance().getMap("runningJobState");
        runningJobStateTimestampsIMap = nodeEngine.getHazelcastInstance().getMap("stateTimestamps");
        ownedSlotProfilesIMap = nodeEngine.getHazelcastInstance().getMap("ownedSlotProfilesIMap");

        ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
        service.scheduleAtFixedRate(() -> printExecutionInfo(), 0, 60, TimeUnit.SECONDS);
    }

    @Override
    public void reset() {

    }

    @Override
    public void shutdown(boolean terminate) {
        if (slotService != null) {
            slotService.close();
        }
        if (resourceManager != null) {
            resourceManager.close();
        }
        executorService.shutdown();
        taskExecutionService.shutdown();
    }

    @Override
    public void memberAdded(MembershipServiceEvent event) {

    }

    @Override
    public void memberRemoved(MembershipServiceEvent event) {
        resourceManager.memberRemoved(event);
        failedTaskOnMemberRemoved(event);
    }

    @Override
    public void populate(LiveOperations liveOperations) {

    }

    /**
     * Used for debugging on call
     */
    public String printMessage(String message) {
        this.logger.info(nodeEngine.getThisAddress() + ":" + message);
        return message;
    }

    public LiveOperationRegistry getLiveOperationRegistry() {
        return liveOperationRegistry;
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

    public TaskExecutionService getTaskExecutionService() {
        return taskExecutionService;
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
                jobMaster.getPhysicalPlan().initStateFuture();
            } catch (Throwable e) {
                LOGGER.severe(String.format("submit job %s error %s ", jobId, ExceptionUtils.getMessage(e)));
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

        // These should be deleted at the end. On the new master node
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
        // This method is called by operation and in the runningJobMaster.getJobStatus() we will get data from IMap.
        // It will cause an error "Waiting for response on this thread is illegal". To solve it we need put
        // runningJobMaster.getJobStatus() in another thread.
        CompletableFuture<JobStatus> future = CompletableFuture.supplyAsync(() -> {
            return runningJobMaster.getJobStatus();
        }, executorService);

        try {
            return future.get();
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }
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
                        new SeaTunnelEngineException(
                            String.format("The taskGroup(%s) deployed node(%s) offline", taskGroupLocation,
                                lostAddress))));
            }
        });
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

    private void printExecutionInfo() {
        ThreadPoolExecutor threadPoolExecutor = (ThreadPoolExecutor) executorService;
        int activeCount = threadPoolExecutor.getActiveCount();
        int corePoolSize = threadPoolExecutor.getCorePoolSize();
        int maximumPoolSize = threadPoolExecutor.getMaximumPoolSize();
        int poolSize = threadPoolExecutor.getPoolSize();
        long completedTaskCount = threadPoolExecutor.getCompletedTaskCount();
        long taskCount = threadPoolExecutor.getTaskCount();
        StringBuffer sbf = new StringBuffer();
        sbf.append("activeCount=")
            .append(activeCount)
            .append("\n")
            .append("corePoolSize=")
            .append(corePoolSize)
            .append("\n")
            .append("maximumPoolSize=")
            .append(maximumPoolSize)
            .append("\n")
            .append("poolSize=")
            .append(poolSize)
            .append("\n")
            .append("completedTaskCount=")
            .append(completedTaskCount)
            .append("\n")
            .append("taskCount=")
            .append(taskCount);
        logger.info(sbf.toString());
    }
}
