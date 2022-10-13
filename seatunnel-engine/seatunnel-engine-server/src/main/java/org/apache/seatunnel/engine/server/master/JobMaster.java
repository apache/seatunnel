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

import org.apache.seatunnel.common.utils.ExceptionUtils;
import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.config.EngineConfig;
import org.apache.seatunnel.engine.common.config.server.CheckpointConfig;
import org.apache.seatunnel.engine.common.config.server.CheckpointStorageConfig;
import org.apache.seatunnel.engine.common.config.server.ServerConfigOptions;
import org.apache.seatunnel.engine.common.loader.SeatunnelChildFirstClassLoader;
import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.core.job.JobStatus;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointManager;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointPlan;
import org.apache.seatunnel.engine.server.dag.physical.PhysicalPlan;
import org.apache.seatunnel.engine.server.dag.physical.PipelineLocation;
import org.apache.seatunnel.engine.server.dag.physical.PlanUtils;
import org.apache.seatunnel.engine.server.dag.physical.SubPlan;
import org.apache.seatunnel.engine.server.execution.TaskExecutionState;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManager;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;
import org.apache.seatunnel.engine.server.scheduler.JobScheduler;
import org.apache.seatunnel.engine.server.scheduler.PipelineBaseScheduler;

import com.google.common.collect.Lists;
import com.hazelcast.cluster.Address;
import com.hazelcast.flakeidgen.FlakeIdGenerator;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.jet.datamodel.Tuple2;
import com.hazelcast.jet.impl.execution.init.CustomClassLoadedObject;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.NodeEngine;
import lombok.NonNull;
import org.apache.commons.collections4.CollectionUtils;

import java.util.Map;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;

public class JobMaster extends Thread {
    private static final ILogger LOGGER = Logger.getLogger(JobMaster.class);

    private PhysicalPlan physicalPlan;
    private final Data jobImmutableInformationData;

    private final NodeEngine nodeEngine;

    private final ExecutorService executorService;

    private final FlakeIdGenerator flakeIdGenerator;

    private final ResourceManager resourceManager;

    private CheckpointManager checkpointManager;

    private CompletableFuture<JobStatus> jobMasterCompleteFuture;

    private JobImmutableInformation jobImmutableInformation;

    private JobScheduler jobScheduler;

    /**
     * we need store slot used by task in Hazelcast IMap and release or reuse it when a new master node active.
     */
    private final IMap<PipelineLocation, Map<TaskGroupLocation, SlotProfile>> ownedSlotProfilesIMap;

    private final IMap<Object, Object> runningJobStateIMap;

    private final IMap<Object, Object> runningJobStateTimestampsIMap;

    private CompletableFuture<Void> scheduleFuture = new CompletableFuture<>();

    private volatile boolean restore = false;

    private final EngineConfig engineConfig;

    public JobMaster(@NonNull Data jobImmutableInformationData,
                     @NonNull NodeEngine nodeEngine,
                     @NonNull ExecutorService executorService,
                     @NonNull ResourceManager resourceManager,
                     @NonNull IMap runningJobStateIMap,
                     @NonNull IMap runningJobStateTimestampsIMap,
                     @NonNull IMap ownedSlotProfilesIMap, EngineConfig engineConfig) {
        this.jobImmutableInformationData = jobImmutableInformationData;
        this.nodeEngine = nodeEngine;
        this.executorService = executorService;
        flakeIdGenerator =
            this.nodeEngine.getHazelcastInstance().getFlakeIdGenerator(Constant.SEATUNNEL_ID_GENERATOR_NAME);
        this.ownedSlotProfilesIMap = ownedSlotProfilesIMap;
        this.resourceManager = resourceManager;
        this.runningJobStateIMap = runningJobStateIMap;
        this.runningJobStateTimestampsIMap = runningJobStateTimestampsIMap;
        this.engineConfig = engineConfig;
    }

    public void init(long initializationTimestamp) throws Exception {
        jobImmutableInformation = nodeEngine.getSerializationService().toObject(
            jobImmutableInformationData);
        LOGGER.info("Job [" + jobImmutableInformation.getJobId() + "] submit");
        LOGGER.info(
            "Job [" + jobImmutableInformation.getJobId() + "] jar urls " + jobImmutableInformation.getPluginJarsUrls());

        LogicalDag logicalDag;
        if (!CollectionUtils.isEmpty(jobImmutableInformation.getPluginJarsUrls())) {
            logicalDag =
                CustomClassLoadedObject.deserializeWithCustomClassLoader(nodeEngine.getSerializationService(),
                    new SeatunnelChildFirstClassLoader(jobImmutableInformation.getPluginJarsUrls()),
                    jobImmutableInformation.getLogicalDag());
        } else {
            logicalDag = nodeEngine.getSerializationService().toObject(jobImmutableInformation.getLogicalDag());
        }

        CheckpointConfig checkpointConfig = mergeEnvAndEngineConfig(engineConfig.getCheckpointConfig(), jobImmutableInformation.getJobConfig().getEnvOptions());

        final Tuple2<PhysicalPlan, Map<Integer, CheckpointPlan>> planTuple = PlanUtils.fromLogicalDAG(logicalDag,
            nodeEngine,
            jobImmutableInformation,
            initializationTimestamp,
            executorService,
            flakeIdGenerator,
            runningJobStateIMap,
            runningJobStateTimestampsIMap);
        this.physicalPlan = planTuple.f0();
        this.physicalPlan.setJobMaster(this);
        this.initStateFuture();
        this.checkpointManager = new CheckpointManager(
            jobImmutableInformation.getJobId(),
            nodeEngine,
            planTuple.f1(),
            checkpointConfig);
    }

    // TODO replace it after ReadableConfig Support parse yaml format, then use only one config to read engine and env config.
    private CheckpointConfig mergeEnvAndEngineConfig(CheckpointConfig engine, Map<String, Object> env) {
        CheckpointConfig checkpointConfig = new CheckpointConfig();
        if (env.containsKey(ServerConfigOptions.CHECKPOINT_INTERVAL.key())) {
            checkpointConfig.setCheckpointInterval((Integer) env.get(ServerConfigOptions.CHECKPOINT_INTERVAL.key()));
        }
        checkpointConfig.setCheckpointTimeout(engine.getCheckpointTimeout());
        checkpointConfig.setTolerableFailureCheckpoints(engine.getTolerableFailureCheckpoints());
        checkpointConfig.setMaxConcurrentCheckpoints(engine.getMaxConcurrentCheckpoints());
        CheckpointStorageConfig storageConfig = new CheckpointStorageConfig();
        storageConfig.setMaxRetainedCheckpoints(engine.getStorage().getMaxRetainedCheckpoints());
        storageConfig.setStorage(engine.getStorage().getStorage());
        checkpointConfig.setStorage(storageConfig);
        return checkpointConfig;
    }

    public void initStateFuture() {
        jobMasterCompleteFuture = new CompletableFuture<>();
        PassiveCompletableFuture<JobStatus> jobStatusFuture = physicalPlan.initStateFuture();
        jobStatusFuture.whenComplete((v, t) -> {
            // We need not handle t, Because we will not return t from physicalPlan
            if (JobStatus.FAILING.equals(v)) {
                cleanJob();
                physicalPlan.updateJobState(JobStatus.FAILING, JobStatus.FAILED);
            }
            jobMasterCompleteFuture.complete(physicalPlan.getJobStatus());
        });
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    public void run() {
        try {
            if (!restore) {
                jobScheduler = new PipelineBaseScheduler(physicalPlan, this);
                scheduleFuture = CompletableFuture.runAsync(() -> jobScheduler.startScheduling(), executorService);
                LOGGER.info(String.format("Job %s waiting for scheduler finished", physicalPlan.getJobFullName()));
                scheduleFuture.join();
                LOGGER.info(String.format("%s scheduler finished", physicalPlan.getJobFullName()));
            }
        } catch (Throwable e) {
            LOGGER.severe(String.format("Job %s (%s) run error with: %s",
                physicalPlan.getJobImmutableInformation().getJobConfig().getName(),
                physicalPlan.getJobImmutableInformation().getJobId(),
                ExceptionUtils.getMessage(e)));
            // try to cancel job
            cancelJob();
        } finally {
            jobMasterCompleteFuture.join();
        }
    }

    public void handleCheckpointTimeout(long pipelineId) {
        this.physicalPlan.getPipelineList().forEach(pipeline -> {
            if (pipeline.getPipelineLocation().getPipelineId() == pipelineId) {
                LOGGER.warning(String.format("%s checkpoint timeout, cancel the pipeline", pipeline.getPipelineFullName()));
                pipeline.cancelPipeline();
            }
        });
    }

    public PassiveCompletableFuture<Void> reSchedulerPipeline(SubPlan subPlan) {
        return new PassiveCompletableFuture<>(jobScheduler.reSchedulerPipeline(subPlan));
    }

    public void releasePipelineResource(SubPlan subPlan) {
        resourceManager.releaseResources(jobImmutableInformation.getJobId(),
            Lists.newArrayList(ownedSlotProfilesIMap.get(subPlan.getPipelineLocation()).values())).join();
        ownedSlotProfilesIMap.remove(subPlan.getPipelineLocation());
    }

    public void cleanJob() {
        // TODO Add some job clean operation
    }

    public Address queryTaskGroupAddress(long taskGroupId) {
        for (PipelineLocation pipelineLocation : ownedSlotProfilesIMap.keySet()) {
            Optional<TaskGroupLocation> currentVertex = ownedSlotProfilesIMap.get(pipelineLocation).keySet().stream()
                .filter(taskGroupLocation -> taskGroupLocation.getTaskGroupId() == taskGroupId)
                .findFirst();
            if (currentVertex.isPresent()) {
                return ownedSlotProfilesIMap.get(pipelineLocation).get(currentVertex.get()).getWorker();
            }
        }
        throw new IllegalArgumentException("can't find task group address from task group id: " + taskGroupId);
    }

    public void cancelJob() {
        physicalPlan.neverNeedRestore();
        physicalPlan.cancelJob();
    }

    public ResourceManager getResourceManager() {
        return resourceManager;
    }

    public CheckpointManager getCheckpointManager() {
        return checkpointManager;
    }

    public PassiveCompletableFuture<JobStatus> getJobMasterCompleteFuture() {
        return new PassiveCompletableFuture<>(jobMasterCompleteFuture);
    }

    public JobImmutableInformation getJobImmutableInformation() {
        return jobImmutableInformation;
    }

    public JobStatus getJobStatus() {
        return physicalPlan.getJobStatus();
    }

    public PhysicalPlan getPhysicalPlan() {
        return physicalPlan;
    }

    public void updateTaskExecutionState(TaskExecutionState taskExecutionState) {
        this.physicalPlan.getPipelineList().forEach(pipeline -> {
            if (pipeline.getPipelineLocation().getPipelineId() !=
                taskExecutionState.getTaskGroupLocation().getPipelineId()) {
                return;
            }

            pipeline.getCoordinatorVertexList().forEach(task -> {
                if (!task.getTaskGroupLocation().equals(taskExecutionState.getTaskGroupLocation())) {
                    return;
                }

                task.updateTaskExecutionState(taskExecutionState);
            });

            pipeline.getPhysicalVertexList().forEach(task -> {
                if (!task.getTaskGroupLocation().equals(taskExecutionState.getTaskGroupLocation())) {
                    return;
                }

                task.updateTaskExecutionState(taskExecutionState);
            });
        });
    }

    public Map<TaskGroupLocation, SlotProfile> getOwnedSlotProfiles(PipelineLocation pipelineLocation) {
        return ownedSlotProfilesIMap.get(pipelineLocation);
    }

    public void setOwnedSlotProfiles(@NonNull PipelineLocation pipelineLocation,
                                     @NonNull Map<TaskGroupLocation, SlotProfile> pipelineOwnedSlotProfiles) {
        ownedSlotProfilesIMap.put(pipelineLocation, pipelineOwnedSlotProfiles);
    }

    public CompletableFuture<Void> getScheduleFuture() {
        return scheduleFuture;
    }

    public ExecutorService getExecutorService() {
        return executorService;
    }

    public void interrupt() {
        try {
            jobMasterCompleteFuture.cancel(true);
        } finally {
            super.interrupt();
        }
    }

    public void markRestore() {
        restore = true;
    }
}
