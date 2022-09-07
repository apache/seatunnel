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
import org.apache.seatunnel.engine.common.loader.SeatunnelChildFirstClassLoader;
import org.apache.seatunnel.engine.common.utils.PassiveCompletableFuture;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.core.job.JobStatus;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointCoordinatorConfiguration;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointManager;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointPlan;
import org.apache.seatunnel.engine.server.checkpoint.CheckpointStorageConfiguration;
import org.apache.seatunnel.engine.server.dag.physical.PhysicalPlan;
import org.apache.seatunnel.engine.server.dag.physical.PhysicalVertex;
import org.apache.seatunnel.engine.server.dag.physical.PlanUtils;
import org.apache.seatunnel.engine.server.dag.physical.SubPlan;
import org.apache.seatunnel.engine.server.execution.TaskExecutionState;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManager;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;
import org.apache.seatunnel.engine.server.scheduler.JobScheduler;
import org.apache.seatunnel.engine.server.scheduler.PipelineBaseScheduler;

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
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

public class JobMaster implements Runnable {
    private static final ILogger LOGGER = Logger.getLogger(JobMaster.class);

    private LogicalDag logicalDag;
    private PhysicalPlan physicalPlan;
    private final Data jobImmutableInformationData;

    private final NodeEngine nodeEngine;

    private final ExecutorService executorService;

    private FlakeIdGenerator flakeIdGenerator;

    private ResourceManager resourceManager;

    private CheckpointManager checkpointManager;

    private CompletableFuture<JobStatus> jobMasterCompleteFuture = new CompletableFuture<>();

    private JobImmutableInformation jobImmutableInformation;

    private JobScheduler jobScheduler;
    private final Map<Integer, Map<PhysicalVertex, SlotProfile>> ownedSlotProfiles;

    private CompletableFuture<Void> scheduleFuture = new CompletableFuture<>();

    private final IMap<Object, Object> runningJobStateIMap;

    private final IMap<Object, Object> runningJobStateTimestampsIMap;

    public JobMaster(@NonNull Data jobImmutableInformationData,
                     @NonNull NodeEngine nodeEngine,
                     @NonNull ExecutorService executorService,
                     @NonNull ResourceManager resourceManager,
                     @NonNull IMap runningJobStateIMap,
                     @NonNull IMap runningJobStateTimestampsIMap) {
        this.jobImmutableInformationData = jobImmutableInformationData;
        this.nodeEngine = nodeEngine;
        this.executorService = executorService;
        flakeIdGenerator =
            this.nodeEngine.getHazelcastInstance().getFlakeIdGenerator(Constant.SEATUNNEL_ID_GENERATOR_NAME);
        this.ownedSlotProfiles = new ConcurrentHashMap<>();
        this.resourceManager = resourceManager;
        this.runningJobStateIMap = runningJobStateIMap;
        this.runningJobStateTimestampsIMap = runningJobStateTimestampsIMap;
    }

    public void init(long initializationTimestamp) throws Exception {
        jobImmutableInformation = nodeEngine.getSerializationService().toObject(
            jobImmutableInformationData);
        LOGGER.info("Job [" + jobImmutableInformation.getJobId() + "] submit");
        LOGGER.info(
            "Job [" + jobImmutableInformation.getJobId() + "] jar urls " + jobImmutableInformation.getPluginJarsUrls());

        if (!CollectionUtils.isEmpty(jobImmutableInformation.getPluginJarsUrls())) {
            this.logicalDag =
                CustomClassLoadedObject.deserializeWithCustomClassLoader(nodeEngine.getSerializationService(),
                    new SeatunnelChildFirstClassLoader(jobImmutableInformation.getPluginJarsUrls()),
                    jobImmutableInformation.getLogicalDag());
        } else {
            this.logicalDag = nodeEngine.getSerializationService().toObject(jobImmutableInformation.getLogicalDag());
        }
        final Tuple2<PhysicalPlan, Map<Integer, CheckpointPlan>> planTuple = PlanUtils.fromLogicalDAG(logicalDag,
            nodeEngine,
            jobImmutableInformation,
            initializationTimestamp,
            executorService,
            flakeIdGenerator,
            runningJobStateIMap,
            runningJobStateTimestampsIMap);
        this.physicalPlan = planTuple.f0();
        this.checkpointManager = new CheckpointManager(
            jobImmutableInformation.getJobId(),
            nodeEngine,
            planTuple.f1(),
            // TODO: checkpoint config
            CheckpointCoordinatorConfiguration.builder().build(),
            CheckpointStorageConfiguration.builder().build());
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    @Override
    public void run() {
        try {
            physicalPlan.initJobMaster(this);

            PassiveCompletableFuture<JobStatus> jobStatusPassiveCompletableFuture =
                physicalPlan.getJobEndCompletableFuture();

            jobStatusPassiveCompletableFuture.whenComplete((v, t) -> {
                // We need not handle t, Because we will not return t from physicalPlan
                if (JobStatus.FAILING.equals(v)) {
                    cleanJob();
                    physicalPlan.updateJobState(JobStatus.FAILING, JobStatus.FAILED);
                }
                jobMasterCompleteFuture.complete(physicalPlan.getJobStatus());
            });
            jobScheduler = new PipelineBaseScheduler(physicalPlan, this);
            scheduleFuture = CompletableFuture.runAsync(() -> {
                ownedSlotProfiles.putAll(jobScheduler.startScheduling());
            }, executorService);
            scheduleFuture.join();
            LOGGER.info(String.format("%s scheduler finished", physicalPlan.getJobFullName()));
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
            if (pipeline.getPipelineId() == pipelineId) {
                pipeline.cancelPipeline();
            }
        });
    }

    public CompletableFuture<Void> reSchedulerPipeline(SubPlan subPlan) {
        return jobScheduler.reSchedulerPipeline(subPlan);
    }

    public void releasePipelineResource(int pipelineIndex) {
        // TODO release pipeline resource
    }

    public void cleanJob() {
        // TODO Add some job clean operation
    }

    public Address queryTaskGroupAddress(long taskGroupId) {
        for (Integer pipelineId : ownedSlotProfiles.keySet()) {
            Optional<PhysicalVertex> currentVertex = ownedSlotProfiles.get(pipelineId).keySet().stream()
                .filter(task -> task.getTaskGroupLocation().getTaskGroupId() == taskGroupId)
                .findFirst();
            if (currentVertex.isPresent()) {
                return ownedSlotProfiles.get(pipelineId).get(currentVertex.get()).getWorker();
            }
        }
        throw new IllegalArgumentException("can't find task group address from task group id: " + taskGroupId);
    }

    public void cancelJob() {
        physicalPlan.neverNeedRestore();
        this.physicalPlan.cancelJob();
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
            if (pipeline.getPipelineId() != taskExecutionState.getTaskGroupLocation().getPipelineId()) {
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

    public Map<Integer, Map<PhysicalVertex, SlotProfile>> getOwnedSlotProfiles() {
        return ownedSlotProfiles;
    }

    public CompletableFuture<Void> getScheduleFuture() {
        return scheduleFuture;
    }
}
