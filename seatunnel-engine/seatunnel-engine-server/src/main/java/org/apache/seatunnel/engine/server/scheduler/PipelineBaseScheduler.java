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

package org.apache.seatunnel.engine.server.scheduler;

import org.apache.seatunnel.engine.common.exception.JobException;
import org.apache.seatunnel.engine.common.exception.JobNoEnoughResourceException;
import org.apache.seatunnel.engine.core.job.JobStatus;
import org.apache.seatunnel.engine.core.job.PipelineState;
import org.apache.seatunnel.engine.server.dag.physical.PhysicalPlan;
import org.apache.seatunnel.engine.server.dag.physical.PhysicalVertex;
import org.apache.seatunnel.engine.server.dag.physical.SubPlan;
import org.apache.seatunnel.engine.server.execution.ExecutionState;
import org.apache.seatunnel.engine.server.master.JobMaster;
import org.apache.seatunnel.engine.server.resourcemanager.NoEnoughResourceException;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManager;
import org.apache.seatunnel.engine.server.resourcemanager.resource.ResourceProfile;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;

import com.google.common.collect.Lists;
import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import lombok.NonNull;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class PipelineBaseScheduler implements JobScheduler {
    private static final ILogger LOGGER = Logger.getLogger(PipelineBaseScheduler.class);
    private final PhysicalPlan physicalPlan;

    private final long jobId;
    private final JobMaster jobMaster;
    private final ResourceManager resourceManager;

    public PipelineBaseScheduler(@NonNull PhysicalPlan physicalPlan, @NonNull JobMaster jobMaster) {
        this.physicalPlan = physicalPlan;
        this.jobMaster = jobMaster;
        this.resourceManager = jobMaster.getResourceManager();
        this.jobId = physicalPlan.getJobImmutableInformation().getJobId();
    }

    @Override
    public void startScheduling() {
        if (physicalPlan.turnToRunning()) {
            List<CompletableFuture<Object>> collect = physicalPlan.getPipelineList().stream().map(pipeline -> {
                if (!pipeline.updatePipelineState(PipelineState.CREATED, PipelineState.SCHEDULED)) {
                    handlePipelineStateUpdateError(pipeline, PipelineState.SCHEDULED);
                    return null;
                }
                Map<PhysicalVertex, SlotProfile> slotProfiles;
                try {
                    slotProfiles = applyResourceForPipeline(pipeline);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
                pipeline.whenComplete((state, error) -> releasePipelineResource(Lists.newArrayList(slotProfiles.values())));
                // deploy pipeline
                return CompletableFuture.supplyAsync(() -> {
                    // TODO before deploy should check slotProfiles is exist, because it maybe can't use when retry.
                    deployPipeline(pipeline, slotProfiles);
                    return null;
                });
            }).filter(Objects::nonNull).collect(Collectors.toList());
            try {
                CompletableFuture<Void> voidCompletableFuture = CompletableFuture.allOf(
                    collect.toArray(new CompletableFuture[0]));
                voidCompletableFuture.get();
            } catch (Exception e) {
                // cancel pipeline and throw an exception
                physicalPlan.cancelJob();
                throw new RuntimeException(e);
            }
        } else if (!JobStatus.CANCELED.equals(physicalPlan.getJobStatus())) {
            throw new JobException(String.format("%s turn to a unexpected state: %s", physicalPlan.getJobFullName(),
                physicalPlan.getJobStatus()));
        }
    }

    private void releasePipelineResource(List<SlotProfile> slotProfiles) {
        if (null == slotProfiles || slotProfiles.isEmpty()) {
            return;
        }
        resourceManager.releaseResources(jobId, slotProfiles).join();
    }

    private Map<PhysicalVertex, SlotProfile> applyResourceForPipeline(@NonNull SubPlan subPlan) throws Exception {
        try {
            Map<PhysicalVertex, CompletableFuture<SlotProfile>> futures = new HashMap<>();
            Map<PhysicalVertex, SlotProfile> slotProfiles = new HashMap<>();
            subPlan.getCoordinatorVertexList().forEach(coordinator -> {
                coordinator.updateTaskState(ExecutionState.CREATED, ExecutionState.SCHEDULED);
            });

            subPlan.getPhysicalVertexList().forEach(task -> {
                // TODO If there is no enough resources for tasks, we need add some wait profile
                if (task.updateTaskState(ExecutionState.CREATED, ExecutionState.SCHEDULED)) {
                    // TODO custom resource size
                    futures.put(task, resourceManager.applyResource(jobId, new ResourceProfile()));
                } else {
                    handleTaskStateUpdateError(task, ExecutionState.SCHEDULED);
                }
            });
            for (Map.Entry<PhysicalVertex, CompletableFuture<SlotProfile>> future : futures.entrySet()) {
                try {
                    slotProfiles.put(future.getKey(), future.getValue().get());
                } catch (NoEnoughResourceException e) {
                    // TODO custom exception with pipelineID, jobName etc.
                    throw new JobNoEnoughResourceException("No enough resource to execute pipeline", e);
                }
            }
            return slotProfiles;
        } catch (JobNoEnoughResourceException | ExecutionException | InterruptedException e) {
            LOGGER.severe(e);
            throw e;
        }
    }

    private CompletableFuture<Void> deployTask(PhysicalVertex task, Supplier<Void> deployMethod) {
        if (task.updateTaskState(ExecutionState.SCHEDULED, ExecutionState.DEPLOYING)) {
            // deploy is a time-consuming operation, so we do it async
            return CompletableFuture.supplyAsync(deployMethod);
        } else {
            handleTaskStateUpdateError(task, ExecutionState.DEPLOYING);
        }
        return null;
    }

    private void deployPipeline(@NonNull SubPlan pipeline, Map<PhysicalVertex, SlotProfile> slotProfiles) {
        if (pipeline.updatePipelineState(PipelineState.SCHEDULED, PipelineState.DEPLOYING)) {
            List<CompletableFuture<?>> deployCoordinatorFuture =
                pipeline.getCoordinatorVertexList().stream().map(coordinator -> deployTask(coordinator, () -> {
                    coordinator.deployOnMaster();
                    return null;
                })).filter(Objects::nonNull).collect(Collectors.toList());

            List<CompletableFuture<?>> deployTaskFuture =
                pipeline.getPhysicalVertexList().stream().map(task -> deployTask(task, () -> {
                    task.deploy(slotProfiles.get(task));
                    return null;
                })).filter(Objects::nonNull).collect(Collectors.toList());

            try {
                deployCoordinatorFuture.addAll(deployTaskFuture);
                CompletableFuture<Void> voidCompletableFuture = CompletableFuture.allOf(
                    deployCoordinatorFuture.toArray(new CompletableFuture[0]));
                voidCompletableFuture.get();
                if (!pipeline.updatePipelineState(PipelineState.DEPLOYING, PipelineState.RUNNING)) {
                    LOGGER.info(
                        String.format("%s turn to state %s, skip the running state.", pipeline.getPipelineFullName(),
                            pipeline.getPipelineState().get()));
                }
            } catch (Exception e) {
                // cancel pipeline and throw an exception
                pipeline.cancelPipeline();
                throw new RuntimeException(e);
            }
        } else {
            handlePipelineStateUpdateError(pipeline, PipelineState.DEPLOYING);
        }
    }

    private void handlePipelineStateUpdateError(SubPlan pipeline, PipelineState targetState) {
        if (PipelineState.CANCELING.equals(pipeline.getPipelineState().get()) ||
            PipelineState.CANCELED.equals(pipeline.getPipelineState().get())) {
            // may be canceled
            LOGGER.info(String.format("%s turn to state %s, skip %s this pipeline.", pipeline.getPipelineFullName(),
                pipeline.getPipelineState().get(), targetState));
        } else {
            throw new JobException(
                String.format("%s turn to a unexpected state: %s, stop scheduler job", pipeline.getPipelineFullName(),
                    pipeline.getPipelineState().get()));
        }
    }

    private void handleTaskStateUpdateError(PhysicalVertex task, ExecutionState targetState) {
        if (ExecutionState.CANCELING.equals(task.getExecutionState().get()) ||
            ExecutionState.CANCELED.equals(task.getExecutionState().get())) {
            LOGGER.info(String.format("%s be canceled, skip %s this task.", task.getTaskFullName(), targetState));
        } else {
            throw new JobException(String.format("%s turn to a unexpected state: %s, stop scheduler job.",
                task.getTaskFullName(), task.getExecutionState().get()));
        }
    }
}
