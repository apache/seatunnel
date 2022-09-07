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
import org.apache.seatunnel.engine.core.job.JobStatus;
import org.apache.seatunnel.engine.core.job.PipelineState;
import org.apache.seatunnel.engine.server.dag.physical.PhysicalPlan;
import org.apache.seatunnel.engine.server.dag.physical.PhysicalVertex;
import org.apache.seatunnel.engine.server.dag.physical.SubPlan;
import org.apache.seatunnel.engine.server.execution.ExecutionState;
import org.apache.seatunnel.engine.server.execution.TaskExecutionState;
import org.apache.seatunnel.engine.server.master.JobMaster;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManager;
import org.apache.seatunnel.engine.server.resourcemanager.resource.ResourceProfile;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import lombok.NonNull;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
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
    public Map<Integer, Map<PhysicalVertex, SlotProfile>> startScheduling() {
        Map<Integer, Map<PhysicalVertex, SlotProfile>> ownedSlotProfiles = new ConcurrentHashMap<>();
        if (physicalPlan.turnToRunning()) {
            List<CompletableFuture<Void>> collect =
                physicalPlan.getPipelineList()
                    .stream()
                    .map(pipeline -> schedulerPipeline(pipeline, ownedSlotProfiles))
                    .filter(Objects::nonNull).collect(Collectors.toList());
            try {
                CompletableFuture<Void> voidCompletableFuture = CompletableFuture.allOf(
                    collect.toArray(new CompletableFuture[0]));
                voidCompletableFuture.get();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else if (!JobStatus.CANCELED.equals(physicalPlan.getJobStatus())) {
            throw new JobException(String.format("%s turn to a unexpected state: %s", physicalPlan.getJobFullName(),
                physicalPlan.getJobStatus()));
        }
        return ownedSlotProfiles;
    }

    // This method cannot throw an exception
    private CompletableFuture<Void> schedulerPipeline(SubPlan pipeline,
                                                      Map<Integer, Map<PhysicalVertex, SlotProfile>> ownedSlotProfiles) {
        try {
            if (!pipeline.updatePipelineState(PipelineState.CREATED, PipelineState.SCHEDULED)) {
                handlePipelineStateTurnError(pipeline, PipelineState.SCHEDULED);
                return null;
            }

            Map<PhysicalVertex, SlotProfile> slotProfiles =
                getOrApplyResourceForPipeline(pipeline, ownedSlotProfiles);

            // deploy pipeline
            return CompletableFuture.runAsync(() -> {
                deployPipeline(pipeline, slotProfiles);
            });
        } catch (Exception e) {
            pipeline.cancelPipeline();
            return null;
        }
    }

    private Map<PhysicalVertex, SlotProfile> getOrApplyResourceForPipeline(SubPlan pipeline,
                                                                           Map<Integer, Map<PhysicalVertex, SlotProfile>> ownedSlotProfiles) {
        Map<PhysicalVertex, SlotProfile> slotProfiles = ownedSlotProfiles.get(pipeline.getPipelineId());
        if (slotProfiles == null || slotProfiles.isEmpty()) {
            slotProfiles = applyResourceForPipeline(pipeline);
            ownedSlotProfiles.put(pipeline.getPipelineId(), slotProfiles);
            return slotProfiles;
        }

        // TODO ensure the slots still exist and is owned by this pipeline
        for (Map.Entry<PhysicalVertex, SlotProfile> entry : slotProfiles.entrySet()) {
            if (entry.getValue() == null) {
                slotProfiles.put(entry.getKey(), applyResourceForTask(entry.getKey()).join());
            } else {
                entry.getKey().updateTaskState(ExecutionState.CREATED, ExecutionState.SCHEDULED);
            }
        }
        ownedSlotProfiles.put(pipeline.getPipelineId(), slotProfiles);
        return slotProfiles;
    }

    private Map<PhysicalVertex, SlotProfile> applyResourceForPipeline(@NonNull SubPlan subPlan) {
        Map<PhysicalVertex, CompletableFuture<SlotProfile>> futures = new HashMap<>();
        Map<PhysicalVertex, SlotProfile> slotProfiles = new HashMap<>();
        // TODO If there is no enough resources for tasks, we need add some wait profile
        subPlan.getCoordinatorVertexList()
            .forEach(
                coordinator -> futures.put(coordinator, applyResourceForTask(coordinator)));

        subPlan.getPhysicalVertexList()
            .forEach(task -> futures.put(task, applyResourceForTask(task)));

        for (Map.Entry<PhysicalVertex, CompletableFuture<SlotProfile>> future : futures.entrySet()) {
            slotProfiles.put(future.getKey(),
                future.getValue() == null ? null : future.getValue().join());
        }
        return slotProfiles;
    }

    private CompletableFuture<SlotProfile> applyResourceForTask(PhysicalVertex task) {
        try {
            if (task.updateTaskState(ExecutionState.CREATED, ExecutionState.SCHEDULED)) {
                // TODO custom resource size
                return resourceManager.applyResource(jobId, new ResourceProfile());
            } else if (ExecutionState.CANCELING.equals(task.getExecutionState().get()) ||
                ExecutionState.CANCELED.equals(task.getExecutionState().get())) {
                LOGGER.info(
                    String.format("%s be canceled, skip %s this task.", task.getTaskFullName(),
                        ExecutionState.SCHEDULED));
                return null;
            } else {
                makeTaskFailed(task,
                    new JobException(String.format("%s turn to a unexpected state: %s, stop scheduler job.",
                        task.getTaskFullName(), task.getExecutionState().get())));
                return null;
            }
        } catch (Throwable e) {
            makeTaskFailed(task, e);
            return null;
        }
    }

    private CompletableFuture<Void> deployTask(PhysicalVertex task, SlotProfile slotProfile) {
        if (task.updateTaskState(ExecutionState.SCHEDULED, ExecutionState.DEPLOYING)) {
            // deploy is a time-consuming operation, so we do it async
            return CompletableFuture.runAsync(() -> {
                task.deploy(slotProfile);
            });
        } else if (ExecutionState.CANCELING.equals(task.getExecutionState().get()) ||
            ExecutionState.CANCELED.equals(task.getExecutionState().get())) {
            LOGGER.info(
                String.format("%s be canceled, skip %s this task.", task.getTaskFullName(), ExecutionState.DEPLOYING));
            return null;
        } else {
            jobMaster.updateTaskExecutionState(
                new TaskExecutionState(
                    task.getTaskGroupLocation(),
                    ExecutionState.FAILED,
                    new JobException(String.format("%s turn to a unexpected state: %s, stop scheduler job.",
                        task.getTaskFullName(), task.getExecutionState().get()))));
            return null;
        }
    }

    private void deployPipeline(@NonNull SubPlan pipeline, Map<PhysicalVertex, SlotProfile> slotProfiles) {
        if (pipeline.updatePipelineState(PipelineState.SCHEDULED, PipelineState.DEPLOYING)) {

            try {
                List<CompletableFuture<?>> deployCoordinatorFuture =
                    pipeline.getCoordinatorVertexList().stream()
                        .map(coordinator -> deployTask(coordinator, slotProfiles.get(coordinator)))
                        .filter(Objects::nonNull).collect(Collectors.toList());

                List<CompletableFuture<?>> deployTaskFuture =
                    pipeline.getPhysicalVertexList().stream().map(task -> deployTask(task, slotProfiles.get(task)))
                        .filter(Objects::nonNull).collect(Collectors.toList());

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
                makePipelineFailed(pipeline, e);
            }
        } else if (PipelineState.CANCELING.equals(pipeline.getPipelineState().get()) ||
            PipelineState.CANCELED.equals(pipeline.getPipelineState().get())) {
            // may be canceled
            LOGGER.info(String.format("%s turn to state %s, skip %s this pipeline.", pipeline.getPipelineFullName(),
                pipeline.getPipelineState().get(), PipelineState.DEPLOYING));
        } else {
            makePipelineFailed(pipeline, new JobException(
                String.format("%s turn to a unexpected state: %s, stop scheduler job", pipeline.getPipelineFullName(),
                    pipeline.getPipelineState().get())));
        }
    }

    @Override
    public CompletableFuture<Void> reSchedulerPipeline(@NonNull SubPlan subPlan) {
        return schedulerPipeline(subPlan, jobMaster.getOwnedSlotProfiles());
    }

    private void handlePipelineStateTurnError(SubPlan pipeline, PipelineState targetState) {
        if (PipelineState.CANCELING.equals(pipeline.getPipelineState().get()) ||
            PipelineState.CANCELED.equals(pipeline.getPipelineState().get())) {
            // may be canceled
            LOGGER.info(
                String.format("%s turn to state %s, skip %s this pipeline.", pipeline.getPipelineFullName(),
                    pipeline.getPipelineState().get(), targetState));
        } else {
            throw new JobException(
                String.format("%s turn to a unexpected state: %s, stop scheduler job",
                    pipeline.getPipelineFullName(),
                    pipeline.getPipelineState().get()));
        }
    }

    private void makePipelineFailed(@NonNull SubPlan pipeline, Throwable e) {
        pipeline.getCoordinatorVertexList().forEach(coordinator -> {
            makeTaskFailed(coordinator, e);
        });

        pipeline.getPhysicalVertexList().forEach(task -> {
            makeTaskFailed(task, e);
        });
    }

    private void makeTaskFailed(@NonNull PhysicalVertex task, Throwable e) {
        jobMaster.updateTaskExecutionState(
            new TaskExecutionState(task.getTaskGroupLocation(), ExecutionState.FAILED, e));
    }
}
