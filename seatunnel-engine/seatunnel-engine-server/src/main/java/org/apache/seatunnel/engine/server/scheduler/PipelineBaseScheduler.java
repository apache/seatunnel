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

import org.apache.seatunnel.common.utils.ExceptionUtils;
import org.apache.seatunnel.engine.common.exception.JobException;
import org.apache.seatunnel.engine.common.exception.SchedulerNotAllowException;
import org.apache.seatunnel.engine.common.exception.SeaTunnelEngineException;
import org.apache.seatunnel.engine.core.job.JobStatus;
import org.apache.seatunnel.engine.core.job.PipelineStatus;
import org.apache.seatunnel.engine.server.dag.physical.PhysicalPlan;
import org.apache.seatunnel.engine.server.dag.physical.PhysicalVertex;
import org.apache.seatunnel.engine.server.dag.physical.SubPlan;
import org.apache.seatunnel.engine.server.execution.ExecutionState;
import org.apache.seatunnel.engine.server.execution.TaskDeployState;
import org.apache.seatunnel.engine.server.execution.TaskExecutionState;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.master.JobMaster;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManager;
import org.apache.seatunnel.engine.server.resourcemanager.resource.ResourceProfile;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

@Slf4j
public class PipelineBaseScheduler implements JobScheduler {
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
        if (physicalPlan.updateJobState(JobStatus.CREATED, JobStatus.SCHEDULED)) {
            List<CompletableFuture<Void>> collect =
                    physicalPlan.getPipelineList().stream()
                            .map(this::schedulerPipeline)
                            .filter(Objects::nonNull)
                            .collect(Collectors.toList());
            try {
                CompletableFuture<Void> voidCompletableFuture =
                        CompletableFuture.allOf(collect.toArray(new CompletableFuture[0]));
                voidCompletableFuture.get();
                physicalPlan.updateJobState(JobStatus.SCHEDULED, JobStatus.RUNNING);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        } else if (!JobStatus.CANCELED.equals(physicalPlan.getJobStatus())) {
            throw new JobException(
                    String.format(
                            "%s turn to a unexpected state: %s",
                            physicalPlan.getJobFullName(), physicalPlan.getJobStatus()));
        }
    }

    // This method cannot throw an exception
    private CompletableFuture<Void> schedulerPipeline(SubPlan pipeline) {
        try {
            if (!pipeline.updatePipelineState(PipelineStatus.CREATED, PipelineStatus.SCHEDULED)) {
                handlePipelineStateTurnError(pipeline, PipelineStatus.SCHEDULED);
            }

            Map<TaskGroupLocation, SlotProfile> slotProfiles =
                    getOrApplyResourceForPipeline(
                            pipeline,
                            jobMaster.getOwnedSlotProfiles(pipeline.getPipelineLocation()));

            log.debug(
                    "slotProfiles: {}, PipelineLocation: {}",
                    slotProfiles,
                    pipeline.getPipelineLocation());

            // To ensure release pipeline resource after new master node active, we need store
            // slotProfiles first and then deploy tasks.
            jobMaster.setOwnedSlotProfiles(pipeline.getPipelineLocation(), slotProfiles);
            // deploy pipeline
            return CompletableFuture.runAsync(
                    () -> {
                        deployPipeline(pipeline, slotProfiles);
                    },
                    jobMaster.getExecutorService());
        } catch (SchedulerNotAllowException e) {
            log.error(
                    String.format(
                            "scheduler %s stop. Because %s",
                            pipeline.getPipelineFullName(), ExceptionUtils.getMessage(e)));
            CompletableFuture<Void> reSchedulerFuture = new CompletableFuture<>();
            reSchedulerFuture.complete(null);
            return reSchedulerFuture;
        } catch (Exception e) {
            log.error(
                    String.format(
                            "scheduler %s error and cancel pipeline. The error is %s",
                            pipeline.getPipelineFullName(), ExceptionUtils.getMessage(e)));
            pipeline.cancelPipeline();
            CompletableFuture<Void> reSchedulerFuture = new CompletableFuture<>();
            reSchedulerFuture.complete(null);
            return reSchedulerFuture;
        }
    }

    private Map<TaskGroupLocation, SlotProfile> getOrApplyResourceForPipeline(
            @NonNull SubPlan pipeline, Map<TaskGroupLocation, SlotProfile> ownedSlotProfiles) {
        if (ownedSlotProfiles == null || ownedSlotProfiles.isEmpty()) {
            return applyResourceForPipeline(pipeline);
        }

        // TODO ensure the slots still exist and is owned by this pipeline
        Map<TaskGroupLocation, SlotProfile> currentOwnedSlotProfiles = new ConcurrentHashMap<>();
        pipeline.getCoordinatorVertexList()
                .forEach(
                        coordinator ->
                                currentOwnedSlotProfiles.put(
                                        coordinator.getTaskGroupLocation(),
                                        getOrApplyResourceForTask(coordinator, ownedSlotProfiles)));

        pipeline.getPhysicalVertexList()
                .forEach(
                        task ->
                                currentOwnedSlotProfiles.put(
                                        task.getTaskGroupLocation(),
                                        getOrApplyResourceForTask(task, ownedSlotProfiles)));

        return currentOwnedSlotProfiles;
    }

    private SlotProfile getOrApplyResourceForTask(
            @NonNull PhysicalVertex task, Map<TaskGroupLocation, SlotProfile> ownedSlotProfiles) {

        SlotProfile oldProfile;
        if (ownedSlotProfiles == null
                || ownedSlotProfiles.isEmpty()
                || ownedSlotProfiles.get(task.getTaskGroupLocation()) == null) {
            oldProfile = null;
        } else {
            oldProfile = ownedSlotProfiles.get(task.getTaskGroupLocation());
        }
        if (oldProfile == null || !resourceManager.slotActiveCheck(oldProfile)) {
            SlotProfile newProfile;
            CompletableFuture<SlotProfile> slotProfileCompletableFuture =
                    applyResourceForTask(task);
            if (slotProfileCompletableFuture != null) {
                newProfile = slotProfileCompletableFuture.join();
            } else {
                throw new SchedulerNotAllowException(
                        String.format(
                                "The task [%s] state is [%s] and the resource can not be retrieved",
                                task.getTaskFullName(), task.getExecutionState()));
            }

            log.info(
                    String.format(
                            "use new profile: %s to replace not active profile: %s for task %s",
                            newProfile, oldProfile, task.getTaskFullName()));
            return newProfile;
        }
        log.info(
                String.format(
                        "use active old profile: %s for task %s",
                        oldProfile, task.getTaskFullName()));
        task.updateTaskState(ExecutionState.CREATED, ExecutionState.SCHEDULED);
        return oldProfile;
    }

    private Map<TaskGroupLocation, SlotProfile> applyResourceForPipeline(@NonNull SubPlan subPlan) {
        Map<TaskGroupLocation, CompletableFuture<SlotProfile>> futures = new HashMap<>();
        Map<TaskGroupLocation, SlotProfile> slotProfiles = new HashMap<>();
        // TODO If there is no enough resources for tasks, we need add some wait profile
        subPlan.getCoordinatorVertexList()
                .forEach(
                        coordinator ->
                                futures.put(
                                        coordinator.getTaskGroupLocation(),
                                        applyResourceForTask(coordinator)));

        subPlan.getPhysicalVertexList()
                .forEach(
                        task ->
                                futures.put(
                                        task.getTaskGroupLocation(), applyResourceForTask(task)));

        for (Map.Entry<TaskGroupLocation, CompletableFuture<SlotProfile>> future :
                futures.entrySet()) {
            slotProfiles.put(
                    future.getKey(), future.getValue() == null ? null : future.getValue().join());
        }
        return slotProfiles;
    }

    private CompletableFuture<SlotProfile> applyResourceForTask(PhysicalVertex task) {
        try {
            if (task.updateTaskState(ExecutionState.CREATED, ExecutionState.SCHEDULED)) {
                // TODO custom resource size
                return resourceManager.applyResource(jobId, new ResourceProfile());
            } else if (ExecutionState.CANCELING.equals(task.getExecutionState())
                    || ExecutionState.CANCELED.equals(task.getExecutionState())) {
                log.info(
                        "{} be canceled, skip {} this task.",
                        task.getTaskFullName(),
                        ExecutionState.SCHEDULED);
                return null;
            } else {
                makeTaskFailed(
                        task.getTaskGroupLocation(),
                        new JobException(
                                String.format(
                                        "%s turn to a unexpected state: %s, stop scheduler job.",
                                        task.getTaskFullName(), task.getExecutionState())));
                return null;
            }
        } catch (Throwable e) {
            makeTaskFailed(task.getTaskGroupLocation(), e);
            return null;
        }
    }

    private CompletableFuture<Void> deployTask(PhysicalVertex task, SlotProfile slotProfile) {
        if (task.updateTaskState(ExecutionState.SCHEDULED, ExecutionState.DEPLOYING)) {
            // deploy is a time-consuming operation, so we do it async
            return CompletableFuture.runAsync(
                    () -> {
                        try {
                            TaskDeployState state = task.deploy(slotProfile);
                            if (!state.isSuccess()) {
                                jobMaster.updateTaskExecutionState(
                                        new TaskExecutionState(
                                                task.getTaskGroupLocation(),
                                                ExecutionState.FAILED,
                                                state.getThrowableMsg()));
                            }
                        } catch (Exception e) {
                            throw new SeaTunnelEngineException(e);
                        }
                    },
                    jobMaster.getExecutorService());
        } else if (ExecutionState.CANCELING.equals(task.getExecutionState())
                || ExecutionState.CANCELED.equals(task.getExecutionState())) {
            log.info(
                    "{} be canceled, skip {} this task.",
                    task.getTaskFullName(),
                    ExecutionState.DEPLOYING);
            return null;
        } else {
            jobMaster.updateTaskExecutionState(
                    new TaskExecutionState(
                            task.getTaskGroupLocation(),
                            ExecutionState.FAILED,
                            new JobException(
                                    String.format(
                                            "%s turn to a unexpected state: %s, stop scheduler job.",
                                            task.getTaskFullName(), task.getExecutionState()))));
            return null;
        }
    }

    private void deployPipeline(
            @NonNull SubPlan pipeline, Map<TaskGroupLocation, SlotProfile> slotProfiles) {
        boolean changeStateSuccess = false;
        try {
            changeStateSuccess =
                    pipeline.updatePipelineState(
                            PipelineStatus.SCHEDULED, PipelineStatus.DEPLOYING);
        } catch (Exception e) {
            log.warn(
                    "{} turn to state {} failed, cancel pipeline",
                    pipeline.getPipelineFullName(),
                    PipelineStatus.DEPLOYING);
            pipeline.cancelPipeline();
        }
        if (changeStateSuccess) {
            try {
                List<CompletableFuture<?>> deployCoordinatorFuture =
                        pipeline.getCoordinatorVertexList().stream()
                                .map(
                                        coordinator ->
                                                deployTask(
                                                        coordinator,
                                                        slotProfiles.get(
                                                                coordinator
                                                                        .getTaskGroupLocation())))
                                .filter(Objects::nonNull)
                                .collect(Collectors.toList());

                List<CompletableFuture<?>> deployTaskFuture =
                        pipeline.getPhysicalVertexList().stream()
                                .map(
                                        task ->
                                                deployTask(
                                                        task,
                                                        slotProfiles.get(
                                                                task.getTaskGroupLocation())))
                                .filter(Objects::nonNull)
                                .collect(Collectors.toList());

                deployCoordinatorFuture.addAll(deployTaskFuture);
                CompletableFuture<Void> voidCompletableFuture =
                        CompletableFuture.allOf(
                                deployCoordinatorFuture.toArray(new CompletableFuture[0]));
                voidCompletableFuture.get();
                if (!pipeline.updatePipelineState(
                        PipelineStatus.DEPLOYING, PipelineStatus.RUNNING)) {
                    log.info(
                            "{} turn to state {}, skip the running state.",
                            pipeline.getPipelineFullName(),
                            pipeline.getPipelineState());
                }
            } catch (Exception e) {
                makePipelineFailed(pipeline, e);
            }
        } else if (PipelineStatus.CANCELING.equals(pipeline.getPipelineState())
                || PipelineStatus.CANCELED.equals(pipeline.getPipelineState())) {
            // may be canceled
            log.info(
                    "{} turn to state {}, skip {} this pipeline.",
                    pipeline.getPipelineFullName(),
                    pipeline.getPipelineState(),
                    PipelineStatus.DEPLOYING);
        } else {
            makePipelineFailed(
                    pipeline,
                    new JobException(
                            String.format(
                                    "%s turn to a unexpected state: %s, stop scheduler job",
                                    pipeline.getPipelineFullName(), pipeline.getPipelineState())));
        }
    }

    @Override
    public CompletableFuture<Void> reSchedulerPipeline(@NonNull SubPlan subPlan) {
        return schedulerPipeline(subPlan);
    }

    private void handlePipelineStateTurnError(SubPlan pipeline, PipelineStatus targetState) {
        if (PipelineStatus.CANCELING.equals(pipeline.getPipelineState())
                || PipelineStatus.CANCELED.equals(pipeline.getPipelineState())) {
            // may be canceled
            throw new SchedulerNotAllowException(
                    String.format(
                            "%s turn to state %s, skip %s this pipeline.",
                            pipeline.getPipelineFullName(),
                            pipeline.getPipelineState(),
                            targetState));
        } else {
            throw new JobException(
                    String.format(
                            "%s turn to a unexpected state: %s, stop scheduler job",
                            pipeline.getPipelineFullName(), pipeline.getPipelineState()));
        }
    }

    private void makePipelineFailed(@NonNull SubPlan pipeline, Throwable e) {
        pipeline.getCoordinatorVertexList()
                .forEach(
                        coordinator -> {
                            makeTaskFailed(coordinator.getTaskGroupLocation(), e);
                        });

        pipeline.getPhysicalVertexList()
                .forEach(
                        task -> {
                            makeTaskFailed(task.getTaskGroupLocation(), e);
                        });
    }

    private void makeTaskFailed(@NonNull TaskGroupLocation taskGroupLocation, Throwable e) {
        jobMaster.updateTaskExecutionState(
                new TaskExecutionState(taskGroupLocation, ExecutionState.FAILED, e));
    }
}
