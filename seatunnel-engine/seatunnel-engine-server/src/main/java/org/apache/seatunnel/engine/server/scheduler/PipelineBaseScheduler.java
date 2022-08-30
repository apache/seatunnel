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
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManager;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import lombok.NonNull;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.stream.Collectors;

public class PipelineBaseScheduler implements JobScheduler {
    private static final ILogger LOGGER = Logger.getLogger(PipelineBaseScheduler.class);
    private final PhysicalPlan physicalPlan;
    private final JobMaster jobMaster;
    private final ResourceManager resourceManager;

    public PipelineBaseScheduler(@NonNull PhysicalPlan physicalPlan, @NonNull JobMaster jobMaster) {
        this.physicalPlan = physicalPlan;
        this.jobMaster = jobMaster;
        this.resourceManager = jobMaster.getResourceManager();
    }

    @Override
    public void startScheduling() {
        if (physicalPlan.turnToRunning()) {
            List<CompletableFuture<Object>> collect = physicalPlan.getPipelineList().stream().map(pipeline -> {
                if (!pipeline.updatePipelineState(PipelineState.CREATED, PipelineState.SCHEDULED)) {
                    handlePipelineStateUpdateError(pipeline, PipelineState.SCHEDULED);
                    return null;
                }
                if (!applyResourceForPipeline(pipeline)) {
                    return null;
                }
                // deploy pipeline
                return CompletableFuture.supplyAsync(() -> {
                    deployPipeline(pipeline);
                    return null;
                });
            }).filter(x -> x != null).collect(Collectors.toList());
            try {
                CompletableFuture<Void> voidCompletableFuture = CompletableFuture.allOf(
                    collect.toArray(new CompletableFuture[collect.size()]));
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

    private boolean applyResourceForPipeline(@NonNull SubPlan subPlan) {
        try {
            // apply resource for coordinators
            subPlan.getCoordinatorVertexList().forEach(coordinator -> applyResourceForTask(coordinator));

            // apply resource for other tasks
            subPlan.getPhysicalVertexList().forEach(task -> applyResourceForTask(task));
        } catch (JobNoEnoughResourceException e) {
            LOGGER.severe(e);
            return false;
        }

        return true;
    }

    private void applyResourceForTask(PhysicalVertex task) {
        try {
            // TODO If there is no enough resources for tasks, we need add some wait profile
            if (task.updateTaskState(ExecutionState.CREATED, ExecutionState.SCHEDULED)) {
                resourceManager.applyForResource(physicalPlan.getJobImmutableInformation().getJobId(),
                    task.getTaskGroup().getTaskGroupLocation());
            } else {
                handleTaskStateUpdateError(task, ExecutionState.SCHEDULED);
            }
        } catch (JobNoEnoughResourceException e) {
            LOGGER.severe(e);
        }
    }

    private CompletableFuture<Void> deployTask(PhysicalVertex task) {
        if (task.updateTaskState(ExecutionState.SCHEDULED, ExecutionState.DEPLOYING)) {
            // deploy is a time-consuming operation, so we do it async
            return CompletableFuture.supplyAsync(() -> {
                task.deploy(
                    resourceManager.getAppliedResource(physicalPlan.getJobImmutableInformation().getJobId(),
                        task.getTaskGroup().getTaskGroupLocation()));
                return null;
            });
        } else {
            handleTaskStateUpdateError(task, ExecutionState.DEPLOYING);
        }
        return null;
    }

    private void deployPipeline(@NonNull SubPlan pipeline) {
        if (pipeline.updatePipelineState(PipelineState.SCHEDULED, PipelineState.DEPLOYING)) {
            List<CompletableFuture> deployCoordinatorFuture =
                pipeline.getCoordinatorVertexList().stream().map(task -> deployTask(task)).filter(x -> x != null)
                    .collect(Collectors.toList());

            List<CompletableFuture> deployTaskFuture =
                pipeline.getPhysicalVertexList().stream().map(task -> deployTask(task)).filter(x -> x != null)
                    .collect(Collectors.toList());

            try {
                deployCoordinatorFuture.addAll(deployTaskFuture);
                CompletableFuture<Void> voidCompletableFuture = CompletableFuture.allOf(
                    deployCoordinatorFuture.toArray(new CompletableFuture[deployCoordinatorFuture.size()]));
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
