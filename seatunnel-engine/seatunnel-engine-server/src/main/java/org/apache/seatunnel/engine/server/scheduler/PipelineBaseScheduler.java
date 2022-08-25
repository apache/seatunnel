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

import org.apache.seatunnel.engine.common.exception.JobNoEnoughResourceException;
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

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import lombok.NonNull;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
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
        physicalPlan.turnToRunning();
        physicalPlan.getPipelineList().forEach(pipeline -> {
            pipeline.updatePipelineState(PipelineState.CREATED, PipelineState.SCHEDULED);
            try {
                Map<PhysicalVertex, SlotProfile> slotProfiles = applyResourceForPipeline(pipeline);
                // deploy pipeline
                deployPipeline(pipeline, slotProfiles);
            } catch (Exception e) {
                pipeline.failedWithNoEnoughResource();
            }
        });
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
                task.updateTaskState(ExecutionState.CREATED, ExecutionState.SCHEDULED);
                // TODO custom resource size
                futures.put(task, resourceManager.applyResource(physicalPlan.getJobImmutableInformation().getJobId(),
                        new ResourceProfile()));
            });
            for (Map.Entry<PhysicalVertex, CompletableFuture<SlotProfile>> future : futures.entrySet()) {
                try {
                    slotProfiles.put(future.getKey(), future.getValue().get());
                } catch (NoEnoughResourceException e) {
                    // TODO custom exception with pipelineID, jobName etc.
                    throw new JobNoEnoughResourceException("No enough resource to execute pipeline");
                }
            }
            return slotProfiles;
        } catch (JobNoEnoughResourceException | ExecutionException | InterruptedException e) {
            LOGGER.severe(e);
            throw e;
        }
    }

    private void deployPipeline(@NonNull SubPlan pipeline, Map<PhysicalVertex, SlotProfile> slotProfiles) {
        pipeline.updatePipelineState(PipelineState.SCHEDULED, PipelineState.DEPLOYING);
        List<CompletableFuture> deployCoordinatorFuture =
                pipeline.getCoordinatorVertexList().stream().map(coordinator -> {
                    if (coordinator.updateTaskState(ExecutionState.SCHEDULED, ExecutionState.DEPLOYING)) {
                        // deploy is a time-consuming operation, so we do it async
                        return CompletableFuture.supplyAsync(() -> {
                            coordinator.deployOnMaster();
                            return null;
                        });
                    }
                    return null;
                }).filter(x -> x != null).collect(Collectors.toList());

        List<CompletableFuture> deployTaskFuture =
            pipeline.getPhysicalVertexList().stream().map(task -> {
                if (task.updateTaskState(ExecutionState.SCHEDULED, ExecutionState.DEPLOYING)) {
                    return CompletableFuture.supplyAsync(() -> {
                        task.deploy(slotProfiles.get(task));
                        return null;
                    });
                }
                return null;
            }).filter(x -> x != null).collect(Collectors.toList());

        deployCoordinatorFuture.addAll(deployTaskFuture);
        CompletableFuture.allOf(deployCoordinatorFuture.toArray(new CompletableFuture[deployCoordinatorFuture.size()]))
            .whenComplete((v, t) -> {
                pipeline.updatePipelineState(PipelineState.DEPLOYING, PipelineState.RUNNING);
            });
    }
}
