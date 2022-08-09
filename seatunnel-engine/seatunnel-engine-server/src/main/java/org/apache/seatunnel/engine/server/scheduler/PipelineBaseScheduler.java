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
import org.apache.seatunnel.engine.common.utils.ExceptionUtil;
import org.apache.seatunnel.engine.server.dag.execution.Pipeline;
import org.apache.seatunnel.engine.server.dag.physical.PhysicalPlan;
import org.apache.seatunnel.engine.server.dag.physical.SubPlan;
import org.apache.seatunnel.engine.server.master.JobMaster;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManager;

import com.hazelcast.cluster.Address;
import lombok.NonNull;

import java.util.concurrent.CompletableFuture;

public class PipelineBaseScheduler implements JobScheduler {
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
        physicalPlan.getPipelineList().forEach(pipeline -> {
            applyResourceForPipeline(pipeline).whenComplete((v, t) -> {
                if (t != null) {
                    ExceptionUtil.rethrow(new JobException(String.format("Apply resources for job %s"));
                }
            });
        });
    }

    private CompletableFuture<Void> applyResourceForPipeline(@NonNull SubPlan subPlan) {
        return CompletableFuture.supplyAsync(() -> {
            // apply resource for coordinators
            subPlan.getCoordinatorVertexList().forEach(coordinator -> {
                // TODO If there is no enough resources for tasks, we need add some wait profile
                resourceManager.applyForResource(physicalPlan.getJobImmutableInformation().getJobId(),
                    coordinator.getPhysicalVertexId());
            });

            // apply resource for other tasks
            subPlan.getPhysicalVertexList().forEach(task -> {
                resourceManager.applyForResource(physicalPlan.getJobImmutableInformation().getJobId(),
                    task.getPhysicalVertexId());
            });
            return null;
        });
    }
}
