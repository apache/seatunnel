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

package org.apache.seatunnel.engine.server.dag.physical;

import org.apache.seatunnel.common.utils.ExceptionUtils;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.master.JobMaster;
import org.apache.seatunnel.engine.server.resourcemanager.NoEnoughResourceException;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManager;
import org.apache.seatunnel.engine.server.resourcemanager.resource.ResourceProfile;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;
import lombok.NonNull;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletionException;

public class ResourceUtils {

    private static final ILogger LOGGER = Logger.getLogger(ResourceUtils.class);

    public static void applyResourceForPipeline(
            @NonNull JobMaster jobMaster, @NonNull SubPlan subPlan) {

        Map<TaskGroupLocation, CompletableFuture<SlotProfile>> futures = new HashMap<>();
        Map<TaskGroupLocation, SlotProfile> slotProfiles = new HashMap<>();
        Map<TaskGroupLocation, CompletableFuture<SlotProfile>> preApplyResourceFutures =
                jobMaster.getPhysicalPlan().getPreApplyResourceFutures();

        // TODO If there is no enough resources for tasks, we need add some wait profile
        allocateResources(subPlan, futures, preApplyResourceFutures);

        futures.forEach(
                (key, value) -> {
                    try {
                        slotProfiles.put(key, value == null ? null : value.join());
                    } catch (CompletionException e) {
                        LOGGER.warning("Failed to join future for task group location: " + key, e);
                    }
                });

        // set it first, avoid can't get it when get resource not enough exception and need release
        // applied resource
        subPlan.getJobMaster().setOwnedSlotProfiles(subPlan.getPipelineLocation(), slotProfiles);

        if (futures.size() != slotProfiles.size()) {
            throw new NoEnoughResourceException();
        }
    }

    private static void allocateResources(
            SubPlan subPlan,
            Map<TaskGroupLocation, CompletableFuture<SlotProfile>> futures,
            Map<TaskGroupLocation, CompletableFuture<SlotProfile>> preApplyResourceFutures) {
        subPlan.getCoordinatorVertexList()
                .forEach(
                        coordinator -> {
                            TaskGroupLocation taskGroupLocation =
                                    coordinator.getTaskGroupLocation();
                            futures.put(
                                    taskGroupLocation,
                                    preApplyResourceFutures.get(taskGroupLocation));
                        });

        subPlan.getPhysicalVertexList()
                .forEach(
                        task -> {
                            TaskGroupLocation taskGroupLocation = task.getTaskGroupLocation();
                            futures.put(
                                    taskGroupLocation,
                                    preApplyResourceFutures.get(taskGroupLocation));
                        });
    }

    public static CompletableFuture<SlotProfile> applyResourceForTask(
            ResourceManager resourceManager, PhysicalVertex task, Map<String, String> tags) {
        // TODO custom resource size
        try {
            return resourceManager.applyResource(
                    task.getTaskGroupLocation().getJobId(), new ResourceProfile(), tags);
        } catch (NoEnoughResourceException e) {
            LOGGER.severe(
                    String.format(
                            "Job Resource not enough, jobId: %s, message: %s",
                            task.getTaskGroupLocation().getJobId(), ExceptionUtils.getMessage(e)));
            return null;
        }
    }
}
