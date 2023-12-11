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

import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManager;
import org.apache.seatunnel.engine.server.resourcemanager.resource.ResourceProfile;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;

import lombok.NonNull;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public class ResourceUtils {

    public static Map<TaskGroupLocation, SlotProfile> applyResourceForPipeline(
            @NonNull ResourceManager resourceManager, @NonNull SubPlan subPlan) {
        Map<TaskGroupLocation, CompletableFuture<SlotProfile>> futures = new HashMap<>();
        Map<TaskGroupLocation, SlotProfile> slotProfiles = new HashMap<>();
        // TODO If there is no enough resources for tasks, we need add some wait profile
        subPlan.getCoordinatorVertexList()
                .forEach(
                        coordinator ->
                                futures.put(
                                        coordinator.getTaskGroupLocation(),
                                        applyResourceForTask(resourceManager, coordinator)));

        subPlan.getPhysicalVertexList()
                .forEach(
                        task ->
                                futures.put(
                                        task.getTaskGroupLocation(),
                                        applyResourceForTask(resourceManager, task)));

        for (Map.Entry<TaskGroupLocation, CompletableFuture<SlotProfile>> future :
                futures.entrySet()) {
            slotProfiles.put(
                    future.getKey(), future.getValue() == null ? null : future.getValue().join());
        }
        return slotProfiles;
    }

    public static CompletableFuture<SlotProfile> applyResourceForTask(
            ResourceManager resourceManager, PhysicalVertex task) {
        // TODO custom resource size
        return resourceManager.applyResource(
                task.getTaskGroupLocation().getJobId(), new ResourceProfile());
    }
}
