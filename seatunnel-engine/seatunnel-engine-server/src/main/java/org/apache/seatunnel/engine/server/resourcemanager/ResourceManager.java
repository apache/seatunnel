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

package org.apache.seatunnel.engine.server.resourcemanager;

import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.server.resourcemanager.resource.ResourceProfile;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;
import org.apache.seatunnel.engine.server.resourcemanager.worker.WorkerProfile;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.services.MembershipServiceEvent;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;

public interface ResourceManager {
    void init();

    CompletableFuture<SlotProfile> applyResource(
            long jobId, ResourceProfile resourceProfile, Map<String, String> tagFilter)
            throws NoEnoughResourceException;

    CompletableFuture<List<SlotProfile>> applyResources(
            long jobId, List<ResourceProfile> resourceProfile, Map<String, String> tagFilter)
            throws NoEnoughResourceException;

    CompletableFuture<Void> releaseResources(long jobId, List<SlotProfile> profiles);

    CompletableFuture<Void> releaseResource(long jobId, SlotProfile profile);

    /**
     * Check {@link SlotProfile} is active or not. Not active meaning can't use this slot to deploy
     * task.
     *
     * @return active or not
     */
    boolean slotActiveCheck(SlotProfile profile);

    /**
     * Every time ResourceManager and Worker communicate, heartbeat method should be called to
     * record the latest Worker status
     *
     * @param workerProfile the worker current worker's profile
     */
    void heartbeat(WorkerProfile workerProfile);

    void memberRemoved(MembershipServiceEvent event);

    void close();

    List<SlotProfile> getUnassignedSlots(Map<String, String> tags);

    List<SlotProfile> getAssignedSlots(Map<String, String> tags);

    int workerCount(Map<String, String> tags);

    ConcurrentMap<Address, WorkerProfile> getRegisterWorker();
}
