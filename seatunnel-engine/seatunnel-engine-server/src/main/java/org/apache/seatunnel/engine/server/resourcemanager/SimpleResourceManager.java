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

import org.apache.seatunnel.engine.common.exception.JobException;

import com.hazelcast.cluster.Address;
import com.hazelcast.spi.impl.NodeEngine;
import lombok.Data;
import lombok.NonNull;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

@Data
public class SimpleResourceManager implements ResourceManager {

    // TODO We may need more detailed resource define, instead of the resource definition method of only Address.
    private Map<Long, Map<Long, Address>> physicalVertexIdAndResourceMap = new HashMap<>();

    private final NodeEngine nodeEngine;

    public SimpleResourceManager(NodeEngine nodeEngine) {
        this.nodeEngine = nodeEngine;
    }

    @SuppressWarnings("checkstyle:MagicNumber")
    @Override
    public Address applyForResource(long jobId, long taskId) {
        Map<Long, Address> jobAddressMap =
            physicalVertexIdAndResourceMap.computeIfAbsent(jobId, k -> new HashMap<>());

            Address localhost =
                    jobAddressMap.putIfAbsent(taskId, new Address("localhost", 5701));
            if (null == localhost) {
                localhost = jobAddressMap.get(taskId);
            }

        return localhost;

    }

    @Override
    @NonNull
    public Address getAppliedResource(long jobId, long taskId) {
        Map<Long, Address> longAddressMap = physicalVertexIdAndResourceMap.get(jobId);
        if (null == longAddressMap || longAddressMap.isEmpty()) {
            throw new JobException(
                String.format("Job %s, Task %s can not found applied resource.", jobId, taskId));
        }

        return longAddressMap.get(taskId);
    }

    @Override
    public CompletableFuture<SlotProfile> applyResource(long jobId, ResourceProfile resourceProfile) throws NoEnoughResourceException {
        return null;
    }

    @Override
    public CompletableFuture<List<SlotProfile>> applyResources(long jobId, List<ResourceProfile> resourceProfile) throws NoEnoughResourceException {
        return null;
    }

    @Override
    public CompletableFuture<Void> releaseResources(long jobId, List<SlotProfile> profiles) {
        return null;
    }

    @Override
    public CompletableFuture<Void> releaseResource(long jobId, SlotProfile profile) {
        return null;
    }

    @Override
    public void workerRegister(WorkerProfile workerProfile) {

    }

    @Override
    public void heartbeatFromWorker(String workerID) {

    }
}
