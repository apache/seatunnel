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

package org.apache.seatunnel.engine.server.resourcemanager.opeartion;

import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManager;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SystemLoadInfo;
import org.apache.seatunnel.engine.server.resourcemanager.resource.WorkerOverviewInfo;
import org.apache.seatunnel.engine.server.resourcemanager.worker.WorkerProfile;
import org.apache.seatunnel.engine.server.serializable.ResourceDataSerializerHook;

import com.hazelcast.cluster.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

/**
 * Projects the resource manager's existing live {@link WorkerProfile} map into a per-worker DTO for
 * the Web UI. This operation only reads already-tracked scheduler state ({@code
 * assignedSlots}/{@code unassignedSlots} counts, {@code systemLoadInfo}); it introduces no new
 * scheduling state and never mutates slot assignment.
 */
public class GetWorkerOverviewOperation extends Operation implements IdentifiedDataSerializable {

    private List<WorkerOverviewInfo> workerOverviewInfos;

    public GetWorkerOverviewOperation() {}

    @Override
    public void run() throws Exception {
        SeaTunnelServer server = getService();
        workerOverviewInfos = getWorkerOverviewInfos(server);
    }

    @Override
    public Object getResponse() {
        return workerOverviewInfos;
    }

    @Override
    public int getFactoryId() {
        return ResourceDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return ResourceDataSerializerHook.GET_WORKER_OVERVIEW_TYPE;
    }

    @Override
    public String getServiceName() {
        return SeaTunnelServer.SERVICE_NAME;
    }

    public static List<WorkerOverviewInfo> getWorkerOverviewInfos(SeaTunnelServer server) {
        ResourceManager resourceManager = server.getCoordinatorService().getResourceManager();
        ConcurrentMap<Address, WorkerProfile> registerWorker = resourceManager.getRegisterWorker();
        return registerWorker.values().stream()
                .map(GetWorkerOverviewOperation::toWorkerOverviewInfo)
                .collect(Collectors.toList());
    }

    // package-private for direct unit testing without mocking the SeaTunnelServer chain
    static WorkerOverviewInfo toWorkerOverviewInfo(WorkerProfile workerProfile) {
        WorkerOverviewInfo info = new WorkerOverviewInfo();
        Address address = workerProfile.getAddress();
        info.setHost(address.getHost());
        info.setPort(address.getPort());

        int assignedSlots =
                workerProfile.getAssignedSlots() == null
                        ? 0
                        : workerProfile.getAssignedSlots().length;
        int unassignedSlots =
                workerProfile.getUnassignedSlots() == null
                        ? 0
                        : workerProfile.getUnassignedSlots().length;
        info.setUsedSlot(assignedSlots);
        info.setTotalSlot(assignedSlots + unassignedSlots);
        info.setDynamicSlot(workerProfile.isDynamicSlot());

        SystemLoadInfo systemLoadInfo = workerProfile.getSystemLoadInfo();
        if (systemLoadInfo != null) {
            info.setCpuPercentage(systemLoadInfo.getCpuPercentage());
            info.setMemPercentage(systemLoadInfo.getMemPercentage());
        }
        info.setAttributes(workerProfile.getAttributes());
        return info;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
    }
}
