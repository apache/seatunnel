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
import org.apache.seatunnel.engine.server.resourcemanager.resource.ResourceProfile;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SystemLoadInfo;
import org.apache.seatunnel.engine.server.resourcemanager.worker.WorkerProfile;
import org.apache.seatunnel.engine.server.serializable.ResourceDataSerializerHook;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.nio.BufferObjectDataInput;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

public class WorkerHeartbeatOperation extends Operation implements IdentifiedDataSerializable {

    private final boolean compactFixedSlots;
    private WorkerProfile workerProfile;

    public WorkerHeartbeatOperation() {
        this(false);
    }

    /** Selects the on-wire format for the operation created by the serializer factory. */
    public WorkerHeartbeatOperation(boolean compactFixedSlots) {
        this.compactFixedSlots = compactFixedSlots;
    }

    public WorkerHeartbeatOperation(WorkerProfile workerProfile) {
        this(workerProfile != null && !workerProfile.isDynamicSlot());
        this.workerProfile = workerProfile;
    }

    @Override
    public void run() throws Exception {
        SeaTunnelServer server = getService();
        server.getCoordinatorService().getResourceManager().heartbeat(workerProfile);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        if (!compactFixedSlots) {
            out.writeObject(workerProfile);
            return;
        }

        Address worker = workerProfile.getAddress();
        SlotProfile[] assigned = workerProfile.getAssignedSlots();
        SlotProfile[] free = workerProfile.getUnassignedSlots();
        SlotProfile representative =
                free.length > 0 ? free[0] : (assigned.length > 0 ? assigned[0] : null);
        String defaultSequence = representative == null ? null : representative.getSequence();
        out.writeObject(worker);
        out.writeObject(workerProfile.getProfile());
        out.writeObject(workerProfile.getUnassignedResource());
        out.writeObject(workerProfile.getAttributes());
        out.writeObject(workerProfile.getSystemLoadInfo());
        out.writeString(defaultSequence);
        writeSlots(out, assigned, worker, defaultSequence);
        writeSlots(out, free, worker, defaultSequence);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        workerProfile = compactFixedSlots ? readFixedWorkerProfile(in) : in.readObject();
    }

    static WorkerProfile readFixedWorkerProfile(ObjectDataInput in) throws IOException {
        Address worker = in.readObject();
        ResourceProfile capacity = in.readObject();
        ResourceProfile available = in.readObject();
        Map<String, String> attributes = in.readObject();
        SystemLoadInfo systemLoadInfo = in.readObject();
        String defaultSequence = in.readString();
        WorkerProfile profile =
                new WorkerProfile(
                        worker,
                        capacity,
                        available,
                        false,
                        readSlots(in, worker, defaultSequence),
                        readSlots(in, worker, defaultSequence),
                        attributes);
        profile.setSystemLoadInfo(systemLoadInfo);
        return profile;
    }

    private static void writeSlots(
            ObjectDataOutput out, SlotProfile[] slots, Address worker, String defaultSequence)
            throws IOException {
        out.writeInt(slots.length);
        for (SlotProfile slot : slots) {
            boolean compact =
                    slot != null
                            && slot.getClass() == SlotProfile.class
                            && worker != null
                            && worker.equals(slot.getWorker())
                            && slot.getResourceProfile() != null
                            && slot.getResourceProfile().getClass() == ResourceProfile.class;
            out.writeBoolean(compact);
            if (compact) {
                slot.writeFixedHeartbeatData(out, defaultSequence);
            } else {
                out.writeObject(slot);
            }
        }
    }

    private static SlotProfile[] readSlots(
            ObjectDataInput in, Address worker, String defaultSequence) throws IOException {
        int count = in.readInt();
        if (count < 0
                || (in instanceof BufferObjectDataInput
                        && in instanceof InputStream
                        && count > ((InputStream) in).available())) {
            throw new IOException("Invalid fixed-slot heartbeat count: " + count);
        }
        SlotProfile[] slots = new SlotProfile[count];
        for (int index = 0; index < count; index++) {
            slots[index] =
                    in.readBoolean()
                            ? SlotProfile.readFixedHeartbeatData(in, worker, defaultSequence)
                            : in.readObject();
        }
        return slots;
    }

    @Override
    public String getServiceName() {
        return SeaTunnelServer.SERVICE_NAME;
    }

    @Override
    public int getFactoryId() {
        return ResourceDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return compactFixedSlots
                ? ResourceDataSerializerHook.FIXED_SLOT_HEARTBEAT_TYPE
                : ResourceDataSerializerHook.WORKER_HEARTBEAT_TYPE;
    }
}
