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

package org.apache.seatunnel.engine.server.service.slot;

import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;
import org.apache.seatunnel.engine.server.resourcemanager.worker.WorkerProfile;
import org.apache.seatunnel.engine.server.serializable.ResourceDataSerializerHook;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

public class SlotAndWorkerProfile implements IdentifiedDataSerializable {

    private WorkerProfile workerProfile;

    // null value means the slot request failed, no suitable slot found
    private SlotProfile slotProfile;

    public SlotAndWorkerProfile() {}

    public SlotAndWorkerProfile(WorkerProfile workerProfile, SlotProfile slotProfile) {
        this.workerProfile = workerProfile;
        this.slotProfile = slotProfile;
    }

    public WorkerProfile getWorkerProfile() {
        return workerProfile;
    }

    /** Get slot profile of worker return. Could be null if no slot can be provided. */
    public SlotProfile getSlotProfile() {
        return slotProfile;
    }

    @Override
    public int getFactoryId() {
        return ResourceDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return ResourceDataSerializerHook.SLOT_AND_WORKER_PROFILE;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(workerProfile);
        out.writeObject(slotProfile);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        workerProfile = in.readObject();
        slotProfile = in.readObject();
    }
}
