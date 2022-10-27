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

package org.apache.seatunnel.engine.server.resourcemanager.resource;

import org.apache.seatunnel.engine.server.serializable.ResourceDataSerializerHook;

import com.hazelcast.cluster.Address;
import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;

import java.io.IOException;

/**
 * Used to describe the status of the current slot, including resource size and assign status
 */
public class SlotProfile implements IdentifiedDataSerializable {

    private Address worker;

    private int slotID;

    private long ownerJobID;

    private volatile boolean assigned;

    private ResourceProfile resourceProfile;

    public SlotProfile() {
        worker = new Address();
    }

    public SlotProfile(Address worker, int slotID, ResourceProfile resourceProfile) {
        this.worker = worker;
        this.slotID = slotID;
        this.resourceProfile = resourceProfile;
    }

    public Address getWorker() {
        return worker;
    }

    public int getSlotID() {
        return slotID;
    }

    public ResourceProfile getResourceProfile() {
        return resourceProfile;
    }

    public long getOwnerJobID() {
        return ownerJobID;
    }

    public void assign(long jobID) {
        if (assigned) {
            throw new UnsupportedOperationException();
        } else {
            ownerJobID = jobID;
            assigned = true;
        }
    }

    public void unassigned() {
        assigned = false;
    }

    @Override
    public String toString() {
        return "SlotProfile{" +
            "worker=" + worker +
            ", slotID=" + slotID +
            ", ownerJobID=" + ownerJobID +
            ", assigned=" + assigned +
            ", resourceProfile=" + resourceProfile +
            '}';
    }

    @Override
    public int getFactoryId() {
        return ResourceDataSerializerHook.FACTORY_ID;
    }

    @Override
    public int getClassId() {
        return ResourceDataSerializerHook.SLOT_PROFILE_TYPE;
    }

    @Override
    public void writeData(ObjectDataOutput out) throws IOException {
        out.writeObject(worker);
        out.writeInt(slotID);
        out.writeLong(ownerJobID);
        out.writeBoolean(assigned);
        out.writeObject(resourceProfile);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        worker = in.readObject();
        slotID = in.readInt();
        ownerJobID = in.readLong();
        assigned = in.readBoolean();
        resourceProfile = in.readObject();
    }
}
