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
import java.util.Objects;

/** Used to describe the status of the current slot, including resource size and assign status */
public class SlotProfile implements IdentifiedDataSerializable {

    private Address worker;

    private int slotID;

    private long ownerJobID;

    private volatile boolean assigned;

    private ResourceProfile resourceProfile;

    private String sequence;

    public SlotProfile() {
        worker = new Address();
    }

    public SlotProfile(
            Address worker, int slotID, ResourceProfile resourceProfile, String sequence) {
        this.worker = worker;
        this.slotID = slotID;
        this.resourceProfile = resourceProfile;
        this.sequence = sequence;
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

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SlotProfile that = (SlotProfile) o;
        return slotID == that.slotID
                && worker.equals(that.worker)
                && sequence.equals(that.sequence);
    }

    @Override
    public int hashCode() {
        return Objects.hash(worker, slotID, sequence);
    }

    public String getSequence() {
        return sequence;
    }

    public void unassigned() {
        assigned = false;
    }

    @Override
    public String toString() {
        return "SlotProfile{"
                + "worker="
                + worker
                + ", slotID="
                + slotID
                + ", ownerJobID="
                + ownerJobID
                + ", assigned="
                + assigned
                + ", resourceProfile="
                + resourceProfile
                + ", sequence='"
                + sequence
                + '\''
                + '}';
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
        out.writeString(sequence);
    }

    @Override
    public void readData(ObjectDataInput in) throws IOException {
        worker = in.readObject();
        slotID = in.readInt();
        ownerJobID = in.readLong();
        assigned = in.readBoolean();
        resourceProfile = in.readObject();
        sequence = in.readString();
    }
}
