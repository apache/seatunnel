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

package org.apache.seatunnel.engine.server.resourcemanager.worker;

import org.apache.seatunnel.engine.server.resourcemanager.resource.ResourceProfile;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;

import com.hazelcast.cluster.Address;

import java.io.Serializable;

public class WorkerProfile implements Serializable {

    private final String workerID;

    private final Address address;

    private ResourceProfile profile;

    private ResourceProfile unassignedResource;

    private SlotProfile[] assignedSlots;

    private SlotProfile[] unassignedSlots;

    public WorkerProfile(String workerID, Address address) {
        this.workerID = workerID;
        this.address = address;
        this.unassignedResource = new ResourceProfile();
    }

    public String getWorkerID() {
        return workerID;
    }

    public Address getAddress() {
        return address;
    }

    public ResourceProfile getProfile() {
        return profile;
    }

    public void setProfile(ResourceProfile profile) {
        this.profile = profile;
    }

    public SlotProfile[] getAssignedSlots() {
        return assignedSlots;
    }

    public void setAssignedSlots(SlotProfile[] assignedSlots) {
        this.assignedSlots = assignedSlots;
    }

    public SlotProfile[] getUnassignedSlots() {
        return unassignedSlots;
    }

    public void setUnassignedSlots(SlotProfile[] unassignedSlots) {
        this.unassignedSlots = unassignedSlots;
    }

    public ResourceProfile getUnassignedResource() {
        return unassignedResource;
    }

    public void setUnassignedResource(ResourceProfile unassignedResource) {
        this.unassignedResource = unassignedResource;
    }
}
