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
import org.apache.seatunnel.engine.server.serializable.ResourceDataSerializerHook;
import org.apache.seatunnel.engine.server.service.slot.SlotAndWorkerProfile;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.io.IOException;

public class RequestSlotOperation extends Operation implements IdentifiedDataSerializable {

    private ResourceProfile resourceProfile;
    private long jobID;
    private SlotAndWorkerProfile result;

    public RequestSlotOperation() {
    }

    public RequestSlotOperation(long jobID, ResourceProfile resourceProfile) {
        this.resourceProfile = resourceProfile;
        this.jobID = jobID;
    }

    @Override
    public void run() throws Exception {
        SeaTunnelServer server = getService();
        result = server.getSlotService().requestSlot(jobID, resourceProfile);
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        out.writeObject(resourceProfile);
        out.writeLong(jobID);
    }

    @Override
    public Object getResponse() {
        return result;
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        resourceProfile = in.readObject();
        jobID = in.readLong();
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
        return ResourceDataSerializerHook.REQUEST_SLOT_TYPE;
    }
}
