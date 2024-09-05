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

import org.apache.seatunnel.common.utils.ExceptionUtils;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;
import org.apache.seatunnel.engine.server.resourcemanager.worker.WorkerProfile;
import org.apache.seatunnel.engine.server.serializable.ResourceDataSerializerHook;
import org.apache.seatunnel.engine.server.service.slot.WrongTargetSlotException;
import org.apache.seatunnel.engine.server.task.operation.TracingOperation;

import com.hazelcast.nio.ObjectDataInput;
import com.hazelcast.nio.ObjectDataOutput;
import com.hazelcast.nio.serialization.IdentifiedDataSerializable;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

@Slf4j
public class ReleaseSlotOperation extends TracingOperation implements IdentifiedDataSerializable {

    private long jobID;
    private SlotProfile slotProfile;
    private WorkerProfile result;

    public ReleaseSlotOperation() {}

    public ReleaseSlotOperation(long jobID, SlotProfile slotProfile) {
        this.jobID = jobID;
        this.slotProfile = slotProfile;
    }

    @Override
    public void runInternal() throws Exception {
        SeaTunnelServer server = getService();
        try {
            server.getSlotService().releaseSlot(jobID, slotProfile);
        } catch (WrongTargetSlotException ignore) {
            log.warn(
                    "wrong target release operation with job {} and slot profile {}, exception: {}",
                    jobID,
                    slotProfile,
                    ExceptionUtils.getMessage(ignore));
        }
        result = server.getSlotService().getWorkerProfile();
    }

    @Override
    public Object getResponse() {
        return result;
    }

    @Override
    protected void writeInternal(ObjectDataOutput out) throws IOException {
        super.writeInternal(out);
        out.writeObject(slotProfile);
        out.writeLong(jobID);
    }

    @Override
    protected void readInternal(ObjectDataInput in) throws IOException {
        super.readInternal(in);
        slotProfile = in.readObject();
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
        return ResourceDataSerializerHook.RELEASE_SLOT_TYPE;
    }
}
