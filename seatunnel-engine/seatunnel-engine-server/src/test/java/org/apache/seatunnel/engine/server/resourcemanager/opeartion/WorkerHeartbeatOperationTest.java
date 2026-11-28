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

import org.apache.seatunnel.engine.server.resourcemanager.resource.CPU;
import org.apache.seatunnel.engine.server.resourcemanager.resource.Memory;
import org.apache.seatunnel.engine.server.resourcemanager.resource.ResourceProfile;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SystemLoadInfo;
import org.apache.seatunnel.engine.server.resourcemanager.worker.WorkerProfile;
import org.apache.seatunnel.engine.server.serializable.ResourceDataSerializerHook;

import org.junit.jupiter.api.Test;

import com.hazelcast.cluster.Address;
import com.hazelcast.internal.nio.BufferObjectDataOutput;
import com.hazelcast.internal.serialization.Data;
import com.hazelcast.internal.serialization.InternalSerializationService;
import com.hazelcast.internal.serialization.impl.DefaultSerializationServiceBuilder;

import java.io.IOException;
import java.util.Collections;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class WorkerHeartbeatOperationTest {

    private final InternalSerializationService serialization =
            new DefaultSerializationServiceBuilder().build();

    @Test
    void compressesFixedSlotsAndPreservesAllocationIdentityAndWorkerLoad() throws Exception {
        Address worker = new Address("127.0.0.1", 5802);
        ResourceProfile resource = new ResourceProfile(CPU.of(2), Memory.of(20L));
        SlotProfile[] assigned = new SlotProfile[512];
        SlotProfile[] free = new SlotProfile[512];
        for (int index = 0; index < assigned.length; index++) {
            assigned[index] = new SlotProfile(worker, index, resource, "worker-sequence");
            assigned[index].assign(5000L + index, new UUID(1L, index).toString());
            free[index] =
                    new SlotProfile(worker, index + assigned.length, resource, "free-sequence");
            free[index].assign(10000L + index);
            free[index].unassigned();
        }
        WorkerProfile profile =
                new WorkerProfile(
                        worker,
                        resource,
                        new ResourceProfile(CPU.of(1), Memory.of(10L)),
                        false,
                        assigned,
                        free,
                        Collections.singletonMap("group", "large"));
        profile.setSystemLoadInfo(new SystemLoadInfo(0.25, 0.75));
        WorkerHeartbeatOperation heartbeat = new WorkerHeartbeatOperation(profile);
        Data data = serialization.toData(heartbeat);
        WorkerHeartbeatOperation restored = serialization.toObject(data);

        assertEquals(ResourceDataSerializerHook.FIXED_SLOT_HEARTBEAT_TYPE, restored.getClassId());
        assertTrue(data.dataSize() * 5L < serialization.toData(profile).dataSize());

        BufferObjectDataOutput payload = serialization.createObjectDataOutput();
        restored.writeInternal(payload);
        WorkerProfile decoded =
                WorkerHeartbeatOperation.readFixedWorkerProfile(
                        serialization.createObjectDataInput(payload.toByteArray()));
        assertArrayEquals(
                serialization.toData(profile).toByteArray(),
                serialization.toData(decoded).toByteArray());
    }

    @Test
    void keepsTheLegacyHeartbeatForDynamicSlots() throws Exception {
        Address worker = new Address("127.0.0.1", 5802);
        ResourceProfile resource = new ResourceProfile();
        SlotProfile slot = new SlotProfile(worker, 1, resource, "dynamic-sequence");
        slot.assign(100L);
        WorkerProfile profile =
                new WorkerProfile(
                        worker,
                        resource,
                        resource,
                        true,
                        new SlotProfile[] {slot},
                        new SlotProfile[0],
                        Collections.emptyMap());
        WorkerHeartbeatOperation heartbeat = new WorkerHeartbeatOperation(profile);
        BufferObjectDataOutput legacy = serialization.createObjectDataOutput();
        legacy.writeObject(profile);
        BufferObjectDataOutput actual = serialization.createObjectDataOutput();
        heartbeat.writeInternal(actual);
        assertEquals(ResourceDataSerializerHook.WORKER_HEARTBEAT_TYPE, heartbeat.getClassId());
        assertArrayEquals(legacy.toByteArray(), actual.toByteArray());
    }

    @Test
    void rejectsAnOversizedCountInATruncatedFrame() throws Exception {
        BufferObjectDataOutput output = serialization.createObjectDataOutput();
        output.writeObject(new Address("127.0.0.1", 5802));
        output.writeObject(new ResourceProfile());
        output.writeObject(new ResourceProfile());
        output.writeObject(Collections.emptyMap());
        output.writeObject(null);
        output.writeString("worker-sequence");
        output.writeInt(Integer.MAX_VALUE);
        assertThrows(
                IOException.class,
                () ->
                        WorkerHeartbeatOperation.readFixedWorkerProfile(
                                serialization.createObjectDataInput(output.toByteArray())));
    }
}
