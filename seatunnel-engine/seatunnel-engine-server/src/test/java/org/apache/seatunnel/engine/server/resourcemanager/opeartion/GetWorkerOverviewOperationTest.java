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

import org.apache.seatunnel.engine.server.resourcemanager.resource.ResourceProfile;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SystemLoadInfo;
import org.apache.seatunnel.engine.server.resourcemanager.resource.WorkerOverviewInfo;
import org.apache.seatunnel.engine.server.resourcemanager.worker.WorkerProfile;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.cluster.Address;

import java.net.UnknownHostException;
import java.util.Collections;

/**
 * Covers {@link GetWorkerOverviewOperation#toWorkerOverviewInfo(WorkerProfile)}, the pure mapping
 * from the resource manager's live {@link WorkerProfile} to the Web UI-facing {@link
 * WorkerOverviewInfo} projection. No new scheduling behavior is under test here - only that the
 * projection reads existing slot/load state correctly and tolerates the fields being absent.
 */
class GetWorkerOverviewOperationTest {

    @Test
    void mapsSlotCountsAndLoadFromAFullyPopulatedWorkerProfile() throws UnknownHostException {
        Address address = new Address("localhost", 5801);
        SlotProfile assignedSlot = new SlotProfile(address, 0, new ResourceProfile(), "sequence-0");
        WorkerProfile workerProfile =
                new WorkerProfile(
                        address,
                        new ResourceProfile(),
                        new ResourceProfile(),
                        true,
                        new SlotProfile[] {assignedSlot},
                        new SlotProfile[] {
                            new SlotProfile(address, 1, new ResourceProfile(), "sequence-1"),
                            new SlotProfile(address, 2, new ResourceProfile(), "sequence-2")
                        },
                        Collections.singletonMap("tag", "value"));
        workerProfile.setSystemLoadInfo(new SystemLoadInfo(0.42, 0.13));

        WorkerOverviewInfo info = GetWorkerOverviewOperation.toWorkerOverviewInfo(workerProfile);

        Assertions.assertEquals("localhost", info.getHost());
        Assertions.assertEquals(5801, info.getPort());
        Assertions.assertEquals(1, info.getUsedSlot());
        Assertions.assertEquals(3, info.getTotalSlot());
        Assertions.assertTrue(info.isDynamicSlot());
        Assertions.assertEquals(0.42, info.getMemPercentage().doubleValue());
        Assertions.assertEquals(0.13, info.getCpuPercentage().doubleValue());
        Assertions.assertEquals("value", info.getAttributes().get("tag"));
    }

    @Test
    void treatsMissingSlotArraysAndLoadInfoAsZeroInsteadOfThrowing() throws UnknownHostException {
        // The no-arg WorkerProfile constructor leaves assignedSlots/unassignedSlots/
        // systemLoadInfo null; a worker that has not fully reported yet must not crash
        // the projection.
        WorkerProfile workerProfile = new WorkerProfile();
        workerProfile.setAddress(new Address("localhost", 5802));

        WorkerOverviewInfo info = GetWorkerOverviewOperation.toWorkerOverviewInfo(workerProfile);

        Assertions.assertEquals(0, info.getUsedSlot());
        Assertions.assertEquals(0, info.getTotalSlot());
        Assertions.assertNull(info.getCpuPercentage());
        Assertions.assertNull(info.getMemPercentage());
    }
}
