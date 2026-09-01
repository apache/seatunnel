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

package org.apache.seatunnel.engine.server.resourcemanager;

import org.apache.seatunnel.engine.common.config.EngineConfig;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.server.resourcemanager.opeartion.ReleaseSlotOperation;
import org.apache.seatunnel.engine.server.resourcemanager.opeartion.SyncWorkerProfileOperation;
import org.apache.seatunnel.engine.server.resourcemanager.resource.ResourceProfile;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;
import org.apache.seatunnel.engine.server.resourcemanager.worker.WorkerProfile;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.util.Arrays;
import java.util.Collections;

public class ResourceReleaseRefreshTest {

    private static final long JOB_ID = 100L;

    @Test
    public void testReleaseResourcesRefreshesWorkerProfileAfterBatchRelease() throws Exception {
        Address worker = new Address("localhost", 5801);
        SlotProfile firstSlot = new SlotProfile(worker, 1, new ResourceProfile(), "sequence");
        SlotProfile secondSlot = new SlotProfile(worker, 2, new ResourceProfile(), "sequence");
        TestResourceManager resourceManager =
                new TestResourceManager(
                        nodeEngineWithMember(worker),
                        workerProfile(worker, new SlotProfile[] {firstSlot, secondSlot}),
                        workerProfile(worker, new SlotProfile[] {}));

        Assertions.assertTrue(resourceManager.slotActiveCheck(firstSlot));
        Assertions.assertTrue(resourceManager.slotActiveCheck(secondSlot));

        resourceManager.releaseResources(JOB_ID, Arrays.asList(firstSlot, secondSlot)).join();

        Assertions.assertFalse(resourceManager.slotActiveCheck(firstSlot));
        Assertions.assertFalse(resourceManager.slotActiveCheck(secondSlot));
        Assertions.assertEquals(2, resourceManager.releaseCalls);
        Assertions.assertEquals(1, resourceManager.refreshCalls);
    }

    @Test
    public void testReleaseResourceRefreshesWorkerProfileAfterSingleRelease() throws Exception {
        Address worker = new Address("localhost", 5801);
        SlotProfile slot = new SlotProfile(worker, 1, new ResourceProfile(), "sequence");
        TestResourceManager resourceManager =
                new TestResourceManager(
                        nodeEngineWithMember(worker),
                        workerProfile(worker, new SlotProfile[] {slot}),
                        workerProfile(worker, new SlotProfile[] {}));

        Assertions.assertTrue(resourceManager.slotActiveCheck(slot));

        resourceManager.releaseResource(JOB_ID, slot).join();

        Assertions.assertFalse(resourceManager.slotActiveCheck(slot));
        Assertions.assertEquals(1, resourceManager.releaseCalls);
        Assertions.assertEquals(1, resourceManager.refreshCalls);
    }

    private static NodeEngine nodeEngineWithMember(Address worker) {
        NodeEngine nodeEngine = Mockito.mock(NodeEngine.class, Mockito.RETURNS_DEEP_STUBS);
        Mockito.when(nodeEngine.getClusterService().getMember(worker))
                .thenReturn(Mockito.mock(MemberImpl.class));
        return nodeEngine;
    }

    private static WorkerProfile workerProfile(Address worker, SlotProfile[] assignedSlots) {
        return new WorkerProfile(
                worker,
                new ResourceProfile(),
                new ResourceProfile(),
                false,
                assignedSlots,
                new SlotProfile[] {},
                Collections.emptyMap());
    }

    private static class TestResourceManager extends AbstractResourceManager {
        private final WorkerProfile refreshedWorkerProfile;
        private int releaseCalls;
        private int refreshCalls;

        private TestResourceManager(
                NodeEngine nodeEngine,
                WorkerProfile initialWorkerProfile,
                WorkerProfile refreshedWorkerProfile) {
            super(nodeEngine, new EngineConfig());
            this.refreshedWorkerProfile = refreshedWorkerProfile;
            registerWorker.put(initialWorkerProfile.getAddress(), initialWorkerProfile);
        }

        @SuppressWarnings("unchecked")
        @Override
        protected <E> CompletableFuture<E> sendToMember(Operation operation, Address address) {
            if (operation instanceof ReleaseSlotOperation) {
                releaseCalls++;
                return (CompletableFuture<E>) CompletableFuture.completedFuture(null);
            }
            if (operation instanceof SyncWorkerProfileOperation) {
                refreshCalls++;
                return (CompletableFuture<E>)
                        CompletableFuture.completedFuture(refreshedWorkerProfile);
            }
            return (CompletableFuture<E>) CompletableFuture.completedFuture(null);
        }
    }
}
