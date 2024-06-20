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

import org.apache.seatunnel.engine.server.AbstractSeaTunnelServerTest;
import org.apache.seatunnel.engine.server.resourcemanager.resource.CPU;
import org.apache.seatunnel.engine.server.resourcemanager.resource.Memory;
import org.apache.seatunnel.engine.server.resourcemanager.resource.ResourceProfile;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.hazelcast.cluster.Address;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

public class ResourceManagerTest extends AbstractSeaTunnelServerTest<ResourceManagerTest> {

    private ResourceManager resourceManager;

    private final long jobId = 5;

    @BeforeAll
    public void before() {
        super.before();
        resourceManager = server.getCoordinatorService().getResourceManager();
        server.getSlotService();
    }

    @Test
    public void testApplyRequest() throws ExecutionException, InterruptedException {
        List<ResourceProfile> resourceProfiles = new ArrayList<>();
        resourceProfiles.add(new ResourceProfile(CPU.of(0), Memory.of(100)));
        resourceProfiles.add(new ResourceProfile(CPU.of(0), Memory.of(200)));
        resourceProfiles.add(new ResourceProfile(CPU.of(0), Memory.of(300)));
        List<SlotProfile> slotProfiles =
                resourceManager.applyResources(jobId, resourceProfiles).get();

        Assertions.assertEquals(
                resourceProfiles.get(0).getHeapMemory().getBytes(),
                slotProfiles.get(0).getResourceProfile().getHeapMemory().getBytes());
        Assertions.assertEquals(
                resourceProfiles.get(1).getHeapMemory().getBytes(),
                slotProfiles.get(1).getResourceProfile().getHeapMemory().getBytes());
        Assertions.assertEquals(
                resourceProfiles.get(2).getHeapMemory().getBytes(),
                slotProfiles.get(2).getResourceProfile().getHeapMemory().getBytes());

        // release not existed job id
        resourceManager.releaseResources(jobId + 1, slotProfiles).get();
        resourceManager.releaseResources(jobId, slotProfiles).get();
        // release already released resource
        resourceManager.releaseResources(jobId, slotProfiles).get();
        Assertions.assertThrows(
                ExecutionException.class,
                () ->
                        resourceManager
                                .applyResource(
                                        jobId,
                                        new ResourceProfile(CPU.of(0), Memory.of(Long.MAX_VALUE)))
                                .get());
    }

    @Test
    public void testApplyResourceWithRandomResult()
            throws ExecutionException, InterruptedException {
        FakeResourceManager resourceManager = new FakeResourceManager(nodeEngine);

        List<ResourceProfile> resourceProfiles = new ArrayList<>();
        resourceProfiles.add(new ResourceProfile());
        resourceProfiles.add(new ResourceProfile());
        resourceProfiles.add(new ResourceProfile());
        resourceProfiles.add(new ResourceProfile());
        resourceProfiles.add(new ResourceProfile());
        List<SlotProfile> slotProfiles = resourceManager.applyResources(1L, resourceProfiles).get();
        Assertions.assertEquals(slotProfiles.size(), 5);

        boolean hasDifferentWorker = false;
        for (int i = 0; i < 5; i++) {
            Set<Address> addresses =
                    slotProfiles.stream().map(SlotProfile::getWorker).collect(Collectors.toSet());
            hasDifferentWorker = addresses.size() > 1;
        }
        Assertions.assertTrue(hasDifferentWorker, "should have different worker for each slot");
    }
}
