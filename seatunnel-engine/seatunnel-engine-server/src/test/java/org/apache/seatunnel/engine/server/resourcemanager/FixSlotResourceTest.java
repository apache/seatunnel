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

import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.config.server.SlotServiceConfig;
import org.apache.seatunnel.engine.server.AbstractSeaTunnelServerTest;
import org.apache.seatunnel.engine.server.resourcemanager.opeartion.ResetResourceOperation;
import org.apache.seatunnel.engine.server.resourcemanager.resource.ResourceProfile;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;
import org.apache.seatunnel.engine.server.resourcemanager.worker.WorkerProfile;
import org.apache.seatunnel.engine.server.service.slot.SlotService;
import org.apache.seatunnel.engine.server.utils.NodeEngineUtil;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.awaitility.Awaitility.await;

public class FixSlotResourceTest extends AbstractSeaTunnelServerTest<FixSlotResourceTest> {

    private final int totalSlots = 3;

    @Override
    public SeaTunnelConfig loadSeaTunnelConfig() {
        SeaTunnelConfig seaTunnelConfig = super.loadSeaTunnelConfig();
        SlotServiceConfig slotServiceConfig =
                seaTunnelConfig.getEngineConfig().getSlotServiceConfig();
        slotServiceConfig.setDynamicSlot(false);
        slotServiceConfig.setSlotNum(totalSlots);
        seaTunnelConfig.getEngineConfig().setSlotServiceConfig(slotServiceConfig);
        return seaTunnelConfig;
    }

    @Test
    public void testEnoughResource() throws ExecutionException, InterruptedException {
        ResourceManager resourceManager = server.getCoordinatorService().getResourceManager();
        // wait all slot ready
        await().atMost(20000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertEquals(
                                    totalSlots, resourceManager.getUnassignedSlots(null).size());
                        });
        long jobId = System.currentTimeMillis();
        List<ResourceProfile> resourceProfiles = new ArrayList<>();
        resourceProfiles.add(new ResourceProfile());
        resourceProfiles.add(new ResourceProfile());
        resourceProfiles.add(new ResourceProfile());
        List<SlotProfile> slotProfiles =
                resourceManager.applyResources(jobId, resourceProfiles, null).get();
        Assertions.assertEquals(slotProfiles.size(), 3);
        resourceManager.releaseResources(jobId, slotProfiles);
    }

    @Test
    public void testNotEnoughResource() throws ExecutionException, InterruptedException {
        ResourceManager resourceManager = server.getCoordinatorService().getResourceManager();
        long jobId = System.currentTimeMillis();
        List<ResourceProfile> resourceProfiles = new ArrayList<>();
        resourceProfiles.add(new ResourceProfile());
        resourceProfiles.add(new ResourceProfile());
        resourceProfiles.add(new ResourceProfile());
        resourceProfiles.add(new ResourceProfile());
        try {
            resourceManager.applyResources(jobId, resourceProfiles, null).get();
        } catch (ExecutionException e) {
            Assertions.assertTrue(e.getMessage().contains("NoEnoughResourceException"));
        }
        // wait for release resource complete
        await().atMost(20000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () -> {
                            Assertions.assertEquals(
                                    totalSlots, resourceManager.getUnassignedSlots(null).size());
                        });
        resourceProfiles.remove(0);
        List<SlotProfile> slotProfiles =
                resourceManager.applyResources(jobId, resourceProfiles, null).get();
        Assertions.assertEquals(slotProfiles.size(), 3);
        resourceManager.releaseResources(jobId, slotProfiles);
    }

    @Test
    public void testResetKeepsLiveSlotAssignment() {
        SlotService slotService = server.getSlotService();
        long jobId = System.currentTimeMillis();
        SlotProfile slot = slotService.requestSlot(jobId, new ResourceProfile()).getSlotProfile();
        Assertions.assertNotNull(slot);
        try {
            slotService.reset();

            WorkerProfile workerProfile = slotService.getWorkerProfile();
            Assertions.assertEquals(1, workerProfile.getAssignedSlots().length);
            SlotProfile assigned = workerProfile.getAssignedSlots()[0];
            Assertions.assertEquals(slot.getSlotID(), assigned.getSlotID());
            Assertions.assertEquals(slot.getSequence(), assigned.getSequence());
            Assertions.assertEquals(totalSlots - 1, workerProfile.getUnassignedSlots().length);
            Assertions.assertNotNull(slotService.getSlotContext(slot));
        } finally {
            slotService.releaseSlot(jobId, slot);
        }
    }

    @Test
    public void testResetResourceOperationKeepsLiveSlotAssignment() {
        SlotService slotService = server.getSlotService();
        long jobId = System.currentTimeMillis();
        SlotProfile slot = slotService.requestSlot(jobId, new ResourceProfile()).getSlotProfile();
        Assertions.assertNotNull(slot);
        try {
            sendResetResourceOperation();

            WorkerProfile workerProfile = slotService.getWorkerProfile();
            Assertions.assertEquals(1, workerProfile.getAssignedSlots().length);
            SlotProfile assigned = workerProfile.getAssignedSlots()[0];
            Assertions.assertEquals(slot.getSlotID(), assigned.getSlotID());
            Assertions.assertEquals(slot.getSequence(), assigned.getSequence());
            Assertions.assertEquals(totalSlots - 1, workerProfile.getUnassignedSlots().length);
            Assertions.assertNotNull(slotService.getSlotContext(slot));
        } finally {
            slotService.releaseSlot(jobId, slot);
        }
    }

    @Test
    public void testResetResourceOperationOnEmptyWorkerCanAllocate() {
        SlotService slotService = server.getSlotService();
        // Other tests release their slots asynchronously.
        await().atMost(20000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        0,
                                        slotService.getWorkerProfile().getAssignedSlots().length));
        Set<String> sequencesBeforeReset =
                slotSequences(slotService.getWorkerProfile().getUnassignedSlots());

        sendResetResourceOperation();

        WorkerProfile workerProfile = slotService.getWorkerProfile();
        Assertions.assertEquals(0, workerProfile.getAssignedSlots().length);
        Assertions.assertEquals(totalSlots, workerProfile.getUnassignedSlots().length);
        Set<String> sequencesAfterReset = slotSequences(workerProfile.getUnassignedSlots());
        sequencesAfterReset.retainAll(sequencesBeforeReset);
        Assertions.assertTrue(
                sequencesAfterReset.isEmpty(), "an idle worker should be rebuilt on reset");

        long jobId = System.currentTimeMillis();
        SlotProfile slot = slotService.requestSlot(jobId, new ResourceProfile()).getSlotProfile();
        Assertions.assertNotNull(slot);
        Assertions.assertNotNull(slotService.getSlotContext(slot));
        slotService.releaseSlot(jobId, slot);
    }

    private void sendResetResourceOperation() {
        NodeEngineUtil.sendOperationToMemberNode(
                        nodeEngine, new ResetResourceOperation(), nodeEngine.getThisAddress())
                .join();
    }

    private static Set<String> slotSequences(SlotProfile[] slots) {
        return Arrays.stream(slots).map(SlotProfile::getSequence).collect(Collectors.toSet());
    }
}
