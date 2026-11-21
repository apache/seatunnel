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

package org.apache.seatunnel.engine.server.master;

import org.apache.seatunnel.common.utils.ReflectionUtils;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.config.server.SlotServiceConfig;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.server.AbstractSeaTunnelServerTest;
import org.apache.seatunnel.engine.server.TestUtils;
import org.apache.seatunnel.engine.server.dag.physical.PhysicalVertex;
import org.apache.seatunnel.engine.server.dag.physical.PipelineLocation;
import org.apache.seatunnel.engine.server.dag.physical.SubPlan;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.resourcemanager.NoEnoughResourceException;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManager;
import org.apache.seatunnel.engine.server.resourcemanager.resource.ResourceProfile;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.map.IMap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ForkJoinPool;

/** Verifies that a restored master keeps using active fixed slots owned by its job. */
public class JobMasterMasterFailoverResourceTest
        extends AbstractSeaTunnelServerTest<JobMasterMasterFailoverResourceTest> {

    private static final int SLOT_NUM = 5;

    @Override
    public SeaTunnelConfig loadSeaTunnelConfig() {
        SeaTunnelConfig seaTunnelConfig = super.loadSeaTunnelConfig();
        SlotServiceConfig slotServiceConfig =
                seaTunnelConfig.getEngineConfig().getSlotServiceConfig();
        slotServiceConfig.setDynamicSlot(false);
        slotServiceConfig.setSlotNum(SLOT_NUM);
        return seaTunnelConfig;
    }

    @Test
    void testReleasePipelineResourceRemovesPendingFuturesAndReleasesSlots() throws Exception {
        long jobId = instance.getFlakeIdGenerator("master-release-pipeline").newId();
        ResourceManager resourceManager = server.getCoordinatorService().getResourceManager();
        Data jobImmutableInformationData = createJobImmutableInformationData(jobId);
        JobMaster jobMaster = newJobMaster(jobId, jobImmutableInformationData);
        jobMaster.init(System.currentTimeMillis(), false);
        SubPlan subPlan = jobMaster.getPhysicalPlan().getPipelineList().get(0);
        Assertions.assertTrue(jobMaster.preApplyResources(subPlan));

        // Reservations have not been assigned to the pipeline ownership map.
        Map<TaskGroupLocation, CompletableFuture<SlotProfile>> preApplyFutures =
                jobMaster.getPhysicalPlan().getPreApplyResourceFutures();
        List<SlotProfile> blockerSlots = Collections.emptyList();

        try {
            // Act: release pipeline resources without persisting pre-applied slots.
            jobMaster.releasePipelineResource(subPlan);

            // Assert: all task group futures for this subPlan are removed.
            for (PhysicalVertex vertex : subPlan.getCoordinatorVertexList()) {
                Assertions.assertFalse(
                        preApplyFutures.containsKey(vertex.getTaskGroupLocation()),
                        "Coordinator future should be removed from pending map");
            }
            for (PhysicalVertex vertex : subPlan.getPhysicalVertexList()) {
                Assertions.assertFalse(
                        preApplyFutures.containsKey(vertex.getTaskGroupLocation()),
                        "Task future should be removed from pending map");
            }

            // Assert: all SLOT_NUM slots are truly available by occupying them.
            blockerSlots = occupyRemainingSlots(resourceManager, jobId + 1);
            Assertions.assertEquals(
                    SLOT_NUM,
                    blockerSlots.size(),
                    "All fixed slots must be released and available");
        } finally {
            if (!blockerSlots.isEmpty()) {
                resourceManager.releaseResources(jobId + 1, blockerSlots).join();
            }
            // Baseline cleanup: if pending map still contains profiles, release them explicitly.
            List<SlotProfile> pendingProfiles = new ArrayList<>();
            for (PhysicalVertex vertex : subPlan.getCoordinatorVertexList()) {
                if (preApplyFutures.containsKey(vertex.getTaskGroupLocation())) {
                    pendingProfiles.add(preApplyFutures.get(vertex.getTaskGroupLocation()).join());
                    preApplyFutures.remove(vertex.getTaskGroupLocation());
                }
            }
            for (PhysicalVertex vertex : subPlan.getPhysicalVertexList()) {
                if (preApplyFutures.containsKey(vertex.getTaskGroupLocation())) {
                    pendingProfiles.add(preApplyFutures.get(vertex.getTaskGroupLocation()).join());
                    preApplyFutures.remove(vertex.getTaskGroupLocation());
                }
            }
            if (!pendingProfiles.isEmpty()) {
                resourceManager.releaseResources(jobId, pendingProfiles).join();
            }
        }
    }

    @Test
    void testReleasePipelineResourcePreservesCompletedFutureTrackingWhenReleaseFails()
            throws Exception {
        long jobId = instance.getFlakeIdGenerator("master-release-failure").newId();
        ResourceManager actual = server.getCoordinatorService().getResourceManager();
        Data jobImmutableInformationData = createJobImmutableInformationData(jobId);
        JobMaster master = newJobMaster(jobId, jobImmutableInformationData);
        master.init(System.currentTimeMillis(), false);
        SubPlan subPlan = master.getPhysicalPlan().getPipelineList().get(0);
        Assertions.assertTrue(master.preApplyResources(subPlan));

        Map<TaskGroupLocation, CompletableFuture<SlotProfile>> map =
                master.getPhysicalPlan().getPreApplyResourceFutures();
        Map<TaskGroupLocation, CompletableFuture<SlotProfile>> snapshot = new HashMap<>(map);

        ResourceManager failing = Mockito.mock(ResourceManager.class);
        CompletableFuture<Void> failed = new CompletableFuture<>();
        failed.completeExceptionally(new IllegalArgumentException("controlled release failure"));
        Mockito.when(failing.releaseResources(Mockito.eq(jobId), Mockito.anyList()))
                .thenReturn(failed);
        ReflectionUtils.setField(master, "resourceManager", failing);

        try {
            master.releasePipelineResource(subPlan);
            Assertions.assertEquals(snapshot, map);
        } finally {
            ReflectionUtils.setField(master, "resourceManager", actual);
            master.releasePipelineResource(subPlan);
            Assertions.assertTrue(map.isEmpty());
        }
    }

    @Test
    void testRestoreReusesActiveSlotsWhenNoFreeFixedSlotsRemain() throws Exception {
        long jobId = instance.getFlakeIdGenerator("master-failover-resource").newId();
        ResourceManager resourceManager = server.getCoordinatorService().getResourceManager();
        Data jobImmutableInformationData = createJobImmutableInformationData(jobId);
        JobMaster original = newJobMaster(jobId, jobImmutableInformationData);
        original.init(System.currentTimeMillis(), false);
        Assertions.assertTrue(original.preApplyResources());
        persistPreAppliedSlots(original);

        List<SlotProfile> blockerSlots = occupyRemainingSlots(resourceManager, jobId + 1);

        JobMaster restored = newJobMaster(jobId, jobImmutableInformationData);
        restored.init(System.currentTimeMillis(), true);

        try {
            Assertions.assertTrue(restored.preApplyResources());
        } finally {
            resourceManager.releaseResources(jobId + 1, blockerSlots).join();
            releasePersistedSlots(resourceManager, original, jobId);
        }
    }

    @Test
    void testSubPlanRestoreClearsFailoverSlotReuseFlag() throws Exception {
        long jobId = instance.getFlakeIdGenerator("master-failover-sub-plan").newId();
        ResourceManager resourceManager = server.getCoordinatorService().getResourceManager();
        Data jobImmutableInformationData = createJobImmutableInformationData(jobId);
        JobMaster original = newJobMaster(jobId, jobImmutableInformationData);
        original.init(System.currentTimeMillis(), false);
        Assertions.assertTrue(original.preApplyResources());
        persistPreAppliedSlots(original);

        List<SlotProfile> blockerSlots = occupyRemainingSlots(resourceManager, jobId + 1);
        JobMaster restored = newJobMaster(jobId, jobImmutableInformationData);
        restored.init(System.currentTimeMillis(), true);
        SubPlan subPlan = restored.getPhysicalPlan().getPipelineList().get(0);

        try {
            Assertions.assertTrue(restored.preApplyResources(subPlan));
            Assertions.assertFalse(
                    ReflectionUtils.getField(restored, "masterFailoverRestore")
                            .map(Boolean.class::cast)
                            .orElseThrow(IllegalStateException::new));
        } finally {
            resourceManager.releaseResources(jobId + 1, blockerSlots).join();
            releasePersistedSlots(resourceManager, original, jobId);
        }
    }

    /**
     * Verifies that a slot reassigned to another task group of the same job cannot satisfy a stale
     * persisted mapping after master failover.
     */
    @Test
    void testRestoreRejectsSlotReassignedWithinSameJob() throws Exception {
        long jobId = instance.getFlakeIdGenerator("master-failover-same-job-reassignment").newId();
        ResourceManager resourceManager = server.getCoordinatorService().getResourceManager();
        Data jobImmutableInformationData = createJobImmutableInformationData(jobId);
        JobMaster original = newJobMaster(jobId, jobImmutableInformationData);
        original.init(System.currentTimeMillis(), false);
        Assertions.assertTrue(original.preApplyResources());
        persistPreAppliedSlots(original);

        SlotProfile staleSlot = getFirstPersistedSlot(original);
        List<SlotProfile> blockerSlots = occupyRemainingSlots(resourceManager, jobId + 1);
        resourceManager.releaseResource(jobId, staleSlot).join();
        SlotProfile reassignedSlot =
                resourceManager.applyResource(jobId, new ResourceProfile(), null).get();
        Assertions.assertEquals(staleSlot.getWorker(), reassignedSlot.getWorker());
        Assertions.assertEquals(staleSlot.getSlotID(), reassignedSlot.getSlotID());
        Assertions.assertNotEquals(staleSlot.getSequence(), reassignedSlot.getSequence());

        JobMaster restored = newJobMaster(jobId, jobImmutableInformationData);
        restored.init(System.currentTimeMillis(), true);

        try {
            Assertions.assertFalse(restored.preApplyResources());
            Assertions.assertFalse(resourceManager.slotActiveCheck(staleSlot));
            Assertions.assertTrue(resourceManager.slotActiveCheck(reassignedSlot));
            resourceManager.releaseResource(jobId, staleSlot).join();
            Assertions.assertTrue(resourceManager.slotActiveCheck(reassignedSlot));
        } finally {
            resourceManager.releaseResources(jobId + 1, blockerSlots).join();
            resourceManager.releaseResource(jobId, reassignedSlot).join();
            releasePersistedSlotsExcept(resourceManager, original, jobId, staleSlot);
        }
    }

    private Data createJobImmutableInformationData(long jobId) {
        LogicalDag logicalDag =
                TestUtils.createTestLogicalPlan(
                        "stream_fake_to_inmemory_with_sleep.conf",
                        "master_failover_restore",
                        jobId);
        JobImmutableInformation jobImmutableInformation =
                new JobImmutableInformation(
                        jobId,
                        "Test",
                        nodeEngine.getSerializationService(),
                        logicalDag,
                        Collections.emptyList(),
                        Collections.emptyList());
        return nodeEngine.getSerializationService().toData(jobImmutableInformation);
    }

    private JobMaster newJobMaster(long jobId, Data jobImmutableInformationData) {
        IMap<Object, Object> runningJobStateIMap =
                nodeEngine.getHazelcastInstance().getMap("runningJobState");
        IMap<Object, Long[]> runningJobStateTimestampsIMap =
                nodeEngine.getHazelcastInstance().getMap("stateTimestamps");
        IMap<PipelineLocation, Map<TaskGroupLocation, SlotProfile>> ownedSlotProfilesIMap =
                nodeEngine.getHazelcastInstance().getMap("ownedSlotProfilesIMap");

        return new JobMaster(
                jobId,
                jobImmutableInformationData,
                nodeEngine,
                ForkJoinPool.commonPool(),
                server.getCoordinatorService().getResourceManager(),
                server.getCoordinatorService().getJobHistoryService(),
                runningJobStateIMap,
                runningJobStateTimestampsIMap,
                ownedSlotProfilesIMap,
                nodeEngine.getHazelcastInstance().getMap("runningJobInfo"),
                loadSeaTunnelConfig().getEngineConfig(),
                server);
    }

    private void persistPreAppliedSlots(JobMaster jobMaster) {
        for (SubPlan subPlan : jobMaster.getPhysicalPlan().getPipelineList()) {
            Map<TaskGroupLocation, SlotProfile> slotProfiles = new HashMap<>();
            for (PhysicalVertex coordinator : subPlan.getCoordinatorVertexList()) {
                addPreAppliedSlot(jobMaster, slotProfiles, coordinator);
            }
            for (PhysicalVertex task : subPlan.getPhysicalVertexList()) {
                addPreAppliedSlot(jobMaster, slotProfiles, task);
            }
            jobMaster.setOwnedSlotProfiles(subPlan.getPipelineLocation(), slotProfiles);
        }
    }

    private void addPreAppliedSlot(
            JobMaster jobMaster,
            Map<TaskGroupLocation, SlotProfile> slotProfiles,
            PhysicalVertex vertex) {
        TaskGroupLocation location = vertex.getTaskGroupLocation();
        slotProfiles.put(
                location,
                jobMaster.getPhysicalPlan().getPreApplyResourceFutures().get(location).join());
    }

    /** Returns one persisted assignment that can be released and reassigned during the test. */
    private SlotProfile getFirstPersistedSlot(JobMaster jobMaster) {
        IMap<PipelineLocation, Map<TaskGroupLocation, SlotProfile>> ownedSlotProfilesIMap =
                nodeEngine.getHazelcastInstance().getMap("ownedSlotProfilesIMap");
        for (SubPlan subPlan : jobMaster.getPhysicalPlan().getPipelineList()) {
            Map<TaskGroupLocation, SlotProfile> slotProfiles =
                    ownedSlotProfilesIMap.get(subPlan.getPipelineLocation());
            if (slotProfiles != null && !slotProfiles.isEmpty()) {
                return slotProfiles.values().iterator().next();
            }
        }
        throw new IllegalStateException("No pre-applied slot was persisted for the test job");
    }

    /**
     * Occupies every actual remaining fixed slot and verifies exhaustion through the worker
     * allocation result. The resource manager cache can briefly retain stale free-slot entries
     * after asynchronous allocations, so it is not a reliable exhaustion assertion here.
     */
    private List<SlotProfile> occupyRemainingSlots(ResourceManager resourceManager, long jobId)
            throws Exception {
        List<SlotProfile> blockerSlots = new ArrayList<>();
        for (int index = 0; index <= SLOT_NUM; index++) {
            try {
                blockerSlots.add(
                        resourceManager.applyResource(jobId, new ResourceProfile(), null).get());
            } catch (ExecutionException exception) {
                if (exception.getCause() instanceof NoEnoughResourceException) {
                    return blockerSlots;
                }
                throw exception;
            }
        }
        throw new AssertionError("Expected fixed-slot allocation to be exhausted");
    }

    private void releasePersistedSlots(
            ResourceManager resourceManager, JobMaster jobMaster, long jobId) {
        releasePersistedSlotsExcept(resourceManager, jobMaster, jobId, null);
    }

    /** Releases persisted test slots except a profile whose stale release must remain rejected. */
    private void releasePersistedSlotsExcept(
            ResourceManager resourceManager,
            JobMaster jobMaster,
            long jobId,
            SlotProfile excludedSlot) {
        IMap<PipelineLocation, Map<TaskGroupLocation, SlotProfile>> ownedSlotProfilesIMap =
                nodeEngine.getHazelcastInstance().getMap("ownedSlotProfilesIMap");
        List<SlotProfile> persistedSlots = new ArrayList<>();
        for (SubPlan subPlan : jobMaster.getPhysicalPlan().getPipelineList()) {
            Map<TaskGroupLocation, SlotProfile> slotProfiles =
                    ownedSlotProfilesIMap.remove(subPlan.getPipelineLocation());
            if (slotProfiles != null) {
                slotProfiles.values().stream()
                        .filter(slotProfile -> !slotProfile.equals(excludedSlot))
                        .forEach(persistedSlots::add);
            }
        }
        if (!persistedSlots.isEmpty()) {
            resourceManager.releaseResources(jobId, persistedSlots).join();
        }
    }
}
