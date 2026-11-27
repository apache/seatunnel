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

import com.hazelcast.core.HazelcastException;
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
    void testPreApplyPersistsSlotOwnershipBeforePipelineDeployment() throws Exception {
        long jobId = instance.getFlakeIdGenerator("master-failover-pre-apply-persistence").newId();
        ResourceManager resourceManager = server.getCoordinatorService().getResourceManager();
        Data jobImmutableInformationData = createJobImmutableInformationData(jobId);
        JobMaster jobMaster = newJobMaster(jobId, jobImmutableInformationData);
        jobMaster.init(System.currentTimeMillis(), false);
        try {
            Assertions.assertTrue(jobMaster.preApplyResources());

            // No pipeline has been started, so this mapping can only come from the pre-apply.
            IMap<PipelineLocation, Map<TaskGroupLocation, SlotProfile>> ownedSlotProfilesIMap =
                    nodeEngine.getHazelcastInstance().getMap("ownedSlotProfilesIMap");
            for (SubPlan subPlan : jobMaster.getPhysicalPlan().getPipelineList()) {
                Map<TaskGroupLocation, SlotProfile> persisted =
                        ownedSlotProfilesIMap.get(subPlan.getPipelineLocation());
                Assertions.assertNotNull(
                        persisted,
                        "pre-applied slots must be persisted before the pipeline is scheduled");
                Assertions.assertEquals(
                        subPlan.getCoordinatorVertexList().size()
                                + subPlan.getPhysicalVertexList().size(),
                        persisted.size());
            }
        } finally {
            releasePersistedSlots(resourceManager, jobMaster, jobId);
        }
    }

    /**
     * A write that returns without an exception is not enough: when the written ownership does not
     * read back unchanged, the pre-apply must reject the job instead of admitting it with
     * unconfirmed ownership.
     */
    @Test
    @SuppressWarnings("unchecked")
    void testPreApplyStopsAdmissionWhenSlotOwnershipReadBackIsUnconfirmed() throws Exception {
        long jobId = instance.getFlakeIdGenerator("master-failover-ownership-read-back").newId();
        ResourceManager resourceManager = server.getCoordinatorService().getResourceManager();
        Data jobImmutableInformationData = createJobImmutableInformationData(jobId);
        IMap<PipelineLocation, Map<TaskGroupLocation, SlotProfile>> unconfirmedOwnershipMap =
                Mockito.mock(IMap.class);
        // The put returns normally, but the value never reads back, for example because another
        // member served the read before the write became visible there.
        Mockito.when(unconfirmedOwnershipMap.get(Mockito.any())).thenReturn(null);

        JobMaster jobMaster =
                newJobMaster(jobId, jobImmutableInformationData, unconfirmedOwnershipMap);
        jobMaster.init(System.currentTimeMillis(), false);

        Assertions.assertFalse(
                jobMaster.preApplyResources(),
                "a pre-apply whose written ownership does not read back must not admit the job");
        Mockito.verify(unconfirmedOwnershipMap, Mockito.atLeastOnce())
                .put(Mockito.any(), Mockito.any());
        // The confirmation must retry the read-back before the ownership is treated as unknown.
        Mockito.verify(unconfirmedOwnershipMap, Mockito.atLeast(2)).get(Mockito.any());
        // The possibly committed record is cleared before the slots are released.
        Mockito.verify(unconfirmedOwnershipMap, Mockito.atLeastOnce()).remove(Mockito.any());

        List<SlotProfile> probeSlots = new ArrayList<>();
        try {
            for (int index = 0; index < SLOT_NUM; index++) {
                probeSlots.add(
                        resourceManager
                                .applyResource(jobId + 1, new ResourceProfile(), null)
                                .get());
            }
        } finally {
            resourceManager.releaseResources(jobId + 1, probeSlots).join();
        }
    }

    /**
     * A failed write of a restored pre-apply must not erase ownership that already existed before
     * the round. The retained mapping is the only way a successor master can reuse or release the
     * slots a previous master allocated, so compensation has to keep it.
     */
    @Test
    @SuppressWarnings("unchecked")
    void testFailedPreApplyKeepsRetainedSlotOwnership() throws Exception {
        long jobId = instance.getFlakeIdGenerator("master-failover-retained-ownership").newId();
        ResourceManager resourceManager = server.getCoordinatorService().getResourceManager();
        Data jobImmutableInformationData = createJobImmutableInformationData(jobId);
        JobMaster original = newJobMaster(jobId, jobImmutableInformationData);
        original.init(System.currentTimeMillis(), false);
        Assertions.assertTrue(original.preApplyResources());
        persistPreAppliedSlots(original);

        IMap<PipelineLocation, Map<TaskGroupLocation, SlotProfile>> realOwnershipMap =
                nodeEngine.getHazelcastInstance().getMap("ownedSlotProfilesIMap");
        Map<PipelineLocation, Map<TaskGroupLocation, SlotProfile>> retainedOwnership =
                readPersistedSlots(realOwnershipMap, original);
        Assertions.assertFalse(retainedOwnership.isEmpty());

        List<SlotProfile> blockerSlots = occupyRemainingSlots(resourceManager, jobId + 1);

        IMap<PipelineLocation, Map<TaskGroupLocation, SlotProfile>> rejectingOwnershipMap =
                Mockito.mock(IMap.class);
        Mockito.when(rejectingOwnershipMap.get(Mockito.any()))
                .thenAnswer(
                        invocation ->
                                realOwnershipMap.get((PipelineLocation) invocation.getArgument(0)));
        Mockito.when(rejectingOwnershipMap.remove(Mockito.any()))
                .thenAnswer(
                        invocation ->
                                realOwnershipMap.remove(
                                        (PipelineLocation) invocation.getArgument(0)));
        Mockito.doThrow(new HazelcastException("owned slot profiles IMap is unavailable"))
                .when(rejectingOwnershipMap)
                .put(Mockito.any(), Mockito.any());

        JobMaster restored =
                newJobMaster(jobId, jobImmutableInformationData, rejectingOwnershipMap);
        restored.init(System.currentTimeMillis(), true);
        try {
            Assertions.assertFalse(
                    restored.preApplyResources(),
                    "a restored pre-apply whose ownership write fails must not admit the job");

            retainedOwnership.forEach(
                    (pipelineLocation, slotProfiles) -> {
                        Assertions.assertEquals(
                                slotProfiles,
                                realOwnershipMap.get(pipelineLocation),
                                "the failed round must not remove ownership it reused from the previous master");
                        slotProfiles
                                .values()
                                .forEach(
                                        slotProfile ->
                                                Assertions.assertTrue(
                                                        resourceManager.slotActiveCheck(
                                                                slotProfile),
                                                        "a retained slot must stay allocated after a failed round"));
                    });
        } finally {
            resourceManager.releaseResources(jobId + 1, blockerSlots).join();
            releasePersistedSlots(resourceManager, original, jobId);
        }
    }

    /**
     * When one pipeline of a round is persisted and another one fails, the confirmed record belongs
     * to ownership that a successor master may already have adopted. The failed round must keep
     * that record and its slots instead of removing and releasing them, while the pipeline without
     * any persisted ownership still gets its slots back.
     */
    @Test
    @SuppressWarnings("unchecked")
    void testPartiallyFailedPreApplyKeepsConfirmedOwnership() throws Exception {
        long jobId = instance.getFlakeIdGenerator("master-failover-partial-persist").newId();
        ResourceManager resourceManager = server.getCoordinatorService().getResourceManager();
        Data jobImmutableInformationData =
                createJobImmutableInformationData(
                        jobId, "stream_two_pipelines_savepoint_partial_failure.conf");
        JobMaster planningJobMaster = newJobMaster(jobId, jobImmutableInformationData);
        planningJobMaster.init(System.currentTimeMillis(), false);
        List<SubPlan> pipelines = planningJobMaster.getPhysicalPlan().getPipelineList();
        Assertions.assertEquals(2, pipelines.size());
        PipelineLocation confirmedPipeline = pipelines.get(0).getPipelineLocation();
        PipelineLocation failedPipeline = pipelines.get(1).getPipelineLocation();

        IMap<PipelineLocation, Map<TaskGroupLocation, SlotProfile>> realOwnershipMap =
                nodeEngine.getHazelcastInstance().getMap("ownedSlotProfilesIMap");
        IMap<PipelineLocation, Map<TaskGroupLocation, SlotProfile>> partiallyAvailableOwnershipMap =
                Mockito.mock(IMap.class);
        Mockito.when(partiallyAvailableOwnershipMap.get(Mockito.any()))
                .thenAnswer(
                        invocation ->
                                realOwnershipMap.get((PipelineLocation) invocation.getArgument(0)));
        Mockito.when(partiallyAvailableOwnershipMap.remove(Mockito.any()))
                .thenAnswer(
                        invocation ->
                                realOwnershipMap.remove(
                                        (PipelineLocation) invocation.getArgument(0)));
        Mockito.doAnswer(
                        invocation -> {
                            PipelineLocation pipelineLocation = invocation.getArgument(0);
                            if (confirmedPipeline.equals(pipelineLocation)) {
                                return realOwnershipMap.put(
                                        pipelineLocation, invocation.getArgument(1));
                            }
                            throw new HazelcastException("owned slot profiles IMap is unavailable");
                        })
                .when(partiallyAvailableOwnershipMap)
                .put(Mockito.any(), Mockito.any());

        JobMaster jobMaster =
                newJobMaster(jobId, jobImmutableInformationData, partiallyAvailableOwnershipMap);
        jobMaster.init(System.currentTimeMillis(), false);
        try {
            Assertions.assertFalse(jobMaster.preApplyResources());

            Map<TaskGroupLocation, SlotProfile> confirmedOwnership =
                    realOwnershipMap.get(confirmedPipeline);
            Assertions.assertNotNull(
                    confirmedOwnership,
                    "a confirmed ownership record must not be removed by a failed round");
            Assertions.assertFalse(confirmedOwnership.isEmpty());
            confirmedOwnership
                    .values()
                    .forEach(
                            slotProfile ->
                                    Assertions.assertTrue(
                                            resourceManager.slotActiveCheck(slotProfile),
                                            "a slot whose ownership was confirmed may already be adopted and must not be released"));
            Assertions.assertNull(
                    realOwnershipMap.get(failedPipeline),
                    "the unpersisted pipeline ownership must be reverted");

            List<SlotProfile> probeSlots = new ArrayList<>();
            try {
                for (int index = 0; index < SLOT_NUM - confirmedOwnership.size(); index++) {
                    probeSlots.add(
                            resourceManager
                                    .applyResource(jobId + 1, new ResourceProfile(), null)
                                    .get());
                }
            } finally {
                resourceManager.releaseResources(jobId + 1, probeSlots).join();
            }
        } finally {
            Map<TaskGroupLocation, SlotProfile> confirmedOwnership =
                    realOwnershipMap.remove(confirmedPipeline);
            if (confirmedOwnership != null) {
                resourceManager
                        .releaseResources(jobId, new ArrayList<>(confirmedOwnership.values()))
                        .join();
            }
            realOwnershipMap.remove(failedPipeline);
        }
    }

    /**
     * A pre-apply whose slot ownership cannot be persisted must not admit the job. The slots are
     * already allocated on the workers, so admitting it without the ownership record would leave a
     * successor master unable to reuse or release them after an active-master switch.
     */
    @Test
    @SuppressWarnings("unchecked")
    void testPreApplyStopsAdmissionWhenSlotOwnershipCannotBePersisted() throws Exception {
        long jobId = instance.getFlakeIdGenerator("master-failover-ownership-write").newId();
        ResourceManager resourceManager = server.getCoordinatorService().getResourceManager();
        Data jobImmutableInformationData = createJobImmutableInformationData(jobId);
        IMap<PipelineLocation, Map<TaskGroupLocation, SlotProfile>> unavailableOwnershipMap =
                Mockito.mock(IMap.class);
        Mockito.doThrow(new HazelcastException("owned slot profiles IMap is unavailable"))
                .when(unavailableOwnershipMap)
                .put(Mockito.any(), Mockito.any());

        JobMaster jobMaster =
                newJobMaster(jobId, jobImmutableInformationData, unavailableOwnershipMap);
        jobMaster.init(System.currentTimeMillis(), false);

        Assertions.assertFalse(
                jobMaster.preApplyResources(),
                "a pre-apply that cannot persist its slot ownership must not admit the job");
        Mockito.verify(unavailableOwnershipMap, Mockito.atLeastOnce())
                .put(Mockito.any(), Mockito.any());
        // The possibly committed record is cleared before the slots are released.
        Mockito.verify(unavailableOwnershipMap, Mockito.atLeastOnce()).remove(Mockito.any());

        // Every pre-applied fixed slot must be free again, otherwise the rejected admission leaked
        // capacity that this master can no longer see.
        List<SlotProfile> probeSlots = new ArrayList<>();
        try {
            for (int index = 0; index < SLOT_NUM; index++) {
                probeSlots.add(
                        resourceManager
                                .applyResource(jobId + 1, new ResourceProfile(), null)
                                .get());
            }
        } finally {
            resourceManager.releaseResources(jobId + 1, probeSlots).join();
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
        return createJobImmutableInformationData(jobId, "stream_fake_to_inmemory_with_sleep.conf");
    }

    private Data createJobImmutableInformationData(long jobId, String jobConfigFile) {
        LogicalDag logicalDag =
                TestUtils.createTestLogicalPlan(jobConfigFile, "master_failover_restore", jobId);
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

        return newJobMaster(jobId, jobImmutableInformationData, ownedSlotProfilesIMap);
    }

    private JobMaster newJobMaster(
            long jobId,
            Data jobImmutableInformationData,
            IMap<PipelineLocation, Map<TaskGroupLocation, SlotProfile>> ownedSlotProfilesIMap) {
        IMap<Object, Object> runningJobStateIMap =
                nodeEngine.getHazelcastInstance().getMap("runningJobState");
        IMap<Object, Long[]> runningJobStateTimestampsIMap =
                nodeEngine.getHazelcastInstance().getMap("stateTimestamps");

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

    /** Returns the ownership currently persisted for every pipeline of the given plan. */
    private Map<PipelineLocation, Map<TaskGroupLocation, SlotProfile>> readPersistedSlots(
            IMap<PipelineLocation, Map<TaskGroupLocation, SlotProfile>> ownedSlotProfilesIMap,
            JobMaster jobMaster) {
        Map<PipelineLocation, Map<TaskGroupLocation, SlotProfile>> persistedSlots = new HashMap<>();
        for (SubPlan subPlan : jobMaster.getPhysicalPlan().getPipelineList()) {
            Map<TaskGroupLocation, SlotProfile> slotProfiles =
                    ownedSlotProfilesIMap.get(subPlan.getPipelineLocation());
            if (slotProfiles != null) {
                persistedSlots.put(subPlan.getPipelineLocation(), slotProfiles);
            }
        }
        return persistedSlots;
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
        for (int index = 0; index < SLOT_NUM; index++) {
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
