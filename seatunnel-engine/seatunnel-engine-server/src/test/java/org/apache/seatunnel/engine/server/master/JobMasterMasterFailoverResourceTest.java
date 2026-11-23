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
import org.apache.seatunnel.engine.server.dag.physical.ResourceUtils;
import org.apache.seatunnel.engine.server.dag.physical.SubPlan;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.resourcemanager.NoEnoughResourceException;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManager;
import org.apache.seatunnel.engine.server.resourcemanager.resource.ResourceProfile;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.internal.serialization.Data;
import com.hazelcast.map.IMap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

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

    /**
     * Two pipelines of one job restore at the same time, the way SubPlan handles FAILED: release
     * the pipeline's slots, pre-apply again and merge into the job's shared map, then consume the
     * merged futures when the pipeline is scheduled. Both pipelines must get every task group back.
     */
    @Test
    void testConcurrentPipelineRestoresKeepEachPipelinesSlots() throws Exception {
        long jobId = instance.getFlakeIdGenerator("concurrent-pipeline-restore").newId();
        ResourceManager resourceManager = server.getCoordinatorService().getResourceManager();
        JobMaster jobMaster =
                newJobMaster(
                        jobId,
                        createJobImmutableInformationData(
                                jobId, "stream_two_pipelines_savepoint_partial_failure.conf"));
        jobMaster.init(System.currentTimeMillis(), false);
        List<SubPlan> pipelines = jobMaster.getPhysicalPlan().getPipelineList();
        Assertions.assertEquals(2, pipelines.size());
        Assertions.assertTrue(jobMaster.preApplyResources());
        persistPreAppliedSlots(jobMaster);

        ExecutorService executor = Executors.newFixedThreadPool(pipelines.size());
        CountDownLatch start = new CountDownLatch(1);
        CyclicBarrier bothReleased = new CyclicBarrier(pipelines.size());
        try {
            Map<SubPlan, Future<Map<TaskGroupLocation, SlotProfile>>> restores = new HashMap<>();
            for (SubPlan subPlan : pipelines) {
                restores.put(
                        subPlan,
                        executor.submit(
                                () -> {
                                    start.await();
                                    jobMaster.releasePipelineResource(subPlan);
                                    // Both pipelines pre-apply and merge at the same time.
                                    bothReleased.await(30, TimeUnit.SECONDS);
                                    Assertions.assertTrue(jobMaster.preApplyResources(subPlan));
                                    return ResourceUtils.applyResourceForPipeline(
                                            jobMaster, subPlan);
                                }));
            }
            start.countDown();

            Set<SlotProfile> allSlots = new HashSet<>();
            for (Map.Entry<SubPlan, Future<Map<TaskGroupLocation, SlotProfile>>> restore :
                    restores.entrySet()) {
                Map<TaskGroupLocation, SlotProfile> slots =
                        restore.getValue().get(60, TimeUnit.SECONDS);
                Assertions.assertEquals(taskGroupLocations(restore.getKey()), slots.keySet());
                for (SlotProfile slot : slots.values()) {
                    Assertions.assertTrue(resourceManager.slotActiveCheck(slot));
                    Assertions.assertTrue(allSlots.add(slot), "slot shared by two pipelines");
                }
            }
            Assertions.assertInstanceOf(
                    ConcurrentHashMap.class,
                    jobMaster.getPhysicalPlan().getPreApplyResourceFutures());
        } finally {
            executor.shutdownNow();
            releaseAllSlots(resourceManager, jobMaster, jobId);
        }
    }

    private Data createJobImmutableInformationData(long jobId) {
        return createJobImmutableInformationData(jobId, "stream_fake_to_inmemory_with_sleep.conf");
    }

    private Data createJobImmutableInformationData(long jobId, String confFile) {
        LogicalDag logicalDag =
                TestUtils.createTestLogicalPlan(confFile, "master_failover_restore", jobId);
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

    private Set<TaskGroupLocation> taskGroupLocations(SubPlan subPlan) {
        Set<TaskGroupLocation> locations = new HashSet<>();
        subPlan.getCoordinatorVertexList().forEach(v -> locations.add(v.getTaskGroupLocation()));
        subPlan.getPhysicalVertexList().forEach(v -> locations.add(v.getTaskGroupLocation()));
        return locations;
    }

    /** Releases persisted slots plus any pre-applied slot a failed restore left unconsumed. */
    private void releaseAllSlots(ResourceManager resourceManager, JobMaster jobMaster, long jobId) {
        IMap<PipelineLocation, Map<TaskGroupLocation, SlotProfile>> ownedSlotProfilesIMap =
                nodeEngine.getHazelcastInstance().getMap("ownedSlotProfilesIMap");
        Set<SlotProfile> slots = new HashSet<>();
        for (SubPlan subPlan : jobMaster.getPhysicalPlan().getPipelineList()) {
            Map<TaskGroupLocation, SlotProfile> owned =
                    ownedSlotProfilesIMap.remove(subPlan.getPipelineLocation());
            if (owned != null) {
                slots.addAll(owned.values());
            }
        }
        for (CompletableFuture<SlotProfile> future :
                jobMaster.getPhysicalPlan().getPreApplyResourceFutures().values()) {
            if (future != null && future.isDone() && !future.isCompletedExceptionally()) {
                slots.add(future.join());
            }
        }
        slots.removeIf(slot -> slot == null || !resourceManager.slotActiveCheck(slot));
        if (!slots.isEmpty()) {
            resourceManager.releaseResources(jobId, new ArrayList<>(slots)).join();
        }
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
