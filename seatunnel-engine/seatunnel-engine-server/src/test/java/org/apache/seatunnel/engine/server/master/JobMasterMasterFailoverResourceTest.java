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
import org.apache.seatunnel.engine.common.job.JobResult;
import org.apache.seatunnel.engine.common.job.JobStatus;
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
import java.util.concurrent.atomic.AtomicBoolean;

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
     *
     * <p>The write may have committed even though its confirmation was lost, and a successor master
     * may already have adopted that exact allocation. Removing the record and releasing the slots
     * would then hand a slot in use to another job, so the failed round has to keep both until the
     * job ends without retrying.
     */
    @Test
    @SuppressWarnings("unchecked")
    void testUnconfirmedOwnershipKeepsPossiblyAdoptedAllocation() throws Exception {
        long jobId = instance.getFlakeIdGenerator("master-failover-ownership-read-back").newId();
        ResourceManager resourceManager = server.getCoordinatorService().getResourceManager();
        Data jobImmutableInformationData = createJobImmutableInformationData(jobId);
        IMap<PipelineLocation, Map<TaskGroupLocation, SlotProfile>> realOwnershipMap =
                nodeEngine.getHazelcastInstance().getMap("ownedSlotProfilesIMap");
        // The write commits to the real map, but the acknowledgement and the confirmation reads are
        // lost until the flag flips, which is how a lost acknowledgement looks to the old master.
        AtomicBoolean confirmationVisible = new AtomicBoolean(false);
        IMap<PipelineLocation, Map<TaskGroupLocation, SlotProfile>> lostAckOwnershipMap =
                Mockito.mock(IMap.class);
        Mockito.when(lostAckOwnershipMap.get(Mockito.any()))
                .thenAnswer(
                        invocation ->
                                confirmationVisible.get()
                                        ? realOwnershipMap.get(
                                                (PipelineLocation) invocation.getArgument(0))
                                        : null);
        Mockito.when(lostAckOwnershipMap.put(Mockito.any(), Mockito.any()))
                .thenAnswer(
                        invocation ->
                                realOwnershipMap.put(
                                        invocation.getArgument(0), invocation.getArgument(1)));
        Mockito.when(lostAckOwnershipMap.remove(Mockito.any()))
                .thenAnswer(
                        invocation ->
                                realOwnershipMap.remove(
                                        (PipelineLocation) invocation.getArgument(0)));

        JobMaster jobMaster = newJobMaster(jobId, jobImmutableInformationData, lostAckOwnershipMap);
        jobMaster.init(System.currentTimeMillis(), false);
        try {
            Assertions.assertFalse(
                    jobMaster.preApplyResources(),
                    "a pre-apply whose written ownership does not read back must not admit the job");
            Mockito.verify(lostAckOwnershipMap, Mockito.atLeastOnce())
                    .put(Mockito.any(), Mockito.any());
            // The confirmation must retry the read-back before the ownership is treated as unknown.
            Mockito.verify(lostAckOwnershipMap, Mockito.atLeast(2)).get(Mockito.any());

            Map<PipelineLocation, Map<TaskGroupLocation, SlotProfile>> committedOwnership =
                    readPersistedSlots(realOwnershipMap, jobMaster);
            Assertions.assertFalse(
                    committedOwnership.isEmpty(),
                    "a committed ownership record must not be removed by the failed round");
            committedOwnership
                    .values()
                    .forEach(
                            slotProfiles ->
                                    slotProfiles
                                            .values()
                                            .forEach(
                                                    slotProfile ->
                                                            Assertions.assertTrue(
                                                                    resourceManager.slotActiveCheck(
                                                                            slotProfile),
                                                                    "a possibly adopted slot must stay allocated out of the failed round")));
            Mockito.verify(lostAckOwnershipMap, Mockito.never()).remove(Mockito.any());

            // The job is cancelled before any retry, so the terminal cleanup owns the release.
            confirmationVisible.set(true);
            jobMaster.cancelJob();
            assertNoOwnershipRecord(realOwnershipMap, jobMaster);
            occupyAllFixedSlots(resourceManager, jobId + 1);
        } finally {
            confirmationVisible.set(true);
            jobMaster.cancelJob();
            releasePersistedSlots(resourceManager, jobMaster, jobId);
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
     * to ownership that a successor master may already have adopted, and the failed write may have
     * committed as well. The failed round therefore keeps every slot it allocated instead of
     * removing records and releasing capacity that may already be in use.
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
                    "a pipeline whose write threw has no ownership record");

            // Every slot of the round is still allocated to the job, including the pipeline without
            // a record: only its remaining capacity is available to another job.
            int jobSlotCount = jobSlotCount(jobMaster);
            List<SlotProfile> probeSlots = new ArrayList<>();
            try {
                for (int index = 0; index < SLOT_NUM - jobSlotCount; index++) {
                    probeSlots.add(
                            resourceManager
                                    .applyResource(jobId + 1, new ResourceProfile(), null)
                                    .get());
                }
                assertNoFreeFixedSlot(resourceManager, jobId + 1);
            } finally {
                resourceManager.releaseResources(jobId + 1, probeSlots).join();
            }

            // Cancelling the pending job returns everything the failed round still holds.
            jobMaster.cancelJob();
            assertNoOwnershipRecord(realOwnershipMap, jobMaster);
            occupyAllFixedSlots(resourceManager, jobId + 1);
        } finally {
            jobMaster.cancelJob();
            releasePersistedSlots(resourceManager, jobMaster, jobId);
        }
    }

    /**
     * A pending job cancelled after a failed admission must release the slots that admission
     * retained.
     *
     * <p>No pipeline of the job ever starts, so the pipeline teardown that normally returns slots
     * never runs. Clearing the attempt ledger would leave a job that will never run holding fixed
     * capacity and, after a few failed admissions, the cluster could not schedule anything else.
     */
    @Test
    void testPendingCancellationReleasesRetainedAdmissionSlots() throws Exception {
        long jobId = instance.getFlakeIdGenerator("master-failover-pending-cancel").newId();
        ResourceManager resourceManager = server.getCoordinatorService().getResourceManager();
        JobMaster jobMaster = preparePendingJobWithRetainedAdmissionSlots(jobId);
        try {
            occupyFreeFixedSlots(resourceManager, jobId + 1, jobSlotCount(jobMaster));

            jobMaster.cancelJob();

            Assertions.assertEquals(JobStatus.CANCELED, jobMaster.getPhysicalPlan().getJobStatus());
            assertNoOwnershipRecord(
                    nodeEngine.getHazelcastInstance().getMap("ownedSlotProfilesIMap"), jobMaster);
            occupyAllFixedSlots(resourceManager, jobId + 1);
        } finally {
            jobMaster.cancelJob();
            releasePersistedSlots(resourceManager, jobMaster, jobId);
        }
    }

    /**
     * A pending job rejected for insufficient resources must release the slots that the failed
     * admission retained, even though it fails instead of being cancelled.
     */
    @Test
    void testRejectedAdmissionReleasesRetainedAdmissionSlots() throws Exception {
        long jobId = instance.getFlakeIdGenerator("master-failover-pending-reject").newId();
        ResourceManager resourceManager = server.getCoordinatorService().getResourceManager();
        JobMaster jobMaster = preparePendingJobWithRetainedAdmissionSlots(jobId);
        try {
            occupyFreeFixedSlots(resourceManager, jobId + 1, jobSlotCount(jobMaster));

            // Same terminal sequence as CoordinatorService.completeFailJob for a non-waiting
            // strategy: complete the pending plan as failed without deploying any pipeline.
            jobMaster.getPhysicalPlan().updateJobState(JobStatus.FAILED);
            jobMaster.getPhysicalPlan().completeJobEndFuture(new JobResult(JobStatus.FAILED, null));

            Assertions.assertEquals(JobStatus.FAILED, jobMaster.getPhysicalPlan().getJobStatus());
            assertNoOwnershipRecord(
                    nodeEngine.getHazelcastInstance().getMap("ownedSlotProfilesIMap"), jobMaster);
            occupyAllFixedSlots(resourceManager, jobId + 1);
        } finally {
            jobMaster.cancelJob();
            releasePersistedSlots(resourceManager, jobMaster, jobId);
        }
    }

    /**
     * Builds a pending job whose ownership writes all fail, so every slot it pre-applied is only
     * tracked by the attempt ledger of this master.
     */
    @SuppressWarnings("unchecked")
    private JobMaster preparePendingJobWithRetainedAdmissionSlots(long jobId) throws Exception {
        Data jobImmutableInformationData =
                createJobImmutableInformationData(
                        jobId, "stream_two_pipelines_savepoint_partial_failure.conf");
        IMap<PipelineLocation, Map<TaskGroupLocation, SlotProfile>> realOwnershipMap =
                nodeEngine.getHazelcastInstance().getMap("ownedSlotProfilesIMap");
        IMap<PipelineLocation, Map<TaskGroupLocation, SlotProfile>> unavailableOwnershipMap =
                Mockito.mock(IMap.class);
        Mockito.when(unavailableOwnershipMap.get(Mockito.any()))
                .thenAnswer(
                        invocation ->
                                realOwnershipMap.get((PipelineLocation) invocation.getArgument(0)));
        Mockito.when(unavailableOwnershipMap.remove(Mockito.any()))
                .thenAnswer(
                        invocation ->
                                realOwnershipMap.remove(
                                        (PipelineLocation) invocation.getArgument(0)));
        Mockito.doThrow(new HazelcastException("owned slot profiles IMap is unavailable"))
                .when(unavailableOwnershipMap)
                .put(Mockito.any(), Mockito.any());
        JobMaster jobMaster =
                newJobMaster(jobId, jobImmutableInformationData, unavailableOwnershipMap);
        jobMaster.init(System.currentTimeMillis(), false);
        Assertions.assertFalse(
                jobMaster.preApplyResources(),
                "a round that cannot persist its ownership must not admit the job");
        Assertions.assertTrue(
                jobSlotCount(jobMaster) > 0, "the failing round must have allocated some slots");
        return jobMaster;
    }

    /**
     * A failed round that keeps a confirmed pipeline's slots must let the next admission attempt of
     * the same master reuse them.
     *
     * <p>When one pipeline's ownership is persisted and another one's write fails, the confirmed
     * pipeline keeps its ownership and its slots because a successor master may already have
     * adopted them. Those slots stay allocated to the job, so under a fixed capacity the retry of a
     * fresh master, for example the {@code WAIT} strategy retrying this very {@link JobMaster},
     * cannot obtain them again from the resource manager. Here the fixed capacity cannot hold a
     * second copy of the retained slots, so without a retry ledger that reuses them the job stays
     * pending after the IMap recovered; with spare capacity a new allocation would instead
     * overwrite the retained ownership while the old allocations stay untracked.
     */
    @Test
    @SuppressWarnings("unchecked")
    void testRetryReusesConfirmedSlotsAfterPartialPersistenceFailure() throws Exception {
        long jobId = instance.getFlakeIdGenerator("master-failover-retry-retained-slots").newId();
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
        int jobSlotCount =
                pipelines.stream()
                        .mapToInt(
                                subPlan ->
                                        subPlan.getCoordinatorVertexList().size()
                                                + subPlan.getPhysicalVertexList().size())
                        .sum();
        Assertions.assertTrue(
                jobSlotCount <= SLOT_NUM,
                "the fixed capacity must fit the whole job, otherwise the first round cannot allocate it");

        IMap<PipelineLocation, Map<TaskGroupLocation, SlotProfile>> realOwnershipMap =
                nodeEngine.getHazelcastInstance().getMap("ownedSlotProfilesIMap");
        // The ownership IMap is faulty during the first round and restored before the retry, which
        // is how the coordinator sees a transient metadata failure.
        AtomicBoolean ownershipMapAvailable = new AtomicBoolean(false);
        IMap<PipelineLocation, Map<TaskGroupLocation, SlotProfile>> faultInjectableOwnershipMap =
                Mockito.mock(IMap.class);
        Mockito.when(faultInjectableOwnershipMap.get(Mockito.any()))
                .thenAnswer(
                        invocation ->
                                realOwnershipMap.get((PipelineLocation) invocation.getArgument(0)));
        Mockito.when(faultInjectableOwnershipMap.remove(Mockito.any()))
                .thenAnswer(
                        invocation ->
                                realOwnershipMap.remove(
                                        (PipelineLocation) invocation.getArgument(0)));
        Mockito.doAnswer(
                        invocation -> {
                            PipelineLocation pipelineLocation = invocation.getArgument(0);
                            if (confirmedPipeline.equals(pipelineLocation)
                                    || ownershipMapAvailable.get()) {
                                return realOwnershipMap.put(
                                        pipelineLocation, invocation.getArgument(1));
                            }
                            throw new HazelcastException("owned slot profiles IMap is unavailable");
                        })
                .when(faultInjectableOwnershipMap)
                .put(Mockito.any(), Mockito.any());

        JobMaster jobMaster =
                newJobMaster(jobId, jobImmutableInformationData, faultInjectableOwnershipMap);
        jobMaster.init(System.currentTimeMillis(), false);
        try {
            Assertions.assertFalse(
                    jobMaster.preApplyResources(),
                    "a round that cannot persist every pipeline must not admit the job");

            Map<TaskGroupLocation, SlotProfile> retainedOwnership =
                    realOwnershipMap.get(confirmedPipeline);
            Assertions.assertNotNull(
                    retainedOwnership, "the confirmed pipeline must keep its ownership record");
            Assertions.assertFalse(retainedOwnership.isEmpty());
            retainedOwnership
                    .values()
                    .forEach(
                            slotProfile ->
                                    Assertions.assertTrue(
                                            resourceManager.slotActiveCheck(slotProfile),
                                            "a retained slot must stay allocated to the job"));
            Assertions.assertNull(
                    realOwnershipMap.get(failedPipeline),
                    "the unpersisted pipeline ownership must be reverted");
            Assertions.assertTrue(
                    retainedOwnership.size() + jobSlotCount > SLOT_NUM,
                    "the test needs a fixed capacity that cannot hold a second copy of the retained slots");

            // The IMap recovers and the coordinator retries the admission with the same master.
            ownershipMapAvailable.set(true);
            Assertions.assertTrue(
                    jobMaster.preApplyResources(),
                    "the retry must reuse the retained slots and admit the job");

            Assertions.assertEquals(
                    retainedOwnership,
                    realOwnershipMap.get(confirmedPipeline),
                    "the retry must not replace the retained ownership with newly acquired profiles");
            retainedOwnership.forEach(
                    (taskGroupLocation, slotProfile) ->
                            Assertions.assertEquals(
                                    slotProfile,
                                    jobMaster
                                            .getPhysicalPlan()
                                            .getPreApplyResourceFutures()
                                            .get(taskGroupLocation)
                                            .join(),
                                    "the retry must reuse the retained slot instead of applying for a new one"));
            Map<TaskGroupLocation, SlotProfile> failedOwnership =
                    realOwnershipMap.get(failedPipeline);
            Assertions.assertNotNull(
                    failedOwnership, "the retried pipeline must persist its own slot ownership");
            Assertions.assertEquals(
                    jobSlotCount - retainedOwnership.size(),
                    failedOwnership.size(),
                    "the retried pipeline must hold one slot per task group");

            // Exactly the capacity of the whole job is allocated now: the retained slots were
            // reused, not duplicated.
            List<SlotProfile> probeSlots = new ArrayList<>();
            try {
                for (int index = 0; index < SLOT_NUM - jobSlotCount; index++) {
                    try {
                        probeSlots.add(
                                resourceManager
                                        .applyResource(jobId + 1, new ResourceProfile(), null)
                                        .get());
                    } catch (ExecutionException exception) {
                        throw new AssertionError(
                                "the retry must not hold more slots than the job requires",
                                exception);
                    }
                }
                assertNoFreeFixedSlot(resourceManager, jobId + 1);
            } finally {
                resourceManager.releaseResources(jobId + 1, probeSlots).join();
            }
        } finally {
            ownershipMapAvailable.set(true);
            releasePersistedSlots(resourceManager, jobMaster, jobId);
        }
    }

    /**
     * A pre-apply whose slot ownership cannot be persisted must not admit the job.
     *
     * <p>A thrown write does not prove that nothing committed: the exception can also mean the
     * acknowledgement was lost after the put landed. The round therefore keeps every allocation it
     * acquired, and the terminal cleanup of the job releases it when no retry happens.
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
        try {
            Assertions.assertFalse(
                    jobMaster.preApplyResources(),
                    "a pre-apply that cannot persist its slot ownership must not admit the job");
            Mockito.verify(unavailableOwnershipMap, Mockito.atLeastOnce())
                    .put(Mockito.any(), Mockito.any());
            // The slots of the failed round stay allocated to the job, because a thrown write does
            // not prove that the allocation has no adopter, and a later attempt reuses them.
            int jobSlotCount = jobSlotCount(jobMaster);
            List<SlotProfile> probeSlots = new ArrayList<>();
            try {
                for (int index = 0; index < SLOT_NUM - jobSlotCount; index++) {
                    probeSlots.add(
                            resourceManager
                                    .applyResource(jobId + 1, new ResourceProfile(), null)
                                    .get());
                }
            } finally {
                resourceManager.releaseResources(jobId + 1, probeSlots).join();
            }

            // Cancelling the pending job releases exactly those retained allocations.
            jobMaster.cancelJob();
            assertNoOwnershipRecord(unavailableOwnershipMap, jobMaster);
            occupyAllFixedSlots(resourceManager, jobId + 1);
        } finally {
            jobMaster.cancelJob();
            releasePersistedSlots(resourceManager, jobMaster, jobId);
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

    /** Number of task groups of the job, which is the number of fixed slots it needs. */
    private int jobSlotCount(JobMaster jobMaster) {
        return jobMaster.getPhysicalPlan().getPipelineList().stream()
                .mapToInt(
                        subPlan ->
                                subPlan.getCoordinatorVertexList().size()
                                        + subPlan.getPhysicalVertexList().size())
                .sum();
    }

    /** Asserts that the job left no ownership record behind for any of its pipelines. */
    private void assertNoOwnershipRecord(
            IMap<PipelineLocation, Map<TaskGroupLocation, SlotProfile>> ownedSlotProfilesIMap,
            JobMaster jobMaster) {
        for (SubPlan subPlan : jobMaster.getPhysicalPlan().getPipelineList()) {
            Assertions.assertNull(
                    ownedSlotProfilesIMap.get(subPlan.getPipelineLocation()),
                    "a job that ended without deploying a pipeline must not keep an ownership record");
        }
    }

    /**
     * Allocates the fixed slots the job does not hold to an unrelated job, which proves that the
     * job still owns exactly its own slot count and nothing else.
     */
    private void occupyFreeFixedSlots(ResourceManager resourceManager, long jobId, int jobSlotCount)
            throws Exception {
        List<SlotProfile> probeSlots = new ArrayList<>();
        try {
            for (int index = 0; index < SLOT_NUM - jobSlotCount; index++) {
                probeSlots.add(
                        resourceManager.applyResource(jobId, new ResourceProfile(), null).get());
            }
            assertNoFreeFixedSlot(resourceManager, jobId);
        } finally {
            resourceManager.releaseResources(jobId, probeSlots).join();
        }
    }

    /**
     * Allocates every fixed slot for an unrelated job, which proves that a terminal job returned
     * all the capacity its failed admission rounds had retained.
     */
    private void occupyAllFixedSlots(ResourceManager resourceManager, long jobId) throws Exception {
        List<SlotProfile> probeSlots = new ArrayList<>();
        try {
            for (int index = 0; index < SLOT_NUM; index++) {
                probeSlots.add(
                        resourceManager.applyResource(jobId, new ResourceProfile(), null).get());
            }
        } catch (ExecutionException exception) {
            throw new AssertionError(
                    "the terminal cleanup must return every fixed slot the job retained",
                    exception);
        } finally {
            resourceManager.releaseResources(jobId, probeSlots).join();
        }
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

    /**
     * Asserts that no fixed slot is left for an unrelated job, which is how the test observes that
     * an admitted job holds exactly the capacity it requires and nothing was acquired twice.
     */
    private void assertNoFreeFixedSlot(ResourceManager resourceManager, long jobId)
            throws Exception {
        List<SlotProfile> probeSlots = new ArrayList<>();
        try {
            probeSlots.add(resourceManager.applyResource(jobId, new ResourceProfile(), null).get());
            throw new AssertionError("Expected the fixed slots of the workers to be exhausted");
        } catch (ExecutionException exception) {
            if (!(exception.getCause() instanceof NoEnoughResourceException)) {
                throw exception;
            }
        } finally {
            resourceManager.releaseResources(jobId, probeSlots).join();
        }
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
