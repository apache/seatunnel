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

package org.apache.seatunnel.engine.server.dag.physical;

import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.server.CoordinatorService;
import org.apache.seatunnel.engine.server.execution.ExecutionState;
import org.apache.seatunnel.engine.server.execution.TaskGroupDefaultImpl;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.flakeidgen.FlakeIdGenerator;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.NodeEngine;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Tests the graceful member-removal classification in {@link PhysicalVertex}: the paired
 * message/flag holder, its first-write-wins and concurrency invariants, and the master-failover
 * restore path that reads the graceful-removal marker directly from the Hazelcast map.
 */
public class PhysicalVertexTest {

    private static final long JOB_ID = 1L;

    private static final int PIPELINE_ID = 2;

    private static final long TASK_GROUP_ID = 3L;

    /**
     * Only the structured graceful-member-removal classification should downgrade the failure log
     * level.
     */
    @Test
    public void shouldWarnOnlyForGracefulMemberRemovalFailureType() {
        Assertions.assertTrue(PhysicalVertex.shouldLogFailureAsWarn(true));
        Assertions.assertFalse(PhysicalVertex.shouldLogFailureAsWarn(false));
    }

    /**
     * Reproduces the race interleaving reported in review: a genuine task failure (RPC-reported,
     * non-graceful) records its classification first, then the coordinator's node-offline
     * classification for the same vertex lands afterwards. Before the message and the flag were
     * paired in one first-write-wins holder, the late graceful write silently re-tagged the
     * already-recorded genuine failure, so a real fault was logged at warn instead of error. The
     * recorded pair must stay the genuine failure's own classification.
     */
    @Test
    public void shouldNotRetagRecordedGenuineFailureAsGraceful() {
        AtomicReference<PhysicalVertex.FailureClassification> slot = new AtomicReference<>();
        PhysicalVertex.recordFailureClassification(slot, "genuine task failure", false);
        PhysicalVertex.recordFailureClassification(slot, "deployed node offline", true);
        Assertions.assertEquals("genuine task failure", slot.get().getErrorMessage());
        Assertions.assertFalse(slot.get().isGracefulMemberRemovalFailure());
    }

    /**
     * The reverse interleaving: when the node-offline classification wins the first write, a later
     * genuine failure report must not strip the graceful flag from the recorded offline failure,
     * matching the pre-existing first-write-wins semantics of the recorded failure message.
     */
    @Test
    public void shouldNotStripGracefulFlagFromRecordedOfflineFailure() {
        AtomicReference<PhysicalVertex.FailureClassification> slot = new AtomicReference<>();
        PhysicalVertex.recordFailureClassification(slot, "deployed node offline", true);
        PhysicalVertex.recordFailureClassification(slot, "genuine task failure", false);
        Assertions.assertEquals("deployed node offline", slot.get().getErrorMessage());
        Assertions.assertTrue(slot.get().isGracefulMemberRemovalFailure());
    }

    /**
     * A {@code null} message must never claim the classification slot, so a later caller that
     * actually carries a failure message still wins the recorded classification. This preserves the
     * pre-existing behavior where a message-less state report left the error slot claimable.
     */
    @Test
    public void shouldIgnoreNullMessageAndKeepSlotClaimable() {
        AtomicReference<PhysicalVertex.FailureClassification> slot = new AtomicReference<>();
        PhysicalVertex.recordFailureClassification(slot, null, true);
        Assertions.assertNull(slot.get());
        PhysicalVertex.recordFailureClassification(slot, "genuine task failure", false);
        Assertions.assertEquals("genuine task failure", slot.get().getErrorMessage());
        Assertions.assertFalse(slot.get().isGracefulMemberRemovalFailure());
    }

    /**
     * A master failover restores task state before its member-removed callback can run. A fresh
     * marker must therefore classify the discovered missing worker as graceful in that path too.
     */
    @Test
    public void shouldClassifyMissingWorkerDuringMasterFailoverAsGraceful() throws Exception {
        AtomicReference<PhysicalVertex.FailureClassification> slot = new AtomicReference<>();
        TaskGroupLocation taskGroupLocation = new TaskGroupLocation(1L, 2, 3L);
        Address lostAddress = new Address("127.0.0.1", 5801);
        long nowMillis = 100_000L;

        PhysicalVertex.recordMemberRemovedFailure(
                slot, taskGroupLocation, lostAddress, nowMillis, nowMillis);

        Assertions.assertEquals(
                "The taskGroup("
                        + taskGroupLocation
                        + ") deployed node("
                        + lostAddress
                        + ") offline",
                slot.get().getErrorMessage());
        Assertions.assertTrue(slot.get().isGracefulMemberRemovalFailure());

        AtomicReference<PhysicalVertex.FailureClassification> staleSlot = new AtomicReference<>();
        PhysicalVertex.recordMemberRemovedFailure(
                staleSlot,
                taskGroupLocation,
                lostAddress,
                nowMillis - Constant.GRACEFUL_MEMBER_REMOVAL_MARK_TTL_MILLIS - 1,
                nowMillis);
        Assertions.assertFalse(staleSlot.get().isGracefulMemberRemovalFailure());
    }

    /**
     * Concurrency invariant for the paired holder: two racing writers (a genuine failure and a
     * graceful node-offline classification) may win the slot in either order, but the recorded flag
     * must always belong to the recorded message. The assertion is order-independent, so the test
     * stays deterministic while still exercising real cross-thread interleavings.
     */
    @Test
    public void shouldKeepMessageAndFlagPairedUnderConcurrentWriters() throws Exception {
        for (int i = 0; i < 100; i++) {
            AtomicReference<PhysicalVertex.FailureClassification> slot = new AtomicReference<>();
            CountDownLatch startLatch = new CountDownLatch(1);
            Thread genuineFailureWriter =
                    new Thread(
                            () -> {
                                awaitStart(startLatch);
                                PhysicalVertex.recordFailureClassification(
                                        slot, "genuine task failure", false);
                            });
            Thread offlineClassificationWriter =
                    new Thread(
                            () -> {
                                awaitStart(startLatch);
                                PhysicalVertex.recordFailureClassification(
                                        slot, "deployed node offline", true);
                            });
            genuineFailureWriter.start();
            offlineClassificationWriter.start();
            startLatch.countDown();
            genuineFailureWriter.join();
            offlineClassificationWriter.join();
            PhysicalVertex.FailureClassification recorded = slot.get();
            Assertions.assertNotNull(recorded);
            Assertions.assertEquals(
                    "deployed node offline".equals(recorded.getErrorMessage()),
                    recorded.isGracefulMemberRemovalFailure());
        }
    }

    /**
     * Cross-checks the restore-path classification against the coordinator's shared helpers at the
     * TTL boundaries. Both consumers of the graceful-removal marker must agree on the message and
     * on which markers count as graceful; if either side is ever edited alone, this test fails.
     */
    @Test
    public void shouldAgreeWithCoordinatorRulesAtMarkerTtlBoundaries() throws Exception {
        TaskGroupLocation taskGroupLocation =
                new TaskGroupLocation(JOB_ID, PIPELINE_ID, TASK_GROUP_ID);
        Address lostAddress = new Address("127.0.0.1", 5801);
        long nowMillis = 100_000L + Constant.GRACEFUL_MEMBER_REMOVAL_MARK_TTL_MILLIS;
        Long[] markers = {
            null,
            nowMillis,
            nowMillis - Constant.GRACEFUL_MEMBER_REMOVAL_MARK_TTL_MILLIS,
            nowMillis - Constant.GRACEFUL_MEMBER_REMOVAL_MARK_TTL_MILLIS - 1,
            nowMillis + Constant.GRACEFUL_MEMBER_REMOVAL_MARK_TTL_MILLIS + 1
        };
        for (Long markedAt : markers) {
            AtomicReference<PhysicalVertex.FailureClassification> slot = new AtomicReference<>();
            PhysicalVertex.recordMemberRemovedFailure(
                    slot, taskGroupLocation, lostAddress, markedAt, nowMillis);
            Assertions.assertEquals(
                    CoordinatorService.buildMemberRemovedOfflineMessage(
                            taskGroupLocation, lostAddress),
                    slot.get().getErrorMessage());
            Assertions.assertEquals(
                    CoordinatorService.isGracefulMemberRemovalMarkerValid(markedAt, nowMillis),
                    slot.get().isGracefulMemberRemovalFailure(),
                    "markedAt=" + markedAt);
        }
    }

    /**
     * Exercises the real master-failover restore path end to end with mocked Hazelcast services:
     * the task group is recorded as RUNNING, its owned slot points at a worker that is no longer a
     * cluster member, and a fresh graceful-removal marker exists for that worker. The vertex must
     * read the marker from the {@code engine_gracefulMemberRemoval} map keyed by the worker
     * address, record the shared offline message classified as graceful, and report the task as not
     * executing without ever invoking the remote check operation.
     */
    @Test
    public void shouldClassifyLeftWorkerAsGracefulFromMarkerMapOnRestore() throws Exception {
        TaskGroupLocation taskGroupLocation =
                new TaskGroupLocation(JOB_ID, PIPELINE_ID, TASK_GROUP_ID);
        Address worker = new Address("127.0.0.1", 5801);
        NodeEngine nodeEngine = mock(NodeEngine.class);
        HazelcastInstance hazelcastInstance = mock(HazelcastInstance.class);
        when(nodeEngine.getHazelcastInstance()).thenReturn(hazelcastInstance);
        stubClusterWithoutWorker(nodeEngine, hazelcastInstance, taskGroupLocation, worker);
        IMap<Address, Long> markerMap = stubMarkerMap(hazelcastInstance);
        when(markerMap.get(worker)).thenReturn(System.currentTimeMillis());
        PhysicalVertex vertex = newRunningVertex(nodeEngine, taskGroupLocation);

        Assertions.assertFalse(vertex.checkTaskGroupIsExecuting(taskGroupLocation));

        PhysicalVertex.FailureClassification recorded = recordedClassification(vertex);
        Assertions.assertNotNull(recorded);
        Assertions.assertEquals(
                CoordinatorService.buildMemberRemovedOfflineMessage(taskGroupLocation, worker),
                recorded.getErrorMessage());
        Assertions.assertTrue(recorded.isGracefulMemberRemovalFailure());
        verify(hazelcastInstance).<Address, Long>getMap(Constant.IMAP_GRACEFUL_MEMBER_REMOVAL);
        verify(markerMap).get(worker);
        verify(nodeEngine, never()).getOperationService();
    }

    /**
     * Same restore path with no marker for the departed worker: the failure is still recorded with
     * the shared offline message but stays unproven, so it is logged at error exactly as on {@code
     * dev}.
     */
    @Test
    public void shouldKeepLeftWorkerUnprovenWhenNoMarkerOnRestore() throws Exception {
        TaskGroupLocation taskGroupLocation =
                new TaskGroupLocation(JOB_ID, PIPELINE_ID, TASK_GROUP_ID);
        Address worker = new Address("127.0.0.1", 5801);
        NodeEngine nodeEngine = mock(NodeEngine.class);
        HazelcastInstance hazelcastInstance = mock(HazelcastInstance.class);
        when(nodeEngine.getHazelcastInstance()).thenReturn(hazelcastInstance);
        stubClusterWithoutWorker(nodeEngine, hazelcastInstance, taskGroupLocation, worker);
        IMap<Address, Long> markerMap = stubMarkerMap(hazelcastInstance);
        when(markerMap.get(worker)).thenReturn(null);
        PhysicalVertex vertex = newRunningVertex(nodeEngine, taskGroupLocation);

        Assertions.assertFalse(vertex.checkTaskGroupIsExecuting(taskGroupLocation));

        PhysicalVertex.FailureClassification recorded = recordedClassification(vertex);
        Assertions.assertNotNull(recorded);
        Assertions.assertEquals(
                CoordinatorService.buildMemberRemovedOfflineMessage(taskGroupLocation, worker),
                recorded.getErrorMessage());
        Assertions.assertFalse(recorded.isGracefulMemberRemovalFailure());
        verify(markerMap).get(worker);
        verify(nodeEngine, never()).getOperationService();
    }

    /**
     * A failing marker read during restore must fail safe: the exception is swallowed, the failure
     * is still recorded with the shared offline message, and it stays classified as unproven so a
     * real fault is never downgraded to warn just because the marker map was unreadable.
     */
    @Test
    public void shouldFailSafeToUnprovenWhenMarkerReadFailsOnRestore() throws Exception {
        TaskGroupLocation taskGroupLocation =
                new TaskGroupLocation(JOB_ID, PIPELINE_ID, TASK_GROUP_ID);
        Address worker = new Address("127.0.0.1", 5801);
        NodeEngine nodeEngine = mock(NodeEngine.class);
        HazelcastInstance hazelcastInstance = mock(HazelcastInstance.class);
        when(nodeEngine.getHazelcastInstance()).thenReturn(hazelcastInstance);
        stubClusterWithoutWorker(nodeEngine, hazelcastInstance, taskGroupLocation, worker);
        IMap<Address, Long> markerMap = stubMarkerMap(hazelcastInstance);
        when(markerMap.get(worker)).thenThrow(new IllegalStateException("map service not ready"));
        PhysicalVertex vertex = newRunningVertex(nodeEngine, taskGroupLocation);

        Assertions.assertFalse(vertex.checkTaskGroupIsExecuting(taskGroupLocation));

        PhysicalVertex.FailureClassification recorded = recordedClassification(vertex);
        Assertions.assertNotNull(recorded);
        Assertions.assertEquals(
                CoordinatorService.buildMemberRemovedOfflineMessage(taskGroupLocation, worker),
                recorded.getErrorMessage());
        Assertions.assertFalse(recorded.isGracefulMemberRemovalFailure());
        verify(nodeEngine, never()).getOperationService();
    }

    /**
     * Builds a vertex whose task group state is already RUNNING, which is the state that makes the
     * restore path consult the worker instead of scheduling a fresh deployment.
     */
    @SuppressWarnings("unchecked")
    private static PhysicalVertex newRunningVertex(
            NodeEngine nodeEngine, TaskGroupLocation taskGroupLocation) {
        IMap<Object, Object> runningJobStateIMap = mock(IMap.class);
        when(runningJobStateIMap.get(taskGroupLocation)).thenReturn(ExecutionState.RUNNING);
        IMap<Object, Long[]> runningJobStateTimestampsIMap = mock(IMap.class);
        TaskGroupDefaultImpl taskGroup = mock(TaskGroupDefaultImpl.class);
        when(taskGroup.getTaskGroupLocation()).thenReturn(taskGroupLocation);
        when(taskGroup.getTaskGroupName()).thenReturn("restore-path-task-group");
        JobImmutableInformation jobImmutableInformation = mock(JobImmutableInformation.class);
        when(jobImmutableInformation.getJobId()).thenReturn(JOB_ID);
        return new PhysicalVertex(
                0,
                1,
                taskGroup,
                mock(FlakeIdGenerator.class),
                PIPELINE_ID,
                1,
                Collections.emptyList(),
                Collections.emptyList(),
                jobImmutableInformation,
                System.currentTimeMillis(),
                nodeEngine,
                runningJobStateIMap,
                runningJobStateTimestampsIMap);
    }

    /**
     * Points the task group's owned slot at {@code worker} and makes the cluster membership exclude
     * that worker, which is exactly what the restore path observes after a scale-down.
     */
    @SuppressWarnings("unchecked")
    private static void stubClusterWithoutWorker(
            NodeEngine nodeEngine,
            HazelcastInstance hazelcastInstance,
            TaskGroupLocation taskGroupLocation,
            Address worker)
            throws Exception {
        SlotProfile slotProfile = mock(SlotProfile.class);
        when(slotProfile.getWorker()).thenReturn(worker);
        IMap<PipelineLocation, Map<TaskGroupLocation, SlotProfile>> ownedSlotProfilesIMap =
                mock(IMap.class);
        when(ownedSlotProfilesIMap.get(taskGroupLocation.getPipelineLocation()))
                .thenReturn(Collections.singletonMap(taskGroupLocation, slotProfile));
        when(hazelcastInstance.<PipelineLocation, Map<TaskGroupLocation, SlotProfile>>getMap(
                        Constant.IMAP_OWNED_SLOT_PROFILES))
                .thenReturn(ownedSlotProfilesIMap);
        Member survivingMember = mock(Member.class);
        when(survivingMember.getAddress()).thenReturn(new Address("127.0.0.1", 5802));
        ClusterService clusterService = mock(ClusterService.class);
        when(clusterService.getMembers()).thenReturn(Collections.singleton(survivingMember));
        when(nodeEngine.getClusterService()).thenReturn(clusterService);
    }

    /**
     * Registers a mocked graceful-removal marker map under the exact map name the engine uses, so a
     * wrong map name in the production read would surface as an unstubbed lookup.
     */
    @SuppressWarnings("unchecked")
    private static IMap<Address, Long> stubMarkerMap(HazelcastInstance hazelcastInstance) {
        IMap<Address, Long> markerMap = mock(IMap.class);
        when(hazelcastInstance.<Address, Long>getMap(Constant.IMAP_GRACEFUL_MEMBER_REMOVAL))
                .thenReturn(markerMap);
        return markerMap;
    }

    /**
     * Reads the vertex's recorded classification through reflection so the production class does
     * not need a test-only accessor.
     */
    @SuppressWarnings("unchecked")
    private static PhysicalVertex.FailureClassification recordedClassification(
            PhysicalVertex vertex) throws Exception {
        Field field =
                PhysicalVertex.class.getDeclaredField("failureClassificationByPhysicalVertex");
        field.setAccessible(true);
        return ((AtomicReference<PhysicalVertex.FailureClassification>) field.get(vertex)).get();
    }

    /**
     * Releases the writer threads at the same moment to maximize the chance of a real interleaving
     * between the two classification writes.
     */
    private static void awaitStart(CountDownLatch startLatch) {
        try {
            startLatch.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException(e);
        }
    }
}
