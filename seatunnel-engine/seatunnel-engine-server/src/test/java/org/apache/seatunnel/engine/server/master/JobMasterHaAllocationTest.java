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

import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.job.JobResult;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.server.dag.physical.PhysicalPlan;
import org.apache.seatunnel.engine.server.dag.physical.PhysicalVertex;
import org.apache.seatunnel.engine.server.dag.physical.PipelineLocation;
import org.apache.seatunnel.engine.server.dag.physical.SubPlan;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.resourcemanager.StandaloneResourceManager;
import org.apache.seatunnel.engine.server.resourcemanager.resource.ResourceProfile;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;

import org.junit.jupiter.api.Test;

import com.hazelcast.cluster.Address;
import com.hazelcast.map.IMap;

import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.awaitility.Awaitility.await;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class JobMasterHaAllocationTest {
    @Test
    void demotionDuringSuccessfulAllocationReleasesFreshSlotsBeforePublication() throws Exception {
        Fixture fixture = new Fixture();
        when(fixture.manager.applyResource(anyLong(), any(), any()))
                .thenAnswer(
                        call -> {
                            fixture.master.interrupt();
                            return CompletableFuture.completedFuture(fixture.slot);
                        });
        assertFalse(
                fixture.master.preApplyResources(
                        fixture.plan, () -> !fixture.master.getJobMasterCompleteFuture().isDone()));
        verify(fixture.manager).releaseResources(1L, Collections.singletonList(fixture.slot));
        verify(fixture.owned, never()).put(any(), any());
        assertTrue(fixture.futures.isEmpty());
    }

    @Test
    void acceptedAllocationIsOwnedBeforeDeployment() throws Exception {
        Fixture fixture = new Fixture();
        assertTrue(fixture.master.preApplyResources(fixture.plan, () -> true));
        verify(fixture.owned)
                .put(fixture.pipeline, Collections.singletonMap(fixture.location, fixture.slot));
        assertEquals(fixture.slot, fixture.futures.get(fixture.location).join());
        verify(fixture.manager, never()).releaseResources(anyLong(), any());
    }

    @Test
    void localRetirementCannotCompleteInsideOwnershipPublication() throws Exception {
        Fixture fixture = new Fixture();
        CountDownLatch publishing = new CountDownLatch(1);
        CountDownLatch finishPublication = new CountDownLatch(1);
        AtomicBoolean wroteAfterRetirement = new AtomicBoolean();
        when(fixture.owned.put(any(), any()))
                .thenAnswer(
                        call -> {
                            publishing.countDown();
                            assertTrue(finishPublication.await(5, TimeUnit.SECONDS));
                            wroteAfterRetirement.set(
                                    fixture.master.getJobMasterCompleteFuture().isDone());
                            return null;
                        });
        CompletableFuture<Boolean> allocation =
                CompletableFuture.supplyAsync(
                        () ->
                                fixture.master.preApplyResources(
                                        fixture.plan,
                                        () ->
                                                !fixture.master
                                                        .getJobMasterCompleteFuture()
                                                        .isDone()));
        Thread retirement = new Thread(fixture.master::interrupt);
        try {
            assertTrue(publishing.await(5, TimeUnit.SECONDS));
            retirement.start();
            await().atMost(5, TimeUnit.SECONDS)
                    .until(() -> retirement.getState() == Thread.State.BLOCKED);
            assertFalse(fixture.master.getJobMasterCompleteFuture().isDone());
            finishPublication.countDown();
            assertTrue(allocation.get(5, TimeUnit.SECONDS));
            retirement.join(5000);
            assertFalse(retirement.isAlive());
            assertTrue(fixture.master.getJobMasterCompleteFuture().isDone());
            assertFalse(wroteAfterRetirement.get());
            assertEquals(fixture.slot, fixture.futures.get(fixture.location).join());
        } finally {
            finishPublication.countDown();
            retirement.join(5000);
        }
    }

    @Test
    void abandonedRecoveryDoesNotReleaseRetainedSlots() throws Exception {
        Fixture fixture = new Fixture();
        set(fixture.master, "masterFailoverRestore", true);
        when(fixture.owned.get(fixture.pipeline))
                .thenReturn(Collections.singletonMap(fixture.location, fixture.slot));
        when(fixture.manager.slotActiveCheck(fixture.slot)).thenReturn(true);
        assertFalse(fixture.master.preApplyResources(fixture.plan, () -> false));
        verify(fixture.manager).releaseResources(1L, Collections.emptyList());
        verify(fixture.manager, never()).applyResource(anyLong(), any(), any());
        assertTrue(fixture.futures.isEmpty());
    }

    @SuppressWarnings("unchecked")
    private static final class Fixture {
        private final JobMaster master = mock(JobMaster.class, CALLS_REAL_METHODS);
        private final StandaloneResourceManager manager = mock(StandaloneResourceManager.class);
        private final SubPlan plan = mock(SubPlan.class);
        private final IMap<PipelineLocation, Map<TaskGroupLocation, SlotProfile>> owned =
                mock(IMap.class);
        private final PipelineLocation pipeline = new PipelineLocation(1L, 1);
        private final TaskGroupLocation location = new TaskGroupLocation(1L, 1, 1);
        private final SlotProfile slot =
                new SlotProfile(
                        new Address("127.0.0.1", 5802),
                        0,
                        new ResourceProfile(),
                        "allocation-sequence");
        private final Map<TaskGroupLocation, CompletableFuture<SlotProfile>> futures =
                new HashMap<>();

        private Fixture() throws Exception {
            slot.assign(1L);
            JobImmutableInformation info = mock(JobImmutableInformation.class);
            when(info.getJobId()).thenReturn(1L);
            when(info.getJobConfig()).thenReturn(new JobConfig());
            set(master, "jobImmutableInformation", info);
            set(master, "resourceManager", manager);
            set(master, "ownedSlotProfilesIMap", owned);
            set(master, "historyExecutionAddress", new HashSet<>());
            set(master, "isRunning", true);
            set(master, "recoveryAdmissionLock", new Object());
            set(master, "jobMasterCompleteFuture", new CompletableFuture<JobResult>());
            PhysicalPlan physical = mock(PhysicalPlan.class);
            set(master, "physicalPlan", physical);
            when(physical.getPreApplyResourceFutures()).thenReturn(futures);
            PhysicalVertex task = mock(PhysicalVertex.class);
            when(task.getTaskGroupLocation()).thenReturn(location);
            when(plan.getPhysicalVertexList()).thenReturn(Collections.singletonList(task));
            when(plan.getCoordinatorVertexList()).thenReturn(Collections.emptyList());
            when(plan.getPipelineLocation()).thenReturn(pipeline);
            when(plan.getTags()).thenReturn(Collections.emptyMap());
            when(manager.applyResource(eq(1L), any(), any()))
                    .thenReturn(CompletableFuture.completedFuture(slot));
            when(manager.releaseResources(anyLong(), any()))
                    .thenReturn(CompletableFuture.completedFuture(null));
            when(owned.get(pipeline)).thenReturn(Collections.singletonMap(location, slot));
        }
    }

    private static void set(JobMaster master, String name, Object value) throws Exception {
        Field field = JobMaster.class.getDeclaredField(name);
        field.setAccessible(true);
        field.set(master, value);
    }
}
