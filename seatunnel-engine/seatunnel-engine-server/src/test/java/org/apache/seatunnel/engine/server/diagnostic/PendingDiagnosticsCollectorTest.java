/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.engine.server.diagnostic;

import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.server.dag.physical.PhysicalPlan;
import org.apache.seatunnel.engine.server.dag.physical.PhysicalVertex;
import org.apache.seatunnel.engine.server.dag.physical.SubPlan;
import org.apache.seatunnel.engine.server.execution.PendingJobInfo;
import org.apache.seatunnel.engine.server.execution.PendingSourceState;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.master.JobMaster;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManager;
import org.apache.seatunnel.engine.server.resourcemanager.resource.CPU;
import org.apache.seatunnel.engine.server.resourcemanager.resource.Memory;
import org.apache.seatunnel.engine.server.resourcemanager.resource.ResourceProfile;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SystemLoadInfo;
import org.apache.seatunnel.engine.server.resourcemanager.worker.WorkerProfile;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.hazelcast.cluster.Address;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public class PendingDiagnosticsCollectorTest {

    @Test
    public void testCollectWorkerResourceSnapshot() throws UnknownHostException {
        ResourceManager resourceManager = Mockito.mock(ResourceManager.class);
        Address dynamicAddress = new Address("localhost", 5801);
        Address fixedAddress = new Address("localhost", 5802);

        SlotProfile dynamicAssigned = slot(dynamicAddress, 0, 100L);
        WorkerProfile dynamicWorker =
                worker(
                        dynamicAddress,
                        true,
                        new ResourceProfile(CPU.of(8), Memory.of(8192)),
                        new ResourceProfile(CPU.of(3), Memory.of(3072)),
                        new SlotProfile[] {dynamicAssigned},
                        new SlotProfile[] {slot(dynamicAddress, 1, 0L)});
        dynamicWorker.setSystemLoadInfo(new SystemLoadInfo(60.0, 25.0));

        SlotProfile fixedAssignedA = slot(fixedAddress, 0, 200L);
        SlotProfile fixedAssignedB = slot(fixedAddress, 1, 200L);
        SlotProfile fixedAssignedC = slot(fixedAddress, 2, 300L);
        WorkerProfile fixedWorker =
                worker(
                        fixedAddress,
                        false,
                        new ResourceProfile(CPU.of(16), Memory.of(16384)),
                        new ResourceProfile(CPU.of(4), Memory.of(4096)),
                        new SlotProfile[] {fixedAssignedA, fixedAssignedB, fixedAssignedC},
                        new SlotProfile[] {slot(fixedAddress, 3, 0L), slot(fixedAddress, 4, 0L)});

        ConcurrentMap<Address, WorkerProfile> workers = new ConcurrentHashMap<>();
        workers.put(fixedAddress, fixedWorker);
        workers.put(dynamicAddress, dynamicWorker);
        Mockito.when(resourceManager.getRegisterWorker()).thenReturn(workers);

        WorkerResourceSnapshot snapshot =
                PendingDiagnosticsCollector.collectWorkerResourceSnapshot(resourceManager);

        Assertions.assertTrue(snapshot.isAvailable());
        Assertions.assertTrue(snapshot.getCollectedAt() > 0);
        Assertions.assertEquals(2, snapshot.getWorkers().size());

        WorkerResourceDiagnostic dynamic = snapshot.getWorkers().get(0);
        Assertions.assertEquals(dynamicAddress.toString(), dynamic.getAddress());
        Assertions.assertTrue(dynamic.isDynamicSlot());
        Assertions.assertEquals(1, dynamic.getUsedSlots());
        Assertions.assertEquals(2, dynamic.getTotalSlots());
        Assertions.assertEquals(1, dynamic.getFreeSlots());
        Assertions.assertEquals(8, dynamic.getTotalCpuCores());
        Assertions.assertEquals(3, dynamic.getAvailableCpuCores());
        Assertions.assertEquals(8192L, dynamic.getTotalHeapMemoryBytes());
        Assertions.assertEquals(3072L, dynamic.getAvailableHeapMemoryBytes());
        Assertions.assertEquals(25.0, dynamic.getCpuUsage());
        Assertions.assertEquals(60.0, dynamic.getMemUsage());
        Assertions.assertEquals(Collections.singletonList(100L), dynamic.getRunningJobIds());

        WorkerResourceDiagnostic fixed = snapshot.getWorkers().get(1);
        Assertions.assertEquals(fixedAddress.toString(), fixed.getAddress());
        Assertions.assertFalse(fixed.isDynamicSlot());
        Assertions.assertEquals(3, fixed.getUsedSlots());
        Assertions.assertEquals(5, fixed.getTotalSlots());
        Assertions.assertEquals(2, fixed.getFreeSlots());
        Assertions.assertEquals(Arrays.asList(200L, 300L), fixed.getRunningJobIds());
    }

    @Test
    public void testCollectWorkerResourceSnapshotWithIncompleteProfile()
            throws UnknownHostException {
        ResourceManager resourceManager = Mockito.mock(ResourceManager.class);
        Address registeredAddress = new Address("localhost", 5801);
        WorkerProfile worker = new WorkerProfile();
        worker.setAddress(null);
        worker.setProfile(null);
        worker.setUnassignedResource(null);
        worker.setAssignedSlots(null);
        worker.setUnassignedSlots(null);
        worker.setAttributes(null);

        ConcurrentMap<Address, WorkerProfile> workers = new ConcurrentHashMap<>();
        workers.put(registeredAddress, worker);
        Mockito.when(resourceManager.getRegisterWorker()).thenReturn(workers);

        WorkerResourceDiagnostic diagnostic =
                PendingDiagnosticsCollector.collectWorkerResourceSnapshot(resourceManager)
                        .getWorkers()
                        .get(0);

        Assertions.assertEquals(registeredAddress.toString(), diagnostic.getAddress());
        Assertions.assertEquals(0, diagnostic.getUsedSlots());
        Assertions.assertEquals(0, diagnostic.getTotalSlots());
        Assertions.assertEquals(0, diagnostic.getFreeSlots());
        Assertions.assertNull(diagnostic.getTotalCpuCores());
        Assertions.assertNull(diagnostic.getAvailableCpuCores());
        Assertions.assertNull(diagnostic.getTotalHeapMemoryBytes());
        Assertions.assertNull(diagnostic.getAvailableHeapMemoryBytes());
        Assertions.assertEquals(Collections.emptyMap(), diagnostic.getTags());
        Assertions.assertEquals(Collections.emptyList(), diagnostic.getRunningJobIds());
    }

    @Test
    public void testCollectWorkerResourceSnapshotAfterSlotReleaseAndReuse()
            throws UnknownHostException {
        ResourceManager resourceManager = Mockito.mock(ResourceManager.class);
        Address address = new Address("localhost", 5801);
        SlotProfile retained = slot(address, 0, 100L);
        SlotProfile released = slot(address, 1, 200L);
        SlotProfile free = slot(address, 2, 0L);
        WorkerProfile worker =
                worker(
                        address,
                        false,
                        new ResourceProfile(CPU.of(8), Memory.of(8192)),
                        new ResourceProfile(CPU.of(4), Memory.of(4096)),
                        new SlotProfile[] {retained, released},
                        new SlotProfile[] {free});
        ConcurrentMap<Address, WorkerProfile> workers = new ConcurrentHashMap<>();
        workers.put(address, worker);
        Mockito.when(resourceManager.getRegisterWorker()).thenReturn(workers);

        released.unassigned();
        worker.setAssignedSlots(new SlotProfile[] {retained});
        worker.setUnassignedSlots(new SlotProfile[] {released, free});

        WorkerResourceDiagnostic afterRelease =
                PendingDiagnosticsCollector.collectWorkerResourceSnapshot(resourceManager)
                        .getWorkers()
                        .get(0);
        Assertions.assertEquals(1, afterRelease.getUsedSlots());
        Assertions.assertEquals(3, afterRelease.getTotalSlots());
        Assertions.assertEquals(2, afterRelease.getFreeSlots());
        Assertions.assertEquals(Collections.singletonList(100L), afterRelease.getRunningJobIds());

        released.assign(300L);
        worker.setAssignedSlots(new SlotProfile[] {retained, released});
        worker.setUnassignedSlots(new SlotProfile[] {free});

        WorkerResourceDiagnostic afterReuse =
                PendingDiagnosticsCollector.collectWorkerResourceSnapshot(resourceManager)
                        .getWorkers()
                        .get(0);
        Assertions.assertEquals(2, afterReuse.getUsedSlots());
        Assertions.assertEquals(3, afterReuse.getTotalSlots());
        Assertions.assertEquals(1, afterReuse.getFreeSlots());
        Assertions.assertEquals(Arrays.asList(100L, 300L), afterReuse.getRunningJobIds());
    }

    @Test
    public void testCollectEmptyAndUnavailableWorkerResourceSnapshots() {
        WorkerResourceSnapshot unavailable =
                PendingDiagnosticsCollector.collectWorkerResourceSnapshot(null);
        Assertions.assertFalse(unavailable.isAvailable());
        Assertions.assertTrue(unavailable.getWorkers().isEmpty());

        ResourceManager emptyResourceManager = Mockito.mock(ResourceManager.class);
        Mockito.when(emptyResourceManager.getRegisterWorker())
                .thenReturn(new ConcurrentHashMap<>());
        WorkerResourceSnapshot empty =
                PendingDiagnosticsCollector.collectWorkerResourceSnapshot(emptyResourceManager);
        Assertions.assertTrue(empty.isAvailable());
        Assertions.assertTrue(empty.getWorkers().isEmpty());

        ResourceManager unavailableResourceManager = Mockito.mock(ResourceManager.class);
        Mockito.when(unavailableResourceManager.getRegisterWorker()).thenReturn(null);
        WorkerResourceSnapshot missing =
                PendingDiagnosticsCollector.collectWorkerResourceSnapshot(
                        unavailableResourceManager);
        Assertions.assertFalse(missing.isAvailable());
        Assertions.assertTrue(missing.getWorkers().isEmpty());
    }

    @Test
    public void testCollectJobDiagnosticWithFailures() {
        JobMaster jobMaster = Mockito.mock(JobMaster.class);
        Mockito.when(jobMaster.getJobId()).thenReturn(1000L);
        JobImmutableInformation jobImmutableInformation =
                Mockito.mock(JobImmutableInformation.class);
        Mockito.when(jobImmutableInformation.getJobName()).thenReturn("test_job");
        Mockito.when(jobMaster.getJobImmutableInformation()).thenReturn(jobImmutableInformation);
        Mockito.when(jobMaster.getJobStatus()).thenReturn(JobStatus.PENDING);

        PhysicalPlan physicalPlan = Mockito.mock(PhysicalPlan.class);
        Mockito.when(jobMaster.getPhysicalPlan()).thenReturn(physicalPlan);

        SubPlan subPlan = Mockito.mock(SubPlan.class);
        Mockito.when(subPlan.getPipelineId()).thenReturn(1);
        Mockito.when(subPlan.getPipelineFullName()).thenReturn("pipeline-1");

        PhysicalVertex vertexSuccess = Mockito.mock(PhysicalVertex.class);
        TaskGroupLocation locationSuccess = new TaskGroupLocation(1000L, 1, 1L);
        Mockito.when(vertexSuccess.getTaskGroupLocation()).thenReturn(locationSuccess);
        Mockito.when(vertexSuccess.getTaskFullName()).thenReturn("task-success");

        PhysicalVertex vertexFailA = Mockito.mock(PhysicalVertex.class);
        TaskGroupLocation locationFailA = new TaskGroupLocation(1000L, 1, 2L);
        Mockito.when(vertexFailA.getTaskGroupLocation()).thenReturn(locationFailA);
        Mockito.when(vertexFailA.getTaskFullName()).thenReturn("task-fail-a");

        PhysicalVertex vertexFailB = Mockito.mock(PhysicalVertex.class);
        TaskGroupLocation locationFailB = new TaskGroupLocation(1000L, 1, 3L);
        Mockito.when(vertexFailB.getTaskGroupLocation()).thenReturn(locationFailB);
        Mockito.when(vertexFailB.getTaskFullName()).thenReturn("task-fail-b");

        Mockito.when(subPlan.getCoordinatorVertexList()).thenReturn(Collections.emptyList());
        Mockito.when(subPlan.getPhysicalVertexList())
                .thenReturn(Arrays.asList(vertexSuccess, vertexFailA, vertexFailB));
        Mockito.when(physicalPlan.getPipelineList()).thenReturn(Collections.singletonList(subPlan));

        Map<TaskGroupLocation, CompletableFuture<SlotProfile>> futures = new HashMap<>();
        CompletableFuture<SlotProfile> successFuture =
                CompletableFuture.completedFuture(Mockito.mock(SlotProfile.class));
        futures.put(locationSuccess, successFuture);

        CompletableFuture<SlotProfile> failFutureA = new CompletableFuture<>();
        failFutureA.completeExceptionally(new RuntimeException("no slot available"));
        futures.put(locationFailA, failFutureA);

        CompletableFuture<SlotProfile> failFutureB = new CompletableFuture<>();
        failFutureB.completeExceptionally(new RuntimeException("worker busy"));
        futures.put(locationFailB, failFutureB);

        Mockito.when(physicalPlan.getPreApplyResourceFutures()).thenReturn(futures);

        PendingJobInfo pendingJobInfo = new PendingJobInfo(PendingSourceState.SUBMIT, jobMaster);

        ResourceManager resourceManager = Mockito.mock(ResourceManager.class);
        SlotProfile blockingSlot = Mockito.mock(SlotProfile.class);
        Mockito.when(blockingSlot.getOwnerJobID()).thenReturn(2000L);
        Mockito.when(resourceManager.getAssignedSlots(Mockito.anyMap()))
                .thenReturn(Collections.singletonList(blockingSlot));

        PendingJobDiagnostic diagnostic =
                PendingDiagnosticsCollector.collectJobDiagnostic(
                        pendingJobInfo, Collections.emptyMap(), resourceManager);

        Assertions.assertEquals(2, diagnostic.getLackingTaskGroups());
        Assertions.assertEquals("REQUEST_FAILED", diagnostic.getFailureReason());
        Assertions.assertEquals(1, diagnostic.getBlockingJobIds().size());
        Assertions.assertEquals(3, diagnostic.getPipelines().get(0).getTotalTaskGroups());
        Assertions.assertEquals(2, diagnostic.getPipelines().get(0).getLackingTaskGroups());
    }

    private static WorkerProfile worker(
            Address address,
            boolean dynamicSlot,
            ResourceProfile profile,
            ResourceProfile unassignedResource,
            SlotProfile[] assignedSlots,
            SlotProfile[] unassignedSlots) {
        return new WorkerProfile(
                address,
                profile,
                unassignedResource,
                dynamicSlot,
                assignedSlots,
                unassignedSlots,
                Collections.singletonMap("region", "test"));
    }

    private static SlotProfile slot(Address address, int slotId, long ownerJobId) {
        SlotProfile slot =
                new SlotProfile(address, slotId, new ResourceProfile(), "slot-" + slotId);
        if (ownerJobId > 0) {
            slot.assign(ownerJobId);
        }
        return slot;
    }
}
