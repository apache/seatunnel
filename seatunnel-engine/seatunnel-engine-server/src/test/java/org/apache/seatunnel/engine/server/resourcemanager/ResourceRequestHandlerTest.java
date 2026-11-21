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

import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.config.EngineConfig;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.server.resourcemanager.allocation.strategy.RandomStrategy;
import org.apache.seatunnel.engine.server.resourcemanager.opeartion.RequestSlotOperation;
import org.apache.seatunnel.engine.server.resourcemanager.resource.CPU;
import org.apache.seatunnel.engine.server.resourcemanager.resource.Memory;
import org.apache.seatunnel.engine.server.resourcemanager.resource.ResourceProfile;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;
import org.apache.seatunnel.engine.server.resourcemanager.worker.WorkerProfile;
import org.apache.seatunnel.engine.server.service.slot.SlotAndWorkerProfile;

import org.junit.jupiter.api.Test;

import com.hazelcast.cluster.Address;
import com.hazelcast.spi.impl.operationservice.Operation;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ResourceRequestHandlerTest {

    @Test
    void partialAllocationFailureWaitsForRollbackBeforeCompletingRequest() throws Exception {
        Address workerAddress = new Address("localhost", 5801);
        SlotProfile successfulSlot = slot(workerAddress, 1);
        CompletableFuture<Void> releaseFuture = new CompletableFuture<>();
        TestResourceManager resourceManager =
                new TestResourceManager(successfulSlot, releaseFuture, 0);

        CompletableFuture<List<SlotProfile>> request =
                requestTwoSlots(workerAddress, resourceManager);

        assertFalse(request.isDone(), "request must wait until the acquired slot is released");
        assertEquals(Collections.singletonList(successfulSlot), resourceManager.releasedSlots);

        releaseFuture.complete(null);

        assertThrows(CompletionException.class, request::join);
        assertEquals(1, resourceManager.releaseAttempts.get());
    }

    @Test
    void partialAllocationFailureRetriesRollback() throws Exception {
        Address workerAddress = new Address("localhost", 5801);
        TestResourceManager resourceManager =
                new TestResourceManager(
                        slot(workerAddress, 1), CompletableFuture.completedFuture(null), 1);

        CompletableFuture<List<SlotProfile>> request =
                requestTwoSlots(workerAddress, resourceManager);

        assertThrows(CompletionException.class, request::join);
        assertEquals(2, resourceManager.releaseAttempts.get());
    }

    @Test
    void rollbackFailureIsAttachedAfterRetriesAreExhausted() throws Exception {
        Address workerAddress = new Address("localhost", 5801);
        TestResourceManager resourceManager =
                new TestResourceManager(
                        slot(workerAddress, 1),
                        CompletableFuture.completedFuture(null),
                        Constant.OPERATION_RETRY_TIME);

        CompletionException exception =
                assertThrows(
                        CompletionException.class,
                        () -> requestTwoSlots(workerAddress, resourceManager).join());

        assertEquals(Constant.OPERATION_RETRY_TIME, resourceManager.releaseAttempts.get());
        assertTrue(hasSuppressedFailure(exception));
    }

    @Test
    void synchronousRollbackFailureIsRetried() throws Exception {
        Address workerAddress = new Address("localhost", 5801);
        SynchronousReleaseResourceManager resourceManager =
                new SynchronousReleaseResourceManager(slot(workerAddress, 1));

        CompletableFuture<List<SlotProfile>> request =
                requestTwoSlots(workerAddress, resourceManager);

        assertThrows(CompletionException.class, request::join);
        assertEquals(2, resourceManager.synchronousReleaseAttempts.get());
    }

    @Test
    void rollbackRetriesOnlyTheSlotWhoseReleaseFailed() throws Exception {
        Address workerAddress = new Address("localhost", 5801);
        SlotProfile first = slot(workerAddress, 1);
        SlotProfile second = slot(workerAddress, 2);
        SelectiveReleaseResourceManager resourceManager =
                new SelectiveReleaseResourceManager(first, second);

        CompletableFuture<List<SlotProfile>> request =
                new ResourceRequestHandler(
                                103L,
                                Arrays.asList(
                                        new ResourceProfile(),
                                        new ResourceProfile(),
                                        new ResourceProfile()),
                                workers(workerAddress, first, second, slot(workerAddress, 3)),
                                resourceManager,
                                new RandomStrategy())
                        .request(Collections.emptyMap());

        assertThrows(CompletionException.class, request::join);
        assertEquals(1, resourceManager.releaseAttempts.get(1).get());
        assertEquals(2, resourceManager.releaseAttempts.get(2).get());
    }

    @Test
    void retryPreservesOriginalRequestIndexes() throws Exception {
        Address workerAddress = new Address("localhost", 5801);
        List<SlotProfile> slots =
                Arrays.asList(
                        slot(workerAddress, 1), slot(workerAddress, 2), slot(workerAddress, 3));
        IndexedRetryResourceManager resourceManager = new IndexedRetryResourceManager(slots);

        List<SlotProfile> result =
                new ResourceRequestHandler(
                                101L,
                                Arrays.asList(
                                        new ResourceProfile(),
                                        new ResourceProfile(),
                                        new ResourceProfile()),
                                workers(workerAddress, slots.toArray(new SlotProfile[0])),
                                resourceManager,
                                new RandomStrategy())
                        .request(Collections.emptyMap())
                        .join();

        assertEquals(slots, result);
        assertEquals(4, resourceManager.requestCount.get());
    }

    @Test
    void dynamicWorkerReceivesEveryMissingResourceIndex() throws Exception {
        Address workerAddress = new Address("localhost", 5801);
        ResourceProfile available = resource(1, 1);
        ResourceProfile missing = resource(2, 2);
        SlotProfile initiallyAllocated = new SlotProfile(workerAddress, 1, available, "initial");
        SlotProfile dynamicallyAllocated = new SlotProfile(workerAddress, 2, missing, "dynamic");
        DynamicResourceManager resourceManager =
                new DynamicResourceManager(initiallyAllocated, dynamicallyAllocated);
        WorkerProfile worker =
                new WorkerProfile(
                        workerAddress,
                        missing,
                        available,
                        true,
                        new SlotProfile[0],
                        new SlotProfile[] {initiallyAllocated},
                        Collections.emptyMap());
        ConcurrentMap<Address, WorkerProfile> workers = new ConcurrentHashMap<>();
        workers.put(workerAddress, worker);

        List<SlotProfile> result =
                new ResourceRequestHandler(
                                102L,
                                Arrays.asList(available, missing),
                                workers,
                                resourceManager,
                                new RandomStrategy())
                        .request(Collections.emptyMap())
                        .join();

        assertEquals(Arrays.asList(initiallyAllocated, dynamicallyAllocated), result);
        assertEquals(1, resourceManager.requestedDynamicResources.size());
        assertSame(missing, resourceManager.requestedDynamicResources.get(0));
        assertEquals(0, resourceManager.fixedSlotReservationCount());
    }

    @Test
    void inFlightFixedSlotReservationHidesSlotsFromConcurrentHandlers() throws Exception {
        Address workerAddress = new Address("localhost", 5801);
        SlotProfile[] slots =
                new SlotProfile[] {
                    slot(workerAddress, 1),
                    slot(workerAddress, 2),
                    slot(workerAddress, 3),
                    slot(workerAddress, 4)
                };
        List<CompletableFuture<SlotAndWorkerProfile>> gates = new ArrayList<>();
        AtomicInteger sends = new AtomicInteger();
        StaleSnapshotResourceManager resourceManager =
                new StaleSnapshotResourceManager(gates, sends);
        ConcurrentMap<Address, WorkerProfile> workers = workers(workerAddress, slots);

        List<CompletableFuture<List<SlotProfile>>> requests = new ArrayList<>();
        for (int i = 0; i < slots.length; i++) {
            requests.add(
                    new ResourceRequestHandler(
                                    200L + i,
                                    Collections.singletonList(new ResourceProfile()),
                                    workers,
                                    resourceManager,
                                    new RandomStrategy())
                            .request(Collections.emptyMap()));
        }

        assertEquals(slots.length, sends.get());
        for (SlotProfile reserved : slots) {
            assertTrue(resourceManager.isFixedSlotReserved(workerAddress, reserved.getSlotID()));
        }
        assertFalse(requests.get(0).isDone());

        CompletableFuture<List<SlotProfile>> extra =
                new ResourceRequestHandler(
                                300L,
                                Collections.singletonList(new ResourceProfile()),
                                workers,
                                resourceManager,
                                new RandomStrategy())
                        .request(Collections.emptyMap());

        assertEquals(slots.length, sends.get());
        assertThrows(ExecutionException.class, () -> extra.get(10, TimeUnit.SECONDS));
        assertEquals(slots.length, resourceManager.fixedSlotReservationCount());

        for (int i = 0; i < slots.length; i++) {
            WorkerProfile assigned =
                    new WorkerProfile(
                            workerAddress,
                            new ResourceProfile(),
                            new ResourceProfile(),
                            false,
                            new SlotProfile[] {slots[i]},
                            new SlotProfile[0],
                            Collections.emptyMap());
            gates.get(i).complete(new SlotAndWorkerProfile(assigned, slots[i]));
        }

        for (int i = 0; i < slots.length; i++) {
            assertEquals(
                    Collections.singletonList(slots[i]), requests.get(i).get(10, TimeUnit.SECONDS));
        }
        assertEquals(0, resourceManager.fixedSlotReservationCount());
        for (SlotProfile reserved : slots) {
            assertFalse(resourceManager.isFixedSlotReserved(workerAddress, reserved.getSlotID()));
        }
    }

    @Test
    void nullSlotResponseReleasesReservationSoRetryCanSendAgain() throws Exception {
        Address workerAddress = new Address("localhost", 5801);
        AtomicInteger sends = new AtomicInteger();
        AbstractResourceManager resourceManager =
                new AbstractResourceManager(null, new EngineConfig()) {
                    @Override
                    protected <E> CompletableFuture<E> sendToMember(
                            Operation operation, Address address) {
                        sends.incrementAndGet();
                        WorkerProfile worker =
                                new WorkerProfile(
                                        address,
                                        new ResourceProfile(),
                                        new ResourceProfile(),
                                        false,
                                        new SlotProfile[0],
                                        new SlotProfile[] {slot(address, 1)},
                                        Collections.emptyMap());
                        return completed(new SlotAndWorkerProfile(worker, null));
                    }

                    @Override
                    public void heartbeat(WorkerProfile workerProfile) {}
                };

        assertThrows(
                CompletionException.class,
                () ->
                        new ResourceRequestHandler(
                                        301L,
                                        Collections.singletonList(new ResourceProfile()),
                                        workers(workerAddress, slot(workerAddress, 1)),
                                        resourceManager,
                                        new RandomStrategy())
                                .request(Collections.emptyMap())
                                .join());

        assertEquals(3, sends.get());
        assertEquals(0, resourceManager.fixedSlotReservationCount());
        assertFalse(resourceManager.isFixedSlotReserved(workerAddress, 1));
    }

    @Test
    void failedSlotRequestReleasesFixedSlotReservation() throws Exception {
        Address workerAddress = new Address("localhost", 5801);
        AbstractResourceManager resourceManager =
                new AbstractResourceManager(null, new EngineConfig()) {
                    @Override
                    protected <E> CompletableFuture<E> sendToMember(
                            Operation operation, Address address) {
                        CompletableFuture<E> failed = new CompletableFuture<>();
                        failed.completeExceptionally(new IllegalStateException("worker down"));
                        return failed;
                    }

                    @Override
                    public void heartbeat(WorkerProfile workerProfile) {}
                };

        assertThrows(
                CompletionException.class,
                () ->
                        new ResourceRequestHandler(
                                        302L,
                                        Collections.singletonList(new ResourceProfile()),
                                        workers(workerAddress, slot(workerAddress, 7)),
                                        resourceManager,
                                        new RandomStrategy())
                                .request(Collections.emptyMap())
                                .join());

        assertFalse(resourceManager.isFixedSlotReserved(workerAddress, 7));
        assertEquals(0, resourceManager.fixedSlotReservationCount());
    }

    @Test
    void dynamicSlotRequestDoesNotReserveFixedSlot() throws Exception {
        Address workerAddress = new Address("localhost", 5801);
        SlotProfile dynamicSlot = slot(workerAddress, 1);
        CompletableFuture<SlotAndWorkerProfile> gate = new CompletableFuture<>();
        AbstractResourceManager resourceManager =
                new AbstractResourceManager(null, new EngineConfig()) {
                    @Override
                    protected <E> CompletableFuture<E> sendToMember(
                            Operation operation, Address address) {
                        return ResourceRequestHandlerTest.completedGate(gate);
                    }

                    @Override
                    public void heartbeat(WorkerProfile workerProfile) {}
                };
        WorkerProfile worker =
                new WorkerProfile(
                        workerAddress,
                        new ResourceProfile(),
                        new ResourceProfile(),
                        true,
                        new SlotProfile[0],
                        new SlotProfile[] {dynamicSlot},
                        Collections.emptyMap());
        ConcurrentMap<Address, WorkerProfile> workers = new ConcurrentHashMap<>();
        workers.put(workerAddress, worker);

        CompletableFuture<List<SlotProfile>> request =
                new ResourceRequestHandler(
                                303L,
                                Collections.singletonList(new ResourceProfile()),
                                workers,
                                resourceManager,
                                new RandomStrategy())
                        .request(Collections.emptyMap());

        assertFalse(request.isDone());
        assertEquals(0, resourceManager.fixedSlotReservationCount());
        assertFalse(resourceManager.isFixedSlotReserved(workerAddress, dynamicSlot.getSlotID()));

        gate.complete(new SlotAndWorkerProfile(worker, dynamicSlot));
        assertEquals(Collections.singletonList(dynamicSlot), request.join());
    }

    @Test
    void expiredFixedSlotReservationDoesNotHideTheSlot() throws Exception {
        Address workerAddress = new Address("localhost", 5801);
        SlotProfile onlySlot = slot(workerAddress, 4);
        AbstractResourceManager resourceManager =
                new AbstractResourceManager(null, new EngineConfig()) {};
        ConcurrentMap<Address, WorkerProfile> workers = workers(workerAddress, onlySlot);
        resourceManager.reserveFixedSlotUntil(
                workerAddress,
                onlySlot.getSlotID(),
                System.nanoTime() + TimeUnit.MINUTES.toNanos(5));
        ResourceRequestHandler handler =
                new ResourceRequestHandler(
                        304L,
                        Collections.singletonList(new ResourceProfile()),
                        workers,
                        resourceManager,
                        new RandomStrategy());

        assertTrue(resourceManager.isFixedSlotReserved(workerAddress, onlySlot.getSlotID()));
        Optional<WorkerProfile> hidden = handler.preCheckWorkerResource(new ResourceProfile());
        assertFalse(hidden.isPresent());

        resourceManager.reserveFixedSlotUntil(
                workerAddress, onlySlot.getSlotID(), System.nanoTime() - 1);
        Optional<WorkerProfile> visible = handler.preCheckWorkerResource(new ResourceProfile());
        assertTrue(visible.isPresent());
        assertFalse(resourceManager.isFixedSlotReserved(workerAddress, onlySlot.getSlotID()));
    }

    @Test
    void lateReleaseOfExpiredLeaseDoesNotDropReplacement() throws Exception {
        Address workerAddress = new Address("localhost", 5801);
        SlotProfile onlySlot = slot(workerAddress, 4);
        AbstractResourceManager resourceManager =
                new AbstractResourceManager(null, new EngineConfig()) {};
        WorkerProfile worker = workers(workerAddress, onlySlot).get(workerAddress);

        FixedSlotReservations.FixedSlotReservation leaseA;
        synchronized (resourceManager.fixedSlotReservationLock()) {
            leaseA = resourceManager.reserveFixedSlot(worker, new ResourceProfile());
        }
        assertNotNull(leaseA);
        assertTrue(resourceManager.isFixedSlotReserved(workerAddress, onlySlot.getSlotID()));

        resourceManager.overrideFixedSlotReservationDeadline(leaseA, System.nanoTime() - 1);
        assertFalse(resourceManager.isFixedSlotReserved(workerAddress, onlySlot.getSlotID()));

        FixedSlotReservations.FixedSlotReservation leaseB;
        synchronized (resourceManager.fixedSlotReservationLock()) {
            leaseB = resourceManager.reserveFixedSlot(worker, new ResourceProfile());
        }
        assertNotNull(leaseB);
        assertTrue(resourceManager.isFixedSlotReserved(workerAddress, onlySlot.getSlotID()));
        assertEquals(1, resourceManager.fixedSlotReservationCount());

        resourceManager.releaseFixedSlotReservation(leaseA);
        assertTrue(resourceManager.isFixedSlotReserved(workerAddress, onlySlot.getSlotID()));
        assertEquals(1, resourceManager.fixedSlotReservationCount());

        resourceManager.releaseFixedSlotReservation(leaseB);
        assertFalse(resourceManager.isFixedSlotReserved(workerAddress, onlySlot.getSlotID()));
        assertEquals(0, resourceManager.fixedSlotReservationCount());
    }

    @Test
    void concurrentHandlersAllocateDistinctSlotsWithOutOfOrderWorkerCompletion()
            throws Exception {
        Address workerAddress = new Address("localhost", 5801);
        SlotProfile[] slots =
                new SlotProfile[] {
                    slot(workerAddress, 1), slot(workerAddress, 2), slot(workerAddress, 3)
                };
        List<PendingWorkerAllocation> pending = new ArrayList<>();
        WorkerSideAllocatingResourceManager resourceManager =
                new WorkerSideAllocatingResourceManager(slots, pending);
        ConcurrentMap<Address, WorkerProfile> workers = workers(workerAddress, slots);

        List<CompletableFuture<List<SlotProfile>>> requests = new ArrayList<>();
        for (int i = 0; i < slots.length; i++) {
            requests.add(
                    new ResourceRequestHandler(
                                    500L + i,
                                    Collections.singletonList(new ResourceProfile()),
                                    workers,
                                    resourceManager,
                                    new RandomStrategy())
                            .request(Collections.emptyMap()));
        }

        assertEquals(slots.length, pending.size());
        for (CompletableFuture<List<SlotProfile>> request : requests) {
            assertFalse(request.isDone());
        }

        // Complete worker responses out of send order: last, first, middle.
        pending.get(2).complete();
        pending.get(0).complete();
        pending.get(1).complete();

        Set<Integer> assignedIds = new HashSet<>();
        for (CompletableFuture<List<SlotProfile>> request : requests) {
            List<SlotProfile> result = request.get(10, TimeUnit.SECONDS);
            assertEquals(1, result.size());
            assertTrue(assignedIds.add(result.get(0).getSlotID()));
        }
        assertEquals(slots.length, assignedIds.size());
        assertEquals(0, resourceManager.fixedSlotReservationCount());
    }

    @Test
    void memberRemovedClearsInFlightFixedSlotReservations() throws Exception {
        Address workerAddress = new Address("localhost", 5801);
        SlotProfile onlySlot = slot(workerAddress, 4);
        List<CompletableFuture<SlotAndWorkerProfile>> gates = new ArrayList<>();
        AtomicInteger sends = new AtomicInteger();
        StaleSnapshotResourceManager resourceManager =
                new StaleSnapshotResourceManager(gates, sends);
        ConcurrentMap<Address, WorkerProfile> workers = workers(workerAddress, onlySlot);

        CompletableFuture<List<SlotProfile>> request =
                new ResourceRequestHandler(
                                401L,
                                Collections.singletonList(new ResourceProfile()),
                                workers,
                                resourceManager,
                                new RandomStrategy())
                        .request(Collections.emptyMap());

        assertEquals(1, sends.get());
        assertTrue(resourceManager.isFixedSlotReserved(workerAddress, onlySlot.getSlotID()));
        assertEquals(1, resourceManager.fixedSlotReservationCount());

        resourceManager.releaseFixedSlotReservationsForWorker(workerAddress);
        assertFalse(resourceManager.isFixedSlotReserved(workerAddress, onlySlot.getSlotID()));
        assertEquals(0, resourceManager.fixedSlotReservationCount());

        WorkerProfile assigned =
                new WorkerProfile(
                        workerAddress,
                        new ResourceProfile(),
                        new ResourceProfile(),
                        false,
                        new SlotProfile[] {onlySlot},
                        new SlotProfile[0],
                        Collections.emptyMap());
        gates.get(0).complete(new SlotAndWorkerProfile(assigned, onlySlot));

        assertEquals(Collections.singletonList(onlySlot), request.get(10, TimeUnit.SECONDS));
        assertEquals(0, resourceManager.fixedSlotReservationCount());
        assertFalse(resourceManager.isFixedSlotReserved(workerAddress, onlySlot.getSlotID()));
    }

    private static CompletableFuture<List<SlotProfile>> requestTwoSlots(
            Address workerAddress, TestResourceManager resourceManager) {
        return new ResourceRequestHandler(
                        100L,
                        Arrays.asList(new ResourceProfile(), new ResourceProfile()),
                        workers(workerAddress),
                        resourceManager,
                        new RandomStrategy())
                .request(Collections.emptyMap());
    }

    private static boolean hasSuppressedFailure(Throwable throwable) {
        Throwable current = throwable;
        while (current != null) {
            if (current.getSuppressed().length > 0) {
                return true;
            }
            current = current.getCause();
        }
        return false;
    }

    private static ConcurrentMap<Address, WorkerProfile> workers(Address address) {
        return workers(address, slot(address, 1), slot(address, 2));
    }

    private static ConcurrentMap<Address, WorkerProfile> workers(
            Address address, SlotProfile... slots) {
        WorkerProfile worker =
                new WorkerProfile(
                        address,
                        new ResourceProfile(),
                        new ResourceProfile(),
                        false,
                        new SlotProfile[0],
                        slots,
                        Collections.emptyMap());
        ConcurrentMap<Address, WorkerProfile> workers = new ConcurrentHashMap<>();
        workers.put(address, worker);
        return workers;
    }

    private static SlotProfile slot(Address address, int id) {
        return new SlotProfile(address, id, new ResourceProfile(), "sequence-" + id);
    }

    private static ResourceProfile resource(int cpu, long memory) {
        return new ResourceProfile(CPU.of(cpu), Memory.of(memory));
    }

    private static class IndexedRetryResourceManager extends AbstractResourceManager {
        private final List<SlotProfile> slots;
        private final AtomicInteger requestCount = new AtomicInteger();

        private IndexedRetryResourceManager(List<SlotProfile> slots) {
            super(null, new EngineConfig());
            this.slots = slots;
        }

        @Override
        protected <E> CompletableFuture<E> sendToMember(Operation operation, Address address) {
            int request = requestCount.getAndIncrement();
            SlotProfile slotProfile;
            if (request == 0) {
                slotProfile = slots.get(0);
            } else if (request == 1) {
                slotProfile = null;
            } else if (request == 2) {
                slotProfile = slots.get(2);
            } else if (request == 3) {
                slotProfile = slots.get(1);
            } else {
                CompletableFuture<E> failed = new CompletableFuture<>();
                failed.completeExceptionally(new NoEnoughResourceException("unexpected retry"));
                return failed;
            }
            WorkerProfile worker = workerProfile(address, slots, slotProfile);
            return completed(new SlotAndWorkerProfile(worker, slotProfile));
        }

        @Override
        public CompletableFuture<Void> releaseResource(long jobId, SlotProfile slotProfile) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void heartbeat(WorkerProfile workerProfile) {}
    }

    private static class DynamicResourceManager extends AbstractResourceManager {
        private final SlotProfile initiallyAllocated;
        private final SlotProfile dynamicallyAllocated;
        private final AtomicInteger requestCount = new AtomicInteger();
        private List<ResourceProfile> requestedDynamicResources = Collections.emptyList();

        private DynamicResourceManager(
                SlotProfile initiallyAllocated, SlotProfile dynamicallyAllocated) {
            super(null, new EngineConfig());
            this.initiallyAllocated = initiallyAllocated;
            this.dynamicallyAllocated = dynamicallyAllocated;
        }

        @Override
        protected boolean supportDynamicWorker() {
            return true;
        }

        @Override
        protected void findNewWorker(
                List<ResourceProfile> resourceProfiles, Map<String, String> tagFilter) {
            requestedDynamicResources = new ArrayList<>(resourceProfiles);
        }

        @Override
        public CompletableFuture<List<SlotProfile>> applyResources(
                long jobId, List<ResourceProfile> resourceProfiles, Map<String, String> tagFilter) {
            if (resourceProfiles.isEmpty()) {
                CompletableFuture<List<SlotProfile>> failed = new CompletableFuture<>();
                failed.completeExceptionally(new NoEnoughResourceException("empty request"));
                return failed;
            }
            return CompletableFuture.completedFuture(
                    Collections.singletonList(dynamicallyAllocated));
        }

        @Override
        protected <E> CompletableFuture<E> sendToMember(Operation operation, Address address) {
            SlotProfile slotProfile =
                    requestCount.getAndIncrement() == 0 ? initiallyAllocated : null;
            WorkerProfile worker =
                    workerProfile(
                            address,
                            Arrays.asList(initiallyAllocated, dynamicallyAllocated),
                            slotProfile);
            return completed(new SlotAndWorkerProfile(worker, slotProfile));
        }

        @Override
        public CompletableFuture<Void> releaseResource(long jobId, SlotProfile slotProfile) {
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void heartbeat(WorkerProfile workerProfile) {}
    }

    private static WorkerProfile workerProfile(
            Address address, List<SlotProfile> slots, SlotProfile assignedSlot) {
        return new WorkerProfile(
                address,
                new ResourceProfile(),
                new ResourceProfile(),
                false,
                assignedSlot == null ? new SlotProfile[0] : new SlotProfile[] {assignedSlot},
                slots.toArray(new SlotProfile[0]),
                Collections.emptyMap());
    }

    @SuppressWarnings("unchecked")
    private static <E> CompletableFuture<E> completed(Object value) {
        return (CompletableFuture<E>) CompletableFuture.completedFuture(value);
    }

    @SuppressWarnings("unchecked")
    private static <E> CompletableFuture<E> completedGate(
            CompletableFuture<SlotAndWorkerProfile> gate) {
        return (CompletableFuture<E>) gate;
    }

    /**
     * Allocates the lowest free slot id at send time, the way {@code DefaultSlotService} does on
     * the worker, and lets the test complete those responses out of order.
     */
    private static final class WorkerSideAllocatingResourceManager extends AbstractResourceManager {
        private final SlotProfile[] slots;
        private final List<PendingWorkerAllocation> pending;
        private final Set<Integer> allocatedIds = new HashSet<>();

        private WorkerSideAllocatingResourceManager(
                SlotProfile[] slots, List<PendingWorkerAllocation> pending) {
            super(null, new EngineConfig());
            this.slots = slots;
            this.pending = pending;
        }

        @Override
        protected <E> CompletableFuture<E> sendToMember(Operation operation, Address address) {
            SlotProfile chosen;
            synchronized (this) {
                chosen = null;
                for (SlotProfile slot : slots) {
                    if (allocatedIds.contains(slot.getSlotID())) {
                        continue;
                    }
                    if (chosen == null || slot.getSlotID() < chosen.getSlotID()) {
                        chosen = slot;
                    }
                }
                if (chosen != null) {
                    allocatedIds.add(chosen.getSlotID());
                }
            }
            CompletableFuture<SlotAndWorkerProfile> gate = new CompletableFuture<>();
            pending.add(new PendingWorkerAllocation(gate, chosen, address, slots));
            return completedGate(gate);
        }

        @Override
        public void heartbeat(WorkerProfile workerProfile) {}
    }

    private static final class PendingWorkerAllocation {
        private final CompletableFuture<SlotAndWorkerProfile> gate;
        private final SlotProfile allocated;
        private final Address address;
        private final SlotProfile[] slots;

        private PendingWorkerAllocation(
                CompletableFuture<SlotAndWorkerProfile> gate,
                SlotProfile allocated,
                Address address,
                SlotProfile[] slots) {
            this.gate = gate;
            this.allocated = allocated;
            this.address = address;
            this.slots = slots;
        }

        private void complete() {
            List<SlotProfile> unassigned = new ArrayList<>();
            for (SlotProfile slot : slots) {
                if (allocated == null || slot.getSlotID() != allocated.getSlotID()) {
                    unassigned.add(slot);
                }
            }
            WorkerProfile worker =
                    new WorkerProfile(
                            address,
                            new ResourceProfile(),
                            new ResourceProfile(),
                            false,
                            allocated == null
                                    ? new SlotProfile[0]
                                    : new SlotProfile[] {allocated},
                            unassigned.toArray(new SlotProfile[0]),
                            Collections.emptyMap());
            gate.complete(new SlotAndWorkerProfile(worker, allocated));
        }
    }

    private static final class StaleSnapshotResourceManager extends AbstractResourceManager {
        private final List<CompletableFuture<SlotAndWorkerProfile>> gates;
        private final AtomicInteger sends;

        private StaleSnapshotResourceManager(
                List<CompletableFuture<SlotAndWorkerProfile>> gates, AtomicInteger sends) {
            super(null, new EngineConfig());
            this.gates = gates;
            this.sends = sends;
        }

        @Override
        protected <E> CompletableFuture<E> sendToMember(Operation operation, Address address) {
            sends.incrementAndGet();
            CompletableFuture<SlotAndWorkerProfile> gate = new CompletableFuture<>();
            gates.add(gate);
            return completedGate(gate);
        }

        @Override
        public void heartbeat(WorkerProfile workerProfile) {
            // Leave the original snapshot in place so later handlers still see every slot as free.
        }
    }

    private static class TestResourceManager extends AbstractResourceManager {
        private final AtomicInteger requestCount = new AtomicInteger();
        private final SlotProfile successfulSlot;
        private final CompletableFuture<Void> releaseFuture;
        private final int releaseFailuresBeforeSuccess;
        private final AtomicInteger releaseAttempts = new AtomicInteger();
        private final List<SlotProfile> releasedSlots = new ArrayList<>();

        private TestResourceManager(
                SlotProfile successfulSlot,
                CompletableFuture<Void> releaseFuture,
                int releaseFailuresBeforeSuccess) {
            super(null, new EngineConfig());
            this.successfulSlot = successfulSlot;
            this.releaseFuture = releaseFuture;
            this.releaseFailuresBeforeSuccess = releaseFailuresBeforeSuccess;
        }

        @Override
        protected <E> CompletableFuture<E> sendToMember(Operation operation, Address address) {
            if (!(operation instanceof RequestSlotOperation)) {
                throw new UnsupportedOperationException();
            }
            if (requestCount.getAndIncrement() == 0) {
                WorkerProfile worker =
                        new WorkerProfile(
                                address,
                                new ResourceProfile(),
                                new ResourceProfile(),
                                false,
                                new SlotProfile[] {successfulSlot},
                                new SlotProfile[0],
                                Collections.emptyMap());
                return ResourceRequestHandlerTest.completed(
                        new SlotAndWorkerProfile(worker, successfulSlot));
            }
            CompletableFuture<E> failed = new CompletableFuture<>();
            failed.completeExceptionally(new NoEnoughResourceException("test failure"));
            return failed;
        }

        @Override
        public CompletableFuture<Void> releaseResource(long jobId, SlotProfile slotProfile) {
            releasedSlots.add(slotProfile);
            if (releaseAttempts.getAndIncrement() < releaseFailuresBeforeSuccess) {
                CompletableFuture<Void> failed = new CompletableFuture<>();
                failed.completeExceptionally(new RuntimeException("transient release failure"));
                return failed;
            }
            return releaseFuture;
        }

        @Override
        public void heartbeat(WorkerProfile workerProfile) {
            // Keep the focused worker snapshot stable while the request and rollback complete.
        }
    }

    private static class SynchronousReleaseResourceManager extends TestResourceManager {
        private final AtomicInteger synchronousReleaseAttempts = new AtomicInteger();

        private SynchronousReleaseResourceManager(SlotProfile successfulSlot) {
            super(successfulSlot, CompletableFuture.completedFuture(null), 0);
        }

        @Override
        public CompletableFuture<Void> releaseResource(long jobId, SlotProfile slotProfile) {
            if (synchronousReleaseAttempts.getAndIncrement() == 0) {
                throw new IllegalStateException("coordinator is closing");
            }
            return CompletableFuture.completedFuture(null);
        }
    }

    private static class SelectiveReleaseResourceManager extends AbstractResourceManager {
        private final List<SlotProfile> successfulSlots;
        private final AtomicInteger requestCount = new AtomicInteger();
        private final ConcurrentMap<Integer, AtomicInteger> releaseAttempts =
                new ConcurrentHashMap<>();

        private SelectiveReleaseResourceManager(SlotProfile first, SlotProfile second) {
            super(null, new EngineConfig());
            this.successfulSlots = Arrays.asList(first, second);
        }

        @Override
        protected <E> CompletableFuture<E> sendToMember(Operation operation, Address address) {
            int request = requestCount.getAndIncrement();
            if (request < successfulSlots.size()) {
                SlotProfile slotProfile = successfulSlots.get(request);
                WorkerProfile worker = workerProfile(address, successfulSlots, slotProfile);
                return completed(new SlotAndWorkerProfile(worker, slotProfile));
            }
            CompletableFuture<E> failed = new CompletableFuture<>();
            failed.completeExceptionally(new NoEnoughResourceException("test failure"));
            return failed;
        }

        @Override
        public CompletableFuture<Void> releaseResource(long jobId, SlotProfile slotProfile) {
            int attempt =
                    releaseAttempts
                            .computeIfAbsent(
                                    slotProfile.getSlotID(), ignored -> new AtomicInteger())
                            .incrementAndGet();
            if (slotProfile.getSlotID() == 2 && attempt == 1) {
                CompletableFuture<Void> failed = new CompletableFuture<>();
                failed.completeExceptionally(new IllegalStateException("transient failure"));
                return failed;
            }
            return CompletableFuture.completedFuture(null);
        }

        @Override
        public void heartbeat(WorkerProfile workerProfile) {}
    }
}
