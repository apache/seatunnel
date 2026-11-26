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

package org.apache.seatunnel.engine.server.resourcemanager.allocation.strategy;

import org.apache.seatunnel.shade.com.google.common.collect.EvictingQueue;

import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SystemLoadInfo;
import org.apache.seatunnel.engine.server.resourcemanager.worker.WorkerProfile;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.hazelcast.cluster.Address;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/** Verifies that slot allocation bookkeeping is isolated per job and thread-safe. */
class SlotAllocationStrategyTest {

    private static final long JOB_ONE = 1L;
    private static final long JOB_TWO = 2L;
    private static final int SLOTS_PER_WORKER = 4;

    private Address workerOneAddress;
    private Address workerTwoAddress;
    private WorkerProfile workerOne;
    private WorkerProfile workerTwo;
    private List<WorkerProfile> workers;

    @BeforeEach
    void setUp() throws UnknownHostException {
        workerOneAddress = new Address("127.0.0.1", 5801);
        workerTwoAddress = new Address("127.0.0.1", 5802);
        workerOne = worker(workerOneAddress);
        workerTwo = worker(workerTwoAddress);
        // Both min() and max() keep the first element when the comparator reports a tie.
        workers = Arrays.asList(workerOne, workerTwo);
    }

    @Test
    void slotRatioSelectsAWorkerWithoutAPreApplyReset() {
        SlotRatioStrategy strategy = new SlotRatioStrategy();

        assertSame(workerOne, strategy.selectWorker(workers, JOB_ONE).orElseThrow());
    }

    @Test
    void slotRatioSpreadsAllocationsOfOneJobAcrossWorkers() {
        SlotRatioStrategy strategy = new SlotRatioStrategy();

        assertSame(workerOne, strategy.selectWorker(workers, JOB_ONE).orElseThrow());
        assertSame(workerTwo, strategy.selectWorker(workers, JOB_ONE).orElseThrow());
    }

    @Test
    void slotRatioResettingOneJobKeepsAnotherJobsCounters() {
        SlotRatioStrategy strategy = new SlotRatioStrategy();
        assertSame(workerOne, strategy.selectWorker(workers, JOB_TWO).orElseThrow());

        // Job one starts its own allocation round; that must not clear job two's counters.
        strategy.resetJobState(JOB_ONE);

        assertSame(workerTwo, strategy.selectWorker(workers, JOB_TWO).orElseThrow());
    }

    @Test
    void slotRatioResetStartsTheGivenJobFromFreshWorkerProfiles() {
        SlotRatioStrategy strategy = new SlotRatioStrategy();
        assertSame(workerOne, strategy.selectWorker(workers, JOB_ONE).orElseThrow());

        strategy.resetJobState(JOB_ONE);

        assertSame(workerOne, strategy.selectWorker(workers, JOB_ONE).orElseThrow());
        assertEquals(1, strategy.recordedAssignedSlots(JOB_ONE, workerOneAddress));
    }

    @Test
    void slotRatioKeepsConcurrentJobsInSeparateCounters() throws Exception {
        SlotRatioStrategy strategy = new SlotRatioStrategy();
        int selectionsPerJob = 32;
        ExecutorService executor = Executors.newFixedThreadPool(8);
        try {
            List<Future<?>> futures = new ArrayList<>();
            for (int i = 0; i < selectionsPerJob; i++) {
                futures.add(executor.submit(() -> strategy.selectWorker(workers, JOB_ONE)));
                futures.add(executor.submit(() -> strategy.selectWorker(workers, JOB_TWO)));
            }
            for (Future<?> future : futures) {
                future.get(30, TimeUnit.SECONDS);
            }
        } finally {
            executor.shutdownNow();
        }

        assertEquals(selectionsPerJob, recordedSlots(strategy, JOB_ONE));
        assertEquals(selectionsPerJob, recordedSlots(strategy, JOB_TWO));

        // Dropping one job's state must leave the counters of the other job intact.
        strategy.resetJobState(JOB_ONE);
        assertEquals(0, recordedSlots(strategy, JOB_ONE));
        assertEquals(selectionsPerJob, recordedSlots(strategy, JOB_TWO));
    }

    @Test
    void systemLoadSelectsAWorkerWithoutAPreApplyReset() {
        SystemLoadStrategy strategy = new SystemLoadStrategy(loadMap(0.2, 0.2));

        assertSame(workerOne, strategy.selectWorker(workers, JOB_ONE).orElseThrow());
    }

    @Test
    void systemLoadResettingOneJobKeepsAnotherJobsCounters() {
        // Worker one is less loaded, so it stays the preferred worker for the first three slots.
        // The fourth selection only moves to worker two while worker one's three recorded slots
        // are still visible, which makes the job isolation observable.
        SystemLoadStrategy strategy = new SystemLoadStrategy(loadMap(0.2, 0.45));

        assertSame(workerOne, strategy.selectWorker(workers, JOB_TWO).orElseThrow());
        assertSame(workerOne, strategy.selectWorker(workers, JOB_TWO).orElseThrow());
        assertSame(workerOne, strategy.selectWorker(workers, JOB_TWO).orElseThrow());

        // Job one starts its own allocation round; that must not clear job two's counters.
        strategy.resetJobState(JOB_ONE);

        // A wiped job-two counter would make worker one the preferred worker again.
        assertSame(workerTwo, strategy.selectWorker(workers, JOB_TWO).orElseThrow());
    }

    private WorkerProfile worker(Address address) {
        WorkerProfile worker = mock(WorkerProfile.class);
        when(worker.getAddress()).thenReturn(address);
        when(worker.getAssignedSlots()).thenReturn(new SlotProfile[0]);
        when(worker.getUnassignedSlots()).thenReturn(new SlotProfile[SLOTS_PER_WORKER]);
        return worker;
    }

    private int recordedSlots(SlotRatioStrategy strategy, long jobId) {
        return strategy.recordedAssignedSlots(jobId, workerOneAddress)
                + strategy.recordedAssignedSlots(jobId, workerTwoAddress);
    }

    private Map<Address, EvictingQueue<SystemLoadInfo>> loadMap(
            double workerOneLoad, double workerTwoLoad) {
        Map<Address, EvictingQueue<SystemLoadInfo>> loadMap = new ConcurrentHashMap<>();
        loadMap.computeIfAbsent(workerOneAddress, key -> EvictingQueue.create(5))
                .add(new SystemLoadInfo(workerOneLoad, workerOneLoad));
        loadMap.computeIfAbsent(workerTwoAddress, key -> EvictingQueue.create(5))
                .add(new SystemLoadInfo(workerTwoLoad, workerTwoLoad));
        return loadMap;
    }
}
