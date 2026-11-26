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

import org.apache.seatunnel.shade.com.google.common.annotations.VisibleForTesting;

import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotAssignedProfile;
import org.apache.seatunnel.engine.server.resourcemanager.worker.WorkerProfile;

import com.hazelcast.cluster.Address;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/** SlotRatioStrategy is a strategy that selects the worker with the lowest slot usage rate. */
public class SlotRatioStrategy implements SlotAllocationStrategy {

    /**
     * Slot bookkeeping used when calling {@link #selectWorker(List)} directly, without a job
     * context.
     */
    private static final long UNSCOPED_JOB_ID = -1L;

    /**
     * Slots handed out by this strategy, scoped by job.
     *
     * <p>The strategy instance is shared by every job on the master. Keeping the bookkeeping per
     * job means one job's allocation round can no longer wipe the in-flight counters of the other
     * jobs (which used to distort placement for concurrent jobs and pipeline restores).
     */
    private final ConcurrentMap<Long, ConcurrentMap<Address, SlotAssignedProfile>>
            workerAssignedSlotsByJob = new ConcurrentHashMap<>();

    @Override
    public Optional<WorkerProfile> selectWorker(List<WorkerProfile> availableWorkers) {
        return selectWorker(availableWorkers, UNSCOPED_JOB_ID);
    }

    @Override
    public Optional<WorkerProfile> selectWorker(List<WorkerProfile> availableWorkers, long jobId) {
        Map<Address, SlotAssignedProfile> workerAssignedSlots = assignedSlotsForJob(jobId);
        Optional<WorkerProfile> workerProfile =
                availableWorkers.stream()
                        .min(
                                Comparator.comparingDouble(
                                        worker -> calculateSlotUsage(worker, workerAssignedSlots)));
        workerProfile.ifPresent(
                profile -> {
                    workerAssignedSlots.merge(
                            profile.getAddress(),
                            new SlotAssignedProfile(0.0, 1, profile.getAssignedSlots().length),
                            (oldVal, newVal) ->
                                    new SlotAssignedProfile(
                                            0.0,
                                            oldVal.getCurrentTaskAssignedSlotsNum() + 1,
                                            oldVal.getAssignedSlotsNum()));
                });
        return workerProfile;
    }

    @Override
    public void resetJobState(long jobId) {
        workerAssignedSlotsByJob.remove(jobId);
    }

    private Map<Address, SlotAssignedProfile> assignedSlotsForJob(long jobId) {
        return workerAssignedSlotsByJob.computeIfAbsent(jobId, key -> new ConcurrentHashMap<>());
    }

    /** Returns the number of slots recorded for the given job and worker. */
    @VisibleForTesting
    int recordedAssignedSlots(long jobId, Address address) {
        Map<Address, SlotAssignedProfile> assignedSlots = workerAssignedSlotsByJob.get(jobId);
        if (assignedSlots == null) {
            return 0;
        }
        SlotAssignedProfile profile = assignedSlots.get(address);
        return profile == null ? 0 : profile.getCurrentTaskAssignedSlotsNum();
    }

    /**
     * Calculate the slot usage rate of the worker
     *
     * @param worker WorkerProfile
     * @param workerAssignedSlots counters of the job the worker is evaluated for
     * @return slot usage rate, range 0.0-1.0
     */
    private double calculateSlotUsage(
            WorkerProfile worker, Map<Address, SlotAssignedProfile> workerAssignedSlots) {
        // computeIfAbsent seeds the counter atomically, so concurrent selections cannot overwrite
        // each other with a stale worker profile count, and a strategy that is used before any
        // job-level reset cannot dereference a null map.
        SlotAssignedProfile slotAssignedProfile =
                workerAssignedSlots.computeIfAbsent(
                        worker.getAddress(),
                        address ->
                                new SlotAssignedProfile(0.0, worker.getAssignedSlots().length, 0));
        // If we manually record the number of assigned slots, we use that number, since
        // worker.getAssignedSlots is not updated in real time.
        int assignedSlots = slotAssignedProfile.getCurrentTaskAssignedSlotsNum();

        int totalSlots = worker.getUnassignedSlots().length + worker.getAssignedSlots().length;
        if (totalSlots == 0) {
            // When using dynamic slots, the default usage rate is 50%
            return 0.5;
        }

        return (double) assignedSlots / totalSlots;
    }
}
