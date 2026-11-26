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

import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotAssignedProfile;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SystemLoadInfo;
import org.apache.seatunnel.engine.server.resourcemanager.worker.WorkerProfile;
import org.apache.seatunnel.engine.server.utils.SystemLoadCalculate;

import com.hazelcast.cluster.Address;

import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/** SystemLoadStrategy is a strategy that selects the worker with the lowest system load. */
public class SystemLoadStrategy implements SlotAllocationStrategy {

    /**
     * Slot bookkeeping used when calling {@link #selectWorker(List)} directly, without a job
     * context.
     */
    private static final long UNSCOPED_JOB_ID = -1L;

    private final Map<Address, EvictingQueue<SystemLoadInfo>> workerLoadMap;

    /**
     * Slots handed out by this strategy, scoped by job.
     *
     * <p>The strategy instance is shared by every job on the master. Keeping the bookkeeping per
     * job means one job's allocation round can no longer wipe the in-flight counters of the other
     * jobs (which used to distort placement for concurrent jobs and pipeline restores).
     */
    private final ConcurrentMap<Long, ConcurrentMap<Address, SlotAssignedProfile>>
            workerAssignedSlotsByJob = new ConcurrentHashMap<>();

    public SystemLoadStrategy(Map<Address, EvictingQueue<SystemLoadInfo>> workerLoadMap) {
        this.workerLoadMap = workerLoadMap;
    }

    public SystemLoadStrategy() {
        this.workerLoadMap = new ConcurrentHashMap<>();
    }

    public void updateWorkerLoad(Address address, SystemLoadInfo systemLoadInfo) {
        workerLoadMap.computeIfAbsent(address, k -> EvictingQueue.create(5)).add(systemLoadInfo);
    }

    /**
     * Returns whether at least one load sample has been collected for the given worker. A worker
     * without load samples gets the lowest weight in {@link #selectWorker}, so callers that need
     * load-based allocation decisions can wait on this signal.
     */
    public boolean hasLoadInfo(Address address) {
        EvictingQueue<SystemLoadInfo> loadInfo = workerLoadMap.get(address);
        return loadInfo != null && !loadInfo.isEmpty();
    }

    @Override
    public Optional<WorkerProfile> selectWorker(List<WorkerProfile> availableWorkers) {
        return selectWorker(availableWorkers, UNSCOPED_JOB_ID);
    }

    @Override
    public Optional<WorkerProfile> selectWorker(List<WorkerProfile> availableWorkers, long jobId) {
        Map<Address, SlotAssignedProfile> workerAssignedSlots = assignedSlotsForJob(jobId);
        Optional<WorkerProfile> workerProfile =
                availableWorkers.stream()
                        .max(
                                Comparator.comparingDouble(
                                        w -> calculateWeight(w, workerAssignedSlots)));

        workerProfile.ifPresent(
                profile -> {
                    workerAssignedSlots.merge(
                            profile.getAddress(),
                            new SlotAssignedProfile(0.0, 1, profile.getAssignedSlots().length),
                            (oldVal, newVal) ->
                                    new SlotAssignedProfile(
                                            oldVal.getSingleSlotUseResource(),
                                            oldVal.getCurrentTaskAssignedSlotsNum() + 1,
                                            oldVal.getAssignedSlotsNum()));
                });
        return workerProfile;
    }

    @Override
    public void resetJobState(long jobId) {
        workerAssignedSlotsByJob.remove(jobId);
    }

    /**
     * Returns the strategy's legacy, job-agnostic slot bookkeeping.
     *
     * <p>Kept so existing compiled extensions and direct callers do not lose the accessor that the
     * former {@code @Getter} field generated. The engine itself records placement counters per job
     * through {@link #selectWorker(List, long)} and {@link #resetJobState(long)}; this view only
     * exposes the unscoped entry used by {@link #selectWorker(List)}.
     *
     * @return mutable map of the unscoped {@code -1} job entry
     * @deprecated use the job-aware {@code selectWorker(List, long)} and {@code
     *     resetJobState(long)}
     */
    @Deprecated
    public Map<Address, SlotAssignedProfile> getWorkerAssignedSlots() {
        return assignedSlotsForJob(UNSCOPED_JOB_ID);
    }

    /**
     * Replaces the strategy's legacy, job-agnostic slot bookkeeping.
     *
     * <p>Kept for compatibility with the former {@code @Setter} field. The supplied map is copied
     * into the unscoped entry, so a caller cannot swap the per-job bookkeeping of another job.
     *
     * @param workerAssignedSlots slot counters to install for the unscoped job entry
     * @deprecated use the job-aware {@code selectWorker(List, long)} and {@code
     *     resetJobState(long)}
     */
    @Deprecated
    public void setWorkerAssignedSlots(Map<Address, SlotAssignedProfile> workerAssignedSlots) {
        ConcurrentMap<Address, SlotAssignedProfile> replacement = new ConcurrentHashMap<>();
        if (workerAssignedSlots != null) {
            replacement.putAll(workerAssignedSlots);
        }
        workerAssignedSlotsByJob.put(UNSCOPED_JOB_ID, replacement);
    }

    private Map<Address, SlotAssignedProfile> assignedSlotsForJob(long jobId) {
        return workerAssignedSlotsByJob.computeIfAbsent(jobId, key -> new ConcurrentHashMap<>());
    }

    public Double calculateWeight(
            WorkerProfile workerProfile, Map<Address, SlotAssignedProfile> workerAssignedSlots) {
        SystemLoadCalculate systemLoadCalculate = new SystemLoadCalculate();
        return systemLoadCalculate.calculate(
                workerLoadMap.get(workerProfile.getAddress()), workerProfile, workerAssignedSlots);
    }
}
