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
     * Key of the legacy, unscoped bookkeeping used by {@link #selectWorker(List)} and the
     * deprecated accessors. No engine job uses this key: real jobs are keyed by their job ID.
     */
    private static final long UNSCOPED_JOB_ID = -1L;

    private final Map<Address, EvictingQueue<SystemLoadInfo>> workerLoadMap;

    /**
     * Slots handed out by this strategy, scoped by job.
     *
     * <p>The strategy instance is shared by every job on the master. Keeping the bookkeeping per
     * job means one job's allocation round can no longer wipe the in-flight counters of the other
     * jobs (which used to distort placement for concurrent jobs and pipeline restores).
     *
     * <p>The {@link #UNSCOPED_JOB_ID} entry holds the map installed through the deprecated {@link
     * #setWorkerAssignedSlots(Map)} accessor. That map is stored as the caller's reference, exactly
     * like the former Lombok field, so identity, aliasing and external mutation keep working for
     * callers of the legacy accessors. The engine itself never reads or writes that entry: it
     * always selects and resets by real job ID.
     */
    private final ConcurrentMap<Long, Map<Address, SlotAssignedProfile>> workerAssignedSlotsByJob =
            new ConcurrentHashMap<>();

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
     * Returns the map installed through the legacy {@link #setWorkerAssignedSlots(Map)} accessor.
     *
     * <p>Kept so existing compiled callers do not lose the accessor that the former {@code @Getter}
     * field generated. The returned value is the exact installed map instance, so mutating it
     * affects {@link #selectWorker(List)} exactly as before, and {@code null} is returned while no
     * map has been installed (the former field's initial value).
     *
     * <p>The engine does <em>not</em> record its own allocations here: it keeps placement counters
     * per job through {@link #selectWorker(List, long)} and drops them through {@link
     * #resetJobState(long)}. The legacy accessors therefore describe the unscoped, caller-owned
     * bookkeeping only, not the placement state of running jobs. No per-job inspection API is
     * supplied; selection and reset are the supported replacements.
     *
     * @return the installed mutable map, or {@code null} if none was installed
     * @deprecated use the job-aware {@link #selectWorker(List, long)} and {@link
     *     #resetJobState(long)}
     */
    @Deprecated
    public Map<Address, SlotAssignedProfile> getWorkerAssignedSlots() {
        return workerAssignedSlotsByJob.get(UNSCOPED_JOB_ID);
    }

    /**
     * Installs the legacy, job-agnostic slot bookkeeping.
     *
     * <p>Compatible with the former {@code @Setter} field: the supplied map reference is installed
     * as-is instead of being copied, so a caller that keeps updating its own map keeps observing
     * and driving the unscoped selection path. Passing {@code null} clears the installed map. The
     * installed map can never replace a running job's bookkeeping, which is keyed by job ID.
     *
     * @param workerAssignedSlots map to install for the unscoped selection path, or {@code null}
     * @deprecated use the job-aware {@link #selectWorker(List, long)} and {@link
     *     #resetJobState(long)}
     */
    @Deprecated
    public void setWorkerAssignedSlots(Map<Address, SlotAssignedProfile> workerAssignedSlots) {
        if (workerAssignedSlots == null) {
            workerAssignedSlotsByJob.remove(UNSCOPED_JOB_ID);
        } else {
            workerAssignedSlotsByJob.put(UNSCOPED_JOB_ID, workerAssignedSlots);
        }
    }

    /**
     * Returns the bookkeeping of one job, creating it lazily.
     *
     * <p>For the unscoped key this also preserves a usable legacy path: a direct caller that never
     * installed a map through {@link #setWorkerAssignedSlots(Map)} gets an empty concurrent map
     * instead of the former {@code NullPointerException}.
     */
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
