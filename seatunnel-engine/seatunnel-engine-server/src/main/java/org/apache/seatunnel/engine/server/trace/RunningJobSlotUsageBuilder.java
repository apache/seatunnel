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

package org.apache.seatunnel.engine.server.trace;

import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.core.job.JobInfo;
import org.apache.seatunnel.engine.server.CoordinatorService;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.dag.physical.PipelineLocation;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManager;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;

import com.hazelcast.cluster.Address;
import com.hazelcast.map.IMap;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

/** Builds the REST response that summarizes slot ownership for running jobs. */
public final class RunningJobSlotUsageBuilder {

    private RunningJobSlotUsageBuilder() {}

    /**
     * Collects running job ids from the coordinator and aggregates their owned slot profiles.
     *
     * <p>The assigned-slot source is evaluated once per response. Its availability is therefore a
     * cluster-wide value shared by every job entry in that response.
     */
    public static List<Map<String, Object>> build(SeaTunnelServer server) {
        IMap<Long, JobInfo> runningJobInfo =
                server.getNodeEngine()
                        .getHazelcastInstance()
                        .getMap(Constant.IMAP_RUNNING_JOB_INFO);
        IMap<PipelineLocation, Map<TaskGroupLocation, SlotProfile>> ownedSlotProfiles =
                server.getNodeEngine()
                        .getHazelcastInstance()
                        .getMap(Constant.IMAP_OWNED_SLOT_PROFILES);
        CoordinatorService coordinatorService = server.getCoordinatorService();

        Set<Long> runningJobIds = new TreeSet<>();
        runningJobInfo
                .keySet()
                .forEach(
                        jobId -> {
                            if (coordinatorService.shouldShowAsRunningJob(jobId)) {
                                runningJobIds.add(jobId);
                            }
                        });

        ResourceManager resourceManager = coordinatorService.getInitializedResourceManager();
        boolean assignedSlotSourceInitialized = resourceManager != null;
        List<SlotProfile> assignedSlots =
                !assignedSlotSourceInitialized
                        ? Collections.emptyList()
                        : resourceManager.getAssignedSlots(Collections.emptyMap());
        return build(
                ownedSlotProfiles, runningJobIds, assignedSlots, assignedSlotSourceInitialized);
    }

    /**
     * Aggregates one slot per assigned task group, grouped by job, pipeline, and worker address.
     */
    static List<Map<String, Object>> build(
            Map<PipelineLocation, Map<TaskGroupLocation, SlotProfile>> ownedSlotProfiles,
            Set<Long> runningJobIds,
            List<SlotProfile> assignedSlots) {
        return build(ownedSlotProfiles, runningJobIds, assignedSlots, true);
    }

    /**
     * Aggregates slot usage against one assigned-slot snapshot and reports whether that snapshot
     * could be trusted.
     *
     * <p>{@code IMAP_OWNED_SLOT_PROFILES} can retain entries after a slot was released or
     * reassigned, so an owned slot is counted only when the resource manager's assigned-slot
     * snapshot still contains the same allocation. {@code assignedSlotSourceInitialized} is false
     * when the resource manager is not initialized yet (for example during master switch); every
     * job is then reported with {@code slotSourceAvailable = false} and zero counts instead of
     * counts that look authoritative.
     *
     * @param ownedSlotProfiles slot profiles recorded per pipeline and task group, may be null
     * @param runningJobIds jobs that must appear in the response even when they own no slot
     * @param assignedSlots assigned slots reported by the resource manager for this response
     * @param assignedSlotSourceInitialized whether {@code assignedSlots} came from an initialized
     *     resource manager
     */
    static List<Map<String, Object>> build(
            Map<PipelineLocation, Map<TaskGroupLocation, SlotProfile>> ownedSlotProfiles,
            Set<Long> runningJobIds,
            List<SlotProfile> assignedSlots,
            boolean assignedSlotSourceInitialized) {
        Map<Long, SlotUsage> usageByJob = new TreeMap<>();
        runningJobIds.forEach(jobId -> usageByJob.put(jobId, new SlotUsage(jobId)));
        Set<SlotKey> assignedSlotSet = buildAssignedSlotSet(assignedSlots);
        // Keep one availability value for the response so all job entries describe the same
        // assigned-slot snapshot.
        boolean slotSourceAvailable =
                aggregateSlotUsage(
                        usageByJob,
                        ownedSlotProfiles,
                        assignedSlotSet,
                        assignedSlotSourceInitialized);

        List<Map<String, Object>> result = new ArrayList<>();
        usageByJob.values().forEach(usage -> result.add(usage.toResponse(slotSourceAvailable)));
        return result;
    }

    /**
     * Adds every verified owned slot to its job and returns the response-wide availability flag.
     *
     * <p>An initialized resource manager that reports no assigned slot is ambiguous: either no job
     * holds a slot, or the manager has not yet restored its worker state. If, in that case, any
     * running job still has non-null owned slot profiles, the empty snapshot contradicts the owned
     * map and the result is reported as unavailable rather than as zero usage. An empty snapshot
     * with no owned slots for running jobs is a genuine zero and stays available.
     */
    private static boolean aggregateSlotUsage(
            Map<Long, SlotUsage> usageByJob,
            Map<PipelineLocation, Map<TaskGroupLocation, SlotProfile>> ownedSlotProfiles,
            Set<SlotKey> assignedSlotSet,
            boolean assignedSlotSourceInitialized) {
        boolean hasOwnedSlotsForRunningJobs = false;
        boolean slotSourceRequiresVerification =
                assignedSlotSourceInitialized && assignedSlotSet.isEmpty();

        if (ownedSlotProfiles != null && !ownedSlotProfiles.isEmpty()) {
            final boolean[] ownedSlotsObserved = {false};
            ownedSlotProfiles.forEach(
                    (pipelineLocation, taskGroupSlots) -> {
                        if (slotSourceRequiresVerification
                                && belongsToRunningJob(usageByJob, pipelineLocation)
                                && containsOwnedSlots(taskGroupSlots)) {
                            ownedSlotsObserved[0] = true;
                        }
                        aggregatePipelineSlots(
                                usageByJob, pipelineLocation, taskGroupSlots, assignedSlotSet);
                    });
            hasOwnedSlotsForRunningJobs = ownedSlotsObserved[0];
        }

        return assignedSlotSourceInitialized
                && !(slotSourceRequiresVerification && hasOwnedSlotsForRunningJobs);
    }

    /**
     * Counts the owned slots of one pipeline that are still present in the assigned-slot snapshot.
     * Pipelines of jobs that are not in the running set are ignored, and stale owned entries that
     * the resource manager no longer reports as assigned are skipped.
     */
    private static void aggregatePipelineSlots(
            Map<Long, SlotUsage> usageByJob,
            PipelineLocation pipelineLocation,
            Map<TaskGroupLocation, SlotProfile> taskGroupSlots,
            Set<SlotKey> assignedSlotSet) {
        if (pipelineLocation == null || taskGroupSlots == null || taskGroupSlots.isEmpty()) {
            return;
        }

        SlotUsage usage = usageByJob.get(pipelineLocation.getJobId());
        if (usage == null) {
            return;
        }

        taskGroupSlots.values().stream()
                .filter(slotProfile -> isAssignedToJob(slotProfile, assignedSlotSet))
                .forEach(
                        slotProfile ->
                                usage.addSlot(pipelineLocation.getPipelineId(), slotProfile));
    }

    /**
     * Returns whether the pipeline belongs to a job in the response, so owned slots of finished or
     * hidden jobs never make the snapshot look inconsistent.
     */
    private static boolean belongsToRunningJob(
            Map<Long, SlotUsage> usageByJob, PipelineLocation pipelineLocation) {
        return pipelineLocation != null && usageByJob.containsKey(pipelineLocation.getJobId());
    }

    /**
     * Returns whether the task-group map holds at least one real slot; null values do not count as
     * ownership evidence against an empty assigned-slot snapshot.
     */
    private static boolean containsOwnedSlots(Map<TaskGroupLocation, SlotProfile> taskGroupSlots) {
        return taskGroupSlots != null
                && !taskGroupSlots.isEmpty()
                && taskGroupSlots.values().stream().anyMatch(Objects::nonNull);
    }

    /**
     * Indexes the assigned-slot snapshot by allocation identity so each owned slot can be verified
     * in constant time. Null entries are dropped.
     */
    private static Set<SlotKey> buildAssignedSlotSet(List<SlotProfile> assignedSlots) {
        if (assignedSlots == null || assignedSlots.isEmpty()) {
            return Collections.emptySet();
        }
        Set<SlotKey> assignedSlotSet = new HashSet<>();
        assignedSlots.stream()
                .filter(slotProfile -> slotProfile != null)
                .forEach(slotProfile -> assignedSlotSet.add(SlotKey.of(slotProfile)));
        return assignedSlotSet;
    }

    /**
     * Returns whether this owned slot is the same allocation the resource manager currently reports
     * as assigned (worker, slot id, owner job and allocation sequence all match).
     */
    private static boolean isAssignedToJob(SlotProfile slotProfile, Set<SlotKey> assignedSlotSet) {
        return slotProfile != null && assignedSlotSet.contains(SlotKey.of(slotProfile));
    }

    /**
     * Allocation identity of a slot used to match owned slots against assigned slots.
     *
     * <p>{@link SlotProfile#equals(Object)} ignores the owner job, so it cannot be used directly.
     * The owner job id and the allocation sequence are both part of this key: the same worker slot
     * id is reused by later allocations, and a stale owned entry for a previous allocation must not
     * be counted against the current one.
     */
    private static final class SlotKey {
        // Worker that hosts the slot.
        private final Address worker;

        // Slot id, unique only within one worker.
        private final int slotId;

        // Job that the allocation was made for.
        private final long ownerJobId;

        // Allocation sequence assigned when the slot was handed to its owner job.
        private final String sequence;

        private SlotKey(Address worker, int slotId, long ownerJobId, String sequence) {
            this.worker = worker;
            this.slotId = slotId;
            this.ownerJobId = ownerJobId;
            this.sequence = sequence;
        }

        private static SlotKey of(SlotProfile slotProfile) {
            return new SlotKey(
                    slotProfile.getWorker(),
                    slotProfile.getSlotID(),
                    slotProfile.getOwnerJobID(),
                    slotProfile.getSequence());
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SlotKey slotKey = (SlotKey) o;
            return slotId == slotKey.slotId
                    && ownerJobId == slotKey.ownerJobId
                    && Objects.equals(worker, slotKey.worker)
                    && Objects.equals(sequence, slotKey.sequence);
        }

        @Override
        public int hashCode() {
            return Objects.hash(worker, slotId, ownerJobId, sequence);
        }
    }

    /** Mutable aggregation holder for one job's slot usage. */
    private static final class SlotUsage {
        private final long jobId;
        private int slotCount;
        private final Map<Integer, Integer> pipelineSlotCounts = new TreeMap<>();
        private final Map<String, Integer> workerSlotCounts = new TreeMap<>();

        private SlotUsage(long jobId) {
            this.jobId = jobId;
        }

        private void addSlot(int pipelineId, SlotProfile slotProfile) {
            slotCount++;
            pipelineSlotCounts.merge(pipelineId, 1, Integer::sum);
            if (slotProfile.getWorker() != null) {
                workerSlotCounts.merge(slotProfile.getWorker().toString(), 1, Integer::sum);
            }
        }

        private Map<String, Object> toResponse(boolean slotSourceAvailable) {
            Map<String, Object> response = new LinkedHashMap<>();
            response.put("jobId", String.valueOf(jobId));
            response.put("slotCount", slotCount);
            response.put("slotSourceAvailable", slotSourceAvailable);
            response.put("pipelineSlotCounts", new LinkedHashMap<>(pipelineSlotCounts));
            response.put("workerSlotCounts", new LinkedHashMap<>(workerSlotCounts));
            return response;
        }
    }
}
