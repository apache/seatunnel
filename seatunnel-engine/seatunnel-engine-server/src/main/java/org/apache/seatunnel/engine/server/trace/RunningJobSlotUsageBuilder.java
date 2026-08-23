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

    /** Collects running job ids from the coordinator and aggregates their owned slot profiles. */
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

    static List<Map<String, Object>> build(
            Map<PipelineLocation, Map<TaskGroupLocation, SlotProfile>> ownedSlotProfiles,
            Set<Long> runningJobIds,
            List<SlotProfile> assignedSlots,
            boolean assignedSlotSourceInitialized) {
        Map<Long, SlotUsage> usageByJob = new TreeMap<>();
        runningJobIds.forEach(jobId -> usageByJob.put(jobId, new SlotUsage(jobId)));
        Set<SlotKey> assignedSlotSet = buildAssignedSlotSet(assignedSlots);
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

    private static boolean belongsToRunningJob(
            Map<Long, SlotUsage> usageByJob, PipelineLocation pipelineLocation) {
        return pipelineLocation != null && usageByJob.containsKey(pipelineLocation.getJobId());
    }

    private static boolean containsOwnedSlots(Map<TaskGroupLocation, SlotProfile> taskGroupSlots) {
        return taskGroupSlots != null
                && !taskGroupSlots.isEmpty()
                && taskGroupSlots.values().stream().anyMatch(Objects::nonNull);
    }

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

    private static boolean isAssignedToJob(SlotProfile slotProfile, Set<SlotKey> assignedSlotSet) {
        return slotProfile != null && assignedSlotSet.contains(SlotKey.of(slotProfile));
    }

    private static final class SlotKey {
        private final Address worker;
        private final int slotId;
        private final long ownerJobId;
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
