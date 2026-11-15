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

import org.apache.seatunnel.common.utils.ExceptionUtils;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.server.dag.physical.PhysicalPlan;
import org.apache.seatunnel.engine.server.dag.physical.PhysicalVertex;
import org.apache.seatunnel.engine.server.dag.physical.SubPlan;
import org.apache.seatunnel.engine.server.execution.PendingJobInfo;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.master.JobMaster;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManager;
import org.apache.seatunnel.engine.server.resourcemanager.resource.ResourceProfile;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SystemLoadInfo;
import org.apache.seatunnel.engine.server.resourcemanager.worker.WorkerProfile;

import com.hazelcast.cluster.Address;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.CompletionException;
import java.util.stream.Collectors;

@Slf4j
public final class PendingDiagnosticsCollector {

    private static final String REASON_WAITING = "WAITING_SLOT_ASSIGNMENT";
    private static final String REASON_RESOURCE_NOT_ENOUGH = "RESOURCE_NOT_ENOUGH";
    private static final String REASON_REQUEST_FAILED = "REQUEST_FAILED";
    private static final String REASON_REQUEST_CANCELLED = "REQUEST_CANCELLED";

    private PendingDiagnosticsCollector() {}

    public static PendingJobDiagnostic collectJobDiagnostic(
            PendingJobInfo pendingJobInfo,
            Map<String, String> tagFilter,
            ResourceManager resourceManager) {
        if (pendingJobInfo == null) {
            return null;
        }
        JobMaster jobMaster = pendingJobInfo.getJobMaster();
        PendingJobDiagnostic diagnostic = new PendingJobDiagnostic();
        diagnostic.setJobId(jobMaster.getJobId());
        diagnostic.setJobName(jobMaster.getJobImmutableInformation().getJobName());
        diagnostic.setPendingSourceState(pendingJobInfo.getPendingSourceState());
        diagnostic.setJobStatus(jobMaster.getJobStatus());
        diagnostic.setEnqueueTimestamp(pendingJobInfo.getEnqueueTimestamp());
        diagnostic.setCheckTime(System.currentTimeMillis());
        diagnostic.setWaitDurationMs(
                diagnostic.getCheckTime() - pendingJobInfo.getEnqueueTimestamp());
        diagnostic.setTagFilter(
                tagFilter == null ? Collections.emptyMap() : new HashMap<>(tagFilter));
        Map<TaskGroupLocation, CompletableFuture<SlotProfile>> requestFutures =
                Optional.ofNullable(jobMaster.getPhysicalPlan())
                        .map(PhysicalPlan::getPreApplyResourceFutures)
                        .map(HashMap::new)
                        .orElseGet(HashMap::new);

        buildPipelineDiagnostics(jobMaster, requestFutures, diagnostic);
        diagnostic.setTotalTaskGroups(
                diagnostic.getPipelines().stream()
                        .mapToInt(PendingPipelineDiagnostic::getTotalTaskGroups)
                        .sum());
        diagnostic.setAllocatedTaskGroups(
                diagnostic.getPipelines().stream()
                        .mapToInt(PendingPipelineDiagnostic::getAllocatedTaskGroups)
                        .sum());
        diagnostic.setLackingTaskGroups(
                diagnostic.getPipelines().stream()
                        .mapToInt(PendingPipelineDiagnostic::getLackingTaskGroups)
                        .sum());

        updateFailureReason(diagnostic);
        diagnostic.setBlockingJobIds(
                collectBlockingJobs(resourceManager, jobMaster.getJobId(), tagFilter));

        return diagnostic;
    }

    private static void buildPipelineDiagnostics(
            JobMaster jobMaster,
            Map<TaskGroupLocation, CompletableFuture<SlotProfile>> requestFutures,
            PendingJobDiagnostic diagnostic) {
        PhysicalPlan plan = jobMaster.getPhysicalPlan();
        if (plan == null) {
            diagnostic.setFailureReason(REASON_WAITING);
            diagnostic.setFailureMessage("Job master not initialized");
            return;
        }
        for (SubPlan subPlan : plan.getPipelineList()) {
            PendingPipelineDiagnostic pipelineDiagnostic = new PendingPipelineDiagnostic();
            pipelineDiagnostic.setPipelineId(subPlan.getPipelineId());
            pipelineDiagnostic.setPipelineName(subPlan.getPipelineFullName());

            List<PhysicalVertex> vertices = new ArrayList<>();
            vertices.addAll(subPlan.getCoordinatorVertexList());
            vertices.addAll(subPlan.getPhysicalVertexList());

            int allocated = 0;
            int lacking = 0;
            for (PhysicalVertex vertex : vertices) {
                TaskGroupLocation location = vertex.getTaskGroupLocation();
                PendingTaskGroupDiagnostic taskDiagnostic =
                        buildTaskDiagnostic(
                                location, vertex.getTaskFullName(), requestFutures.get(location));
                pipelineDiagnostic.getTaskGroupDiagnostics().add(taskDiagnostic);
                if (taskDiagnostic.isAllocated()) {
                    allocated++;
                } else {
                    lacking++;
                    diagnostic.getLackingTaskGroupDiagnostics().add(taskDiagnostic);
                }
            }

            pipelineDiagnostic.setTotalTaskGroups(vertices.size());
            pipelineDiagnostic.setAllocatedTaskGroups(allocated);
            pipelineDiagnostic.setLackingTaskGroups(lacking);
            diagnostic.getPipelines().add(pipelineDiagnostic);
        }
    }

    private static PendingTaskGroupDiagnostic buildTaskDiagnostic(
            TaskGroupLocation location,
            String taskFullName,
            CompletableFuture<SlotProfile> future) {
        PendingTaskGroupDiagnostic diagnostic = new PendingTaskGroupDiagnostic();
        diagnostic.setTaskGroupLocation(location);
        diagnostic.setTaskFullName(taskFullName);

        if (future == null) {
            diagnostic.setAllocated(false);
            diagnostic.setFailureReason(REASON_RESOURCE_NOT_ENOUGH);
            diagnostic.setFailureMessage("Slot request future not created");
            return diagnostic;
        }

        if (future.isCancelled()) {
            diagnostic.setAllocated(false);
            diagnostic.setFailureReason(REASON_REQUEST_CANCELLED);
            diagnostic.setFailureMessage("Slot request cancelled by resource manager");
            return diagnostic;
        }

        if (!future.isDone()) {
            diagnostic.setAllocated(false);
            diagnostic.setFailureReason(REASON_WAITING);
            diagnostic.setFailureMessage("Slot request still pending");
            return diagnostic;
        }
        try {
            SlotProfile slotProfile = future.join();
            if (slotProfile != null) {
                diagnostic.setAllocated(true);
                return diagnostic;
            }
            diagnostic.setAllocated(false);
            diagnostic.setFailureReason(REASON_RESOURCE_NOT_ENOUGH);
            diagnostic.setFailureMessage("No available slot profile");
        } catch (CompletionException e) {
            diagnostic.setAllocated(false);
            diagnostic.setFailureReason(REASON_REQUEST_FAILED);
            diagnostic.setFailureMessage(ExceptionUtils.getMessage(e));
        }
        return diagnostic;
    }

    private static void updateFailureReason(PendingJobDiagnostic diagnostic) {
        if (diagnostic.getLackingTaskGroupDiagnostics().isEmpty()) {
            if (diagnostic.getFailureReason() == null) {
                diagnostic.setFailureReason(REASON_WAITING);
                diagnostic.setFailureMessage("Job is waiting for scheduler to retry");
            }
            return;
        }

        Map<String, Long> reasonCounter =
                diagnostic.getLackingTaskGroupDiagnostics().stream()
                        .collect(
                                Collectors.groupingBy(
                                        PendingTaskGroupDiagnostic::getFailureReason,
                                        Collectors.counting()));
        String dominantReason =
                reasonCounter.entrySet().stream()
                        .max(Map.Entry.comparingByValue())
                        .map(Map.Entry::getKey)
                        .orElse(REASON_RESOURCE_NOT_ENOUGH);
        diagnostic.setFailureReason(dominantReason);
        diagnostic.setFailureMessage(
                diagnostic.getLackingTaskGroupDiagnostics().stream()
                        .filter(diag -> dominantReason.equals(diag.getFailureReason()))
                        .map(PendingTaskGroupDiagnostic::getFailureMessage)
                        .filter(message -> message != null && !message.isEmpty())
                        .distinct()
                        .collect(Collectors.joining("; ")));
    }

    private static List<Long> collectBlockingJobs(
            ResourceManager resourceManager, long jobId, Map<String, String> tagFilter) {
        if (resourceManager == null) {
            return Collections.emptyList();
        }
        Map<String, String> tags =
                tagFilter == null ? Collections.emptyMap() : new HashMap<>(tagFilter);
        List<SlotProfile> assignedSlots = Collections.emptyList();
        try {
            assignedSlots = resourceManager.getAssignedSlots(tags);
        } catch (Exception e) {
            log.warn("Collect assigned slots failed: {}", ExceptionUtils.getMessage(e));
        }
        Set<Long> blocking = new HashSet<>();
        for (SlotProfile slotProfile : assignedSlots) {
            long ownerId = slotProfile.getOwnerJobID();
            if (ownerId > 0 && ownerId != jobId) {
                blocking.add(ownerId);
            }
        }
        return new ArrayList<>(blocking);
    }

    public static PendingClusterSnapshot collectClusterSnapshot(
            ResourceManager resourceManager, Map<String, String> tagFilter) {
        PendingClusterSnapshot snapshot = new PendingClusterSnapshot();
        if (resourceManager == null) {
            return snapshot;
        }
        Map<String, String> tags =
                tagFilter == null ? Collections.emptyMap() : new HashMap<>(tagFilter);
        List<SlotProfile> assignedSlots = Collections.emptyList();
        List<SlotProfile> unassignedSlots = Collections.emptyList();
        try {
            assignedSlots = resourceManager.getAssignedSlots(tags);
            unassignedSlots = resourceManager.getUnassignedSlots(tags);
        } catch (Exception e) {
            log.warn("Collect slots info failed: {}", ExceptionUtils.getMessage(e));
        }
        snapshot.setAssignedSlots(assignedSlots.size());
        snapshot.setFreeSlots(unassignedSlots.size());
        snapshot.setTotalSlots(assignedSlots.size() + unassignedSlots.size());
        try {
            snapshot.setWorkerCount(resourceManager.workerCount(tags));
        } catch (Exception e) {
            log.warn("Collect worker count failed: {}", ExceptionUtils.getMessage(e));
        }
        snapshot.setWorkers(collectWorkerResourceSnapshot(resourceManager).getWorkers());
        return snapshot;
    }

    /** Collects a point-in-time projection of the resources registered by the master. */
    public static WorkerResourceSnapshot collectWorkerResourceSnapshot(
            ResourceManager resourceManager) {
        WorkerResourceSnapshot snapshot = new WorkerResourceSnapshot();
        snapshot.setCollectedAt(System.currentTimeMillis());
        if (resourceManager == null) {
            return snapshot;
        }
        try {
            Map<Address, WorkerProfile> registerWorker = resourceManager.getRegisterWorker();
            if (registerWorker == null) {
                return snapshot;
            }
            List<WorkerResourceDiagnostic> workers =
                    registerWorker.entrySet().stream()
                            .map(entry -> convertWorker(entry.getKey(), entry.getValue()))
                            .filter(worker -> worker != null)
                            .sorted(Comparator.comparing(WorkerResourceDiagnostic::getAddress))
                            .collect(Collectors.toList());
            snapshot.setAvailable(true);
            snapshot.setWorkers(workers);
        } catch (Exception e) {
            log.warn("Collect worker resource snapshot failed: {}", ExceptionUtils.getMessage(e));
        }
        return snapshot;
    }

    private static WorkerResourceDiagnostic convertWorker(
            Address registeredAddress, WorkerProfile workerProfile) {
        if (workerProfile == null) {
            return null;
        }
        WorkerResourceDiagnostic diagnostic = new WorkerResourceDiagnostic();
        Address address = workerProfile.getAddress();
        if (address == null) {
            address = registeredAddress;
        }
        diagnostic.setAddress(address == null ? "UNKNOWN" : address.toString());
        if (workerProfile.getAttributes() != null) {
            diagnostic.setTags(new HashMap<>(workerProfile.getAttributes()));
        } else {
            diagnostic.setTags(Collections.emptyMap());
        }
        diagnostic.setDynamicSlot(workerProfile.isDynamicSlot());
        int assignedSlots =
                workerProfile.getAssignedSlots() == null
                        ? 0
                        : workerProfile.getAssignedSlots().length;
        int unassignedSlots =
                workerProfile.getUnassignedSlots() == null
                        ? 0
                        : workerProfile.getUnassignedSlots().length;
        diagnostic.setUsedSlots(assignedSlots);
        diagnostic.setTotalSlots(assignedSlots + unassignedSlots);
        diagnostic.setFreeSlots(unassignedSlots);
        setResourceValues(diagnostic, workerProfile.getProfile(), true);
        setResourceValues(diagnostic, workerProfile.getUnassignedResource(), false);
        SystemLoadInfo systemLoadInfo = workerProfile.getSystemLoadInfo();
        if (systemLoadInfo != null) {
            diagnostic.setCpuUsage(systemLoadInfo.getCpuPercentage());
            diagnostic.setMemUsage(systemLoadInfo.getMemPercentage());
        }
        List<Long> runningJobs =
                workerProfile.getAssignedSlots() == null
                        ? Collections.emptyList()
                        : Arrays.stream(workerProfile.getAssignedSlots())
                                .filter(slot -> slot != null && slot.getOwnerJobID() > 0)
                                .map(SlotProfile::getOwnerJobID)
                                .distinct()
                                .sorted()
                                .collect(Collectors.toList());
        diagnostic.setRunningJobIds(runningJobs);
        return diagnostic;
    }

    private static void setResourceValues(
            WorkerResourceDiagnostic diagnostic, ResourceProfile resource, boolean total) {
        if (resource == null) {
            return;
        }
        if (total) {
            diagnostic.setTotalCpuCores(
                    resource.getCpu() == null ? null : resource.getCpu().getCore());
            diagnostic.setTotalHeapMemoryBytes(
                    resource.getHeapMemory() == null ? null : resource.getHeapMemory().getBytes());
        } else {
            diagnostic.setAvailableCpuCores(
                    resource.getCpu() == null ? null : resource.getCpu().getCore());
            diagnostic.setAvailableHeapMemoryBytes(
                    resource.getHeapMemory() == null ? null : resource.getHeapMemory().getBytes());
        }
    }
}
