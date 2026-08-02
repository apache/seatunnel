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
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.dag.physical.PhysicalPlan;
import org.apache.seatunnel.engine.server.dag.physical.PhysicalVertex;
import org.apache.seatunnel.engine.server.dag.physical.SubPlan;
import org.apache.seatunnel.engine.server.execution.Task;
import org.apache.seatunnel.engine.server.execution.TaskGroup;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.master.JobMaster;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;

import com.hazelcast.map.IMap;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Builds the REST response that maps running task groups to workers and task ids for one job. */
public final class TaskMappingBuilder {
    private TaskMappingBuilder() {}

    /** Collects task metadata from the current physical plan and slot ownership maps. */
    public static Map<String, Object> build(SeaTunnelServer server, long jobId) {
        Map<String, Object> root = new HashMap<>();
        root.put("jobId", String.valueOf(jobId));
        root.put("generatedAt", System.currentTimeMillis());

        List<Map<String, Object>> items = new ArrayList<>();
        root.put("items", items);

        JobMaster jobMaster = server.getCoordinatorService().getJobMaster(jobId);
        if (jobMaster == null) {
            return root;
        }

        PhysicalPlan plan = jobMaster.getPhysicalPlan();
        if (plan == null) {
            return root;
        }

        IMap ownedSlotProfilesIMap =
                server.getNodeEngine()
                        .getHazelcastInstance()
                        .getMap(Constant.IMAP_OWNED_SLOT_PROFILES);

        for (SubPlan subPlan : plan.getPipelineList()) {
            appendVertices(items, ownedSlotProfilesIMap, subPlan.getPhysicalVertexList());
            appendVertices(items, ownedSlotProfilesIMap, subPlan.getCoordinatorVertexList());
        }
        return root;
    }

    /**
     * Appends one flat item per task so the UI can map logical vertices back to runtime workers.
     */
    private static void appendVertices(
            List<Map<String, Object>> items,
            IMap ownedSlotProfilesIMap,
            List<PhysicalVertex> vertices) {
        for (PhysicalVertex v : vertices) {
            TaskGroup taskGroup = v.getTaskGroup();
            TaskGroupLocation location = taskGroup.getTaskGroupLocation();
            SlotProfile slotProfile = getSlotProfile(ownedSlotProfilesIMap, location);
            String worker = slotProfile == null ? null : slotProfile.getWorker().toString();
            String taskGroupName = null;
            try {
                taskGroupName =
                        (String)
                                taskGroup
                                        .getClass()
                                        .getMethod("getTaskGroupName")
                                        .invoke(taskGroup);
            } catch (Exception ignored) {
                // keep null for non-default task group implementations
            }

            for (Task task : taskGroup.getTasks()) {
                Map<String, Object> item = new HashMap<>();
                item.put("taskId", String.valueOf(task.getTaskID()));
                item.put("pipelineId", location.getPipelineId());
                item.put("taskGroupId", String.valueOf(location.getTaskGroupId()));
                item.put("taskGroupName", taskGroupName);
                item.put("worker", worker);
                item.put("taskClass", task.getClass().getName());
                items.add(item);
            }
        }
    }

    private static SlotProfile getSlotProfile(
            IMap ownedSlotProfilesIMap, TaskGroupLocation location) {
        Object map = ownedSlotProfilesIMap.get(location.getPipelineLocation());
        if (!(map instanceof Map)) {
            return null;
        }
        Map owned = (Map) map;
        Object slot = owned.get(location);
        if (slot instanceof SlotProfile) {
            return (SlotProfile) slot;
        }
        return null;
    }
}
