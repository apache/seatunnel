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
import org.apache.seatunnel.engine.server.resourcemanager.resource.ResourceProfile;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.hazelcast.cluster.Address;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiConsumer;

class RunningJobSlotUsageBuilderTest {

    @Test
    void shouldAggregateSlotUsageForRunningJobsOnly() throws UnknownHostException {
        Address firstWorker = new Address("127.0.0.1", 5801);
        Address secondWorker = new Address("127.0.0.1", 5802);
        Map<PipelineLocation, Map<TaskGroupLocation, SlotProfile>> ownedSlotProfiles =
                new HashMap<>();
        ownedSlotProfiles.put(
                new PipelineLocation(1L, 1),
                taskGroupSlots(slot(1L, firstWorker, 1), slot(1L, secondWorker, 2)));
        ownedSlotProfiles.put(
                new PipelineLocation(1L, 2), taskGroupSlots(slot(1L, firstWorker, 3), null));
        ownedSlotProfiles.put(
                new PipelineLocation(3L, 1), taskGroupSlots(slot(3L, firstWorker, 4)));

        SlotProfile reusedByOtherJob = slot(2L, firstWorker, 3);
        List<SlotProfile> assignedSlots =
                new ArrayList<>(
                        Arrays.asList(
                                ownedSlotProfiles
                                        .get(new PipelineLocation(1L, 1))
                                        .values()
                                        .toArray(new SlotProfile[0])));
        assignedSlots.add(reusedByOtherJob);
        List<Map<String, Object>> result =
                RunningJobSlotUsageBuilder.build(
                        ownedSlotProfiles, new HashSet<>(Arrays.asList(1L, 2L)), assignedSlots);

        Assertions.assertEquals(2, result.size());
        Assertions.assertEquals("1", result.get(0).get("jobId"));
        Assertions.assertEquals(2, result.get(0).get("slotCount"));
        Map<Integer, Integer> firstPipelineSlotCounts = pipelineSlotCounts(result.get(0));
        Map<String, Integer> firstWorkerSlotCounts = workerSlotCounts(result.get(0));
        Assertions.assertEquals(2, firstPipelineSlotCounts.get(1));
        Assertions.assertFalse(firstPipelineSlotCounts.containsKey(2));
        Assertions.assertEquals(1, firstWorkerSlotCounts.get(firstWorker.toString()));
        Assertions.assertEquals(1, firstWorkerSlotCounts.get(secondWorker.toString()));
        Assertions.assertTrue(slotSourceAvailable(result.get(0)));
        Assertions.assertEquals("2", result.get(1).get("jobId"));
        Assertions.assertEquals(0, result.get(1).get("slotCount"));
        Assertions.assertTrue(pipelineSlotCounts(result.get(1)).isEmpty());
        Assertions.assertTrue(workerSlotCounts(result.get(1)).isEmpty());
        Assertions.assertTrue(slotSourceAvailable(result.get(1)));
    }

    @Test
    void shouldMarkSlotSourceUnavailableWhenResourceManagerIsNotInitialized()
            throws UnknownHostException {
        SeaTunnelServer server =
                mockServer(
                        Collections.singleton(1L), ownedSlotProfilesForRunningJob(), null, false);

        List<Map<String, Object>> result = RunningJobSlotUsageBuilder.build(server);

        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals("1", result.get(0).get("jobId"));
        Assertions.assertEquals(0, result.get(0).get("slotCount"));
        Assertions.assertFalse(slotSourceAvailable(result.get(0)));
    }

    @Test
    void shouldMarkSlotSourceUnavailableWhenAssignedSlotsLagBehindOwnedSlots()
            throws UnknownHostException {
        SeaTunnelServer server =
                mockServer(
                        Collections.singleton(1L),
                        ownedSlotProfilesForRunningJob(),
                        Collections.emptyList(),
                        true);

        List<Map<String, Object>> result = RunningJobSlotUsageBuilder.build(server);

        Assertions.assertEquals(1, result.size());
        Assertions.assertEquals("1", result.get(0).get("jobId"));
        Assertions.assertEquals(0, result.get(0).get("slotCount"));
        Assertions.assertFalse(slotSourceAvailable(result.get(0)));
    }

    private Map<TaskGroupLocation, SlotProfile> taskGroupSlots(SlotProfile... slots) {
        Map<TaskGroupLocation, SlotProfile> taskGroupSlots = new HashMap<>();
        for (int i = 0; i < slots.length; i++) {
            SlotProfile slot = slots[i];
            long jobId = slot == null ? 1L : slot.getOwnerJobID();
            taskGroupSlots.put(new TaskGroupLocation(jobId, 1, i), slot);
        }
        return taskGroupSlots;
    }

    private SlotProfile slot(long jobId, Address worker, int slotId) {
        SlotProfile slotProfile = new SlotProfile(worker, slotId, new ResourceProfile(), "test");
        slotProfile.assign(jobId);
        return slotProfile;
    }

    private Map<PipelineLocation, Map<TaskGroupLocation, SlotProfile>>
            ownedSlotProfilesForRunningJob() throws UnknownHostException {
        Address worker = new Address("127.0.0.1", 5801);
        Map<PipelineLocation, Map<TaskGroupLocation, SlotProfile>> ownedSlotProfiles =
                new HashMap<>();
        ownedSlotProfiles.put(new PipelineLocation(1L, 1), taskGroupSlots(slot(1L, worker, 1)));
        return ownedSlotProfiles;
    }

    @SuppressWarnings("unchecked")
    private SeaTunnelServer mockServer(
            Set<Long> runningJobIds,
            Map<PipelineLocation, Map<TaskGroupLocation, SlotProfile>> ownedSlotProfiles,
            List<SlotProfile> assignedSlots,
            boolean resourceManagerInitialized) {
        SeaTunnelServer server = Mockito.mock(SeaTunnelServer.class);
        NodeEngineImpl nodeEngine = Mockito.mock(NodeEngineImpl.class);
        HazelcastInstanceImpl hazelcastInstance = Mockito.mock(HazelcastInstanceImpl.class);
        CoordinatorService coordinatorService = Mockito.mock(CoordinatorService.class);
        IMap<Long, JobInfo> runningJobInfo = Mockito.mock(IMap.class);
        IMap<PipelineLocation, Map<TaskGroupLocation, SlotProfile>> ownedSlotProfilesMap =
                Mockito.mock(IMap.class);

        Mockito.when(server.getNodeEngine()).thenReturn(nodeEngine);
        Mockito.when(nodeEngine.getHazelcastInstance()).thenReturn(hazelcastInstance);
        Mockito.when(server.getCoordinatorService()).thenReturn(coordinatorService);
        Mockito.when(runningJobInfo.keySet()).thenReturn(runningJobIds);
        runningJobIds.forEach(
                jobId ->
                        Mockito.when(coordinatorService.shouldShowAsRunningJob(jobId))
                                .thenReturn(true));

        if (resourceManagerInitialized) {
            ResourceManager resourceManager = Mockito.mock(ResourceManager.class);
            Mockito.when(coordinatorService.getInitializedResourceManager())
                    .thenReturn(resourceManager);
            Mockito.when(resourceManager.getAssignedSlots(Mockito.anyMap()))
                    .thenReturn(assignedSlots);
        } else {
            Mockito.when(coordinatorService.getInitializedResourceManager()).thenReturn(null);
        }

        Mockito.when(ownedSlotProfilesMap.isEmpty()).thenReturn(ownedSlotProfiles.isEmpty());
        Mockito.doAnswer(
                        invocation -> {
                            BiConsumer<PipelineLocation, Map<TaskGroupLocation, SlotProfile>>
                                    consumer = invocation.getArgument(0);
                            ownedSlotProfiles.forEach(consumer);
                            return null;
                        })
                .when(ownedSlotProfilesMap)
                .forEach(Mockito.any());

        Mockito.when(hazelcastInstance.getMap(Constant.IMAP_RUNNING_JOB_INFO))
                .thenReturn((IMap) runningJobInfo);
        Mockito.when(hazelcastInstance.getMap(Constant.IMAP_OWNED_SLOT_PROFILES))
                .thenReturn((IMap) ownedSlotProfilesMap);
        return server;
    }

    @SuppressWarnings("unchecked")
    private Map<Integer, Integer> pipelineSlotCounts(Map<String, Object> usage) {
        return (Map<Integer, Integer>) usage.get("pipelineSlotCounts");
    }

    @SuppressWarnings("unchecked")
    private Map<String, Integer> workerSlotCounts(Map<String, Object> usage) {
        return (Map<String, Integer>) usage.get("workerSlotCounts");
    }

    private boolean slotSourceAvailable(Map<String, Object> usage) {
        return Boolean.TRUE.equals(usage.get("slotSourceAvailable"));
    }
}
