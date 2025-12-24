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

import org.apache.seatunnel.engine.server.AbstractSeaTunnelServerTest;
import org.apache.seatunnel.engine.server.resourcemanager.resource.CPU;
import org.apache.seatunnel.engine.server.resourcemanager.resource.Memory;
import org.apache.seatunnel.engine.server.resourcemanager.resource.ResourceProfile;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

public class WorkerTagTest extends AbstractSeaTunnelServerTest<WorkerTagTest> {

    private ResourceManager resourceManager;

    private final long jobId = 5;

    @BeforeAll
    public void before() {
        super.before();
        resourceManager = server.getCoordinatorService().getResourceManager();
        server.getSlotService();
    }

    @Override
    protected String getHazelcastConfig() {
        // for the use case not set node attribute, it tested in ResourceManagerTest and
        // FixSlotResourceTest
        return "hazelcast:\n"
                + "  cluster-name: seatunnel\n"
                + "  network:\n"
                + "    rest-api:\n"
                + "      enabled: true\n"
                + "      endpoint-groups:\n"
                + "        CLUSTER_WRITE:\n"
                + "          enabled: true\n"
                + "    join:\n"
                + "      tcp-ip:\n"
                + "        enabled: true\n"
                + "        member-list:\n"
                + "          - localhost\n"
                + "    port:\n"
                + "      auto-increment: true\n"
                + "      port-count: 100\n"
                + "      port: 5801\n"
                + "\n"
                + "  properties:\n"
                + "    hazelcast.invocation.max.retry.count: 200\n"
                + "    hazelcast.tcp.join.port.try.count: 30\n"
                + "    hazelcast.invocation.retry.pause.millis: 2000\n"
                + "    hazelcast.slow.operation.detector.stacktrace.logging.enabled: true\n"
                + "    hazelcast.logging.type: log4j2\n"
                + "    hazelcast.operation.generic.thread.count: 200\n"
                + "  member-attributes:\n"
                + "    group:\n"
                + "      type: string\n"
                + "      value: platform\n"
                + "    team:\n"
                + "      type: string\n"
                + "      value: team1";
    }

    @Test
    public void testTagMatch() {
        Map<String, String> tag = new HashMap<>();
        tag.put("group", "platform");
        tag.put("team", "team1");
        Assertions.assertDoesNotThrow(() -> testApplyResourceByTag(tag));
    }

    @Test
    public void testNullTag() {
        Assertions.assertDoesNotThrow(() -> testApplyResourceByTag(null));
    }

    @Test
    public void testTagNotMatch() {
        Map<String, String> tag = new HashMap<>();
        tag.put("group", "platform");
        tag.put("team", "team2");
        Assertions.assertThrows(NoEnoughResourceException.class, () -> testApplyResourceByTag(tag));
    }

    private void testApplyResourceByTag(Map<String, String> tag)
            throws ExecutionException, InterruptedException {
        List<ResourceProfile> resourceProfiles = new ArrayList<>();
        resourceProfiles.add(new ResourceProfile(CPU.of(0), Memory.of(100)));
        List<SlotProfile> slotProfiles =
                resourceManager.applyResources(jobId, resourceProfiles, tag).get();

        Assertions.assertEquals(
                resourceProfiles.get(0).getHeapMemory().getBytes(),
                slotProfiles.get(0).getResourceProfile().getHeapMemory().getBytes());

        resourceManager.releaseResources(jobId, slotProfiles).get();
    }
}
