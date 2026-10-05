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

package org.apache.seatunnel.engine.server.master.cleanup;

import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.core.job.PipelineStatus;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.TestUtils;
import org.apache.seatunnel.engine.server.dag.physical.PipelineLocation;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.cluster.Address;
import com.hazelcast.config.Config;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.map.IMap;

import java.io.IOException;
import java.net.ServerSocket;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

class PipelineCleanupRecordHazelcastSerializationTest {

    @Test
    void testPutAndGetAcrossMembers() {
        String clusterName =
                TestUtils.getClusterName(
                        "PipelineCleanupRecordHazelcastSerializationTest_testPutAndGetAcrossMembers");
        int[] ports = findTwoFreePorts();
        HazelcastInstanceImpl instance1 = createHazelcastInstance(clusterName, ports[0], ports[1]);
        HazelcastInstanceImpl instance2 = createHazelcastInstance(clusterName, ports[1], ports[0]);
        try {
            await().atMost(30, TimeUnit.SECONDS)
                    .until(() -> instance1.getCluster().getMembers().size() == 2);

            PipelineLocation pipelineLocation = new PipelineLocation(1L, 1);
            TaskGroupLocation taskGroupLocation = new TaskGroupLocation(1L, 1, 1L);
            Address workerAddress = instance1.getCluster().getLocalMember().getAddress();
            Map<TaskGroupLocation, Address> taskGroups = new HashMap<>();
            taskGroups.put(taskGroupLocation, workerAddress);

            PipelineCleanupRecord record =
                    new PipelineCleanupRecord(
                            pipelineLocation,
                            PipelineStatus.CANCELED,
                            false,
                            taskGroups,
                            new HashSet<>(Collections.singleton(taskGroupLocation)),
                            true,
                            100L,
                            200L,
                            3);

            IMap<PipelineLocation, PipelineCleanupRecord> map1 =
                    instance1.getMap(Constant.IMAP_PENDING_PIPELINE_CLEANUP);
            IMap<PipelineLocation, PipelineCleanupRecord> map2 =
                    instance2.getMap(Constant.IMAP_PENDING_PIPELINE_CLEANUP);

            map1.put(pipelineLocation, record);

            await().atMost(30, TimeUnit.SECONDS).until(() -> map2.containsKey(pipelineLocation));

            PipelineCleanupRecord read = map2.get(pipelineLocation);
            Assertions.assertNotNull(read);
            Assertions.assertEquals(pipelineLocation, read.getPipelineLocation());
            Assertions.assertEquals(PipelineStatus.CANCELED, read.getFinalStatus());
            Assertions.assertFalse(read.isSavepointEnd());
            Assertions.assertTrue(read.isMetricsImapCleaned());
            Assertions.assertEquals(100L, read.getCreateTimeMillis());
            Assertions.assertEquals(200L, read.getLastAttemptTimeMillis());
            Assertions.assertEquals(3, read.getAttemptCount());
            Assertions.assertEquals(workerAddress, read.getTaskGroups().get(taskGroupLocation));
            Assertions.assertTrue(read.getCleanedTaskGroups().contains(taskGroupLocation));
            Assertions.assertTrue(read.isCleaned());
        } finally {
            instance1.shutdown();
            instance2.shutdown();
        }
    }

    private HazelcastInstanceImpl createHazelcastInstance(
            String clusterName, int localPort, int peerPort) {
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        Config hazelcastConfig =
                Config.loadFromString(buildHazelcastConfig(clusterName, localPort, peerPort));
        seaTunnelConfig.setHazelcastConfig(hazelcastConfig);
        return SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);
    }

    private String buildHazelcastConfig(String clusterName, int localPort, int peerPort) {
        return "hazelcast:\n"
                + "  cluster-name: "
                + clusterName
                + "\n"
                + "  network:\n"
                + "    join:\n"
                + "      tcp-ip:\n"
                + "        enabled: true\n"
                + "        member-list:\n"
                + "          - 127.0.0.1:"
                + localPort
                + "\n"
                + "          - 127.0.0.1:"
                + peerPort
                + "\n"
                + "    port:\n"
                + "      auto-increment: false\n"
                + "      port-count: 1\n"
                + "      port: "
                + localPort
                + "\n";
    }

    private int[] findTwoFreePorts() {
        try (ServerSocket first = new ServerSocket(0);
                ServerSocket second = new ServerSocket(0)) {
            first.setReuseAddress(true);
            second.setReuseAddress(true);
            return new int[] {first.getLocalPort(), second.getLocalPort()};
        } catch (IOException e) {
            throw new RuntimeException("No free Hazelcast ports available", e);
        }
    }
}
