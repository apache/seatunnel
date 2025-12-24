/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.seatunnel.engine.e2e.resourceIsolation;

import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.e2e.TestUtils;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManager;

import org.awaitility.Awaitility;
import org.awaitility.core.ThrowingRunnable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import com.hazelcast.config.Config;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.spi.impl.NodeEngineImpl;
import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Slf4j
public class WorkerTagClusterTest {

    HazelcastInstanceImpl masterNode1 = null;
    HazelcastInstanceImpl workerNode1 = null;
    String testClusterName = "WorkerTagClusterTest";

    @BeforeEach
    public void before() {
        SeaTunnelConfig masterNode1Config = getSeaTunnelConfig(testClusterName);
        SeaTunnelConfig workerNode1Config = getSeaTunnelConfig(testClusterName);
        masterNode1 = SeaTunnelServerStarter.createMasterHazelcastInstance(masterNode1Config);
        workerNode1 = SeaTunnelServerStarter.createWorkerHazelcastInstance(workerNode1Config);
    }

    @AfterEach
    void afterClass() {
        if (masterNode1 != null) {
            masterNode1.shutdown();
        }
        if (workerNode1 != null) {
            workerNode1.shutdown();
        }
    }

    @Test
    public void testTagMatch() throws Exception {
        Map<String, String> tag = new HashMap<>();
        tag.put("group", "platform");
        tag.put("team", "team1");
        testTagFilter(tag, 1);
    }

    @Test
    public void testTagMatch2() throws Exception {
        testTagFilter(null, 1);
    }

    @Test
    public void testTagNotMatch() throws Exception {
        Map<String, String> tag = new HashMap<>();
        tag.put("group", "platform");
        tag.put("team", "team1111111");
        testTagFilter(tag, 0);
    }

    @Test
    public void testTagNotMatch2() throws Exception {
        testTagFilter(new HashMap<>(), 1);
    }

    public void testTagFilter(Map<String, String> tagFilter, int expectedWorkerCount)
            throws Exception {
        // waiting all node added to cluster
        Awaitility.await()
                .atMost(10000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        new ThrowingRunnable() {
                            @Override
                            public void run() throws Throwable {
                                Thread.sleep(2000);
                                // check master and worker node
                                Assertions.assertEquals(
                                        2, masterNode1.getCluster().getMembers().size());
                                NodeEngineImpl nodeEngine = masterNode1.node.nodeEngine;
                                SeaTunnelServer server =
                                        nodeEngine.getService(SeaTunnelServer.SERVICE_NAME);
                                ResourceManager resourceManager =
                                        server.getCoordinatorService().getResourceManager();
                                // if tag matched, then worker count is 1  else 0
                                int workerCount = resourceManager.workerCount(tagFilter);
                                Assertions.assertEquals(expectedWorkerCount, workerCount);
                            }
                        });
    }

    private static SeaTunnelConfig getSeaTunnelConfig(String testClusterName) {
        Config hazelcastConfig = Config.loadFromString(getHazelcastConfig());
        hazelcastConfig.setClusterName(TestUtils.getClusterName(testClusterName));
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig.setHazelcastConfig(hazelcastConfig);
        return seaTunnelConfig;
    }

    protected static String getHazelcastConfig() {
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
}
