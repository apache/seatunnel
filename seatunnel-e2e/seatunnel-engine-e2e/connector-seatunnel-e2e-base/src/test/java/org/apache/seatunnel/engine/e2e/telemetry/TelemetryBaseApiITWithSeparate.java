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

package org.apache.seatunnel.engine.e2e.telemetry;

import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.config.server.TelemetryConfig;
import org.apache.seatunnel.engine.common.config.server.TelemetryMetricConfig;
import org.apache.seatunnel.engine.e2e.TestUtils;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.hazelcast.config.Config;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

@Slf4j
public class TelemetryBaseApiITWithSeparate {
    private static String testClusterNameWithSeparate = "TelemetryApiITWithSeparate";

    private static HazelcastInstanceImpl workerNode1 = null;
    private static HazelcastInstanceImpl workerNode2 = null;
    private static HazelcastInstanceImpl masterNode = null;

    @BeforeAll
    static void beforeClass() throws Exception {
        createMasterAndWorker();
    }

    @Test
    public void testGetMetricsWithSeparate() throws InterruptedException {
        // TelemetryTestUtils.testGetMetrics(masterNode, testClusterNameWithSeparate);
    }

    private static void createMasterAndWorker() throws ExecutionException, InterruptedException {
        testClusterNameWithSeparate = TestUtils.getClusterName(testClusterNameWithSeparate);
        SeaTunnelConfig seaTunnelConfig = getSeaTunnelConfig(testClusterNameWithSeparate);

        // master node must start first in ci
        masterNode = SeaTunnelServerStarter.createMasterHazelcastInstance(seaTunnelConfig);
        Awaitility.await()
                .atMost(10000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        1, masterNode.getCluster().getMembers().size()));
        // start two worker nodes
        workerNode1 = SeaTunnelServerStarter.createWorkerHazelcastInstance(seaTunnelConfig);
        workerNode2 = SeaTunnelServerStarter.createWorkerHazelcastInstance(seaTunnelConfig);

        Awaitility.await()
                .atMost(10000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        3, workerNode1.getCluster().getMembers().size()));
        Awaitility.await()
                .atMost(10000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        3, workerNode2.getCluster().getMembers().size()));

        TelemetryTestUtils.runJob(seaTunnelConfig, testClusterNameWithSeparate);
    }

    private static SeaTunnelConfig getSeaTunnelConfig(String testClusterName) {
        Config hazelcastConfig = Config.loadFromString(getHazelcastConfig());
        hazelcastConfig.setClusterName(testClusterName);
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig.setHazelcastConfig(hazelcastConfig);
        TelemetryMetricConfig telemetryMetricConfig = new TelemetryMetricConfig();
        telemetryMetricConfig.setEnabled(true);
        TelemetryConfig telemetryConfig = new TelemetryConfig();
        telemetryConfig.setMetric(telemetryMetricConfig);
        seaTunnelConfig.getEngineConfig().setTelemetryConfig(telemetryConfig);
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
                + "  properties:\n"
                + "    hazelcast.invocation.max.retry.count: 200\n"
                + "    hazelcast.tcp.join.port.try.count: 30\n"
                + "    hazelcast.invocation.retry.pause.millis: 2000\n"
                + "    hazelcast.slow.operation.detector.stacktrace.logging.enabled: true\n"
                + "    hazelcast.logging.type: log4j2\n"
                + "    hazelcast.operation.generic.thread.count: 200\n";
    }

    @AfterAll
    static void afterClass() {
        if (workerNode1 != null) {
            workerNode1.shutdown();
        }
        if (workerNode2 != null) {
            workerNode2.shutdown();
        }
        if (masterNode != null) {
            masterNode.shutdown();
        }
    }
}
