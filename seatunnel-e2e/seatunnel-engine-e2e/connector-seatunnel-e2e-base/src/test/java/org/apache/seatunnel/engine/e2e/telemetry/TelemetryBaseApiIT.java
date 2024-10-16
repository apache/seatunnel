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

import org.apache.seatunnel.engine.client.job.ClientJobProxy;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.config.server.TelemetryConfig;
import org.apache.seatunnel.engine.common.config.server.TelemetryMetricConfig;
import org.apache.seatunnel.engine.e2e.TestUtils;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ExecutionException;

@Slf4j
public class TelemetryBaseApiIT {

    private static ClientJobProxy clientJobProxy;

    private static HazelcastInstanceImpl hazelcastInstance;

    private static String testClusterName = "TelemetryApiIT";

    @BeforeAll
    static void beforeClass() throws Exception {
        createBaseTestNode();
    }

    private static void createBaseTestNode() throws ExecutionException, InterruptedException {
        testClusterName = TestUtils.getClusterName(testClusterName);
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig.getHazelcastConfig().setClusterName(testClusterName);
        TelemetryMetricConfig telemetryMetricConfig = new TelemetryMetricConfig();
        telemetryMetricConfig.setEnabled(true);
        TelemetryConfig telemetryConfig = new TelemetryConfig();
        telemetryConfig.setMetric(telemetryMetricConfig);
        seaTunnelConfig.getEngineConfig().setTelemetryConfig(telemetryConfig);
        hazelcastInstance = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);
        TelemetryTestUtils.runJob(seaTunnelConfig, testClusterName);
    }

    @Test
    public void testGetMetrics() throws InterruptedException {
        TelemetryTestUtils.testGetMetrics(hazelcastInstance, testClusterName);
    }

    @AfterAll
    static void afterClass() {
        if (hazelcastInstance != null) {
            hazelcastInstance.shutdown();
        }
    }
}
