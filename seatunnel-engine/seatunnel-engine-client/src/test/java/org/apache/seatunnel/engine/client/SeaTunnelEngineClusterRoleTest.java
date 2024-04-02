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

package org.apache.seatunnel.engine.client;

import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

@DisabledOnOs(OS.WINDOWS)
@Slf4j
public class SeaTunnelEngineClusterRoleTest {

    @SneakyThrows
    @Test
    public void testClusterWillDownWhenNoMasterNode() {
        HazelcastInstanceImpl workerNode1 = null;
        HazelcastInstanceImpl workerNode2 = null;

        String testClusterName = "Test_testElection";

        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig
                .getHazelcastConfig()
                .setClusterName(TestUtils.getClusterName(testClusterName));

        try {
            workerNode1 = SeaTunnelServerStarter.createWorkerHazelcastInstance(seaTunnelConfig);
            workerNode2 = SeaTunnelServerStarter.createWorkerHazelcastInstance(seaTunnelConfig);
            await().atMost(30000, TimeUnit.MILLISECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertTrue(
                                            jobClient.getJobDetailStatus(jobId).contains("RUNNING")
                                                    && jobClient
                                                    .listJobStatus(true)
                                                    .contains("RUNNING")));

        } finally {

            if (node1 != null) {
                node1.shutdown();
            }
        }
    }

    @SneakyThrows
    @Test
    public void testClusterElectionInMixedCluster() {
        HazelcastInstanceImpl masterAndWorkerNode1 = null;
        HazelcastInstanceImpl masterAndWorkerNode2 = null;
        HazelcastInstanceImpl masterNode1 = null;
        HazelcastInstanceImpl masterNode2 = null;
        HazelcastInstanceImpl workerNode1 = null;
        HazelcastInstanceImpl workerNode2 = null;

        String testClusterName = "Test_testElection";

        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig
                .getHazelcastConfig()
                .setClusterName(TestUtils.getClusterName(testClusterName));

        try {
            workerNode1 = SeaTunnelServerStarter.createWorkerHazelcastInstance(seaTunnelConfig);
            workerNode2 = SeaTunnelServerStarter.createWorkerHazelcastInstance(seaTunnelConfig);
            while (true && node1.getLifecycleService().isRunning()) {
                Thread.sleep(1000);
            }

        } finally {

            if (node1 != null) {
                node1.shutdown();
            }
        }
    }
}
