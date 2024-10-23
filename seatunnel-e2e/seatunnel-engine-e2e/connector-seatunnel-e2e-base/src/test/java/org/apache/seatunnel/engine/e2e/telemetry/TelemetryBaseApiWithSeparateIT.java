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

import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.e2e.TestUtils;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;

import com.hazelcast.instance.impl.HazelcastInstanceImpl;

import java.util.concurrent.TimeUnit;

public class TelemetryBaseApiWithSeparateIT extends AbstractTelemetryBaseIT {
    private static String testClusterNameWithSeparate = "TelemetryApiWithSeparateIT";

    private static HazelcastInstanceImpl workerNode1 = null;
    private static HazelcastInstanceImpl workerNode2 = null;
    private static HazelcastInstanceImpl masterNode = null;

    @Override
    public void open(SeaTunnelConfig[] seaTunnelConfigs) {
        createMasterAndWorker(seaTunnelConfigs);
    }

    public void createMasterAndWorker(SeaTunnelConfig[] seaTunnelConfigs) {

        // master node must start first in ci
        masterNode = SeaTunnelServerStarter.createMasterHazelcastInstance(seaTunnelConfigs[0]);
        Awaitility.await()
                .atMost(10000, TimeUnit.MILLISECONDS)
                .untilAsserted(
                        () ->
                                Assertions.assertEquals(
                                        1, masterNode.getCluster().getMembers().size()));
        // start two worker nodes
        workerNode1 = SeaTunnelServerStarter.createWorkerHazelcastInstance(seaTunnelConfigs[1]);
        workerNode2 = SeaTunnelServerStarter.createWorkerHazelcastInstance(seaTunnelConfigs[2]);

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
    }

    @Override
    public void close() throws Exception {
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

    @Override
    public HazelcastInstanceImpl getHazelcastInstance() throws Exception {
        return masterNode;
    }

    @Override
    public int getNodeCount() {
        return 3;
    }

    @Override
    public String getClusterName() {
        return TestUtils.getClusterName(testClusterNameWithSeparate);
    }
}
