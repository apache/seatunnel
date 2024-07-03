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

package org.apache.seatunnel.engine.e2e;

import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.server.SeaTunnelServerStarter;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.config.Config;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;

import java.util.Collections;
import java.util.Map;

public class LocalModeIT {

    SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();

    @Test
    public void localModeWithPortNotInDefaultRange() {

        HazelcastInstanceImpl node1 = null;
        SeaTunnelClient engineClient = null;
        try {
            Config hazelcastConfig = seaTunnelConfig.getHazelcastConfig();
            hazelcastConfig.getNetworkConfig().setPort(9999);
            SeaTunnelConfig updatedConfig = new SeaTunnelConfig();
            updatedConfig.setHazelcastConfig(hazelcastConfig);
            node1 = SeaTunnelServerStarter.createHazelcastInstance(updatedConfig);
            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig
                    .getConnectionStrategyConfig()
                    .getConnectionRetryConfig()
                    .setClusterConnectTimeoutMillis(3000);
            Assertions.assertThrows(
                    IllegalStateException.class,
                    () -> new SeaTunnelClient(clientConfig),
                    "Unable to connect to any cluster.");
        } finally {
            if (engineClient != null) {
                engineClient.close();
            }
            if (node1 != null) {
                node1.shutdown();
            }
        }
    }

    @Test
    public void localMode() {
        HazelcastInstanceImpl node1 = null;
        HazelcastInstanceImpl node2 = null;
        SeaTunnelClient engineClient = null;
        String cluster_name = "new_cluster_name";
        try {
            node1 = SeaTunnelServerStarter.createHazelcastInstance(seaTunnelConfig);

            Config hazelcastConfig = seaTunnelConfig.getHazelcastConfig();
            hazelcastConfig.setClusterName(cluster_name).getNetworkConfig().setPort(9999);
            SeaTunnelConfig updatedConfig = new SeaTunnelConfig();
            updatedConfig.setHazelcastConfig(hazelcastConfig);
            node2 = SeaTunnelServerStarter.createHazelcastInstance(updatedConfig);

            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            clientConfig.setClusterName(cluster_name);
            clientConfig
                    .getNetworkConfig()
                    .setAddresses(Collections.singletonList("localhost:9999"));
            engineClient = new SeaTunnelClient(clientConfig);

            Map<String, String> clusterHealthMetrics = engineClient.getClusterHealthMetrics();
            Assertions.assertEquals(1, clusterHealthMetrics.size());
            Assertions.assertTrue(clusterHealthMetrics.containsKey("[localhost]:9999"));
        } finally {
            if (engineClient != null) {
                engineClient.close();
            }
            if (node1 != null) {
                node1.shutdown();
            }
            if (node2 != null) {
                node2.shutdown();
            }
        }
    }
}
