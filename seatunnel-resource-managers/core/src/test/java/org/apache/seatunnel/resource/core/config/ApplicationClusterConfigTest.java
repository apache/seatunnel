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

package org.apache.seatunnel.resource.core.config;

import org.apache.seatunnel.engine.common.config.EngineConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.config.server.CheckpointConfig;
import org.apache.seatunnel.engine.common.config.server.CheckpointStorageConfig;

import org.junit.jupiter.api.Test;

import com.hazelcast.config.JoinConfig;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ApplicationClusterConfigTest {
    @Test
    void applicationRetentionPreservesNativeCheckpointSettings() {
        SeaTunnelConfig config = new SeaTunnelConfig();
        CheckpointConfig original = new CheckpointConfig();
        original.setCheckpointInterval(1500);
        original.setCheckpointTimeout(9000);
        original.setCheckpointMinPause(700);
        original.setSchemaChangeCheckpointTimeout(12000);
        original.setCheckpointEnable(false);
        original.setRetainAfterJobCancelled(false);
        CheckpointStorageConfig storage = new CheckpointStorageConfig();
        storage.setMaxRetainedCheckpoints(7);
        Map<String, String> options = new HashMap<>();
        options.put("namespace", "/persistent-checkpoints");
        options.put("seatunnel.hadoop.dfs.client.socket-timeout", "8000");
        storage.setStoragePluginConfig(options);
        original.setStorage(storage);
        config.getEngineConfig().setCheckpointConfig(original);

        ApplicationClusterConfig.configureCheckpointRetention(config);

        CheckpointConfig actual = config.getEngineConfig().getCheckpointConfig();
        assertEquals(1500, actual.getCheckpointInterval());
        assertEquals(9000, actual.getCheckpointTimeout());
        assertEquals(700, actual.getCheckpointMinPause());
        assertEquals(12000, actual.getSchemaChangeCheckpointTimeout());
        assertFalse(actual.isCheckpointEnable());
        assertTrue(actual.isRetainAfterJobCancelled());
        assertEquals(storage, actual.getStorage());
        assertFalse(original.isRetainAfterJobCancelled());
        actual.getStorage().getStoragePluginConfig().put("namespace", "/different");
        assertEquals("/persistent-checkpoints", options.get("namespace"));
    }

    @Test
    void retainsFullNativeStorageConfigurationWithoutInterpretingBackends() {
        for (String type : new String[] {"local", "hdfs", "oss", "s3", "cos", "custom"}) {
            SeaTunnelConfig config = new SeaTunnelConfig();
            CheckpointConfig checkpoint = new CheckpointConfig();
            CheckpointStorageConfig storage = new CheckpointStorageConfig();
            storage.setStorage(type.equals("custom") ? "custom-plugin" : "hdfs");
            Map<String, String> options = new HashMap<>();
            options.put("storage.type", type);
            options.put("namespace", "/persistent/checkpoints/");
            options.put(type + ".bucket", type + "://bucket");
            options.put("fs." + type + ".endpoint", "object-storage.internal");
            options.put("fs." + type + ".access.key", "test-key");
            options.put("fs." + type + ".secret.key", "test-secret");
            storage.setStoragePluginConfig(options);
            checkpoint.setStorage(storage);
            config.getEngineConfig().setCheckpointConfig(checkpoint);

            ApplicationClusterConfig.configureCheckpointRetention(config);

            CheckpointConfig actual = config.getEngineConfig().getCheckpointConfig();
            assertEquals(storage.getStorage(), actual.getStorage().getStorage());
            assertEquals(options, actual.getStorage().getStoragePluginConfig());
            assertTrue(actual.isRetainAfterJobCancelled());
            assertFalse(checkpoint.isRetainAfterJobCancelled());
        }
    }

    @Test
    void isolatesMasterFromExistingSessionDiscovery() {
        SeaTunnelConfig config = new SeaTunnelConfig();
        config.getHazelcastConfig()
                .getNetworkConfig()
                .getJoin()
                .getTcpIpConfig()
                .setEnabled(true)
                .addMember("session-master:5801");
        config.getHazelcastConfig()
                .getNetworkConfig()
                .getJoin()
                .getKubernetesConfig()
                .setEnabled(true);
        ApplicationClusterConfig.configure(config, "unique-application", null, 2);
        JoinConfig join = config.getHazelcastConfig().getNetworkConfig().getJoin();
        assertFalse(join.getMulticastConfig().isEnabled());
        assertFalse(join.getAutoDetectionConfig().isEnabled());
        assertTrue(join.getTcpIpConfig().isEnabled());
        assertTrue(join.getTcpIpConfig().getMembers().isEmpty());
        assertNull(join.getTcpIpConfig().getRequiredMember());
        assertFalse(join.getKubernetesConfig().isEnabled());
        assertEquals("unique-application", config.getHazelcastConfig().getClusterName());
        assertEquals(EngineConfig.ClusterRole.MASTER, config.getEngineConfig().getClusterRole());
        assertEquals(0, config.getEngineConfig().getBackupCount());
        assertFalse(config.getHazelcastConfig().isLiteMember());
    }

    @Test
    void workerRequiresMasterAndUsesFixedSlots() {
        SeaTunnelConfig config = new SeaTunnelConfig();
        ApplicationClusterConfig.configure(config, "unique-application", "master:5801", 3);
        JoinConfig join = config.getHazelcastConfig().getNetworkConfig().getJoin();
        assertTrue(join.getTcpIpConfig().isEnabled());
        assertEquals("master:5801", join.getTcpIpConfig().getRequiredMember());
        assertEquals(1, join.getTcpIpConfig().getMembers().size());
        assertTrue(config.getHazelcastConfig().isLiteMember());
        assertEquals(EngineConfig.ClusterRole.WORKER, config.getEngineConfig().getClusterRole());
        assertFalse(config.getEngineConfig().getSlotServiceConfig().isDynamicSlot());
        assertEquals(3, config.getEngineConfig().getSlotServiceConfig().getSlotNum());
        assertThrows(
                IllegalArgumentException.class,
                () -> ApplicationClusterConfig.configure(config, "app", "", 1));
    }
}
