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
import org.apache.seatunnel.engine.common.config.server.ConnectorJarStorageConfig;
import org.apache.seatunnel.engine.common.config.server.HttpConfig;
import org.apache.seatunnel.engine.common.config.server.SlotServiceConfig;
import org.apache.seatunnel.engine.common.runtime.ExecutionMode;

import com.hazelcast.config.JoinConfig;

import java.util.HashMap;
import java.util.Map;

/**
 * Prepares caller-owned Engine configuration for an isolated, fixed-worker application.
 *
 * <p>This stateless utility does not start processes or allocate resources. Call its methods before
 * constructing an Engine member, while the caller has exclusive access to the configuration.
 * Concurrent mutation of the same configuration is not supported.
 */
public final class ApplicationClusterConfig {
    private ApplicationClusterConfig() {}

    /**
     * Selects application membership, node role, fixed slots and distribution-local connector jars.
     *
     * <p>A null master address configures the sole master. Workers require the specified master and
     * cannot start an independent cluster. Both roles use TCP discovery without inherited session
     * peers, zero backup replicas and lifecycle hooks owned by the application runtime. This method
     * mutates only the supplied configuration and does not transfer its ownership.
     *
     * @param config exclusively owned configuration to prepare before member startup
     * @param clusterName nonempty identity shared only by this application's members
     * @param masterAddress master host and port for workers, or null for the master
     * @param slots fixed slot count already validated by the deployment specification
     * @throws IllegalArgumentException if the cluster name or a supplied master address is empty
     * @throws NullPointerException if config is null
     */
    public static void configure(
            SeaTunnelConfig config, String clusterName, String masterAddress, int slots) {
        if (clusterName == null || clusterName.trim().isEmpty()) {
            throw new IllegalArgumentException("An application cluster name is required");
        }
        boolean worker = masterAddress != null;
        JoinConfig join = new JoinConfig();
        join.getAutoDetectionConfig().setEnabled(false);
        join.getMulticastConfig().setEnabled(false);
        // Hazelcast requires every member to use the same joiner. The master starts alone with
        // no discovery peers; workers join its explicit address in this unique application cluster.
        join.getTcpIpConfig().setEnabled(true);
        if (worker) {
            if (masterAddress.trim().isEmpty()) {
                throw new IllegalArgumentException("An application master address is required");
            }
            // requiredMember prevents a worker from forming its own cluster if the master is down.
            join.getTcpIpConfig()
                    .addMember(masterAddress)
                    .setRequiredMember(masterAddress)
                    .setConnectionTimeoutSeconds(10);
        }
        config.getHazelcastConfig().setClusterName(clusterName).setLiteMember(worker);
        config.getHazelcastConfig().getNetworkConfig().setJoin(join).setPublicAddress(null);
        config.getHazelcastConfig().setProperty("hazelcast.shutdownhook.enabled", "false");
        config.getHazelcastConfig().setProperty("hazelcast.discovery.enabled", "false");
        config.getHazelcastConfig().setProperty("hazelcast.graceful.shutdown.max.wait", "10");
        config.getHazelcastConfig().setProperty("hazelcast.max.join.seconds", "60");
        config.getEngineConfig()
                .setClusterRole(
                        worker ? EngineConfig.ClusterRole.WORKER : EngineConfig.ClusterRole.MASTER);
        config.getEngineConfig().setBackupCount(0);
        config.getEngineConfig().setMode(ExecutionMode.CLUSTER);
        // Every application container localizes the same distribution. Upload identifiers contain
        // absolute master paths and are unsuitable for YARN's per-container localization roots.
        ConnectorJarStorageConfig jars = new ConnectorJarStorageConfig();
        jars.setEnable(false);
        config.getEngineConfig().setConnectorJarStorageConfig(jars);
        HttpConfig http = new HttpConfig();
        http.setEnabled(false);
        http.setEnableHttps(false);
        config.getEngineConfig().setHttpConfig(http);
        SlotServiceConfig slotConfig = new SlotServiceConfig();
        slotConfig.setDynamicSlot(false);
        slotConfig.setSlotNum(slots);
        config.getEngineConfig().setSlotServiceConfig(slotConfig);
    }

    /**
     * Configures checkpoint retention for the application lifecycle.
     *
     * <p>Application cleanup cancels an unfinished native job, including after worker failure, so
     * cancellation retains checkpoints by default. An explicit job-level retention option still
     * overrides this default. Existing timing, retention limits, and plugin options are copied so
     * distribution defaults are not mutated. The complete native storage backend is preserved
     * without interpreting plugin options, including credentials and endpoint settings.
     *
     * <p>Call before master startup with exclusive access to the configuration. The replacement
     * checkpoint configuration and its plugin map are owned by the caller; prior configuration
     * objects remain unchanged. This method neither opens storage nor validates backend options.
     *
     * @param config exclusively owned application master configuration to update
     * @throws NullPointerException if config or its native checkpoint settings are null
     */
    public static void configureCheckpointRetention(SeaTunnelConfig config) {
        CheckpointConfig previous = config.getEngineConfig().getCheckpointConfig();
        CheckpointConfig checkpoint = new CheckpointConfig();
        checkpoint.setCheckpointInterval(previous.getCheckpointInterval());
        checkpoint.setCheckpointTimeout(previous.getCheckpointTimeout());
        checkpoint.setCheckpointMinPause(previous.getCheckpointMinPause());
        checkpoint.setSchemaChangeCheckpointTimeout(previous.getSchemaChangeCheckpointTimeout());
        checkpoint.setRetainAfterJobCancelled(true);
        checkpoint.setCheckpointEnable(previous.isCheckpointEnable());
        CheckpointStorageConfig storage = new CheckpointStorageConfig();
        storage.setStorage(previous.getStorage().getStorage());
        storage.setMaxRetainedCheckpoints(previous.getStorage().getMaxRetainedCheckpoints());
        Map<String, String> options = new HashMap<>(previous.getStorage().getStoragePluginConfig());
        storage.setStoragePluginConfig(options);
        checkpoint.setStorage(storage);
        config.getEngineConfig().setCheckpointConfig(checkpoint);
    }
}
