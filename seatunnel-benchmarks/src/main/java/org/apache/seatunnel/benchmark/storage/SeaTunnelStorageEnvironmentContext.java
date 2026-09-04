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

package org.apache.seatunnel.benchmark.storage;

import org.apache.seatunnel.benchmark.BenchmarkTemplates;
import org.apache.seatunnel.benchmark.SeaTunnelEnvironmentContext;
import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.common.statestore.EngineStateStores;

import com.hazelcast.config.Config;
import com.hazelcast.config.YamlConfigBuilder;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;

/** Single-member Zeta environment with local checkpoint and IMap persistence enabled. */
public class SeaTunnelStorageEnvironmentContext extends SeaTunnelEnvironmentContext {

    private static final String ENGINE_STORAGE_TEMPLATE =
            BenchmarkTemplates.load("/benchmark/engine-storage.yaml.template");
    private static final String HAZELCAST_STORAGE_TEMPLATE =
            BenchmarkTemplates.load("/benchmark/hazelcast-storage.yaml.template");

    @Override
    protected String embeddedEngineConfiguration() {
        return BenchmarkTemplates.render(
                ENGINE_STORAGE_TEMPLATE,
                "slot_count",
                SLOT_COUNT,
                "checkpoint_directory",
                checkpointDirectory().toAbsolutePath());
    }

    @Override
    protected SeaTunnelConfig createSeaTunnelConfig(String clusterName) {
        SeaTunnelConfig config = super.createSeaTunnelConfig(clusterName);
        SeaTunnelConfig fileConfig =
                ConfigProvider.locateAndGetSeaTunnelConfigFromString(embeddedEngineConfiguration());
        config.getEngineConfig()
                .setCheckpointConfig(fileConfig.getEngineConfig().getCheckpointConfig());
        config.getEngineConfig()
                .setStateCleanupDelayMillis(
                        fileConfig.getEngineConfig().getStateCleanupDelayMillis());

        String hazelcastYaml =
                BenchmarkTemplates.render(
                        HAZELCAST_STORAGE_TEMPLATE,
                        "cluster_name",
                        clusterName,
                        "imap_directory",
                        imapDirectory().toAbsolutePath());
        Config hazelcastConfig =
                new YamlConfigBuilder(
                                new ByteArrayInputStream(
                                        hazelcastYaml.getBytes(StandardCharsets.UTF_8)))
                        .build();
        config.setHazelcastConfig(hazelcastConfig);
        return config;
    }

    /** Returns the active server backing the embedded storage benchmark member. */
    public final SeaTunnelServer getServer() {
        HazelcastInstanceImpl instance = (HazelcastInstanceImpl) getMiniCluster();
        return instance.node.getNodeEngine().getService(SeaTunnelServer.SERVICE_NAME);
    }

    /** Returns production state-store facades backed by the embedded Hazelcast maps. */
    public final EngineStateStores getStateStores() {
        return getServer().getEngineContext().getStateStores();
    }

    /** Returns the client used only to prepare real benchmark fixtures outside measured code. */
    public final SeaTunnelClient storageClient() {
        return getClient();
    }

    /** Returns the client/server configuration used to submit fixture jobs. */
    public final SeaTunnelConfig storageConfig() {
        return getSeaTunnelConfig();
    }

    /** Returns the temporary home owned by this JMH trial. */
    public final Path storageHome() {
        return getMiniClusterHome();
    }

    /** Returns the directory used by checkpoint result storage. */
    public final Path checkpointDirectory() {
        return getMiniClusterHome().resolve("storage").resolve("checkpoint");
    }

    /** Returns the directory used by the file-backed IMap MapStore. */
    public final Path imapDirectory() {
        return getMiniClusterHome().resolve("storage").resolve("imap");
    }
}
