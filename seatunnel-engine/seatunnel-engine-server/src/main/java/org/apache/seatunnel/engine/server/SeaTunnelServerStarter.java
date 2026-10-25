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

package org.apache.seatunnel.engine.server;

import org.apache.seatunnel.engine.common.Constant;
import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.EngineConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.server.resourcemanager.ResourceManagerFactory;
import org.apache.seatunnel.engine.server.telemetry.metrics.ExportsInstanceInitializer;

import com.hazelcast.config.MapConfig;
import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.instance.impl.HazelcastInstanceProxy;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.util.ConcurrencyUtil;
import lombok.NonNull;

public class SeaTunnelServerStarter {
    /** Starts the server using the configured cluster role. */
    public static void main(String[] args) {
        createHazelcastInstance();
    }

    public static HazelcastInstanceImpl createHazelcastInstance(String clusterName) {
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig.getHazelcastConfig().setClusterName(clusterName);
        return createHazelcastInstance(seaTunnelConfig);
    }

    public static HazelcastInstanceImpl createHazelcastInstance(
            @NonNull SeaTunnelConfig seaTunnelConfig) {
        return createHazelcastInstance(seaTunnelConfig, null);
    }

    public static HazelcastInstanceImpl createHazelcastInstance(
            @NonNull SeaTunnelConfig seaTunnelConfig, String customInstanceName) {
        return initializeHazelcastInstance(seaTunnelConfig, customInstanceName);
    }

    private static HazelcastInstanceImpl initializeHazelcastInstance(
            @NonNull SeaTunnelConfig seaTunnelConfig, String customInstanceName) {
        return createHazelcastInstance(
                seaTunnelConfig, customInstanceName, new ResourceManagerFactory());
    }

    /** Creates a configured member with its resource manager factory. */
    public static HazelcastInstanceImpl createHazelcastInstance(
            @NonNull SeaTunnelConfig seaTunnelConfig,
            String customInstanceName,
            @NonNull ResourceManagerFactory resourceManagerFactory) {
        configureDirtyJobMaps(seaTunnelConfig);

        // set the default async executor for Hazelcast InvocationFuture
        ConcurrencyUtil.setDefaultAsyncExecutor(CompletableFuture.EXECUTOR);

        boolean condition = checkTelemetryConfig(seaTunnelConfig);
        String instanceName =
                customInstanceName != null
                        ? customInstanceName
                        : HazelcastInstanceFactory.createInstanceName(
                                seaTunnelConfig.getHazelcastConfig());

        HazelcastInstanceImpl original =
                ((HazelcastInstanceProxy)
                                HazelcastInstanceFactory.newHazelcastInstance(
                                        seaTunnelConfig.getHazelcastConfig(),
                                        instanceName,
                                        new SeaTunnelNodeContext(
                                                seaTunnelConfig, resourceManagerFactory)))
                        .getOriginal();
        // init telemetry instance
        if (condition) {
            initTelemetryInstance(original.node);
        }

        return original;
    }

    /**
     * Installs synchronous dirty-tracking backups before Hazelcast creates any distributed maps. At
     * least one synchronous backup is required for member-loss evidence to survive failover.
     */
    static void configureDirtyJobMaps(SeaTunnelConfig seaTunnelConfig) {
        int backupCount = Math.max(1, seaTunnelConfig.getEngineConfig().getBackupCount());
        configureDirtyJobMap(seaTunnelConfig, Constant.IMAP_DIRTY_JOB_STATE, backupCount);
        configureDirtyJobMap(
                seaTunnelConfig, Constant.IMAP_DIRTY_JOB_MEMBER_EVENT_SEQUENCE, backupCount);
        configureDirtyJobMap(
                seaTunnelConfig, Constant.IMAP_DIRTY_JOB_PENDING_MEMBER_EVENTS, backupCount);
        configureDirtyJobMap(
                seaTunnelConfig, Constant.IMAP_DIRTY_JOB_ENABLED_THRESHOLDS, backupCount);
    }

    private static void configureDirtyJobMap(
            SeaTunnelConfig seaTunnelConfig, String mapName, int backupCount) {
        MapConfig mapConfig = seaTunnelConfig.getHazelcastConfig().getMapConfigs().get(mapName);
        if (mapConfig == null) {
            mapConfig = new MapConfig(mapName).setBackupCount(backupCount).setAsyncBackupCount(0);
            seaTunnelConfig.getHazelcastConfig().addMapConfig(mapConfig);
            return;
        }
        if (mapConfig.getBackupCount() != backupCount || mapConfig.getAsyncBackupCount() != 0) {
            mapConfig.setBackupCount(backupCount).setAsyncBackupCount(0);
        }
    }

    public static HazelcastInstanceImpl createMasterAndWorkerHazelcastInstance(
            @NonNull SeaTunnelConfig seaTunnelConfig) {
        seaTunnelConfig
                .getEngineConfig()
                .setClusterRole(EngineConfig.ClusterRole.MASTER_AND_WORKER);
        return initializeHazelcastInstance(seaTunnelConfig, null);
    }

    public static HazelcastInstanceImpl createMasterHazelcastInstance(
            @NonNull SeaTunnelConfig seaTunnelConfig) {
        seaTunnelConfig.getEngineConfig().setClusterRole(EngineConfig.ClusterRole.MASTER);
        return initializeHazelcastInstance(seaTunnelConfig, null);
    }

    public static HazelcastInstanceImpl createMasterHazelcastInstance(
            @NonNull SeaTunnelConfig seaTunnelConfig,
            @NonNull ResourceManagerFactory resourceManagerFactory) {
        seaTunnelConfig.getEngineConfig().setClusterRole(EngineConfig.ClusterRole.MASTER);
        return createHazelcastInstance(seaTunnelConfig, null, resourceManagerFactory);
    }

    /**
     * Starts a lite worker.
     *
     * @param seaTunnelConfig configuration whose role and lite-member flag are set for a worker
     * @return started worker instance owned and eventually shut down by the caller
     */
    public static HazelcastInstanceImpl createWorkerHazelcastInstance(
            @NonNull SeaTunnelConfig seaTunnelConfig) {
        seaTunnelConfig.getEngineConfig().setClusterRole(EngineConfig.ClusterRole.WORKER);
        // in hazelcast lite node will not store IMap data.
        seaTunnelConfig.getHazelcastConfig().setLiteMember(true);
        return initializeHazelcastInstance(seaTunnelConfig, null);
    }

    public static HazelcastInstanceImpl createHazelcastInstance() {
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        return createHazelcastInstance(seaTunnelConfig);
    }

    public static void initTelemetryInstance(@NonNull Node node) {
        ExportsInstanceInitializer.init(node);
    }

    private static boolean checkTelemetryConfig(SeaTunnelConfig seaTunnelConfig) {
        // "hazelcast.jmx" need to set "true", for hazelcast metrics
        if (seaTunnelConfig.getEngineConfig().getTelemetryConfig().getMetric().isEnabled()) {
            seaTunnelConfig
                    .getHazelcastConfig()
                    .getProperties()
                    .setProperty("hazelcast.jmx", "true");
            return true;
        }
        return false;
    }
}
