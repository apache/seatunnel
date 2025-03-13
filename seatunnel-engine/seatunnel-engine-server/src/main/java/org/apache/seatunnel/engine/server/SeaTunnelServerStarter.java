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

import org.apache.seatunnel.engine.common.config.ConfigProvider;
import org.apache.seatunnel.engine.common.config.EngineConfig;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;
import org.apache.seatunnel.engine.server.telemetry.metrics.ExportsInstanceInitializer;

import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.instance.impl.HazelcastInstanceProxy;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.util.ConcurrencyUtil;
import lombok.NonNull;

public class SeaTunnelServerStarter {

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
                                        new SeaTunnelNodeContext(seaTunnelConfig)))
                        .getOriginal();
        // init telemetry instance
        if (condition) {
            initTelemetryInstance(original.node);
        }

        return original;
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
