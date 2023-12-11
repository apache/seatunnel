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
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.server.telemetry.metrics.ExportsInstanceInitializer;

import com.hazelcast.instance.impl.HazelcastInstanceFactory;
import com.hazelcast.instance.impl.HazelcastInstanceImpl;
import com.hazelcast.instance.impl.HazelcastInstanceProxy;
import com.hazelcast.instance.impl.Node;
import lombok.NonNull;

public class SeaTunnelServerStarter {

    public static void main(String[] args) {
        createHazelcastInstance();
    }

    public static HazelcastInstanceImpl createHazelcastInstance(String clusterName) {
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        seaTunnelConfig.getHazelcastConfig().setClusterName(clusterName);
        HazelcastInstanceImpl hazelcastInstance = createHazelcastInstance(seaTunnelConfig);
        initTelemetryInstance(hazelcastInstance.node);
        return hazelcastInstance;
    }

    public static HazelcastInstanceImpl createHazelcastInstance(
            @NonNull SeaTunnelConfig seaTunnelConfig) {
        checkTelemetryConfig(seaTunnelConfig);
        return ((HazelcastInstanceProxy)
                        HazelcastInstanceFactory.newHazelcastInstance(
                                seaTunnelConfig.getHazelcastConfig(),
                                HazelcastInstanceFactory.createInstanceName(
                                        seaTunnelConfig.getHazelcastConfig()),
                                new SeaTunnelNodeContext(seaTunnelConfig)))
                .getOriginal();
    }

    public static HazelcastInstanceImpl createHazelcastInstance() {
        SeaTunnelConfig seaTunnelConfig = ConfigProvider.locateAndGetSeaTunnelConfig();
        HazelcastInstanceImpl hazelcastInstance = createHazelcastInstance(seaTunnelConfig);
        initTelemetryInstance(hazelcastInstance.node);
        return hazelcastInstance;
    }

    public static void initTelemetryInstance(@NonNull Node node) {
        ExportsInstanceInitializer.init(node);
    }

    private static void checkTelemetryConfig(SeaTunnelConfig seaTunnelConfig) {
        // "hazelcast.jmx" need to set "true", for hazelcast metrics
        if (seaTunnelConfig.getEngineConfig().getTelemetryConfig().getMetric().isEnabled()) {
            seaTunnelConfig
                    .getHazelcastConfig()
                    .getProperties()
                    .setProperty("hazelcast.jmx", "true");
        }
    }
}
