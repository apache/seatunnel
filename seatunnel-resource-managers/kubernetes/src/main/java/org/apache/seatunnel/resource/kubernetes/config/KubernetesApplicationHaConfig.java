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

package org.apache.seatunnel.resource.kubernetes.config;

import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.common.config.SeatunnelApplicationConfig;
import org.apache.seatunnel.engine.common.config.server.ScheduleStrategy;

import java.util.List;

/** Deployment configuration for native Zeta members; no engine lifecycle is replaced. */
public final class KubernetesApplicationHaConfig {
    private KubernetesApplicationHaConfig() {}

    /** Configures distinct native members with one synchronous metadata replica. */
    public static void configure(
            SeaTunnelConfig config,
            String clusterName,
            List<String> addresses,
            boolean worker,
            int slots,
            int backups) {
        if (addresses == null
                || addresses.isEmpty()
                || (!worker && addresses.size() != 2)
                || backups != 1
                || addresses.stream().anyMatch(a -> a == null || a.trim().isEmpty())) {
            throw new IllegalArgumentException(
                    "Application HA requires two masters and one backup");
        }
        SeatunnelApplicationConfig.configure(
                config, clusterName, worker ? addresses.get(0) : null, slots);
        config.getEngineConfig().setBackupCount(backups);
        config.getHazelcastConfig().getMapConfig("default");
        config.getHazelcastConfig()
                .getMapConfigs()
                .values()
                .forEach(map -> map.setAsyncBackupCount(0).setBackupCount(backups));
        config.getEngineConfig().setScheduleStrategy(ScheduleStrategy.WAIT);
        config.getHazelcastConfig()
                .getNetworkConfig()
                .getJoin()
                .getTcpIpConfig()
                .setMembers(addresses);
        if (!worker) {
            config.getHazelcastConfig()
                    .getNetworkConfig()
                    .getJoin()
                    .getTcpIpConfig()
                    .setRequiredMember(null);
        }
    }
}
