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

import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;

import com.hazelcast.config.JoinConfig;
import com.hazelcast.instance.impl.DefaultNodeContext;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.instance.impl.NodeExtension;
import com.hazelcast.internal.cluster.Joiner;
import com.hazelcast.internal.config.AliasedDiscoveryConfigUtils;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import static com.hazelcast.config.ConfigAccessor.getActiveMemberNetworkConfig;
import static com.hazelcast.internal.config.AliasedDiscoveryConfigUtils.allUsePublicAddress;
import static com.hazelcast.spi.properties.ClusterProperty.DISCOVERY_SPI_ENABLED;
import static com.hazelcast.spi.properties.ClusterProperty.DISCOVERY_SPI_PUBLIC_IP_ENABLED;

@Slf4j
public class SeaTunnelNodeContext extends DefaultNodeContext {

    private final SeaTunnelConfig seaTunnelConfig;

    public SeaTunnelNodeContext(@NonNull SeaTunnelConfig seaTunnelConfig) {
        this.seaTunnelConfig = seaTunnelConfig;
    }

    @Override
    public NodeExtension createNodeExtension(@NonNull Node node) {
        return new org.apache.seatunnel.engine.server.NodeExtension(node, seaTunnelConfig);
    }

    @Override
    public Joiner createJoiner(Node node) {
        JoinConfig join =
                getActiveMemberNetworkConfig(seaTunnelConfig.getHazelcastConfig()).getJoin();
        join.verify();

        if (node.shouldUseMulticastJoiner(join) && node.multicastService != null) {
            return super.createJoiner(node);
        } else if (join.getTcpIpConfig().isEnabled()) {
            log.info("Using LiteNodeDropOutTcpIpJoiner TCP/IP discovery");
            return new LiteNodeDropOutTcpIpJoiner(node);
        } else if (node.getProperties().getBoolean(DISCOVERY_SPI_ENABLED)
                || isAnyAliasedConfigEnabled(join)
                || join.isAutoDetectionEnabled()) {
            return super.createJoiner(node);
        }
        return null;
    }

    private static boolean isAnyAliasedConfigEnabled(JoinConfig join) {
        return !AliasedDiscoveryConfigUtils.createDiscoveryStrategyConfigs(join).isEmpty();
    }

    private boolean usePublicAddress(JoinConfig join, Node node) {
        return node.getProperties().getBoolean(DISCOVERY_SPI_PUBLIC_IP_ENABLED)
                || allUsePublicAddress(
                        AliasedDiscoveryConfigUtils.aliasedDiscoveryConfigsFrom(join));
    }
}
