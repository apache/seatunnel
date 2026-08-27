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

package org.apache.seatunnel.engine.server.joiner;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.config.JoinConfig;
import com.hazelcast.instance.EndpointQualifier;
import com.hazelcast.instance.ProtocolType;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.config.AliasedDiscoveryConfigUtils;
import com.hazelcast.internal.util.Preconditions;
import com.hazelcast.internal.util.concurrent.BackoffIdleStrategy;
import com.hazelcast.internal.util.concurrent.IdleStrategy;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.discovery.integration.DiscoveryService;
import com.hazelcast.spi.properties.ClusterProperty;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.hazelcast.internal.config.AliasedDiscoveryConfigUtils.allUsePublicAddress;
import static com.hazelcast.spi.properties.ClusterProperty.DISCOVERY_SPI_PUBLIC_IP_ENABLED;

public class LiteNodeDropOutDiscoveryJoiner extends LiteNodeDropOutTcpIpJoiner {

    private final DiscoveryService discoveryService;
    private final boolean usePublicAddress;
    private final IdleStrategy idleStrategy;
    private final int maximumWaitingTimeBeforeJoinSeconds;

    public LiteNodeDropOutDiscoveryJoiner(Node node) {
        super(node);
        this.idleStrategy =
                new BackoffIdleStrategy(
                        0L,
                        0L,
                        TimeUnit.MILLISECONDS.toNanos(10L),
                        TimeUnit.MILLISECONDS.toNanos(500L));
        this.maximumWaitingTimeBeforeJoinSeconds =
                node.getProperties().getInteger(ClusterProperty.WAIT_SECONDS_BEFORE_JOIN);
        this.discoveryService = node.discoveryService;
        this.usePublicAddress = usePublicAddress(node.getConfig().getNetworkConfig().getJoin());
    }

    private boolean usePublicAddress(JoinConfig join) {
        return node.getProperties().getBoolean(DISCOVERY_SPI_PUBLIC_IP_ENABLED)
                || allUsePublicAddress(
                        AliasedDiscoveryConfigUtils.aliasedDiscoveryConfigsFrom(join));
    }

    protected Collection<Address> getPossibleAddressesForInitialJoin() {
        long deadLine =
                System.nanoTime()
                        + TimeUnit.SECONDS.toNanos((long) this.maximumWaitingTimeBeforeJoinSeconds);

        for (int i = 0; System.nanoTime() < deadLine; ++i) {
            Collection<Address> possibleAddresses = this.getPossibleAddresses();
            if (!possibleAddresses.isEmpty()) {
                return possibleAddresses;
            }

            this.idleStrategy.idle((long) i);
        }

        return Collections.emptyList();
    }

    protected Collection<Address> getPossibleAddresses() {
        Iterable<DiscoveryNode> discoveredNodes =
                (Iterable)
                        Preconditions.checkNotNull(
                                this.discoveryService.discoverNodes(),
                                "Discovered nodes cannot be null!");
        MemberImpl localMember = this.node.nodeEngine.getLocalMember();
        Set<Address> localAddresses = this.node.getLocalAddressRegistry().getLocalAddresses();
        Collection<Address> possibleMembers = new ArrayList();
        Iterator var5 = discoveredNodes.iterator();

        while (var5.hasNext()) {
            DiscoveryNode discoveryNode = (DiscoveryNode) var5.next();
            Address discoveredAddress =
                    this.usePublicAddress
                            ? discoveryNode.getPublicAddress()
                            : discoveryNode.getPrivateAddress();
            if (localAddresses.contains(discoveredAddress)) {
                if (!this.usePublicAddress && discoveryNode.getPublicAddress() != null) {
                    localMember
                            .getAddressMap()
                            .put(
                                    EndpointQualifier.resolve(ProtocolType.CLIENT, "public"),
                                    this.publicAddress(localMember, discoveryNode));
                }
            } else {
                possibleMembers.add(discoveredAddress);
            }
        }

        return possibleMembers;
    }

    private Address publicAddress(MemberImpl localMember, DiscoveryNode discoveryNode) {
        if (localMember.getAddressMap().containsKey(EndpointQualifier.CLIENT)) {
            try {
                String publicHost = discoveryNode.getPublicAddress().getHost();
                int clientPort =
                        ((Address) localMember.getAddressMap().get(EndpointQualifier.CLIENT))
                                .getPort();
                return new Address(publicHost, clientPort);
            } catch (Exception var5) {
                Exception e = var5;
                this.logger.fine(e);
            }
        }

        return discoveryNode.getPublicAddress();
    }
}
