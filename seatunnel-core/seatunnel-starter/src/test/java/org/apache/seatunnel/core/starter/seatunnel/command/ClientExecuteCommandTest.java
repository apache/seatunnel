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

package org.apache.seatunnel.core.starter.seatunnel.command;

import org.apache.seatunnel.core.starter.enums.MasterType;
import org.apache.seatunnel.core.starter.seatunnel.args.ClientCommandArgs;
import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import com.hazelcast.cluster.Address;
import com.hazelcast.config.Config;
import com.hazelcast.core.HazelcastInstance;

import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Verifies local port overrides preserve unconfigured users' network and join semantics. Command
 * startup coverage also checks that the client reaches the member's actual ephemeral endpoint.
 */
class ClientExecuteCommandTest {
    @Test
    @Timeout(60)
    void testCommandEntryUsesEphemeralMemberPort() throws Exception {
        ClientCommandArgs args = new ClientCommandArgs();
        args.setMasterType(MasterType.LOCAL);
        args.setHazelcastPort(0);
        args.setListJob(true);
        AtomicInteger configuredPort = new AtomicInteger(-1);
        AtomicInteger boundPort = new AtomicInteger(-1);
        ClientExecuteCommand command =
                new ClientExecuteCommand(args) {
                    @Override
                    HazelcastInstance createHazelcastInstance(SeaTunnelConfig config) {
                        configuredPort.set(
                                config.getHazelcastConfig().getNetworkConfig().getPort());
                        HazelcastInstance member = super.createHazelcastInstance(config);
                        boundPort.set(
                                member.getCluster().getLocalMember().getSocketAddress().getPort());
                        return member;
                    }
                };

        // Listing jobs drives the normal execute -> local member -> client connection path.
        command.execute();

        Assertions.assertEquals(0, configuredPort.get());
        Assertions.assertTrue(boundPort.get() > 0);
    }

    @Test
    void testClientEndpointFormatsHostnameIpv4AndScopedIpv6() throws Exception {
        Assertions.assertEquals(
                "localhost:6123",
                ClientExecuteCommand.formatMemberAddress(new Address("localhost", 6123)));
        Assertions.assertEquals(
                "127.0.0.1:6124",
                ClientExecuteCommand.formatMemberAddress(new Address("127.0.0.1", 6124)));
        Address ipv6 = new Address("::1", 6125);
        ipv6.setScopeId("7");
        Assertions.assertEquals("[::1%7]:6125", ClientExecuteCommand.formatMemberAddress(ipv6));
    }

    @Test
    void testAbsentPortPreservesConfiguredPortAndJoinMembers() {
        Config config = new Config();
        config.getNetworkConfig().setPort(6123);
        config.getNetworkConfig().getJoin().getTcpIpConfig().addMember("192.0.2.1:6123");

        new ClientExecuteCommand(new ClientCommandArgs()).configureLocalNetwork(config);

        Assertions.assertEquals(6123, config.getNetworkConfig().getPort());
        Assertions.assertEquals(
                Collections.singletonList("192.0.2.1:6123"),
                config.getNetworkConfig().getJoin().getTcpIpConfig().getMembers());
        Assertions.assertTrue(config.getNetworkConfig().isPortAutoIncrement());
    }

    @Test
    void testExplicitPortClearsJoinMembersAndDisablesPortFallback() {
        ClientCommandArgs args = new ClientCommandArgs();
        args.setHazelcastPort(6124);
        Config config = new Config();
        config.getNetworkConfig().getJoin().getTcpIpConfig().addMember("192.0.2.1:6123");

        new ClientExecuteCommand(args).configureLocalNetwork(config);

        Assertions.assertEquals(6124, config.getNetworkConfig().getPort());
        Assertions.assertEquals(
                Collections.emptyList(),
                config.getNetworkConfig().getJoin().getTcpIpConfig().getMembers());
        Assertions.assertFalse(config.getNetworkConfig().isPortAutoIncrement());
    }
}
