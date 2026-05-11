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

package org.apache.seatunnel.edge.agent.transport;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.client.config.ClientConfig;

import java.util.Arrays;
import java.util.Collections;

class SeaTunnelEdgeTransportClientsTest {

    @Test
    void normalizeMemberAddressesTrimsAndDedupes() {
        Assertions.assertEquals(
                Arrays.asList("a:5701", "b"),
                SeaTunnelEdgeTransportClients.normalizeMemberAddresses(
                        Arrays.asList(" a:5701 ", "", "b", "a:5701")));
    }

    @Test
    void normalizeMemberAddressesFromCsvSplitsCommas() {
        Assertions.assertEquals(
                Arrays.asList("h1", "h2:5801"),
                SeaTunnelEdgeTransportClients.normalizeMemberAddressesFromCsv(" h1 , h2:5801 "));
    }

    @Test
    void newHazelcastClientConfigRejectsEmptyAddresses() {
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () ->
                        SeaTunnelEdgeTransportClients.newHazelcastClientConfig(
                                "demo", Collections.emptyList()));
    }

    @Test
    void configureClusterNetworkSetsNameAndAddresses() {
        ClientConfig cc = new ClientConfig();
        SeaTunnelEdgeTransportClients.configureClusterNetwork(cc, "x", Arrays.asList("10.0.0.1"));
        Assertions.assertEquals("x", cc.getClusterName());
        Assertions.assertEquals(
                Collections.singletonList("10.0.0.1"), cc.getNetworkConfig().getAddresses());
    }
}
