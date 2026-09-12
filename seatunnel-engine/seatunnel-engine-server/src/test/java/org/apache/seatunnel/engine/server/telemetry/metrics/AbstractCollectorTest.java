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

package org.apache.seatunnel.engine.server.telemetry.metrics;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.instance.impl.Node;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.version.MemberVersion;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;

/**
 * Verifies telemetry helper methods resolve the active SeaTunnel coordinator instead of the raw
 * Hazelcast master when worker-only lite members lead the member list.
 */
public class AbstractCollectorTest {

    @Test
    public void testIsMasterUsesActiveCoordinatorAddress() throws Exception {
        Address workerAddress = new Address("127.0.0.1", 5801);
        Address coordinatorAddress = new Address("127.0.0.1", 5802);
        MemberImpl workerMember = newMember(workerAddress, true);
        MemberImpl coordinatorMember = newMember(coordinatorAddress, false);

        TestCollector collector =
                new TestCollector(
                        newNode(
                                coordinatorAddress,
                                workerAddress,
                                workerMember,
                                workerMember,
                                coordinatorMember));

        Assertions.assertTrue(collector.isMaster());
    }

    @Test
    public void testMasterAddressUsesActiveCoordinatorAddress() throws Exception {
        Address workerAddress = new Address("127.0.0.1", 5801);
        Address coordinatorAddress = new Address("127.0.0.1", 5802);
        MemberImpl workerMember = newMember(workerAddress, true);
        MemberImpl coordinatorMember = newMember(coordinatorAddress, false);

        TestCollector collector =
                new TestCollector(
                        newNode(
                                workerAddress,
                                workerAddress,
                                workerMember,
                                workerMember,
                                coordinatorMember));

        Assertions.assertEquals("127.0.0.1:5802", collector.masterAddress());
    }

    private Node newNode(
            Address localAddress,
            Address hazelcastMasterAddress,
            MemberImpl hazelcastMasterMember,
            Member... members)
            throws UnknownHostException {
        Node node = Mockito.mock(Node.class);
        NodeEngineImpl nodeEngine = Mockito.mock(NodeEngineImpl.class);
        ClusterServiceImpl clusterService = Mockito.mock(ClusterServiceImpl.class);

        Mockito.when(node.getNodeEngine()).thenReturn(nodeEngine);
        Mockito.when(nodeEngine.getThisAddress()).thenReturn(localAddress);
        Mockito.when(nodeEngine.getMasterAddress()).thenReturn(hazelcastMasterAddress);
        Mockito.when(nodeEngine.getClusterService()).thenReturn(clusterService);
        Mockito.when(node.getClusterService()).thenReturn(clusterService);
        Mockito.when(clusterService.getMember(hazelcastMasterAddress))
                .thenReturn(hazelcastMasterMember);
        Mockito.when(clusterService.getMembers())
                .thenReturn(new LinkedHashSet<>(Arrays.asList(members)));
        Mockito.when(clusterService.getMasterAddress()).thenReturn(hazelcastMasterAddress);
        return node;
    }

    private MemberImpl newMember(Address address, boolean liteMember) {
        return new MemberImpl.Builder(address)
                .version(MemberVersion.of(5, 1, 0))
                .liteMember(liteMember)
                .build();
    }

    private static class TestCollector extends AbstractCollector {

        private TestCollector(Node node) {
            super(node);
        }

        @Override
        public List<MetricFamilySamples> collect() {
            return Collections.emptyList();
        }
    }
}
