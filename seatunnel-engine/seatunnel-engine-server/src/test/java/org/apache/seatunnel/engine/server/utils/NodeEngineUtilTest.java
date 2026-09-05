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

package org.apache.seatunnel.engine.server.utils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.version.MemberVersion;

import java.util.Arrays;
import java.util.LinkedHashSet;

/**
 * Covers active coordinator selection when Hazelcast mastership and SeaTunnel coordinator
 * capability are different in separated master and worker deployments.
 */
public class NodeEngineUtilTest {

    @Test
    public void testChooseFirstCoordinatorWhenHazelcastMasterIsLiteWorker() throws Exception {
        Address workerAddress = new Address("localhost", 5801);
        Address coordinatorAddress = new Address("localhost", 5802);
        MemberImpl workerMember = newMember(workerAddress, true);
        MemberImpl coordinatorMember = newMember(coordinatorAddress, false);

        NodeEngine nodeEngine =
                newNodeEngine(workerAddress, workerMember, workerMember, coordinatorMember);

        Assertions.assertEquals(
                coordinatorAddress, NodeEngineUtil.getActiveMasterAddress(nodeEngine));
    }

    @Test
    public void testKeepHazelcastMasterWhenItCanCoordinate() throws Exception {
        Address coordinatorAddress = new Address("localhost", 5801);
        Address workerAddress = new Address("localhost", 5802);
        MemberImpl coordinatorMember = newMember(coordinatorAddress, false);
        MemberImpl workerMember = newMember(workerAddress, true);

        NodeEngine nodeEngine =
                newNodeEngine(
                        coordinatorAddress, coordinatorMember, coordinatorMember, workerMember);

        Assertions.assertEquals(
                coordinatorAddress, NodeEngineUtil.getActiveMasterAddress(nodeEngine));
    }

    /**
     * Verifies that lite-worker mastership does not masquerade as a coordinator.
     *
     * <p>This covers the case where no coordinator-capable member is visible.
     */
    @Test
    public void testReturnNullWhenLiteMasterHasNoCoordinatorMember() throws Exception {
        Address workerAddress = new Address("localhost", 5801);
        MemberImpl workerMember = newMember(workerAddress, true);

        NodeEngine nodeEngine = newNodeEngine(workerAddress, workerMember, workerMember);

        Assertions.assertNull(NodeEngineUtil.getActiveMasterAddress(nodeEngine));
    }

    /**
     * Verifies that stale master metadata is treated as coordinator-unavailable.
     *
     * <p>This covers the case where the Hazelcast master address is known but member metadata is
     * missing from the current membership view.
     */
    @Test
    public void testReturnNullWhenMasterMetadataIsUnavailable() throws Exception {
        Address workerAddress = new Address("localhost", 5801);
        Address coordinatorAddress = new Address("localhost", 5802);
        MemberImpl workerMember = newMember(workerAddress, true);
        MemberImpl coordinatorMember = newMember(coordinatorAddress, false);

        NodeEngine nodeEngine = newNodeEngine(workerAddress, null, workerMember, coordinatorMember);

        Assertions.assertNull(NodeEngineUtil.getActiveMasterAddress(nodeEngine));
    }

    /**
     * Verifies that a newly visible coordinator becomes the active routing target.
     *
     * <p>This covers the case where a replacement coordinator rejoins after an unavailable window.
     */
    @Test
    public void testChooseReplacementCoordinatorAfterCoordinatorRejoins() throws Exception {
        Address workerAddress = new Address("localhost", 5801);
        Address replacementCoordinatorAddress = new Address("localhost", 5803);
        MemberImpl workerMember = newMember(workerAddress, true);
        MemberImpl replacementCoordinatorMember = newMember(replacementCoordinatorAddress, false);

        NodeEngine nodeEngine = Mockito.mock(NodeEngine.class);
        ClusterService clusterService = Mockito.mock(ClusterService.class);
        Mockito.when(nodeEngine.getMasterAddress()).thenReturn(workerAddress);
        Mockito.when(nodeEngine.getClusterService()).thenReturn(clusterService);
        Mockito.when(clusterService.getMember(workerAddress)).thenReturn(workerMember);
        Mockito.when(clusterService.getMembers())
                .thenReturn(new LinkedHashSet<>(Arrays.asList(workerMember)))
                .thenReturn(
                        new LinkedHashSet<>(
                                Arrays.asList(workerMember, replacementCoordinatorMember)));

        Assertions.assertNull(NodeEngineUtil.getActiveMasterAddress(nodeEngine));
        Assertions.assertEquals(
                replacementCoordinatorAddress, NodeEngineUtil.getActiveMasterAddress(nodeEngine));
    }

    private NodeEngine newNodeEngine(
            Address hazelcastMasterAddress, MemberImpl hazelcastMasterMember, Member... members) {
        NodeEngine nodeEngine = Mockito.mock(NodeEngine.class);
        ClusterService clusterService = Mockito.mock(ClusterService.class);
        Mockito.when(nodeEngine.getMasterAddress()).thenReturn(hazelcastMasterAddress);
        Mockito.when(nodeEngine.getClusterService()).thenReturn(clusterService);
        Mockito.when(clusterService.getMember(hazelcastMasterAddress))
                .thenReturn(hazelcastMasterMember);
        Mockito.when(clusterService.getMembers())
                .thenReturn(new LinkedHashSet<>(Arrays.asList(members)));
        return nodeEngine;
    }

    private MemberImpl newMember(Address address, boolean liteMember) {
        return new MemberImpl.Builder(address)
                .version(MemberVersion.of(5, 1, 0))
                .liteMember(liteMember)
                .build();
    }
}
