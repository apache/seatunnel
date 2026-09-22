/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.engine.server.rest.service;

import org.apache.seatunnel.engine.common.exception.SeaTunnelEngineException;
import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.diagnostic.WorkerResourceSnapshot;

import org.junit.jupiter.api.Test;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.internal.cluster.impl.ClusterServiceImpl;
import com.hazelcast.spi.exception.TargetNotMemberException;
import com.hazelcast.spi.impl.NodeEngineImpl;
import com.hazelcast.version.MemberVersion;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.CompletionException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class WorkerResourceServiceTest {

    @Test
    void shouldReturnUnavailableSnapshotBeforeMasterElectionCompletes() {
        NodeEngineImpl nodeEngine = mock(NodeEngineImpl.class);
        when(nodeEngine.getMasterAddress()).thenReturn(null);
        WorkerResourceService service = new TestWorkerResourceService(nodeEngine, null, null);

        WorkerResourceSnapshot snapshot = service.getWorkerResources();

        assertFalse(snapshot.isAvailable());
        assertTrue(snapshot.getCollectedAt() > 0);
        assertNotNull(snapshot.getWorkers());
        assertTrue(snapshot.getWorkers().isEmpty());
    }

    @Test
    void shouldReturnUnavailableSnapshotWhenMasterLeavesDuringForwarding()
            throws UnknownHostException {
        NodeEngineImpl nodeEngine = nodeEngineWithCoordinatorMaster();
        RuntimeException failure =
                new CompletionException(new TargetNotMemberException("master left"));
        WorkerResourceService service = new TestWorkerResourceService(nodeEngine, null, failure);

        WorkerResourceSnapshot snapshot = service.getWorkerResources();

        assertFalse(snapshot.isAvailable());
        assertTrue(snapshot.getWorkers().isEmpty());
    }

    @Test
    void shouldPropagateUnrelatedForwardingFailure() throws UnknownHostException {
        NodeEngineImpl nodeEngine = nodeEngineWithCoordinatorMaster();
        RuntimeException failure = new IllegalStateException("unexpected failure");
        WorkerResourceService service = new TestWorkerResourceService(nodeEngine, null, failure);

        assertThrows(IllegalStateException.class, service::getWorkerResources);
    }

    @Test
    void shouldReturnUnavailableSnapshotWhenTargetLosesMastership() throws UnknownHostException {
        NodeEngineImpl nodeEngine = nodeEngineWithCoordinatorMaster();
        RuntimeException failure =
                new CompletionException(
                        new SeaTunnelEngineException("This is not a master node now."));
        WorkerResourceService service = new TestWorkerResourceService(nodeEngine, null, failure);

        WorkerResourceSnapshot snapshot = service.getWorkerResources();

        assertFalse(snapshot.isAvailable());
        assertTrue(snapshot.getWorkers().isEmpty());
    }

    /**
     * Verifies that a non-coordinator node forwards the read to the active SeaTunnel coordinator
     * when Hazelcast mastership sits on a worker-only lite member, which cannot answer it.
     */
    @Test
    void shouldForwardToActiveCoordinatorWhenHazelcastMasterIsLiteWorker()
            throws UnknownHostException {
        Address liteMasterAddress = new Address("localhost", 5801);
        Address coordinatorAddress = new Address("localhost", 5802);
        MemberImpl liteMaster = newMember(liteMasterAddress, true);
        MemberImpl coordinator = newMember(coordinatorAddress, false);
        NodeEngineImpl nodeEngine = mock(NodeEngineImpl.class);
        ClusterServiceImpl clusterService = mock(ClusterServiceImpl.class);
        when(nodeEngine.getMasterAddress()).thenReturn(liteMasterAddress);
        when(nodeEngine.getClusterService()).thenReturn(clusterService);
        // Membership order matters: the lite worker is the oldest member and therefore the
        // Hazelcast master, the coordinator-capable member joined after it.
        Set<Member> members = new LinkedHashSet<>(Arrays.<Member>asList(liteMaster, coordinator));
        when(clusterService.getMember(liteMasterAddress)).thenReturn(liteMaster);
        when(clusterService.getMembers()).thenReturn(members);
        TestWorkerResourceService service = new TestWorkerResourceService(nodeEngine, null, null);

        WorkerResourceSnapshot snapshot = service.getWorkerResources();

        assertTrue(snapshot.isAvailable());
        assertEquals(coordinatorAddress, service.forwardedAddress);
    }

    /**
     * Builds a node engine whose Hazelcast master is coordinator-capable, which is the mixed
     * cluster default where the active coordinator equals the Hazelcast master.
     */
    private static NodeEngineImpl nodeEngineWithCoordinatorMaster() throws UnknownHostException {
        Address masterAddress = new Address("localhost", 5801);
        NodeEngineImpl nodeEngine = mock(NodeEngineImpl.class);
        ClusterServiceImpl clusterService = mock(ClusterServiceImpl.class);
        when(nodeEngine.getMasterAddress()).thenReturn(masterAddress);
        when(nodeEngine.getClusterService()).thenReturn(clusterService);
        when(clusterService.getMember(masterAddress)).thenReturn(newMember(masterAddress, false));
        return nodeEngine;
    }

    private static MemberImpl newMember(Address address, boolean liteMember) {
        return new MemberImpl.Builder(address)
                .version(MemberVersion.of(5, 1, 0))
                .liteMember(liteMember)
                .build();
    }

    private static class TestWorkerResourceService extends WorkerResourceService {
        private final SeaTunnelServer seaTunnelServer;
        private final RuntimeException invocationFailure;
        private Address forwardedAddress;

        private TestWorkerResourceService(
                NodeEngineImpl nodeEngine,
                SeaTunnelServer seaTunnelServer,
                RuntimeException invocationFailure) {
            super(nodeEngine);
            this.seaTunnelServer = seaTunnelServer;
            this.invocationFailure = invocationFailure;
        }

        @Override
        protected SeaTunnelServer getSeaTunnelServer(boolean shouldBeMaster) {
            return seaTunnelServer;
        }

        @Override
        protected WorkerResourceSnapshot invokeOnMaster(Address masterAddress) {
            forwardedAddress = masterAddress;
            if (invocationFailure != null) {
                throw invocationFailure;
            }
            return new WorkerResourceSnapshot(true, 1L, Collections.emptyList());
        }
    }
}
