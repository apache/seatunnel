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

package org.apache.seatunnel.engine.server.observability.cluster;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.hazelcast.cluster.Address;
import com.hazelcast.instance.impl.Node;

class ClusterObservabilityServiceTest {

    @Test
    void shouldTrackMembershipEventsAndMasterChanges() throws Exception {
        Node node = Mockito.mock(Node.class);
        Address masterOne = new Address("127.0.0.1", 5801);
        Address masterTwo = new Address("127.0.0.2", 5801);
        Mockito.when(node.getMasterAddress())
                .thenReturn(masterOne, masterOne, masterTwo, masterTwo);

        ClusterObservabilityService service = new ClusterObservabilityService(node);

        service.recordMemberAdded();
        service.recordMemberRemoved();

        ClusterObservabilityService.ClusterObservabilitySnapshot snapshot = service.snapshot();

        Assertions.assertEquals(1L, snapshot.getMemberJoinTotal());
        Assertions.assertEquals(1L, snapshot.getMemberLeaveTotal());
        Assertions.assertEquals(1L, snapshot.getMasterChangeTotal());
        Assertions.assertTrue(snapshot.getLastMemberJoinTimestampMs() > 0L);
        Assertions.assertTrue(snapshot.getLastMemberLeaveTimestampMs() > 0L);
        Assertions.assertTrue(snapshot.getLastMasterChangeTimestampMs() > 0L);
    }

    @Test
    void shouldNotCountInitialMasterDiscoveryAsChange() throws Exception {
        Node node = Mockito.mock(Node.class);
        Address master = new Address("127.0.0.1", 5801);
        Mockito.when(node.getMasterAddress()).thenReturn(null, master, master);

        ClusterObservabilityService service = new ClusterObservabilityService(node);

        service.recordMemberAdded();

        ClusterObservabilityService.ClusterObservabilitySnapshot snapshot = service.snapshot();

        Assertions.assertEquals(0L, snapshot.getMasterChangeTotal());
        Assertions.assertEquals(0L, snapshot.getLastMasterChangeTimestampMs());
    }
}
