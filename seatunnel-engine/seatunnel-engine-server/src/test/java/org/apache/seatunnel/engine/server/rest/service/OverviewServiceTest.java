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

package org.apache.seatunnel.engine.server.rest.service;

import org.apache.seatunnel.engine.server.SeaTunnelServer;
import org.apache.seatunnel.engine.server.resourcemanager.resource.OverviewInfo;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import com.hazelcast.cluster.Address;
import com.hazelcast.cluster.Member;
import com.hazelcast.cluster.impl.MemberImpl;
import com.hazelcast.internal.cluster.ClusterService;
import com.hazelcast.spi.impl.NodeEngineImpl;

import java.util.Collections;

/**
 * Covers overview routing while Hazelcast mastership and SeaTunnel coordinator leadership differ.
 */
public class OverviewServiceTest {

    /**
     * Verifies that startup returns an empty overview instead of routing to a lite Hazelcast master
     * when no active coordinator is visible.
     */
    @Test
    void testReturnsEmptyOverviewWhenOnlyLiteMasterIsVisible() {
        NodeEngineImpl nodeEngine = Mockito.mock(NodeEngineImpl.class);
        ClusterService clusterService = Mockito.mock(ClusterService.class);
        Address liteMasterAddress = Address.createUnresolvedAddress("localhost", 5801);
        MemberImpl liteMaster = Mockito.mock(MemberImpl.class);
        Mockito.when(nodeEngine.getMasterAddress()).thenReturn(liteMasterAddress);
        Mockito.when(nodeEngine.getClusterService()).thenReturn(clusterService);
        Mockito.when(clusterService.getMember(liteMasterAddress)).thenReturn(liteMaster);
        Mockito.when(clusterService.getMembers())
                .thenReturn(Collections.<Member>singleton(liteMaster));
        Mockito.when(liteMaster.isLiteMember()).thenReturn(true);
        OverviewService overviewService =
                new OverviewService(nodeEngine) {
                    @Override
                    protected SeaTunnelServer getSeaTunnelServer(boolean shouldBeMaster) {
                        return null;
                    }
                };

        OverviewInfo overviewInfo = overviewService.getOverviewInfo(Collections.emptyMap());

        Assertions.assertEquals(0, overviewInfo.getWorkers());
        Assertions.assertEquals(0, overviewInfo.getTotalSlot());
        Mockito.verify(nodeEngine, Mockito.never()).getOperationService();
    }
}
