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

package org.apache.seatunnel.engine.server.operation;

import org.apache.seatunnel.engine.common.config.SeaTunnelConfig;
import org.apache.seatunnel.engine.server.AbstractSeaTunnelServerTest;
import org.apache.seatunnel.engine.server.utils.NodeEngineUtil;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.hazelcast.cluster.Address;

public class GetNodeHttpPortOperationTest
        extends AbstractSeaTunnelServerTest<GetNodeHttpPortOperationTest> {

    private static final int HTTP_PORT = 18085;

    @Override
    public SeaTunnelConfig loadSeaTunnelConfig() {
        SeaTunnelConfig config = super.loadSeaTunnelConfig();
        config.getEngineConfig().getHttpConfig().setPort(HTTP_PORT);
        return config;
    }

    @Test
    public void testReturnsConfiguredHttpPort() throws Exception {
        Address localAddress = instance.getCluster().getLocalMember().getAddress();

        int result =
                (int)
                        NodeEngineUtil.sendOperationToMemberNode(
                                        nodeEngine, new GetNodeHttpPortOperation(), localAddress)
                                .get();

        Assertions.assertEquals(HTTP_PORT, result);
    }
}
