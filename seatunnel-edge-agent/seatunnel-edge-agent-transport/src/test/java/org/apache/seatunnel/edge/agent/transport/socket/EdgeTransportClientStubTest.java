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

package org.apache.seatunnel.edge.agent.transport.socket;

import org.apache.seatunnel.edge.agent.transport.config.EdgeTransportConfig;
import org.apache.seatunnel.edge.agent.transport.config.EdgeTransportConfigTestHelper;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class EdgeTransportClientStubTest {

    @Test
    void clientOpenAndProbeAreSafe() throws Exception {
        EdgeTransportConfig cfg = EdgeTransportConfigTestHelper.config("127.0.0.1:19999", "t");
        EdgeTransportClient client = new EdgeTransportClient(cfg);
        Assertions.assertFalse(client.probeReachable());
        client.close();
    }

    @Test
    void configRequiresEndpoint() {
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> EdgeTransportConfig.from(EdgeTransportConfigTestHelper.minimalMap("", "t")));
    }

    @Test
    void configRequiresValidPort() {
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () ->
                        EdgeTransportConfig.from(
                                EdgeTransportConfigTestHelper.minimalMap("localhost:0", "t")));
    }
}
