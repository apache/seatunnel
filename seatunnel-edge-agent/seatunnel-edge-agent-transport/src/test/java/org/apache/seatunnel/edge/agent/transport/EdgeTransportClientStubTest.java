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

import java.io.IOException;

class EdgeTransportClientStubTest {

    @Test
    void stubOpenAndProbeAreSafe() throws Exception {
        EdgeTransportClient client = new EdgeTransportClient();
        client.open();
        Assertions.assertFalse(client.probeReachable());
        client.close();
    }

    @Test
    void stubDiscoverEndpointsFails() {
        EdgeTransportClient client = new EdgeTransportClient();
        Assertions.assertThrows(IOException.class, client::discoverEndpoints);
    }

    @Test
    void stubSendFails() {
        EdgeTransportClient client = new EdgeTransportClient();
        Assertions.assertThrows(IOException.class, () -> client.sendBatchAndAwaitAck(1L, "{}"));
    }

    @Test
    void mismatchedConfigAndLookupRejected() {
        EdgeTransportConfig cfg =
                EdgeTransportConfig.builder().jobId(1).authToken("t").edgeIngressPort(1).build();
        Assertions.assertThrows(
                IllegalArgumentException.class, () -> new EdgeTransportClient(cfg, null));
        Assertions.assertThrows(
                IllegalArgumentException.class, () -> new EdgeTransportClient(null, jobId -> "[]"));
    }

    @Test
    void configuredClientRequiresNonEmptyDiscoveryHosts() {
        EdgeTransportConfig cfg =
                EdgeTransportConfig.builder().jobId(7).authToken("t").edgeIngressPort(1).build();
        EdgeTransportClient client = new EdgeTransportClient(cfg, jobId -> "[]");
        Assertions.assertThrows(IOException.class, client::discoverEndpoints);
    }
}
