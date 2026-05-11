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

/** Guards stable wire tokens referenced by collector and engine EdgeSocket dialog. */
class EdgeSocketProtocolTest {

    @Test
    void responseTokensMatchCollectorSemantics() {
        Assertions.assertEquals("ACK", EdgeSocketProtocol.RESP_ACK);
        Assertions.assertTrue("ACK:42".startsWith(EdgeSocketProtocol.RESP_ACK_PREFIX));
        Assertions.assertEquals("PENDING", EdgeSocketProtocol.RESP_PENDING);
        Assertions.assertEquals("RETRY", EdgeSocketProtocol.RESP_RETRY);
        Assertions.assertEquals("RECEIVED", EdgeSocketProtocol.RESP_RECEIVED);
        Assertions.assertEquals("AUTH_FAILED", EdgeSocketProtocol.RESP_AUTH_FAILED);
    }
}
