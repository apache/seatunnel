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

package org.apache.seatunnel.edge.agent.transport.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class EdgeTransportConfigTest {

    @Test
    void backoffCapsAtMax() {
        Assertions.assertEquals(40L, EdgeTransportConfig.computeBackoffMillis(10L, 5L, 40L));
        Assertions.assertEquals(40L, EdgeTransportConfig.computeBackoffMillis(3L, 5L, 40L));
        Assertions.assertEquals(20L, EdgeTransportConfig.computeBackoffMillis(2L, 5L, 40L));
    }

    @Test
    void fromReadonlyConfigAcceptsValidEndpoint() {
        EdgeTransportConfig config = EdgeTransportConfigTestHelper.config("localhost:10001", "tok");
        Assertions.assertEquals("localhost:10001", config.getEndpoint());
        Assertions.assertEquals("tok", config.getToken());
    }

    @Test
    void rejectsInvalidEndpoint() {
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () ->
                        EdgeTransportConfig.from(
                                EdgeTransportConfigTestHelper.minimalMap("localhost", "x")));
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () ->
                        EdgeTransportConfig.from(
                                EdgeTransportConfigTestHelper.minimalMap("localhost:0", "x")));
    }

    @Test
    void rejectsMissingToken() {
        Map<String, Object> map = new HashMap<>();
        map.put(EdgeTransportOptions.ENDPOINT.key(), "127.0.0.1:19999");
        map.put(EdgeTransportOptions.AUTH_TYPE.key(), "token");

        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> EdgeTransportConfig.from(ReadonlyConfig.fromMap(map)));
    }

    @Test
    void rejectsAuthTypeNone() {
        Map<String, Object> map = new HashMap<>();
        map.put(EdgeTransportOptions.ENDPOINT.key(), "127.0.0.1:19999");
        map.put(EdgeTransportOptions.AUTH_TYPE.key(), "none");
        map.put(EdgeTransportOptions.TOKEN.key(), "secret");

        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> EdgeTransportConfig.from(ReadonlyConfig.fromMap(map)));
    }
}
