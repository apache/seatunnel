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
import org.apache.seatunnel.api.configuration.util.ConfigValidator;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class EdgeTransportOptionRulesTest {

    @Test
    void ruleIsDefined() {
        Assertions.assertNotNull(EdgeTransportOptionRules.rule());
    }

    @Test
    void missingEndpointFailsValidation() {
        Map<String, Object> map = new HashMap<>();
        map.put(EdgeOutputOptions.TYPE.key(), "transport");
        map.put(EdgeTransportOptions.TOKEN.key(), "secret");

        Assertions.assertThrows(
                Exception.class,
                () ->
                        ConfigValidator.of(ReadonlyConfig.fromMap(map))
                                .validate(EdgeTransportOptionRules.rule()));
    }

    @Test
    void missingTokenFailsValidation() {
        Map<String, Object> map = new HashMap<>();
        map.put(EdgeOutputOptions.TYPE.key(), "transport");
        map.put(EdgeTransportOptions.ENDPOINT.key(), "localhost:1");
        map.put(EdgeTransportOptions.AUTH_TYPE.key(), "token");

        Assertions.assertThrows(
                Exception.class,
                () ->
                        ConfigValidator.of(ReadonlyConfig.fromMap(map))
                                .validate(EdgeTransportOptionRules.rule()));
    }

    @Test
    void authTypeNoneFailsValidation() {
        Map<String, Object> map = new HashMap<>();
        map.put(EdgeOutputOptions.TYPE.key(), "transport");
        map.put(EdgeTransportOptions.ENDPOINT.key(), "localhost:1");
        map.put(EdgeTransportOptions.AUTH_TYPE.key(), "none");
        map.put(EdgeTransportOptions.TOKEN.key(), "secret");

        ConfigValidator.of(ReadonlyConfig.fromMap(map)).validate(EdgeTransportOptionRules.rule());
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> EdgeTransportConfig.from(ReadonlyConfig.fromMap(map)));
    }

    @Test
    void packetAesGcmRequiresSecretKey() {
        Map<String, Object> map = new HashMap<>();
        map.put(EdgeOutputOptions.TYPE.key(), "transport");
        map.put(EdgeTransportOptions.ENDPOINT.key(), "localhost:1");
        map.put(EdgeTransportOptions.TOKEN.key(), "secret");
        map.put(EdgeTransportOptions.PACKET_MODE.key(), "PACKET");
        map.put(EdgeTransportOptions.ENCRYPTION.key(), "aes_gcm");

        Assertions.assertThrows(
                Exception.class,
                () ->
                        ConfigValidator.of(ReadonlyConfig.fromMap(map))
                                .validate(EdgeTransportOptionRules.rule()));
    }

    @Test
    void minimalConfigPassesValidation() {
        ConfigValidator.of(EdgeTransportConfigTestHelper.minimalMap("localhost:1", "secret"))
                .validate(EdgeTransportOptionRules.rule());
    }
}
