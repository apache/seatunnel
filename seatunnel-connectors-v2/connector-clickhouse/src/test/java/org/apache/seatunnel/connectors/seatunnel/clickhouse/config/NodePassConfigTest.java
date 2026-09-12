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

package org.apache.seatunnel.connectors.seatunnel.clickhouse.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class NodePassConfigTest {

    @Test
    public void testNodePassConfigParsesDocumentedSnakeCaseKeys() {
        // The documented node_pass entries use snake_case keys
        // (node_pass.node_address / node_pass.username / node_pass.password).
        // ReadonlyConfig.get(NODE_PASS) must convert them into NodePassConfig;
        // before the fix the plain ObjectMapper conversion failed with
        // UnrecognizedPropertyException and the sink could not be created
        // (issue #9889).
        Map<String, Object> nodePass = new HashMap<>();
        nodePass.put("node_address", "10.0.0.1:8123");
        nodePass.put("username", "default");
        nodePass.put("password", "secret");

        ReadonlyConfig config =
                ReadonlyConfig.fromMap(
                        Collections.singletonMap(
                                ClickhouseFileSinkOptions.NODE_PASS.key(),
                                Collections.singletonList(nodePass)));

        List<NodePassConfig> parsed = config.get(ClickhouseFileSinkOptions.NODE_PASS);
        Assertions.assertEquals(1, parsed.size());
        Assertions.assertEquals("10.0.0.1:8123", parsed.get(0).getNodeAddress());
        Assertions.assertEquals("default", parsed.get(0).getUsername());
        Assertions.assertEquals("secret", parsed.get(0).getPassword());
    }

    @Test
    public void testNodePassConfigAcceptsCamelCaseAlias() {
        Map<String, Object> nodePass = new HashMap<>();
        nodePass.put("nodeAddress", "10.0.0.2:8123");
        nodePass.put("password", "secret2");

        ReadonlyConfig config =
                ReadonlyConfig.fromMap(
                        Collections.singletonMap(
                                ClickhouseFileSinkOptions.NODE_PASS.key(),
                                Collections.singletonList(nodePass)));

        List<NodePassConfig> parsed = config.get(ClickhouseFileSinkOptions.NODE_PASS);
        Assertions.assertEquals(1, parsed.size());
        Assertions.assertEquals("10.0.0.2:8123", parsed.get(0).getNodeAddress());
        Assertions.assertEquals("secret2", parsed.get(0).getPassword());
    }
}
