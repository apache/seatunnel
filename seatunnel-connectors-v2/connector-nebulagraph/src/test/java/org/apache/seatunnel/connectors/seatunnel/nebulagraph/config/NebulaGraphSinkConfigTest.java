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

package org.apache.seatunnel.connectors.seatunnel.nebulagraph.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.nebulagraph.exception.NebulaGraphConnectorException;

import org.junit.jupiter.api.Test;

import com.vesoft.nebula.client.graph.data.HostAddress;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class NebulaGraphSinkConfigTest {

    @Test
    void parsesHostNamesAndBracketedIpv6Addresses() {
        HostAddress hostname = NebulaGraphSinkConfig.parseHost("graphd:9669");
        HostAddress ipv6 = NebulaGraphSinkConfig.parseHost("[2001:db8::1]:9669");

        assertEquals("graphd", hostname.getHost());
        assertEquals(9669, hostname.getPort());
        assertEquals("2001:db8::1", ipv6.getHost());
        assertEquals(9669, ipv6.getPort());
    }

    @Test
    void rejectsMalformedAddresses() {
        assertThrows(
                NebulaGraphConnectorException.class,
                () -> NebulaGraphSinkConfig.parseHost("2001:db8::1:9669"));
        assertThrows(
                NebulaGraphConnectorException.class,
                () -> NebulaGraphSinkConfig.parseHost("graphd:70000"));
        assertThrows(
                NebulaGraphConnectorException.class,
                () -> NebulaGraphSinkConfig.parseHost("graphd"));
    }

    @Test
    void validatesNumericOptionsAndIdentifiers() {
        Map<String, Object> invalidBatchSize = validValues();
        invalidBatchSize.put("batch_size", 0);
        assertThrows(
                NebulaGraphConnectorException.class,
                () -> NebulaGraphSinkConfig.of(ReadonlyConfig.fromMap(invalidBatchSize)));

        Map<String, Object> invalidTag = validValues();
        invalidTag.put("tag", "person; DROP SPACE test");
        NebulaGraphConnectorException exception =
                assertThrows(
                        NebulaGraphConnectorException.class,
                        () -> NebulaGraphSinkConfig.of(ReadonlyConfig.fromMap(invalidTag)));
        assertTrue(exception.getMessage().contains("tag"));

        Map<String, Object> invalidSpace = validValues();
        invalidSpace.put("space", "test` MATCH (v) RETURN v");
        assertThrows(
                NebulaGraphConnectorException.class,
                () -> NebulaGraphSinkConfig.of(ReadonlyConfig.fromMap(invalidSpace)));
    }

    @Test
    void rejectsDuplicateWriteFields() {
        Map<String, Object> values = validValues();
        values.put("write_fields", Arrays.asList("name", "name"));

        NebulaGraphConnectorException exception =
                assertThrows(
                        NebulaGraphConnectorException.class,
                        () -> NebulaGraphSinkConfig.of(ReadonlyConfig.fromMap(values)));
        assertTrue(exception.getMessage().contains("duplicate"));
    }

    static NebulaGraphSinkConfig validConfig(Map<String, Object> overrides) {
        Map<String, Object> values = validValues();
        values.putAll(overrides);
        return NebulaGraphSinkConfig.of(ReadonlyConfig.fromMap(values));
    }

    private static Map<String, Object> validValues() {
        Map<String, Object> values = new HashMap<>();
        values.put("hosts", Arrays.asList("graphd:9669"));
        values.put("username", "root");
        values.put("password", "nebula");
        values.put("space", "test");
        values.put("tag", "person");
        values.put("vid_field", "id");
        return values;
    }
}
