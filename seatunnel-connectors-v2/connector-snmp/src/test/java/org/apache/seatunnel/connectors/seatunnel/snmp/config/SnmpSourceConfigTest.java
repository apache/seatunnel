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

package org.apache.seatunnel.connectors.seatunnel.snmp.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

class SnmpSourceConfigTest {

    @Test
    void testDefaultsAndOidNormalization() {
        SnmpSourceConfig config = new SnmpSourceConfig(ReadonlyConfig.fromMap(baseConfig()));

        Assertions.assertEquals("127.0.0.1", config.getHost());
        Assertions.assertEquals(161, config.getPort());
        Assertions.assertEquals("monitoring-community", config.getCommunity());
        Assertions.assertEquals("1.3.6.1.2.1.1.3.0", config.getOids().get(0).toString());
        Assertions.assertEquals("1.3.6.1.2.1.1.5.0", config.getOids().get(1).toString());
        Assertions.assertEquals(5000L, config.getTimeoutMillis());
        Assertions.assertEquals(1, config.getRetries());
        Assertions.assertEquals(60000L, config.getPollIntervalMillis());
    }

    @Test
    void testTrimsHost() {
        Map<String, Object> values = baseConfig();
        values.put("host", " 127.0.0.1 ");

        SnmpSourceConfig config = new SnmpSourceConfig(ReadonlyConfig.fromMap(values));

        Assertions.assertEquals("127.0.0.1", config.getHost());
    }

    @Test
    void testExplicitRequestSettings() {
        Map<String, Object> values = baseConfig();
        values.put("port", 1161);
        values.put("timeout_millis", 1200L);
        values.put("retries", 3);
        values.put("poll_interval_millis", 15000L);

        SnmpSourceConfig config = new SnmpSourceConfig(ReadonlyConfig.fromMap(values));

        Assertions.assertEquals(1161, config.getPort());
        Assertions.assertEquals(1200L, config.getTimeoutMillis());
        Assertions.assertEquals(3, config.getRetries());
        Assertions.assertEquals(15000L, config.getPollIntervalMillis());
    }

    @Test
    void testInvalidValuesFailWithoutDisclosingCommunity() {
        Map<String, Object> values = baseConfig();
        values.put("port", 0);

        IllegalArgumentException exception =
                Assertions.assertThrows(
                        IllegalArgumentException.class,
                        () -> new SnmpSourceConfig(ReadonlyConfig.fromMap(values)));

        Assertions.assertFalse(exception.getMessage().contains("monitoring-community"));
    }

    @Test
    void testInvalidAndDuplicateOidsFail() {
        Map<String, Object> invalid = baseConfig();
        invalid.put("oids", Arrays.asList("SNMPv2-MIB::sysName.0"));
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> new SnmpSourceConfig(ReadonlyConfig.fromMap(invalid)));

        Map<String, Object> duplicate = baseConfig();
        duplicate.put("oids", Arrays.asList(".1.3.6.1.2.1.1.3.0", "1.3.6.1.2.1.1.3.0"));
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> new SnmpSourceConfig(ReadonlyConfig.fromMap(duplicate)));
    }

    @Test
    void testRequestBoundsFail() {
        Map<String, Object> timeout = baseConfig();
        timeout.put("timeout_millis", 0);
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> new SnmpSourceConfig(ReadonlyConfig.fromMap(timeout)));

        Map<String, Object> retries = baseConfig();
        retries.put("retries", -1);
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> new SnmpSourceConfig(ReadonlyConfig.fromMap(retries)));

        Map<String, Object> interval = baseConfig();
        interval.put("poll_interval_millis", 0);
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> new SnmpSourceConfig(ReadonlyConfig.fromMap(interval)));
    }

    static Map<String, Object> baseConfig() {
        Map<String, Object> values = new HashMap<>();
        values.put("host", "127.0.0.1");
        values.put("community", "monitoring-community");
        values.put("oids", Arrays.asList(".1.3.6.1.2.1.1.3.0", "1.3.6.1.2.1.1.5.0"));
        return values;
    }
}
