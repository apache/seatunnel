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

package org.apache.seatunnel.connectors.seatunnel.influxdb;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.connectors.seatunnel.influxdb.sink.InfluxDBSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.influxdb.source.InfluxDBSourceFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class InfluxDBFactoryTest {

    private final InfluxDBSinkFactory sinkFactory = new InfluxDBSinkFactory();

    private static Map<String, Object> validSinkConfig() {
        Map<String, Object> map = new HashMap<>();
        map.put("url", "http://127.0.0.1:8086");
        map.put("database", "test_db");
        return map;
    }

    private void validateSink(Map<String, Object> map) {
        ConfigValidator.of(ReadonlyConfig.fromMap(map)).validate(sinkFactory.optionRule());
    }

    private void assertRejected(String key, Object value) {
        Map<String, Object> map = validSinkConfig();
        map.put(key, value);
        Assertions.assertThrows(OptionValidationException.class, () -> validateSink(map));
    }

    @Test
    void optionRule() {
        Assertions.assertNotNull((new InfluxDBSourceFactory()).optionRule());
        Assertions.assertNotNull(sinkFactory.optionRule());
    }

    @Test
    void validSinkConfigPassesValidation() {
        validateSink(validSinkConfig());
    }

    @Test
    void validSinkConfigWithOptionalBoundsPassesValidation() {
        Map<String, Object> map = validSinkConfig();
        map.put("connect_timeout_ms", 1L);
        map.put("query_timeout_sec", 1);
        map.put("batch_size", 1);
        map.put("write_timeout", 1);
        validateSink(map);
    }

    @Test
    void missingUrlIsRejected() {
        Map<String, Object> map = validSinkConfig();
        map.remove("url");
        OptionValidationException ex =
                Assertions.assertThrows(OptionValidationException.class, () -> validateSink(map));
        Assertions.assertTrue(ex.getMessage().contains("url"));
    }

    @Test
    void blankUrlIsRejected() {
        Map<String, Object> map = validSinkConfig();
        map.put("url", "   ");
        Assertions.assertThrows(OptionValidationException.class, () -> validateSink(map));
    }

    @Test
    void missingDatabaseIsRejected() {
        Map<String, Object> map = validSinkConfig();
        map.remove("database");
        OptionValidationException ex =
                Assertions.assertThrows(OptionValidationException.class, () -> validateSink(map));
        Assertions.assertTrue(ex.getMessage().contains("database"));
    }

    @Test
    void blankDatabaseIsRejected() {
        Map<String, Object> map = validSinkConfig();
        map.put("database", "");
        Assertions.assertThrows(OptionValidationException.class, () -> validateSink(map));
    }

    @Test
    void zeroTimeoutsStayValid() {
        // 0 keeps the "no timeout" meaning of the underlying HTTP client, so it stays accepted
        Map<String, Object> map = validSinkConfig();
        map.put("connect_timeout_ms", 0L);
        map.put("query_timeout_sec", 0);
        validateSink(map);
    }

    @Test
    void nonPositiveBatchSizeIsRejected() {
        Map<String, Object> map = validSinkConfig();
        map.put("batch_size", 0);
        Assertions.assertThrows(OptionValidationException.class, () -> validateSink(map));
    }

    @Test
    void writeTimeoutIsNotConstrained() {
        // write_timeout is declared but never read at runtime, so no value constraint is applied
        Map<String, Object> map = validSinkConfig();
        map.put("write_timeout", 0);
        validateSink(map);
    }

    @Test
    void negativeOptionalValuesAreRejected() {
        assertRejected("connect_timeout_ms", -1L);
        assertRejected("query_timeout_sec", -1);
        assertRejected("batch_size", -1);
    }

    @Test
    void missingUrlAndDatabaseReportBothKeys() {
        OptionValidationException ex =
                Assertions.assertThrows(
                        OptionValidationException.class, () -> validateSink(new HashMap<>()));
        String message = ex.getMessage();
        Assertions.assertTrue(message.contains("url"), () -> "missing url in: " + message);
        Assertions.assertTrue(
                message.contains("database"), () -> "missing database in: " + message);
    }

    @Test
    void bundledUsernamePasswordPassValidation() {
        Map<String, Object> map = validSinkConfig();
        map.put("username", "user");
        map.put("password", "pass");
        validateSink(map);
    }

    @Test
    void usernameWithoutPasswordIsRejected() {
        Map<String, Object> map = validSinkConfig();
        map.put("username", "user");
        Assertions.assertThrows(OptionValidationException.class, () -> validateSink(map));
    }

    @Test
    void multiTableSinkReplicaPassesValidation() {
        Map<String, Object> map = validSinkConfig();
        map.put("multi_table_sink_replica", 2);
        validateSink(map);
        // validate() alone stays green if the option is dropped from the rule,
        // so pin the declaration with the unknown-keys check as well
        ConfigValidator.validateUnknownKeys(
                ReadonlyConfig.fromMap(map), sinkFactory.optionRule(), "InfluxDB");
    }
}
