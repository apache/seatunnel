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
import org.apache.seatunnel.connectors.seatunnel.influxdb.config.InfluxDBSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.influxdb.sink.InfluxDBSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.influxdb.source.InfluxDBSourceFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class InfluxDBFactoryTest {

    @Test
    void optionRule() {
        Assertions.assertNotNull((new InfluxDBSourceFactory()).optionRule());
        Assertions.assertNotNull((new InfluxDBSinkFactory()).optionRule());
    }

    @Test
    void testSinkOptionRuleSuccess() {
        Map<String, Object> map = new HashMap<>();
        map.put(InfluxDBSinkOptions.URL.key(), "http://localhost:8086");
        map.put(InfluxDBSinkOptions.DATABASES.key(), "test_db");
        map.put(InfluxDBSinkOptions.USERNAME.key(), "admin");
        map.put(InfluxDBSinkOptions.PASSWORD.key(), "123456");

        ReadonlyConfig config = ReadonlyConfig.fromMap(map);
        Assertions.assertDoesNotThrow(
                () ->
                        ConfigValidator.of(config)
                                .validate((new InfluxDBSinkFactory()).optionRule()));
    }

    @Test
    void testSinkOptionRuleMissingRequired() {
        Map<String, Object> map = new HashMap<>();
        map.put(InfluxDBSinkOptions.URL.key(), "http://localhost:8086");

        ReadonlyConfig config = ReadonlyConfig.fromMap(map);
        Assertions.assertThrows(
                OptionValidationException.class,
                () ->
                        ConfigValidator.of(config)
                                .validate((new InfluxDBSinkFactory()).optionRule()));
    }

    @Test
    void testSinkOptionRuleBundledFailed() {
        Map<String, Object> map = new HashMap<>();
        map.put(InfluxDBSinkOptions.URL.key(), "http://localhost:8086");
        map.put(InfluxDBSinkOptions.DATABASES.key(), "test_db");
        map.put(InfluxDBSinkOptions.USERNAME.key(), "admin");

        ReadonlyConfig config = ReadonlyConfig.fromMap(map);
        Assertions.assertThrows(
                OptionValidationException.class,
                () ->
                        ConfigValidator.of(config)
                                .validate((new InfluxDBSinkFactory()).optionRule()));
    }
}
