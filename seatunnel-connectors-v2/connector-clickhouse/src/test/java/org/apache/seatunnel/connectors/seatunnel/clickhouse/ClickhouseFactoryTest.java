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

package org.apache.seatunnel.connectors.seatunnel.clickhouse;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.config.ClickhouseBaseOptions;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.sink.client.ClickhouseSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.sink.file.ClickhouseFileSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.clickhouse.source.ClickhouseSourceFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class ClickhouseFactoryTest {

    private static final ClickhouseFileSinkFactory FILE_SINK_FACTORY =
            new ClickhouseFileSinkFactory();

    private void validateSource(Map<String, Object> configMap) {
        ClickhouseSourceFactory factory = new ClickhouseSourceFactory();
        ConfigValidator.of(ReadonlyConfig.fromMap(configMap)).validate(factory.optionRule());
    }

    private Map<String, Object> createValidSourceConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put(ClickhouseBaseOptions.HOST.key(), "localhost:8123");
        config.put(ClickhouseBaseOptions.USERNAME.key(), "default");
        config.put(ClickhouseBaseOptions.PASSWORD.key(), "password");
        return config;
    }

    @Test
    public void testSourceHostValidation() {
        Map<String, Object> validConfig = createValidSourceConfig();
        Assertions.assertDoesNotThrow(() -> validateSource(validConfig));

        Map<String, Object> missingHost = createValidSourceConfig();
        missingHost.remove(ClickhouseBaseOptions.HOST.key());
        Assertions.assertThrows(OptionValidationException.class, () -> validateSource(missingHost));

        Map<String, Object> emptyHost = createValidSourceConfig();
        emptyHost.put(ClickhouseBaseOptions.HOST.key(), "");
        Assertions.assertThrows(OptionValidationException.class, () -> validateSource(emptyHost));

        Map<String, Object> whitespaceHost = createValidSourceConfig();
        whitespaceHost.put(ClickhouseBaseOptions.HOST.key(), "   ");
        Assertions.assertThrows(
                OptionValidationException.class, () -> validateSource(whitespaceHost));

        Map<String, Object> paddedHost = createValidSourceConfig();
        paddedHost.put(ClickhouseBaseOptions.HOST.key(), "  localhost:8123  ");
        Assertions.assertDoesNotThrow(() -> validateSource(paddedHost));
    }

    @Test
    public void testOptionRule() {
        Assertions.assertNotNull((new ClickhouseSourceFactory()).optionRule());
        Assertions.assertNotNull((new ClickhouseSinkFactory()).optionRule());
        Assertions.assertNotNull(FILE_SINK_FACTORY.optionRule());
    }

    private static Map<String, Object> validFileSinkConfig() {
        Map<String, Object> map = new HashMap<>();
        map.put("host", "127.0.0.1:8123");
        map.put("table", "test_table");
        map.put("database", "test_db");
        map.put("username", "root");
        map.put("password", "");
        map.put("clickhouse_local_path", "/usr/bin/clickhouse");
        return map;
    }

    private void validateFileSink(Map<String, Object> map) {
        ConfigValidator.of(ReadonlyConfig.fromMap(map)).validate(FILE_SINK_FACTORY.optionRule());
    }

    @Test
    public void singleCharacterDelimiterPassesValidation() {
        Map<String, Object> map = validFileSinkConfig();
        map.put("file_fields_delimiter", ",");
        validateFileSink(map);
    }

    @Test
    public void absentDelimiterSkipsValidationAndUsesDefault() {
        validateFileSink(validFileSinkConfig());
    }

    @Test
    public void multiCharacterDelimiterIsRejected() {
        Map<String, Object> map = validFileSinkConfig();
        map.put("file_fields_delimiter", ",,,");
        OptionValidationException exception =
                Assertions.assertThrows(
                        OptionValidationException.class, () -> validateFileSink(map));
        Assertions.assertTrue(
                exception.getMessage().contains("file_fields_delimiter"),
                () -> "missing file_fields_delimiter in: " + exception.getMessage());
    }

    @Test
    public void emptyDelimiterIsRejected() {
        Map<String, Object> map = validFileSinkConfig();
        map.put("file_fields_delimiter", "");
        OptionValidationException exception =
                Assertions.assertThrows(
                        OptionValidationException.class, () -> validateFileSink(map));
        Assertions.assertTrue(
                exception.getMessage().contains("file_fields_delimiter"),
                () -> "missing file_fields_delimiter in: " + exception.getMessage());
    }
}
