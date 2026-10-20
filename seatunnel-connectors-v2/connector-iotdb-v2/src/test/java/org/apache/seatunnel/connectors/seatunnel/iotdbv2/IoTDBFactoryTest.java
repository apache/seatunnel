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

package org.apache.seatunnel.connectors.seatunnel.iotdbv2;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.connectors.seatunnel.iotdbv2.config.IoTDBv2CommonOptions;
import org.apache.seatunnel.connectors.seatunnel.iotdbv2.config.IoTDBv2SinkOptions;
import org.apache.seatunnel.connectors.seatunnel.iotdbv2.config.IoTDBv2SourceOptions;
import org.apache.seatunnel.connectors.seatunnel.iotdbv2.sink.IoTDBv2SinkFactory;
import org.apache.seatunnel.connectors.seatunnel.iotdbv2.source.IoTDBv2SourceFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

class IoTDBFactoryTest {

    private final OptionRule sourceRule = new IoTDBv2SourceFactory().optionRule();
    private final OptionRule sinkRule = new IoTDBv2SinkFactory().optionRule();

    @Test
    void optionRule() {
        Assertions.assertNotNull(sourceRule);
        Assertions.assertNotNull(sinkRule);
    }

    @Test
    void omittedSqlDialectIsAccepted() {
        Assertions.assertDoesNotThrow(() -> validate(validSourceConfig(), sourceRule));
        Assertions.assertDoesNotThrow(() -> validate(validSinkConfig(), sinkRule));
    }

    @Test
    void supportedSqlDialectsAreAcceptedCaseInsensitively() {
        for (String dialect : Arrays.asList("tree", "table", "TrEe", "TaBlE")) {
            Map<String, Object> sourceConfig = validSourceConfig();
            sourceConfig.put(IoTDBv2CommonOptions.SQL_DIALECT.key(), dialect);
            Assertions.assertDoesNotThrow(() -> validate(sourceConfig, sourceRule));

            Map<String, Object> sinkConfig = validSinkConfig();
            sinkConfig.put(IoTDBv2CommonOptions.SQL_DIALECT.key(), dialect);
            Assertions.assertDoesNotThrow(() -> validate(sinkConfig, sinkRule));
        }
    }

    @Test
    void unsupportedSqlDialectIsRejected() {
        Map<String, Object> sourceConfig = validSourceConfig();
        sourceConfig.put(IoTDBv2CommonOptions.SQL_DIALECT.key(), "unsupported");
        OptionValidationException sourceException =
                Assertions.assertThrows(
                        OptionValidationException.class, () -> validate(sourceConfig, sourceRule));
        Assertions.assertTrue(
                sourceException.getMessage().contains(IoTDBv2CommonOptions.SQL_DIALECT.key()));

        Map<String, Object> sinkConfig = validSinkConfig();
        sinkConfig.put(IoTDBv2CommonOptions.SQL_DIALECT.key(), "unsupported");
        OptionValidationException sinkException =
                Assertions.assertThrows(
                        OptionValidationException.class, () -> validate(sinkConfig, sinkRule));
        Assertions.assertTrue(
                sinkException.getMessage().contains(IoTDBv2CommonOptions.SQL_DIALECT.key()));
    }

    private void validate(Map<String, Object> config, OptionRule rule) {
        ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(rule);
    }

    private Map<String, Object> validSourceConfig() {
        Map<String, Object> config = commonConfig();
        config.put(IoTDBv2SourceOptions.SQL.key(), "select * from root.test");
        config.put(ConnectorCommonOptions.SCHEMA.key(), Collections.emptyMap());
        return config;
    }

    private Map<String, Object> validSinkConfig() {
        Map<String, Object> config = commonConfig();
        config.put(IoTDBv2SinkOptions.STORAGE_GROUP.key(), "root.test");
        config.put(IoTDBv2SinkOptions.KEY_DEVICE.key(), "device");
        return config;
    }

    private Map<String, Object> commonConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put(
                IoTDBv2CommonOptions.NODE_URLS.key(), Collections.singletonList("127.0.0.1:6667"));
        config.put(IoTDBv2CommonOptions.USERNAME.key(), "root");
        config.put(IoTDBv2CommonOptions.PASSWORD.key(), "root");
        return config;
    }
}
