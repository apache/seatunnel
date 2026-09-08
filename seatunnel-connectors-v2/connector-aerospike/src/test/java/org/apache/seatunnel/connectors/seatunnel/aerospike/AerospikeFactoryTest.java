/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.aerospike;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.connectors.seatunnel.aerospike.config.AerospikeSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.aerospike.sink.AerospikeSinkFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class AerospikeFactoryTest {

    private final OptionRule sinkRule = new AerospikeSinkFactory().optionRule();

    @Test
    void supportedDataFormatsPassValidation() {
        for (String dataFormat : new String[] {"map", "string", "kv"}) {
            Assertions.assertDoesNotThrow(() -> validate(dataFormat));
        }
    }

    @Test
    void dataFormatValidationIsCaseInsensitive() {
        for (String dataFormat : new String[] {"MAP", "String", "Kv"}) {
            Assertions.assertDoesNotThrow(() -> validate(dataFormat));
        }
    }

    @Test
    void unsupportedDataFormatFailsValidation() {
        OptionValidationException exception =
                Assertions.assertThrows(
                        OptionValidationException.class, () -> validate("unsupported"));

        Assertions.assertTrue(
                exception.getMessage().contains(AerospikeSinkOptions.DATA_FORMAT.key()));
    }

    @Test
    void defaultDataFormatPassesValidation() {
        Assertions.assertDoesNotThrow(() -> validate(validConfig()));
    }

    private void validate(String dataFormat) {
        Map<String, Object> config = validConfig();
        config.put(AerospikeSinkOptions.DATA_FORMAT.key(), dataFormat);
        validate(config);
    }

    private void validate(Map<String, Object> config) {
        ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(sinkRule);
    }

    private Map<String, Object> validConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put(AerospikeSinkOptions.HOST.key(), "localhost");
        config.put(AerospikeSinkOptions.PORT.key(), 3000);
        config.put(AerospikeSinkOptions.NAMESPACE.key(), "test");
        config.put(AerospikeSinkOptions.SET.key(), "test-set");
        return config;
    }
}
