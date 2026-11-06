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
package org.apache.seatunnel.connectors.seatunnel.fluss.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.connectors.seatunnel.fluss.config.FlussSinkOptions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class FlussSinkFactoryTest {

    private final OptionRule sinkRule = new FlussSinkFactory().optionRule();

    @Test
    void testValidConfig() {
        Assertions.assertDoesNotThrow(() -> validate(validConfig()));
    }

    @Test
    void testMissingBootstrapServersRejected() {
        Map<String, Object> cfg = validConfig();
        cfg.remove(FlussSinkOptions.BOOTSTRAP_SERVERS.key());
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testBlankBootstrapServersRejected() {
        Map<String, Object> emptyCfg = validConfig();
        emptyCfg.put(FlussSinkOptions.BOOTSTRAP_SERVERS.key(), "");
        Assertions.assertThrows(OptionValidationException.class, () -> validate(emptyCfg));

        Map<String, Object> whitespaceCfg = validConfig();
        whitespaceCfg.put(FlussSinkOptions.BOOTSTRAP_SERVERS.key(), "   ");
        Assertions.assertThrows(OptionValidationException.class, () -> validate(whitespaceCfg));
    }

    private void validate(Map<String, Object> config) {
        ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(sinkRule);
    }

    private Map<String, Object> validConfig() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(FlussSinkOptions.BOOTSTRAP_SERVERS.key(), "localhost:9123");
        return cfg;
    }
}
