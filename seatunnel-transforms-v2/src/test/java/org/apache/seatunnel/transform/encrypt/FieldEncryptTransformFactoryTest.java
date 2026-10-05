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

package org.apache.seatunnel.transform.encrypt;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

class FieldEncryptTransformFactoryTest {

    private final OptionRule rule = new FieldEncryptTransformFactory().optionRule();

    private void validate(Map<String, Object> config) {
        ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(rule);
    }

    private Map<String, Object> validConfig() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("fields", Arrays.asList("password", "ssn"));
        cfg.put("key", "1234567890abcdef");
        return cfg;
    }

    @Test
    void testValidConfig() {
        Assertions.assertDoesNotThrow(() -> validate(validConfig()));
    }

    @Test
    void testMissingFieldsFails() {
        Map<String, Object> cfg = validConfig();
        cfg.remove("fields");
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testEmptyFieldsFails() {
        Map<String, Object> cfg = validConfig();
        cfg.put("fields", Collections.emptyList());
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testMissingKeyFails() {
        Map<String, Object> cfg = validConfig();
        cfg.remove("key");
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testBlankKeyFails() {
        Map<String, Object> cfg = validConfig();
        cfg.put("key", "   ");
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testMaxFieldLengthZeroFails() {
        Map<String, Object> cfg = validConfig();
        cfg.put("max_field_length", 0);
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testMaxFieldLengthNegativeFails() {
        Map<String, Object> cfg = validConfig();
        cfg.put("max_field_length", -1);
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testMaxFieldLengthPositiveValid() {
        Map<String, Object> cfg = validConfig();
        cfg.put("max_field_length", 1024);
        Assertions.assertDoesNotThrow(() -> validate(cfg));
    }
}
