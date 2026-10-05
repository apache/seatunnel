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

package org.apache.seatunnel.transform.regexextract;

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

class RegexExtractTransformFactoryTest {

    private final OptionRule rule = new RegexExtractTransformFactory().optionRule();

    private void validate(Map<String, Object> config) {
        ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(rule);
    }

    private Map<String, Object> validConfig() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("source_field", "log_line");
        cfg.put("regex_pattern", "(\\d+)-(\\w+)");
        cfg.put("output_fields", Arrays.asList("id", "name"));
        return cfg;
    }

    @Test
    void testValidConfig() {
        Assertions.assertDoesNotThrow(() -> validate(validConfig()));
    }

    @Test
    void testValidConfigWithMatchingDefaults() {
        Map<String, Object> cfg = validConfig();
        cfg.put("default_values", Arrays.asList("0", "unknown"));
        Assertions.assertDoesNotThrow(() -> validate(cfg));
    }

    @Test
    void testMissingSourceFieldFails() {
        Map<String, Object> cfg = validConfig();
        cfg.remove("source_field");
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testMissingRegexPatternFails() {
        Map<String, Object> cfg = validConfig();
        cfg.remove("regex_pattern");
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testMissingOutputFieldsFails() {
        Map<String, Object> cfg = validConfig();
        cfg.remove("output_fields");
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testDefaultValuesLengthMismatchFails() {
        Map<String, Object> cfg = validConfig();
        cfg.put("default_values", Arrays.asList("0", "unknown", "extra"));
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testEmptyOutputFieldsFails() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("source_field", "log_line");
        cfg.put("regex_pattern", "(\\d+)");
        cfg.put("output_fields", Collections.emptyList());
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }
}
