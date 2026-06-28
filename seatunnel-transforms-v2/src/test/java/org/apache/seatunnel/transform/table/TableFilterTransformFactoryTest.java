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

package org.apache.seatunnel.transform.table;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

class TableFilterTransformFactoryTest {

    private final OptionRule rule = new TableFilterTransformFactory().optionRule();

    private void validate(Map<String, Object> config) {
        ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(rule);
    }

    @Test
    void testValidWithTablePattern() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("table_pattern", "user_.*");
        Assertions.assertDoesNotThrow(() -> validate(cfg));
    }

    @Test
    void testValidWithDatabasePattern() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("database_pattern", "prod_.*");
        Assertions.assertDoesNotThrow(() -> validate(cfg));
    }

    @Test
    void testValidWithSchemaPattern() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("schema_pattern", "public");
        Assertions.assertDoesNotThrow(() -> validate(cfg));
    }

    @Test
    void testInvalidDatabasePatternFails() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("database_pattern", "[unclosed");
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testInvalidSchemaPatternFails() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("schema_pattern", "(missing_paren");
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testInvalidTablePatternFails() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("table_pattern", "*invalid");
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testPatternModeWithTablePatternValid() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("pattern_mode", "EXCLUDE");
        cfg.put("table_pattern", "tmp_.*");
        Assertions.assertDoesNotThrow(() -> validate(cfg));
    }
}
