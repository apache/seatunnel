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

package org.apache.seatunnel.transform.validator;

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

class DataValidatorTransformFactoryTest {

    private final OptionRule rule = new DataValidatorTransformFactory().optionRule();

    private void validate(Map<String, Object> config) {
        ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(rule);
    }

    @Test
    void testValidConfig() {
        Map<String, Object> cfg = new HashMap<>();
        Map<String, Object> fieldRule = new HashMap<>();
        fieldRule.put("field_name", "age");
        fieldRule.put("rule_type", "NOT_NULL");
        cfg.put("field_rules", Arrays.asList(fieldRule));
        Assertions.assertDoesNotThrow(() -> validate(cfg));
    }

    @Test
    void testMissingFieldRulesFails() {
        Map<String, Object> cfg = new HashMap<>();
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testEmptyFieldRulesFails() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("field_rules", Collections.emptyList());
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testMissingFieldNameFails() {
        Map<String, Object> cfg = new HashMap<>();
        Map<String, Object> fieldRule = new HashMap<>();
        fieldRule.put("rule_type", "NOT_NULL");
        cfg.put("field_rules", Arrays.asList(fieldRule));
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testMissingRuleTypeAndRulesFails() {
        Map<String, Object> cfg = new HashMap<>();
        Map<String, Object> fieldRule = new HashMap<>();
        fieldRule.put("field_name", "age");
        cfg.put("field_rules", Arrays.asList(fieldRule));
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testNestedRulesValid() {
        Map<String, Object> cfg = new HashMap<>();
        Map<String, Object> fieldRule = new HashMap<>();
        fieldRule.put("field_name", "email");
        Map<String, Object> nestedRule = new HashMap<>();
        nestedRule.put("rule_type", "NOT_NULL");
        fieldRule.put("rules", Arrays.asList(nestedRule));
        cfg.put("field_rules", Arrays.asList(fieldRule));
        Assertions.assertDoesNotThrow(() -> validate(cfg));
    }

    @Test
    void testEmptyRulesListFails() {
        Map<String, Object> cfg = new HashMap<>();
        Map<String, Object> fieldRule = new HashMap<>();
        fieldRule.put("field_name", "email");
        fieldRule.put("rules", Collections.emptyList());
        cfg.put("field_rules", Arrays.asList(fieldRule));
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }
}
