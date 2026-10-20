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

package org.apache.seatunnel.transform.copy;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.transform.common.TransformCommonOptions;
import org.apache.seatunnel.transform.common.TransformLevelRuleAssertions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class CopyFieldTransformFactoryTest {

    @Test
    void testTransformLevelRuleDetectionMatchesTheTableRule() {
        TransformLevelRuleAssertions.assertMatchesFactoryRule(
                new CopyFieldTransformFactory(),
                CopyTransformConfig.FIELDS,
                CopyTransformConfig.SRC_FIELD);
    }

    private final OptionRule rule = new CopyFieldTransformFactory().optionRule();

    private void validate(Map<String, Object> config) {
        ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(rule);
    }

    @Test
    void testValidFieldsMapConfig() {
        Map<String, Object> cfg = new HashMap<>();
        Map<String, String> fields = new HashMap<>();
        fields.put("new_col", "old_col");
        cfg.put("fields", fields);
        Assertions.assertDoesNotThrow(() -> validate(cfg));
    }

    @Test
    void testValidSrcDestConfig() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("src_field", "old_col");
        cfg.put("dest_field", "new_col");
        Assertions.assertDoesNotThrow(() -> validate(cfg));
    }

    @Test
    void testEmptyFieldsMapFails() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("fields", Collections.emptyMap());
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testNeitherFieldsNorSrcFieldProvidedFails() {
        Map<String, Object> cfg = new HashMap<>();
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testSrcFieldWithoutDestFieldFails() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("src_field", "old_col");
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testDestFieldWithoutSrcFieldFails() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("dest_field", "new_col");
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testTableTransformOnlyPasses() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("table_transform", Collections.singletonList(srcDestRule()));
        Assertions.assertDoesNotThrow(() -> validate(cfg));
    }

    @Test
    void testTableTransformWithFieldsMapPasses() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("table_transform", Collections.singletonList(fieldsRule()));
        Assertions.assertDoesNotThrow(() -> validate(cfg));
    }

    @Test
    void testTableTransformWithTransformLevelRulePasses() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("src_field", "old_col");
        cfg.put("dest_field", "new_col");
        cfg.put("table_transform", Collections.singletonList(fieldsRule()));
        Assertions.assertDoesNotThrow(() -> validate(cfg));
    }

    @Test
    void testTableTransformEntryWithoutRulesFails() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(
                "table_transform",
                Collections.singletonList(
                        entry(TransformCommonOptions.TABLE_PATH.key(), "test.xyz")));
        OptionValidationException exception =
                Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
        Assertions.assertTrue(exception.getMessage().contains("fields"));
        Assertions.assertTrue(exception.getMessage().contains("src_field"));
    }

    @Test
    void testTableTransformEntryWithFieldsAndSrcFieldFails() {
        Map<String, Object> cfg = new HashMap<>();
        Map<String, Object> entry = fieldsRule();
        entry.put("src_field", "old_col");
        entry.put("dest_field", "new_col");
        cfg.put("table_transform", Collections.singletonList(entry));
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testTableTransformEntryWithSrcFieldWithoutDestFieldFails() {
        Map<String, Object> cfg = new HashMap<>();
        Map<String, Object> entry = entry(TransformCommonOptions.TABLE_PATH.key(), "test.xyz");
        entry.put("src_field", "old_col");
        cfg.put("table_transform", Collections.singletonList(entry));
        OptionValidationException exception =
                Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
        Assertions.assertTrue(exception.getMessage().contains("dest_field"));
    }

    @Test
    void testTableTransformEntryWithEmptyFieldsMapFails() {
        Map<String, Object> cfg = new HashMap<>();
        Map<String, Object> entry = entry(TransformCommonOptions.TABLE_PATH.key(), "test.xyz");
        entry.put("fields", Collections.emptyMap());
        cfg.put("table_transform", Collections.singletonList(entry));
        OptionValidationException exception =
                Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
        Assertions.assertTrue(
                exception.getMessage().contains("fields")
                        || exception.getMessage().contains("src_field"));
    }

    @Test
    void testTableTransformEntryWithoutTablePathFails() {
        Map<String, Object> cfg = new HashMap<>();
        Map<String, Object> entry = new HashMap<>();
        entry.put("src_field", "old_col");
        entry.put("dest_field", "new_col");
        cfg.put("table_transform", Collections.singletonList(entry));
        OptionValidationException exception =
                Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
        Assertions.assertTrue(exception.getMessage().contains("table_path"));
    }

    @Test
    void testEmptyTableTransformListStillRequiresTransformLevelRule() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("table_transform", Collections.<List<Map<String, Object>>>emptyList());
        OptionValidationException exception =
                Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
        Assertions.assertTrue(exception.getMessage().contains("exclusive"));
    }

    @Test
    void testBothTransformLevelRulesWithTableTransformFails() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put("fields", Collections.singletonMap("new_col", "old_col"));
        cfg.put("src_field", "old_col");
        cfg.put("dest_field", "new_col");
        cfg.put("table_transform", Collections.singletonList(fieldsRule()));
        OptionValidationException exception =
                Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
        Assertions.assertTrue(exception.getMessage().contains("mutually exclusive"));
    }

    private Map<String, Object> fieldsRule() {
        Map<String, Object> entry = entry(TransformCommonOptions.TABLE_PATH.key(), "test.xyz");
        entry.put("fields", Collections.singletonMap("name2", "name"));
        return entry;
    }

    private Map<String, Object> srcDestRule() {
        Map<String, Object> entry = entry(TransformCommonOptions.TABLE_PATH.key(), "test.xyz");
        entry.put("src_field", "name");
        entry.put("dest_field", "name2");
        return entry;
    }

    private Map<String, Object> entry(String key, Object value) {
        Map<String, Object> entry = new HashMap<>();
        entry.put(key, value);
        return entry;
    }
}
