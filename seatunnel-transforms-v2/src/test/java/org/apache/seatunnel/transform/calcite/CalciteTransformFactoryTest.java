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

package org.apache.seatunnel.transform.calcite;

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
import java.util.Map;

class CalciteTransformFactoryTest {

    @Test
    void testTransformLevelRuleDetectionMatchesTheTableRule() {
        TransformLevelRuleAssertions.assertMatchesFactoryRule(
                new CalciteTransformFactory(), CalciteTransformConfig.SQL);
    }

    @Test
    void testFactoryIdentifier() {
        CalciteTransformFactory factory = new CalciteTransformFactory();
        Assertions.assertEquals("Calcite", factory.factoryIdentifier());
    }

    @Test
    void testOptionRule() {
        CalciteTransformFactory factory = new CalciteTransformFactory();
        OptionRule rule = factory.optionRule();
        Assertions.assertNotNull(rule);
        // 'sql' can be configured at the transform level or per table, so it is declared as an
        // optional option at the top level.
        Assertions.assertTrue(rule.getOptionalOptions().contains(CalciteTransformConfig.SQL));
        // A single table still has to configure it: the mandatory part lives in the conditional
        // rule
        // that applies while no per-table rule is configured.
        Assertions.assertFalse(rule.getConditionRules().isEmpty());
        OptionRule tableRule = rule.getConditionRules().get(0).getOptionRule();
        Assertions.assertTrue(
                tableRule.getRequiredOptions().stream()
                        .anyMatch(
                                requiredOption ->
                                        requiredOption
                                                .getOptions()
                                                .contains(CalciteTransformConfig.SQL)));
    }

    @Test
    void testTableTransformOnlyConfigPasses() {
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(TransformCommonOptions.MULTI_TABLES.key(), Collections.singletonList(tableRule()));
        Assertions.assertDoesNotThrow(() -> validate(cfg));
    }

    @Test
    void testTableTransformEntryWithoutTablePathFails() {
        Map<String, Object> entry = tableRule();
        entry.remove(TransformCommonOptions.TABLE_PATH.key());
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(TransformCommonOptions.MULTI_TABLES.key(), Collections.singletonList(entry));
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testTableTransformEntryMustSatisfyTheRule() {
        Map<String, Object> entry = tableRule();
        entry.remove("sql");
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(TransformCommonOptions.MULTI_TABLES.key(), Collections.singletonList(entry));
        Assertions.assertThrows(OptionValidationException.class, () -> validate(cfg));
    }

    @Test
    void testGlobalPolicyOptionDoesNotRequireTheTransformLevelRule() {
        // 'row_error_handle_way' is a global policy, it must not make the per-table rule partial
        Map<String, Object> cfg = new HashMap<>();
        cfg.put(TransformCommonOptions.MULTI_TABLES.key(), Collections.singletonList(tableRule()));
        cfg.put("row_error_handle_way", "SKIP");
        Assertions.assertDoesNotThrow(() -> validate(cfg));
    }

    /** A complete rule for a single table, used as a {@code table_transform} entry. */
    private Map<String, Object> tableRule() {
        Map<String, Object> entry = new HashMap<>();
        entry.put(TransformCommonOptions.TABLE_PATH.key(), "test.xyz");
        entry.put("sql", "select * from test");
        return entry;
    }

    private void validate(Map<String, Object> config) {
        ConfigValidator.of(ReadonlyConfig.fromMap(config))
                .validate(new CalciteTransformFactory().optionRule());
    }
}
