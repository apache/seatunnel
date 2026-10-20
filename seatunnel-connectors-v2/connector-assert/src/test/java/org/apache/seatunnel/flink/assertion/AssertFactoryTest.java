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

package org.apache.seatunnel.flink.assertion;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConditionOperator;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.connectors.seatunnel.assertion.sink.AssertSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.assertion.sink.AssertSinkOptions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class AssertFactoryTest {

    private static final OptionRule SINK_RULE = new AssertSinkFactory().optionRule();

    @Test
    public void testOptionRule() throws Exception {
        AssertSinkFactory factory = new AssertSinkFactory();
        OptionRule optionRule = factory.optionRule();
        Assertions.assertNotNull(optionRule);
        Assertions.assertEquals(1, optionRule.getValueConstraints().size());
        Assertions.assertEquals(
                ConditionOperator.MAP_NOT_EMPTY,
                optionRule.getValueConstraints().get(0).getOperator());
        Assertions.assertEquals(
                AssertSinkOptions.RULES, optionRule.getValueConstraints().get(0).getOption());
    }

    /**
     * The factory rule is now the single enforcement point for {@code rules}; a missing map must be
     * rejected before the sink is constructed.
     */
    @Test
    public void testMissingRulesRejectedByFactoryRule() {
        Map<String, Object> config = new HashMap<>();
        config.put("plugin_name", "Assert");

        Assertions.assertThrows(OptionValidationException.class, () -> validateSink(config));
    }

    /**
     * A present but empty {@code rules {}} map must be rejected by the MAP_NOT_EMPTY constraint.
     */
    @Test
    public void testEmptyRulesRejectedByFactoryRule() {
        Map<String, Object> config = new HashMap<>();
        config.put("plugin_name", "Assert");
        config.put("rules", Collections.emptyMap());

        Assertions.assertThrows(OptionValidationException.class, () -> validateSink(config));
    }

    /** A non-empty {@code rules} map (one row rule) still passes validation. */
    @Test
    public void testNonEmptyRulesAcceptedByFactoryRule() {
        Map<String, Object> rowRule = new HashMap<>();
        rowRule.put("rule_type", "MAX_ROW");
        rowRule.put("rule_value", 10);
        Map<String, Object> rules = new HashMap<>();
        rules.put("row_rules", Collections.singletonList(rowRule));
        Map<String, Object> config = new HashMap<>();
        config.put("plugin_name", "Assert");
        config.put("rules", rules);

        Assertions.assertDoesNotThrow(() -> validateSink(config));
    }

    private static void validateSink(Map<String, Object> config) {
        ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(SINK_RULE);
    }
}
