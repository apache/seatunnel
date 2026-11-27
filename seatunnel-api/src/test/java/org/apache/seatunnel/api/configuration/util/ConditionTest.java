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

package org.apache.seatunnel.api.configuration.util;

import org.apache.seatunnel.api.configuration.OptionTest;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.apache.seatunnel.api.configuration.OptionTest.TEST_MODE;
import static org.apache.seatunnel.api.configuration.OptionTest.TEST_NUM;

public class ConditionTest {
    private static final Condition<OptionTest.TestMode> TEST_CONDITION =
            Condition.of(TEST_MODE, OptionTest.TestMode.EARLIEST)
                    .or(TEST_MODE, OptionTest.TestMode.LATEST)
                    .and(TEST_NUM, 1000);

    @Test
    public void testToString() {
        Assertions.assertEquals(
                "'option.mode' == EARLIEST || ('option.mode' == LATEST && 'option.num' == 1000)",
                TEST_CONDITION.toString());
    }

    @Test
    public void testGetCount() {
        Assertions.assertEquals(3, TEST_CONDITION.getCount());
    }

    @Test
    public void testGetTailCondition() {
        Assertions.assertEquals(Condition.of(TEST_NUM, 1000), TEST_CONDITION.getTailCondition());
    }

    @Test
    public void testChainEqualityAndHashCode() {
        Condition<?> first =
                Condition.of(TEST_MODE, OptionTest.TestMode.EARLIEST).or(TEST_NUM, 1000);
        Condition<?> second =
                Condition.of(TEST_MODE, OptionTest.TestMode.EARLIEST).or(TEST_NUM, 1000);

        Assertions.assertEquals(first, second);
        Assertions.assertEquals(first.hashCode(), second.hashCode());

        Condition<?> differentOperator =
                Condition.of(TEST_MODE, OptionTest.TestMode.EARLIEST).and(TEST_NUM, 1000);
        Assertions.assertNotEquals(first, differentOperator);
    }

    @Test
    public void testChainCanBeUsedAsMapKey() {
        Map<Condition<?>, String> rules = new HashMap<>();
        rules.put(
                Condition.of(TEST_MODE, OptionTest.TestMode.EARLIEST).or(TEST_NUM, 1000), "first");
        rules.put(
                Condition.of(TEST_MODE, OptionTest.TestMode.EARLIEST).or(TEST_NUM, 1000), "second");

        Assertions.assertEquals(1, rules.size());
        Assertions.assertEquals(
                "second",
                rules.get(
                        Condition.of(TEST_MODE, OptionTest.TestMode.EARLIEST).or(TEST_NUM, 1000)));
    }

    @Test
    public void testExtensionConditionsAreDistinctMapKeys() {
        ConditionExtension<Integer> firstExtension = maximumValue(1000);
        ConditionExtension<Integer> secondExtension = maximumValue(2000);
        Map<String, Object> values = new HashMap<>();
        values.put(TEST_NUM.key(), 1500);
        ReadonlyConfig config = ReadonlyConfig.fromMap(values);

        for (boolean chained : new boolean[] {false, true}) {
            Condition<?> first = Conditions.extension(TEST_NUM, firstExtension);
            Condition<?> second = Conditions.extension(TEST_NUM, secondExtension);
            Condition<?> same = Conditions.extension(TEST_NUM, firstExtension);
            if (chained) {
                first = Conditions.greaterThan(TEST_NUM, 0).and(first);
                second = Conditions.greaterThan(TEST_NUM, 0).and(second);
                same = Conditions.greaterThan(TEST_NUM, 0).and(same);
            }

            OptionRule firstRule = OptionRule.builder().optional(TEST_NUM, first).build();
            OptionRule secondRule = OptionRule.builder().optional(TEST_NUM, second).build();
            Assertions.assertThrows(
                    OptionValidationException.class,
                    () -> ConfigValidator.of(config).validate(firstRule));
            Assertions.assertDoesNotThrow(() -> ConfigValidator.of(config).validate(secondRule));
            Assertions.assertNotEquals(first, second);
            Assertions.assertEquals(first, same);
            Assertions.assertEquals(first.hashCode(), same.hashCode());
            Assertions.assertNotEquals(firstRule, secondRule);

            Map<Condition<?>, String> rules = new HashMap<>();
            rules.put(first, "first");
            rules.put(second, "second");
            Assertions.assertEquals(2, rules.size());
            Assertions.assertEquals("first", rules.get(same));
            Assertions.assertEquals("second", rules.get(second));
        }
    }

    private static ConditionExtension<Integer> maximumValue(int maximum) {
        return new ConditionExtension<Integer>() {
            @Override
            public String description() {
                return "must not exceed " + maximum;
            }

            @Override
            public boolean evaluate(ReadonlyConfig config, Integer value) {
                return value != null && value <= maximum;
            }
        };
    }
}
