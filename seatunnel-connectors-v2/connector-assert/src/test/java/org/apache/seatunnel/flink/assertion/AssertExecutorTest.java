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

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.assertion.excecutor.AssertExecutor;
import org.apache.seatunnel.connectors.seatunnel.assertion.rule.AssertFieldRule;

import org.junit.jupiter.api.Test;

import com.google.common.collect.Lists;

import java.math.BigDecimal;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;

public class AssertExecutorTest {
    SeaTunnelRow row = new SeaTunnelRow(new Object[] {"jared", 17});
    SeaTunnelRowType rowType =
            new SeaTunnelRowType(
                    new String[] {"name", "age"},
                    new SeaTunnelDataType[] {BasicType.STRING_TYPE, BasicType.INT_TYPE});
    AssertExecutor assertExecutor = new AssertExecutor();

    @Test
    public void testFailWithType() {
        List<AssertFieldRule> rules = Lists.newArrayList();
        AssertFieldRule rule1 = new AssertFieldRule();
        rule1.setFieldName("name");
        rule1.setFieldType(BasicType.INT_TYPE);
        rules.add(rule1);

        AssertFieldRule failRule = assertExecutor.fail(row, rowType, rules).orElse(null);
        assertNotNull(failRule);
    }

    @Test
    public void testFailWithValue() {
        List<AssertFieldRule> rules = Lists.newArrayList();
        AssertFieldRule rule1 = getFieldRule4Name();
        AssertFieldRule rule2 = getFieldRule4Age();

        rules.add(rule1);
        rules.add(rule2);

        AssertFieldRule failRule = assertExecutor.fail(row, rowType, rules).orElse(null);
        assertNull(failRule);
    }

    private AssertFieldRule getFieldRule4Age() {
        AssertFieldRule rule = new AssertFieldRule();
        rule.setFieldName("age");
        rule.setFieldType(BasicType.INT_TYPE);

        List<AssertFieldRule.AssertRule> valueRules = Lists.newArrayList();

        AssertFieldRule.AssertRule valueRule = new AssertFieldRule.AssertRule();
        valueRule.setRuleType(AssertFieldRule.AssertRuleType.NOT_NULL);
        AssertFieldRule.AssertRule valueRule1 = new AssertFieldRule.AssertRule();
        valueRule1.setRuleType(AssertFieldRule.AssertRuleType.MIN);
        valueRule1.setRuleValue(13.0);
        AssertFieldRule.AssertRule valueRule2 = new AssertFieldRule.AssertRule();
        valueRule2.setRuleType(AssertFieldRule.AssertRuleType.MAX);
        valueRule2.setRuleValue(25.0);

        valueRules.add(valueRule);
        valueRules.add(valueRule1);
        valueRules.add(valueRule2);
        rule.setFieldRules(valueRules);
        return rule;
    }

    private AssertFieldRule getFieldRule4Name() {
        AssertFieldRule rule = new AssertFieldRule();
        rule.setFieldName("name");
        rule.setFieldType(BasicType.STRING_TYPE);

        List<AssertFieldRule.AssertRule> valueRules = Lists.newArrayList();

        AssertFieldRule.AssertRule valueRule = new AssertFieldRule.AssertRule();
        valueRule.setRuleType(AssertFieldRule.AssertRuleType.NOT_NULL);
        AssertFieldRule.AssertRule valueRule1 = new AssertFieldRule.AssertRule();
        valueRule1.setRuleType(AssertFieldRule.AssertRuleType.MIN_LENGTH);
        valueRule1.setRuleValue(3.0);
        AssertFieldRule.AssertRule valueRule2 = new AssertFieldRule.AssertRule();
        valueRule2.setRuleType(AssertFieldRule.AssertRuleType.MAX_LENGTH);
        valueRule2.setRuleValue(5.0);

        valueRules.add(valueRule);
        valueRules.add(valueRule1);
        valueRules.add(valueRule2);
        rule.setFieldRules(valueRules);
        return rule;
    }

    @Test
    public void testDecimalTypeCheck() {
        List<AssertFieldRule> rules = Lists.newArrayList();
        AssertFieldRule rule = new AssertFieldRule();
        rule.setFieldName("c_mock");
        DecimalType assertFieldType = new DecimalType(10, 2);
        rule.setFieldType(assertFieldType);

        AssertFieldRule.AssertRule valueRule = new AssertFieldRule.AssertRule();
        valueRule.setEqualTo("99999999.90");

        rule.setFieldRules(Collections.singletonList(valueRule));
        rules.add(rule);

        SeaTunnelRow mockRow = new SeaTunnelRow(new Object[] {new BigDecimal("99999999.90")});
        SeaTunnelRowType mockType =
                new SeaTunnelRowType(
                        new String[] {"c_mock"}, new SeaTunnelDataType[] {new DecimalType(10, 2)});

        AssertFieldRule failRule = assertExecutor.fail(mockRow, mockType, rules).orElse(null);
        assertNull(failRule);
    }

    @Test
    public void testDecimalTypeCheckError() {
        List<AssertFieldRule> rules = Lists.newArrayList();
        AssertFieldRule rule = new AssertFieldRule();
        rule.setFieldName("c_mock");
        DecimalType assertFieldType = new DecimalType(1, 0);
        rule.setFieldType(assertFieldType);

        AssertFieldRule.AssertRule valueRule = new AssertFieldRule.AssertRule();
        valueRule.setRuleType(AssertFieldRule.AssertRuleType.NOT_NULL);
        rule.setFieldRules(Collections.singletonList(valueRule));
        rules.add(rule);

        SeaTunnelRow mockRow = new SeaTunnelRow(new Object[] {BigDecimal.valueOf(99999999.99)});
        SeaTunnelRowType mockType =
                new SeaTunnelRowType(
                        new String[] {"c_mock"}, new SeaTunnelDataType[] {new DecimalType(10, 2)});

        AssertFieldRule failRule = assertExecutor.fail(mockRow, mockType, rules).orElse(null);
        assertNotNull(failRule);
        assertEquals(assertFieldType, failRule.getFieldType());
        assertEquals("c_mock", failRule.getFieldName());
    }
}
