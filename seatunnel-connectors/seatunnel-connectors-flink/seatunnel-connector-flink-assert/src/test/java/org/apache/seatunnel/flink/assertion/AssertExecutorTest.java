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

import org.apache.seatunnel.flink.assertion.rule.AssertFieldRule;

import junit.framework.TestCase;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.types.Row;

import java.util.List;

public class AssertExecutorTest extends TestCase {
    Row row = Row.withNames();
    AssertExecutor assertExecutor = new AssertExecutor();

    @Override
    protected void setUp() throws Exception {
        row.setField("name", "jared");
        row.setField("age", 17);
    }

    public void testFailWithType() {
        List<AssertFieldRule> rules = Lists.newArrayList();
        AssertFieldRule rule1 = new AssertFieldRule();
        rule1.setFieldName("name");
        rule1.setFieldType(Types.INT);
        rules.add(rule1);

        AssertFieldRule failRule = assertExecutor.fail(row, rules).orElse(null);
        assertNotNull(failRule);
    }

    public void testFailWithValue() {
        List<AssertFieldRule> rules = Lists.newArrayList();
        AssertFieldRule rule1 = getFieldRule4Name();
        AssertFieldRule rule2 = getFieldRule4Age();

        rules.add(rule1);
        rules.add(rule2);

        AssertFieldRule failRule = assertExecutor.fail(row, rules).orElse(null);
        assertNull(failRule);
    }

    private AssertFieldRule getFieldRule4Age() {
        AssertFieldRule rule = new AssertFieldRule();
        rule.setFieldName("age");
        rule.setFieldType(Types.INT);

        List<AssertFieldRule.AssertValueRule> valueRules = Lists.newArrayList();

        AssertFieldRule.AssertValueRule valueRule = new AssertFieldRule.AssertValueRule();
        valueRule.setFieldValueRuleType(AssertFieldRule.AssertValueRuleType.NOT_NULL);
        AssertFieldRule.AssertValueRule valueRule1 = new AssertFieldRule.AssertValueRule();
        valueRule1.setFieldValueRuleType(AssertFieldRule.AssertValueRuleType.MIN);
        valueRule1.setFieldValueRuleValue(13.0);
        AssertFieldRule.AssertValueRule valueRule2 = new AssertFieldRule.AssertValueRule();
        valueRule2.setFieldValueRuleType(AssertFieldRule.AssertValueRuleType.MAX);
        valueRule2.setFieldValueRuleValue(25.0);

        valueRules.add(valueRule);
        valueRules.add(valueRule1);
        valueRules.add(valueRule2);
        rule.setFieldValueRules(valueRules);
        return rule;
    }

    private AssertFieldRule getFieldRule4Name() {
        AssertFieldRule rule = new AssertFieldRule();
        rule.setFieldName("name");
        rule.setFieldType(Types.STRING);

        List<AssertFieldRule.AssertValueRule> valueRules = Lists.newArrayList();

        AssertFieldRule.AssertValueRule valueRule = new AssertFieldRule.AssertValueRule();
        valueRule.setFieldValueRuleType(AssertFieldRule.AssertValueRuleType.NOT_NULL);
        AssertFieldRule.AssertValueRule valueRule1 = new AssertFieldRule.AssertValueRule();
        valueRule1.setFieldValueRuleType(AssertFieldRule.AssertValueRuleType.MIN_LENGTH);
        valueRule1.setFieldValueRuleValue(3.0);
        AssertFieldRule.AssertValueRule valueRule2 = new AssertFieldRule.AssertValueRule();
        valueRule2.setFieldValueRuleType(AssertFieldRule.AssertValueRuleType.MAX_LENGTH);
        valueRule2.setFieldValueRuleValue(5.0);

        valueRules.add(valueRule);
        valueRules.add(valueRule1);
        valueRules.add(valueRule2);
        rule.setFieldValueRules(valueRules);
        return rule;
    }
}
