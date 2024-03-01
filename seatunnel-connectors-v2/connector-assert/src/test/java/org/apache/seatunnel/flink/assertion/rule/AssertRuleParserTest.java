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

package org.apache.seatunnel.flink.assertion.rule;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.assertion.rule.AssertFieldRule;
import org.apache.seatunnel.connectors.seatunnel.assertion.rule.AssertRuleParser;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AssertRuleParserTest {
    AssertRuleParser parser = new AssertRuleParser();

    @Test
    public void testParseRules() {
        List<? extends Config> ruleConfigList = assembleConfig();
        List<AssertFieldRule> assertFieldRules = parser.parseRules(ruleConfigList);
        assertEquals(4, assertFieldRules.size());

        AssertFieldRule nameRule = assertFieldRules.get(0);
        List<AssertFieldRule.AssertRule> nameValueRules = nameRule.getFieldRules();
        assertEquals(BasicType.STRING_TYPE, nameRule.getFieldType());
        assertEquals("name", nameRule.getFieldName());
        assertEquals(3, nameValueRules.size());
        assertEquals(AssertFieldRule.AssertRuleType.NOT_NULL, nameValueRules.get(0).getRuleType());
        assertEquals(
                AssertFieldRule.AssertRuleType.MIN_LENGTH, nameValueRules.get(1).getRuleType());
        assertEquals(3.0, nameValueRules.get(1).getRuleValue());
        assertEquals(
                AssertFieldRule.AssertRuleType.MAX_LENGTH, nameValueRules.get(2).getRuleType());
        assertEquals(5.0, nameValueRules.get(2).getRuleValue());

        AssertFieldRule ageRule = assertFieldRules.get(1);
        List<AssertFieldRule.AssertRule> ageValueRules = ageRule.getFieldRules();
        assertEquals("age", ageRule.getFieldName());
        assertEquals(3, ageValueRules.size());
        assertEquals(AssertFieldRule.AssertRuleType.NOT_NULL, ageValueRules.get(0).getRuleType());
        assertEquals(AssertFieldRule.AssertRuleType.MIN, ageValueRules.get(1).getRuleType());
        assertEquals(10.0, ageValueRules.get(1).getRuleValue());
        assertEquals(AssertFieldRule.AssertRuleType.MAX, ageValueRules.get(2).getRuleType());
        assertEquals(20.0, ageValueRules.get(2).getRuleValue());

        AssertFieldRule decimalRule = assertFieldRules.get(2);
        List<AssertFieldRule.AssertRule> decimalValueRules = decimalRule.getFieldRules();
        assertEquals("c_decimal", decimalRule.getFieldName());
        assertEquals(new DecimalType(10, 2), decimalRule.getFieldType());
        assertEquals(2, decimalValueRules.size());
        assertEquals(
                AssertFieldRule.AssertRuleType.NOT_NULL, decimalValueRules.get(0).getRuleType());
        assertEquals("12.12", (String) decimalValueRules.get(1).getEqualTo());

        AssertFieldRule rowRule = assertFieldRules.get(3);
        List<AssertFieldRule.AssertRule> rowValueRules = rowRule.getFieldRules();
        SeaTunnelRowType expectedRowType =
                new SeaTunnelRowType(
                        new String[] {"c_0"},
                        new SeaTunnelDataType[] {
                            new SeaTunnelRowType(
                                    new String[] {"c_0_0"},
                                    new SeaTunnelDataType[] {BasicType.INT_TYPE})
                        });
        assertEquals("c_row", rowRule.getFieldName());
        assertEquals(expectedRowType, rowRule.getFieldType());
        assertEquals(2, rowValueRules.size());
        assertEquals(AssertFieldRule.AssertRuleType.NOT_NULL, rowValueRules.get(0).getRuleType());

        final List<List<?>> cRow = (List<List<?>>) rowValueRules.get(1).getEqualTo();
        assertEquals(1, cRow.size());
        assertEquals(ArrayList.class, cRow.get(0).getClass());
        assertEquals(1, ((List) cRow.get(0)).size());
        assertEquals(1, ((Integer) ((List) cRow.get(0)).get(0)));
    }

    private List<? extends Config> assembleConfig() {
        String s =
                "Assert {\n"
                        + "    rules = \n"
                        + "        [{\n"
                        + "            field_name = name\n"
                        + "            field_type = string\n"
                        + "            field_value = [\n"
                        + "                {\n"
                        + "                    rule_type = NOT_NULL\n"
                        + "                },\n"
                        + "                {\n"
                        + "                    rule_type = MIN_LENGTH\n"
                        + "                    rule_value = 3\n"
                        + "                },\n"
                        + "                {\n"
                        + "                     rule_type = MAX_LENGTH\n"
                        + "                     rule_value = 5\n"
                        + "                }\n"
                        + "            ]\n"
                        + "        },{\n"
                        + "            field_name = age\n"
                        + "            field_value = [\n"
                        + "                {\n"
                        + "                    rule_type = NOT_NULL\n"
                        + "                },\n"
                        + "                {\n"
                        + "                    rule_type = MIN\n"
                        + "                    rule_value = 10\n"
                        + "                },\n"
                        + "                {\n"
                        + "                     rule_type = MAX\n"
                        + "                     rule_value = 20\n"
                        + "                }\n"
                        + "            ]\n"
                        + "        },{\n"
                        + "            field_name = c_decimal\n"
                        + "            field_type= \" decimal( 10 , 2 ) \"\n"
                        + "            field_value = [\n"
                        + "                {\n"
                        + "                    rule_type = NOT_NULL\n"
                        + "                },\n"
                        + "                {\n"
                        + "                    equals_to = \"12.12\"\n"
                        + "                }\n"
                        + "            ]\n"
                        + "        },{\n"
                        + "            field_name = c_row\n"
                        + "            field_type= {c_0 = {c_0_0=int}}\n"
                        + "            field_value = [\n"
                        + "                {\n"
                        + "                    rule_type = NOT_NULL\n"
                        + "                },\n"
                        + "                {\n"
                        + "                    equals_to = [[1]]\n"
                        + "                }\n"
                        + "            ]\n"
                        + "        }\n"
                        + "        ]\n"
                        + "    \n"
                        + "}\n";
        Config config = ConfigFactory.parseString(s);

        return config.getConfig("Assert").getConfigList("rules");
    }
}
