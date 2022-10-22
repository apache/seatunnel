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

package org.apache.seatunnel.connectors.seatunnel.assertion.rule;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class AssertRuleParser {

    public List<AssertFieldRule.AssertRule> parseRowRules(List<? extends Config> rowRuleList){

        return assembleFieldValueRules(rowRuleList);
    }

    public List<AssertFieldRule> parseRules(List<? extends Config> ruleConfigList) {
        return ruleConfigList.stream()
            .map(config -> {
                AssertFieldRule fieldRule = new AssertFieldRule();
                fieldRule.setFieldName(config.getString("field_name"));
                if (config.hasPath("field_type")) {
                    fieldRule.setFieldType(getFieldType(config.getString("field_type")));
                }

                if (config.hasPath("field_value")) {
                    List<AssertFieldRule.AssertRule> fieldValueRules = assembleFieldValueRules(config.getConfigList("field_value"));
                    fieldRule.setFieldRules(fieldValueRules);
                }
                return fieldRule;
            })
            .collect(Collectors.toList());
    }

    private List<AssertFieldRule.AssertRule> assembleFieldValueRules(List<? extends Config> fieldValueConfigList) {
        return fieldValueConfigList.stream()
            .map(config -> {
                AssertFieldRule.AssertRule valueRule = new AssertFieldRule.AssertRule();
                if (config.hasPath("rule_type")) {
                    valueRule.setRuleType(AssertFieldRule.AssertRuleType.valueOf(config.getString("rule_type")));
                }
                if (config.hasPath("rule_value")) {
                    valueRule.setRuleValue(config.getDouble("rule_value"));
                }
                return valueRule;
            })
            .collect(Collectors.toList());
    }

    private SeaTunnelDataType<?> getFieldType(String fieldTypeStr) {
        return TYPES.get(fieldTypeStr.toLowerCase());
    }

    private static final Map<String, SeaTunnelDataType<?>> TYPES = Maps.newHashMap();

    static {
        TYPES.put("string", BasicType.STRING_TYPE);
        TYPES.put("boolean", BasicType.BOOLEAN_TYPE);
        TYPES.put("byte", BasicType.BYTE_TYPE);
        TYPES.put("short", BasicType.SHORT_TYPE);
        TYPES.put("int", BasicType.INT_TYPE);
        TYPES.put("long", BasicType.LONG_TYPE);
        TYPES.put("float", BasicType.FLOAT_TYPE);
        TYPES.put("double", BasicType.DOUBLE_TYPE);
        TYPES.put("void", BasicType.VOID_TYPE);
    }
}
