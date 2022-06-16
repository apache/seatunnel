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

import com.google.common.collect.Maps;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.seatunnel.shade.com.typesafe.config.Config;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class AssertRuleParser {

    public List<AssertFieldRule> parseRules(List<? extends Config> ruleConfigList) {
        return ruleConfigList.stream()
                .map(config -> {
                    AssertFieldRule fieldRule = new AssertFieldRule();
                    fieldRule.setFieldName(config.getString("field_name"));
                    fieldRule.setFieldType(getFieldType(config.getString("field_type")));
                    List<AssertFieldRule.AssertValueRule> fieldValueRules = assembleFieldValueRules(config.getConfigList("field_value"));
                    fieldRule.setFieldValueRules(fieldValueRules);
                    return fieldRule;
                })
                .collect(Collectors.toList());
    }

    private List<AssertFieldRule.AssertValueRule> assembleFieldValueRules(List<? extends Config> fieldValueConfigList) {
        return fieldValueConfigList.stream()
                .map(config -> {
                    AssertFieldRule.AssertValueRule valueRule = new AssertFieldRule.AssertValueRule();
                    if (config.hasPath("rule_type")) {
                        valueRule.setFieldValueRuleType(AssertFieldRule.AssertValueRuleType.valueOf(config.getString("rule_type")));
                    }
                    if (config.hasPath("rule_value")) {
                        valueRule.setFieldValueRuleValue(config.getDouble("rule_value"));
                    }
                    return valueRule;
                })
                .collect(Collectors.toList());
    }

    private TypeInformation<?> getFieldType(String fieldTypeStr) {
        return TYPES.get(fieldTypeStr);
    }

    private static final Map<String, BasicTypeInfo<?>> TYPES = Maps.newHashMap();

    static {
        TYPES.put("string", BasicTypeInfo.STRING_TYPE_INFO);
        TYPES.put("Boolean", BasicTypeInfo.BOOLEAN_TYPE_INFO);
        TYPES.put("boolean", BasicTypeInfo.BOOLEAN_TYPE_INFO);
        TYPES.put("Byte", BasicTypeInfo.BYTE_TYPE_INFO);
        TYPES.put("byte", BasicTypeInfo.BYTE_TYPE_INFO);
        TYPES.put("Short", BasicTypeInfo.SHORT_TYPE_INFO);
        TYPES.put("short", BasicTypeInfo.SHORT_TYPE_INFO);
        TYPES.put("Integer", BasicTypeInfo.INT_TYPE_INFO);
        TYPES.put("Int", BasicTypeInfo.INT_TYPE_INFO);
        TYPES.put("int", BasicTypeInfo.INT_TYPE_INFO);
        TYPES.put("Long", BasicTypeInfo.LONG_TYPE_INFO);
        TYPES.put("long", BasicTypeInfo.LONG_TYPE_INFO);
        TYPES.put("Float", BasicTypeInfo.FLOAT_TYPE_INFO);
        TYPES.put("float", BasicTypeInfo.FLOAT_TYPE_INFO);
        TYPES.put("Double", BasicTypeInfo.DOUBLE_TYPE_INFO);
        TYPES.put("double", BasicTypeInfo.DOUBLE_TYPE_INFO);
        TYPES.put("Character", BasicTypeInfo.CHAR_TYPE_INFO);
        TYPES.put("char", BasicTypeInfo.CHAR_TYPE_INFO);
        TYPES.put("Date", BasicTypeInfo.DATE_TYPE_INFO);
        TYPES.put("Void", BasicTypeInfo.VOID_TYPE_INFO);
        TYPES.put("void", BasicTypeInfo.VOID_TYPE_INFO);
        TYPES.put("BigInteger", BasicTypeInfo.BIG_INT_TYPE_INFO);
        TYPES.put("BigDecimal", BasicTypeInfo.BIG_DEC_TYPE_INFO);
        TYPES.put("Instant", BasicTypeInfo.INSTANT_TYPE_INFO);
    }
}
