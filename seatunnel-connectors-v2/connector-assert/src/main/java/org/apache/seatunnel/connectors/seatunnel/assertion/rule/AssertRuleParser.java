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

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

import com.google.common.collect.Maps;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.seatunnel.connectors.seatunnel.assertion.sink.AssertConfig.EQUALS_TO;
import static org.apache.seatunnel.connectors.seatunnel.assertion.sink.AssertConfig.FIELD_NAME;
import static org.apache.seatunnel.connectors.seatunnel.assertion.sink.AssertConfig.FIELD_TYPE;
import static org.apache.seatunnel.connectors.seatunnel.assertion.sink.AssertConfig.FIELD_VALUE;
import static org.apache.seatunnel.connectors.seatunnel.assertion.sink.AssertConfig.RULE_TYPE;
import static org.apache.seatunnel.connectors.seatunnel.assertion.sink.AssertConfig.RULE_VALUE;

public class AssertRuleParser {

    public List<AssertFieldRule.AssertRule> parseRowRules(List<? extends Config> rowRuleList) {

        return assembleFieldValueRules(rowRuleList);
    }

    public AssertCatalogTableRule parseCatalogTableRule(Config catalogTableRule) {
        return new AssertCatalogTableRuleParser().parseCatalogTableRule(catalogTableRule);
    }

    public List<AssertFieldRule> parseRules(List<? extends Config> ruleConfigList) {
        return ruleConfigList.stream()
                .map(
                        config -> {
                            AssertFieldRule fieldRule = new AssertFieldRule();
                            fieldRule.setFieldName(config.getString(FIELD_NAME));
                            if (config.hasPath(FIELD_TYPE)) {
                                fieldRule.setFieldType(getFieldType(config.getString(FIELD_TYPE)));
                            }

                            if (config.hasPath(FIELD_VALUE)) {
                                List<AssertFieldRule.AssertRule> fieldValueRules =
                                        assembleFieldValueRules(config.getConfigList(FIELD_VALUE));
                                fieldRule.setFieldRules(fieldValueRules);
                            }
                            return fieldRule;
                        })
                .collect(Collectors.toList());
    }

    private List<AssertFieldRule.AssertRule> assembleFieldValueRules(
            List<? extends Config> fieldValueConfigList) {
        return fieldValueConfigList.stream()
                .map(
                        config -> {
                            AssertFieldRule.AssertRule valueRule = new AssertFieldRule.AssertRule();
                            if (config.hasPath(RULE_TYPE)) {
                                valueRule.setRuleType(
                                        AssertFieldRule.AssertRuleType.valueOf(
                                                config.getString(RULE_TYPE)));
                            }
                            if (config.hasPath(RULE_VALUE)) {
                                valueRule.setRuleValue(config.getDouble(RULE_VALUE));
                            }
                            if (config.hasPath(EQUALS_TO)) {
                                valueRule.setEqualTo(config.getString(EQUALS_TO));
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
        TYPES.put("timestamp", LocalTimeType.LOCAL_DATE_TIME_TYPE);
        TYPES.put("datetime", LocalTimeType.LOCAL_DATE_TIME_TYPE);
        TYPES.put("date", LocalTimeType.LOCAL_DATE_TYPE);
        TYPES.put("time", LocalTimeType.LOCAL_TIME_TYPE);
        TYPES.put("decimal", new DecimalType(38, 18));
    }
}
