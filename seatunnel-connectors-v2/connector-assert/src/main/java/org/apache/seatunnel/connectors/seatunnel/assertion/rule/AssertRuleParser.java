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
import org.apache.seatunnel.shade.com.typesafe.config.ConfigObject;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigValue;

import org.apache.seatunnel.api.table.catalog.SeaTunnelDataTypeConvertorUtil;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.stream.Collectors;

import static org.apache.seatunnel.connectors.seatunnel.assertion.sink.AssertConfig.EQUALS_TO;
import static org.apache.seatunnel.connectors.seatunnel.assertion.sink.AssertConfig.FIELD_NAME;
import static org.apache.seatunnel.connectors.seatunnel.assertion.sink.AssertConfig.FIELD_TYPE;
import static org.apache.seatunnel.connectors.seatunnel.assertion.sink.AssertConfig.FIELD_VALUE;
import static org.apache.seatunnel.connectors.seatunnel.assertion.sink.AssertConfig.RULE_TYPE;
import static org.apache.seatunnel.connectors.seatunnel.assertion.sink.AssertConfig.RULE_VALUE;

@Slf4j
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
                            String fieldName = config.getString(FIELD_NAME);
                            fieldRule.setFieldName(config.getString(FIELD_NAME));
                            if (config.hasPath(FIELD_TYPE)) {
                                ConfigValue fieldTypeConf = config.getValue(FIELD_TYPE);
                                switch (fieldTypeConf.valueType()) {
                                    case STRING:
                                        {
                                            String basicTypeStr = config.getString(FIELD_TYPE);
                                            SeaTunnelDataType<?> fieldType =
                                                    SeaTunnelDataTypeConvertorUtil
                                                            .deserializeSeaTunnelDataType(
                                                                    fieldName, basicTypeStr);
                                            fieldRule.setFieldType(fieldType);
                                        }
                                        ;
                                        break;
                                    case OBJECT:
                                        {
                                            ConfigObject rowTypeConf = config.getObject(FIELD_TYPE);
                                            SeaTunnelDataType<?> fieldType =
                                                    SeaTunnelDataTypeConvertorUtil
                                                            .deserializeSeaTunnelDataType(
                                                                    fieldName,
                                                                    rowTypeConf.render());
                                            fieldRule.setFieldType(fieldType);
                                        }
                                        ;
                                        break;
                                    case BOOLEAN:
                                    case NUMBER:
                                    case LIST:
                                    case NULL:
                                        log.warn(
                                                String.format(
                                                        "Assert Field Rule[%s] doesn't support '%s' type value.",
                                                        FIELD_TYPE, fieldTypeConf.valueType()));
                                }
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
                                valueRule.setEqualTo(config.getValue(EQUALS_TO).unwrapped());
                            }
                            return valueRule;
                        })
                .collect(Collectors.toList());
    }
}
