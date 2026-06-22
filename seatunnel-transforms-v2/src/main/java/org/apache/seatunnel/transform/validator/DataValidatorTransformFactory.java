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

package org.apache.seatunnel.transform.validator;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConditionExtension;
import org.apache.seatunnel.api.configuration.util.Conditions;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.api.table.connector.TableTransform;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableTransformFactory;
import org.apache.seatunnel.api.table.factory.TableTransformFactoryContext;
import org.apache.seatunnel.transform.common.TransformCommonOptions;

import com.google.auto.service.AutoService;

import java.util.List;
import java.util.Map;

import static org.apache.seatunnel.transform.validator.DataValidatorTransformConfig.FIELD_RULES;

/** Factory for creating DataValidator Transform instances. */
@AutoService(Factory.class)
public class DataValidatorTransformFactory implements TableTransformFactory {

    @Override
    public String factoryIdentifier() {
        return DataValidatorTransform.PLUGIN_NAME;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        FIELD_RULES,
                        Conditions.notEmpty(FIELD_RULES)
                                .and(Conditions.extension(FIELD_RULES, new FieldRulesValidator())))
                .optional(TransformCommonOptions.MULTI_TABLES)
                .optional(TransformCommonOptions.TABLE_MATCH_REGEX)
                .optional(TransformCommonOptions.ROW_ERROR_HANDLE_WAY_OPTION)
                .optional(TransformCommonOptions.ERROR_TABLE_OPTION)
                .build();
    }

    @Override
    public TableTransform createTransform(TableTransformFactoryContext context) {
        return () ->
                new DataValidatorTransform(context.getOptions(), context.getCatalogTables().get(0));
    }

    static class FieldRulesValidator implements ConditionExtension<List<Map<String, Object>>> {
        @Override
        public String description() {
            return "each field_rules entry must contain 'field_name' and either 'rule_type' or 'rules'";
        }

        @Override
        public boolean evaluate(ReadonlyConfig config, List<Map<String, Object>> value)
                throws OptionValidationException {
            if (value == null) {
                return false;
            }
            for (int i = 0; i < value.size(); i++) {
                Map<String, Object> entry = value.get(i);
                Object fieldName = entry.get("field_name");
                if (fieldName == null
                        || (fieldName instanceof String && ((String) fieldName).trim().isEmpty())) {
                    throw new OptionValidationException(
                            String.format(
                                    "field_rules[%d]: 'field_name' must not be null or empty", i));
                }
                boolean hasRuleType = entry.containsKey("rule_type");
                Object rulesObj = entry.get("rules");
                boolean hasRules = rulesObj instanceof List && !((List<?>) rulesObj).isEmpty();
                if (!hasRuleType && !hasRules) {
                    throw new OptionValidationException(
                            String.format(
                                    "field_rules[%d]: must contain 'rule_type' or non-empty 'rules'",
                                    i));
                }
            }
            return true;
        }
    }
}
