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

import org.apache.seatunnel.shade.com.fasterxml.jackson.annotation.JsonAlias;
import org.apache.seatunnel.shade.com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.transform.validator.rule.LengthValidationRule;
import org.apache.seatunnel.transform.validator.rule.NotNullValidationRule;
import org.apache.seatunnel.transform.validator.rule.RangeValidationRule;
import org.apache.seatunnel.transform.validator.rule.RegexValidationRule;
import org.apache.seatunnel.transform.validator.rule.UDFValidationRule;
import org.apache.seatunnel.transform.validator.rule.ValidationRule;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Slf4j
public class DataValidatorTransformConfig implements Serializable {

    public static final Option<List<Map<String, Object>>> FIELD_RULES =
            Options.key("field_rules")
                    .type(new TypeReference<List<Map<String, Object>>>() {})
                    .noDefaultValue()
                    .withDescription("Field validation rules");

    private List<FieldValidationRule> fieldRules = new ArrayList<>();

    @Data
    @NoArgsConstructor
    @AllArgsConstructor
    @JsonIgnoreProperties(ignoreUnknown = true)
    public static class FieldValidationRule implements Serializable {

        @JsonAlias("field_name")
        private String fieldName;

        @JsonAlias("rules")
        private List<ValidationRule> rules = new ArrayList<>();
    }

    public static DataValidatorTransformConfig of(ReadonlyConfig config) {
        DataValidatorTransformConfig validatorConfig = new DataValidatorTransformConfig();
        List<Map<String, Object>> fieldRulesMap = config.get(FIELD_RULES);
        List<FieldValidationRule> fieldRules = parseFieldRules(fieldRulesMap);
        validatorConfig.setFieldRules(fieldRules);

        return validatorConfig;
    }

    private static List<FieldValidationRule> parseFieldRules(
            List<Map<String, Object>> fieldRulesMap) {
        List<FieldValidationRule> fieldRules = new ArrayList<>();

        for (Map<String, Object> ruleMap : fieldRulesMap) {
            String fieldName = (String) ruleMap.get("field_name");
            if (fieldName == null) {
                log.warn("Field name is missing in rule configuration: {}", ruleMap);
                continue;
            }

            FieldValidationRule fieldRule = new FieldValidationRule();
            fieldRule.setFieldName(fieldName);
            Object rulesObj = ruleMap.get("rules");
            if (rulesObj != null) {
                List<ValidationRule> rules = parseNestedRules(rulesObj);
                fieldRule.setRules(rules);
                fieldRules.add(fieldRule);
            } else {
                ValidationRule validationRule = parseValidationRuleFromMap(ruleMap);
                if (validationRule != null) {
                    fieldRule.setRules(Lists.newArrayList(validationRule));
                    fieldRules.add(fieldRule);
                }
            }
        }
        return groupFlatRulesByField(fieldRules);
    }

    @SuppressWarnings("unchecked")
    private static List<ValidationRule> parseNestedRules(Object rulesObj) {
        List<ValidationRule> rules = new ArrayList<>();

        try {
            if (rulesObj instanceof List) {
                List<Object> rulesList = (List<Object>) rulesObj;
                for (Object ruleObj : rulesList) {
                    if (ruleObj instanceof Map) {
                        Map<String, Object> ruleMap = (Map<String, Object>) ruleObj;
                        // Parse rule using the same logic as flat format
                        ValidationRule rule = parseValidationRuleFromMap(ruleMap);
                        if (rule != null) {
                            rules.add(rule);
                        }
                    }
                }
            }
        } catch (Exception e) {
            log.error("Failed to parse nested validation rules: {}", rulesObj, e);
        }

        return rules;
    }

    private static List<FieldValidationRule> groupFlatRulesByField(
            List<FieldValidationRule> fieldRules) {
        Map<String, List<ValidationRule>> fieldRulesGroup = new HashMap<>();

        for (FieldValidationRule fieldRule : fieldRules) {
            String fieldName = fieldRule.getFieldName();
            List<ValidationRule> existingRules = fieldRulesGroup.get(fieldName);
            if (existingRules == null) {
                fieldRulesGroup.put(fieldName, new ArrayList<>(fieldRule.getRules()));
            } else {
                existingRules.addAll(fieldRule.getRules());
            }
        }

        List<FieldValidationRule> groupedRules = new ArrayList<>();
        for (Map.Entry<String, List<ValidationRule>> entry : fieldRulesGroup.entrySet()) {
            FieldValidationRule fieldRule = new FieldValidationRule();
            fieldRule.setFieldName(entry.getKey());
            fieldRule.setRules(entry.getValue());
            groupedRules.add(fieldRule);
        }

        return groupedRules;
    }

    private static ValidationRule parseValidationRuleFromMap(Map<String, Object> ruleData) {
        Object ruleTypeObj = ruleData.get("rule_type");
        if (ruleTypeObj == null) {
            log.warn("Rule type is missing in rule configuration: {}", ruleData);
            return null;
        }

        String ruleType = String.valueOf(ruleTypeObj).toUpperCase();

        try {
            switch (ruleType) {
                case "NOT_NULL":
                    return parseNotNullRuleFromMap(ruleData);
                case "RANGE":
                    return parseRangeRuleFromMap(ruleData);
                case "LENGTH":
                    return parseLengthRuleFromMap(ruleData);
                case "REGEX":
                    return parseRegexRuleFromMap(ruleData);
                case "UDF":
                    return parseUDFRuleFromMap(ruleData);
                default:
                    log.warn(
                            "Unknown validation rule type: {}. Supported types: NOT_NULL, RANGE, LENGTH, REGEX, UDF",
                            ruleType);
                    return null;
            }
        } catch (Exception e) {
            log.error("Failed to parse validation rule of type '{}': {}", ruleType, ruleData, e);
            return null;
        }
    }

    private static NotNullValidationRule parseNotNullRuleFromMap(Map<String, Object> ruleData) {
        try {
            NotNullValidationRule rule = new NotNullValidationRule();
            Object customMessage = ruleData.get("custom_message");
            if (customMessage != null) {
                rule.setCustomMessage(String.valueOf(customMessage));
            }
            log.debug("Successfully parsed NOT_NULL rule: {}", rule);
            return rule;
        } catch (Exception e) {
            log.error("Failed to parse NOT_NULL rule from data: {}", ruleData, e);
            throw e;
        }
    }

    private static RangeValidationRule parseRangeRuleFromMap(Map<String, Object> ruleData) {
        try {
            RangeValidationRule rule = new RangeValidationRule();

            Object minValue = ruleData.get("min_value");
            if (minValue != null) {
                rule.setMinValue(parseComparable(String.valueOf(minValue)));
            }

            Object maxValue = ruleData.get("max_value");
            if (maxValue != null) {
                rule.setMaxValue(parseComparable(String.valueOf(maxValue)));
            }

            Object minInclusive = ruleData.get("min_inclusive");
            if (minInclusive != null) {
                rule.setMinInclusive(parseBooleanValue(minInclusive));
            }

            Object maxInclusive = ruleData.get("max_inclusive");
            if (maxInclusive != null) {
                rule.setMaxInclusive(parseBooleanValue(maxInclusive));
            }

            Object customMessage = ruleData.get("custom_message");
            if (customMessage != null) {
                rule.setCustomMessage(String.valueOf(customMessage));
            }

            log.debug("Successfully parsed RANGE rule: {}", rule);
            return rule;
        } catch (Exception e) {
            log.error("Failed to parse RANGE rule from data: {}", ruleData, e);
            throw e;
        }
    }

    private static LengthValidationRule parseLengthRuleFromMap(Map<String, Object> ruleData) {
        try {
            LengthValidationRule rule = new LengthValidationRule();

            Object minLength = ruleData.get("min_length");
            if (minLength != null) {
                rule.setMinLength(parseIntegerValue(minLength));
            }

            Object maxLength = ruleData.get("max_length");
            if (maxLength != null) {
                rule.setMaxLength(parseIntegerValue(maxLength));
            }

            Object exactLength = ruleData.get("exact_length");
            if (exactLength != null) {
                rule.setExactLength(parseIntegerValue(exactLength));
            }

            Object customMessage = ruleData.get("custom_message");
            if (customMessage != null) {
                rule.setCustomMessage(String.valueOf(customMessage));
            }

            log.debug("Successfully parsed LENGTH rule: {}", rule);
            return rule;
        } catch (Exception e) {
            log.error("Failed to parse LENGTH rule from data: {}", ruleData, e);
            throw e;
        }
    }

    private static RegexValidationRule parseRegexRuleFromMap(Map<String, Object> ruleData) {
        try {
            RegexValidationRule rule = new RegexValidationRule();

            Object pattern = ruleData.get("pattern");
            if (pattern != null) {
                rule.setPattern(String.valueOf(pattern));
            } else {
                throw new IllegalArgumentException("Pattern is required for REGEX rule");
            }

            Object caseSensitive = ruleData.get("case_sensitive");
            if (caseSensitive != null) {
                rule.setCaseSensitive(parseBooleanValue(caseSensitive));
            }

            Object customMessage = ruleData.get("custom_message");
            if (customMessage != null) {
                rule.setCustomMessage(String.valueOf(customMessage));
            }

            log.debug("Successfully parsed REGEX rule: {}", rule);
            return rule;
        } catch (Exception e) {
            log.error("Failed to parse REGEX rule from data: {}", ruleData, e);
            throw e;
        }
    }

    private static Comparable parseComparable(String value) {
        if (value == null || value.trim().isEmpty()) {
            return value;
        }

        String trimmedValue = value.trim();
        try {
            if (trimmedValue.contains(".")) {
                return Double.parseDouble(trimmedValue);
            } else {
                long longValue = Long.parseLong(trimmedValue);
                if (longValue >= Integer.MIN_VALUE && longValue <= Integer.MAX_VALUE) {
                    return (int) longValue;
                }
                return longValue;
            }
        } catch (NumberFormatException e) {
            log.debug("Value '{}' is not a number, treating as string", value);
            return value;
        }
    }

    private static boolean parseBooleanValue(Object value) {
        if (value == null) {
            return false;
        }
        if (value instanceof Boolean) {
            return (Boolean) value;
        }
        String stringValue = String.valueOf(value).trim().toLowerCase();
        return "true".equals(stringValue) || "1".equals(stringValue) || "yes".equals(stringValue);
    }

    private static Integer parseIntegerValue(Object value) {
        if (value == null) {
            return null;
        }
        if (value instanceof Integer) {
            return (Integer) value;
        }
        if (value instanceof Number) {
            return ((Number) value).intValue();
        }
        try {
            return Integer.parseInt(String.valueOf(value).trim());
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid integer value: " + value, e);
        }
    }

    private static UDFValidationRule parseUDFRuleFromMap(Map<String, Object> ruleData) {
        try {
            UDFValidationRule rule = new UDFValidationRule();

            Object functionName = ruleData.get("function_name");
            if (functionName != null) {
                rule.setFunctionName(String.valueOf(functionName));
            } else {
                throw new IllegalArgumentException("function_name is required for UDF rule");
            }

            Object customMessage = ruleData.get("custom_message");
            if (customMessage != null) {
                rule.setCustomMessage(String.valueOf(customMessage));
            }

            log.debug("Successfully parsed UDF rule: {}", rule);
            return rule;
        } catch (Exception e) {
            log.error("Failed to parse UDF rule from data: {}", ruleData, e);
            throw e;
        }
    }
}
