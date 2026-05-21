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

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.SingleChoiceOption;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.options.EnvCommonOptions;
import org.apache.seatunnel.api.options.SourceConnectorCommonOptions;
import org.apache.seatunnel.api.options.table.CatalogOptions;
import org.apache.seatunnel.api.options.table.TableSchemaOptions;

import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import static org.apache.seatunnel.api.configuration.util.OptionUtil.getOptionKeys;

public class ConfigValidator {
    private final ReadonlyConfig config;

    private static final Set<String> COMMON_KEYS = new HashSet<>();

    static {
        collectKeys(
                COMMON_KEYS,
                Arrays.asList(
                        ConnectorCommonOptions.PLUGIN_NAME,
                        ConnectorCommonOptions.PLUGIN_INPUT,
                        ConnectorCommonOptions.PLUGIN_OUTPUT,
                        ConnectorCommonOptions.METADATA_DATASOURCE_ID,
                        EnvCommonOptions.PARALLELISM,
                        TableSchemaOptions.SCHEMA,
                        CatalogOptions.CATALOG_OPTIONS,
                        SourceConnectorCommonOptions.DAG_PARSING_MODE));
    }

    private ConfigValidator(ReadonlyConfig config) {
        this.config = config;
    }

    public static ConfigValidator of(ReadonlyConfig config) {
        return new ConfigValidator(config);
    }

    /**
     * Validates that all user-provided config keys are declared in the connector's OptionRule.
     * Unknown keys are treated as errors to catch typos and misconfigured options early.
     *
     * @param config the user-provided configuration
     * @param rule the connector's declared option rule
     * @param connectorName the connector name for error messages
     * @throws OptionValidationException if unknown keys are detected
     */
    public static void validateUnknownKeys(
            ReadonlyConfig config, OptionRule rule, String connectorName) {
        Set<String> declaredKeys = collectDeclaredKeys(rule);
        declaredKeys.addAll(COMMON_KEYS);

        List<String> unknownKeys = new ArrayList<>();
        validatePaths(config.getSourceMap(), "", declaredKeys, unknownKeys);

        if (!unknownKeys.isEmpty()) {
            throw new OptionValidationException(
                    "Connector '%s' has unknown option keys: %s. "
                            + "Please check for typos. Declared options are: %s",
                    connectorName,
                    unknownKeys,
                    declaredKeys.stream().sorted().collect(Collectors.toList()));
        }
    }

    private static void validatePaths(
            Map<String, Object> map,
            String prefix,
            Set<String> declaredKeys,
            List<String> unknownKeys) {
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            String fullKey = prefix.isEmpty() ? entry.getKey() : prefix + "." + entry.getKey();

            boolean isValid =
                    declaredKeys.contains(fullKey)
                            || declaredKeys.stream().anyMatch(dk -> dk.startsWith(fullKey + "."));

            if (!isValid) {
                unknownKeys.add(fullKey);
            } else if (entry.getValue() instanceof Map && !declaredKeys.contains(fullKey)) {
                validatePaths(
                        (Map<String, Object>) entry.getValue(), fullKey, declaredKeys, unknownKeys);
            }
        }
    }

    private static Set<String> collectDeclaredKeys(OptionRule rule) {
        Set<String> keys = new HashSet<>();

        if (rule != null) {
            collectKeys(keys, rule.getOptionalOptions());
            for (RequiredOption requiredOption : rule.getRequiredOptions()) {
                collectKeys(keys, requiredOption.getOptions());
            }
            for (ConditionRule conditionRule : rule.getConditionRules()) {
                keys.addAll(collectDeclaredKeys(conditionRule.getOptionRule()));
            }
        }
        return keys;
    }

    private static void collectKeys(Set<String> keys, List<? extends Option<?>> options) {
        for (Option<?> option : options) {
            keys.add(option.key());
            option.getFallbackKeys().forEach(keys::add);
        }
    }

    public void validate(OptionRule rule) {
        validate(rule, null);
    }

    public void validate(OptionRule rule, Expression expression) {
        List<RequiredOption> requiredOptions = rule.getRequiredOptions();
        for (RequiredOption requiredOption : requiredOptions) {
            validate(requiredOption, expression);

            for (Option<?> option : requiredOption.getOptions()) {
                if (SingleChoiceOption.class.isAssignableFrom(option.getClass())) {
                    // is required option and not match condition, skip validate
                    if (isConditionOption(requiredOption)
                            && !matchCondition(
                                    (RequiredOption.ConditionalRequiredOptions) requiredOption)) {
                        continue;
                    }
                    validateSingleChoice(option);
                }
            }
        }

        for (Option option : rule.getOptionalOptions()) {
            if (SingleChoiceOption.class.isAssignableFrom(option.getClass())) {
                validateSingleChoice(option);
            }
        }

        List<ConditionRule> conditionRules = rule.getConditionRules();
        for (ConditionRule conditionRule : conditionRules) {
            if (validate(conditionRule.getExpression())) {
                validate(conditionRule.getOptionRule(), conditionRule.getExpression());
            }
        }
    }

    void validateSingleChoice(Option option) {
        SingleChoiceOption singleChoiceOption = (SingleChoiceOption) option;
        List optionValues = singleChoiceOption.getOptionValues();
        if (CollectionUtils.isEmpty(optionValues)) {
            throw new OptionValidationException(
                    "These options(%s) are SingleChoiceOption, the optionValues must not be null.",
                    getOptionKeys(Arrays.asList(singleChoiceOption)));
        }

        Object o = singleChoiceOption.defaultValue();
        if (o != null && !optionValues.contains(o)) {
            throw new OptionValidationException(
                    "These options(%s) are SingleChoiceOption, the defaultValue(%s) must be one of the optionValues(%s).",
                    getOptionKeys(Arrays.asList(singleChoiceOption)), o, optionValues);
        }

        Object value = config.get(option);
        if (value != null && !optionValues.contains(value)) {
            throw new OptionValidationException(
                    "These options(%s) are SingleChoiceOption, the value(%s) must be one of the optionValues(%s).",
                    getOptionKeys(Arrays.asList(singleChoiceOption)), value, optionValues);
        }
    }

    void validate(RequiredOption requiredOption, Expression expression) {
        if (requiredOption instanceof RequiredOption.AbsolutelyRequiredOptions) {
            validate((RequiredOption.AbsolutelyRequiredOptions) requiredOption, expression);
            return;
        }
        if (requiredOption instanceof RequiredOption.BundledRequiredOptions) {
            validate((RequiredOption.BundledRequiredOptions) requiredOption);
            return;
        }
        if (requiredOption instanceof RequiredOption.ExclusiveRequiredOptions) {
            validate((RequiredOption.ExclusiveRequiredOptions) requiredOption);
            return;
        }
        if (isConditionOption(requiredOption)) {
            validate((RequiredOption.ConditionalRequiredOptions) requiredOption);
            return;
        }
        throw new UnsupportedOperationException(
                String.format(
                        "This type option(%s) of validation is not supported",
                        requiredOption.getClass()));
    }

    private List<Option<?>> getAbsentOptions(List<Option<?>> requiredOption) {
        List<Option<?>> absent = new ArrayList<>();
        for (Option<?> option : requiredOption) {
            // If the required option have default values, we will take the default values
            if (!hasOption(option) && option.defaultValue() == null) {
                absent.add(option);
            }
        }
        return absent;
    }

    void validate(RequiredOption.AbsolutelyRequiredOptions requiredOption, Expression expression) {
        List<Option<?>> absentOptions = getAbsentOptions(requiredOption.getRequiredOption());
        if (absentOptions.size() == 0) {
            return;
        }
        throw new OptionValidationException(
                "There are unconfigured options, the options(%s) are required%s",
                getOptionKeys(absentOptions), getExpressionExceptionHintMessage(expression));
    }

    String getExpressionExceptionHintMessage(Expression expression) {
        if (expression == null) {
            return ".";
        } else {
            return " when [" + expression + "].";
        }
    }

    boolean hasOption(Option<?> option) {
        return config.getOptional(option).isPresent();
    }

    boolean validate(RequiredOption.BundledRequiredOptions bundledRequiredOptions) {
        List<Option<?>> bundledOptions = bundledRequiredOptions.getRequiredOption();
        List<Option<?>> present = new ArrayList<>();
        List<Option<?>> absent = new ArrayList<>();
        for (Option<?> option : bundledOptions) {
            if (hasOption(option)) {
                present.add(option);
            } else {
                absent.add(option);
            }
        }
        if (present.size() == bundledOptions.size()) {
            return true;
        }
        if (absent.size() == bundledOptions.size()) {
            return false;
        }
        throw new OptionValidationException(
                "These options(%s) are bundled, must be present or absent together. The options present are: %s. The options absent are %s.",
                getOptionKeys(bundledOptions), getOptionKeys(present), getOptionKeys(absent));
    }

    void validate(RequiredOption.ExclusiveRequiredOptions exclusiveRequiredOptions) {
        List<Option<?>> presentOptions = new ArrayList<>();

        for (Option<?> option : exclusiveRequiredOptions.getExclusiveOptions()) {
            if (hasOption(option)) {
                presentOptions.add(option);
            }
        }
        int count = presentOptions.size();
        if (count == 1) {
            return;
        }
        if (count == 0) {
            throw new OptionValidationException(
                    "There are unconfigured options, these options(%s) are mutually exclusive, allowing only one set(\"[] for a set\") of options to be configured.",
                    getOptionKeys(exclusiveRequiredOptions.getExclusiveOptions()));
        }
        if (count > 1) {
            throw new OptionValidationException(
                    "These options(%s) are mutually exclusive, allowing only one set(\"[] for a set\") of options to be configured.",
                    getOptionKeys(presentOptions));
        }
    }

    void validate(RequiredOption.ConditionalRequiredOptions conditionalRequiredOptions) {
        boolean match = matchCondition(conditionalRequiredOptions);
        if (!match) {
            return;
        }
        List<Option<?>> absentOptions =
                getAbsentOptions(conditionalRequiredOptions.getRequiredOption());
        if (absentOptions.size() == 0) {
            return;
        }
        throw new OptionValidationException(
                "There are unconfigured options, the options(%s) are required because [%s] is true.",
                getOptionKeys(absentOptions),
                conditionalRequiredOptions.getExpression().toString());
    }

    private boolean validate(Expression expression) {
        Condition<?> condition = expression.getCondition();
        boolean match = validate(condition);
        if (!expression.hasNext()) {
            return match;
        }
        if (expression.and()) {
            return match && validate(expression.getNext());
        } else {
            return match || validate(expression.getNext());
        }
    }

    private <T> boolean validate(Condition<T> condition) {
        Option<T> option = condition.getOption();

        boolean match = Objects.equals(condition.getExpectValue(), config.get(option));
        if (!condition.hasNext()) {
            return match;
        }
        if (condition.and()) {
            return match && validate(condition.getNext());
        } else {
            return match || validate(condition.getNext());
        }
    }

    private boolean isConditionOption(RequiredOption requiredOption) {
        return requiredOption instanceof RequiredOption.ConditionalRequiredOptions;
    }

    private boolean matchCondition(
            RequiredOption.ConditionalRequiredOptions conditionalRequiredOptions) {
        Expression expression = conditionalRequiredOptions.getExpression();
        return validate(expression);
    }
}
