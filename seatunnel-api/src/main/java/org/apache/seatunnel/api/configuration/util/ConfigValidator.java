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
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
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
            for (Condition<?> constraint : rule.getValueConstraints()) {
                collectConditionKeys(keys, constraint);
            }
        }
        return keys;
    }

    private static void collectConditionKeys(Set<String> keys, Condition<?> condition) {
        if (condition == null) {
            return;
        }
        keys.add(condition.getOption().key());
        keys.addAll(condition.getOption().getFallbackKeys());
        if (condition.getCompareOption() != null) {
            keys.add(condition.getCompareOption().key());
            keys.addAll(condition.getCompareOption().getFallbackKeys());
        }
        if (condition.hasNext()) {
            collectConditionKeys(keys, condition.getNext());
        }
    }

    private static void collectKeys(Set<String> keys, List<? extends Option<?>> options) {
        for (Option<?> option : options) {
            keys.add(option.key());
            keys.addAll(option.getFallbackKeys());
        }
    }

    public void validate(OptionRule rule) {
        validate(rule, null);
    }

    public void validate(OptionRule rule, Expression expression) {
        List<String> errors = new ArrayList<>();
        collectErrors(rule, expression, errors);
        if (!errors.isEmpty()) {
            StringBuilder sb = new StringBuilder();
            sb.append(
                    String.format(
                            "Option validation failed (%d error%s):",
                            errors.size(), errors.size() > 1 ? "s" : ""));
            for (int i = 0; i < errors.size(); i++) {
                sb.append(String.format("\n  [%d] %s", i + 1, errors.get(i)));
            }
            throw new OptionValidationException(sb.toString());
        }
    }

    private void collectErrors(OptionRule rule, Expression expression, List<String> errors) {
        Set<String> structurallyAbsentKeys = new HashSet<>();

        for (RequiredOption requiredOption : rule.getRequiredOptions()) {
            String error = checkRequiredOption(requiredOption, expression);
            if (error != null) {
                errors.add(error);
                collectAbsentKeys(requiredOption, structurallyAbsentKeys);
            }

            for (Option<?> option : requiredOption.getOptions()) {
                if (SingleChoiceOption.class.isAssignableFrom(option.getClass())) {
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

        for (ConditionRule conditionRule : rule.getConditionRules()) {
            if (validate(conditionRule.getExpression())) {
                collectErrors(conditionRule.getOptionRule(), conditionRule.getExpression(), errors);
            }
        }

        for (Condition<?> constraint : rule.getValueConstraints()) {
            if (structurallyAbsentKeys.contains(constraint.getOption().key())) {
                continue;
            }
            if (isConstraintApplicable(constraint, rule) && !validate(constraint)) {
                errors.add(
                        String.format(
                                "option: %s\n      type: value\n      constraint: %s",
                                constraint.getOption().key(), constraint.toString()));
            }
        }
    }

    private void collectAbsentKeys(RequiredOption requiredOption, Set<String> keys) {
        if (requiredOption instanceof RequiredOption.AbsolutelyRequiredOptions) {
            for (Option<?> opt :
                    getAbsentOptions(
                            ((RequiredOption.AbsolutelyRequiredOptions) requiredOption)
                                    .getRequiredOption())) {
                keys.add(opt.key());
            }
        } else if (isConditionOption(requiredOption)) {
            RequiredOption.ConditionalRequiredOptions cond =
                    (RequiredOption.ConditionalRequiredOptions) requiredOption;
            if (matchCondition(cond)) {
                for (Option<?> opt : getAbsentOptions(cond.getRequiredOption())) {
                    keys.add(opt.key());
                }
            }
        }
    }

    /**
     * Determines whether a value constraint should be evaluated.
     *
     * <p>If the constraint's head option is absolutely required, the constraint is always
     * applicable. Only the head option (the option the constraint is "about") is checked — compare
     * fields referenced by cross-field operators do not force applicability. This ensures that
     * {@code optional(MAX, lessThanField(MAX, START_TS))} correctly skips when MAX is absent, even
     * if START_TS is required elsewhere.
     *
     * <p>For optional constraints, the chain is split into OR-separated AND segments. Each AND
     * segment requires ALL its options to be present. The constraint is applicable if ANY segment
     * has all options present. This ensures:
     *
     * <ul>
     *   <li>Cross-field within one AND segment: both fields must exist (no false positive)
     *   <li>OR chains: each branch evaluated independently (no false negative)
     *   <li>All absent: every segment fails → constraint skipped
     * </ul>
     */
    private boolean isConstraintApplicable(Condition<?> condition, OptionRule rule) {
        Option<?> headOption = condition.getOption();
        for (RequiredOption requiredOption : rule.getRequiredOptions()) {
            if (requiredOption instanceof RequiredOption.AbsolutelyRequiredOptions
                    && requiredOption.getOptions().contains(headOption)) {
                return true;
            }
        }
        return anyOrSegmentFullyPresent(condition);
    }

    /**
     * Splits the condition chain at OR boundaries into AND segments, and returns true if any
     * segment has all of its referenced options present in the config.
     */
    private boolean anyOrSegmentFullyPresent(Condition<?> condition) {
        Condition<?> cur = condition;
        while (cur != null) {
            Set<Option<?>> segmentOptions = new HashSet<>();
            while (cur != null) {
                segmentOptions.add(cur.getOption());
                if (cur.getCompareOption() != null) {
                    segmentOptions.add(cur.getCompareOption());
                }
                if (!cur.hasNext()) {
                    cur = null;
                    break;
                }
                if (Boolean.TRUE.equals(cur.and())) {
                    cur = cur.getNext();
                } else {
                    cur = cur.getNext();
                    break;
                }
            }
            boolean allPresent = true;
            for (Option<?> opt : segmentOptions) {
                if (!hasOption(opt)) {
                    allPresent = false;
                    break;
                }
            }
            if (allPresent) {
                return true;
            }
        }
        return false;
    }

    void validateSingleChoice(Option option) {
        SingleChoiceOption singleChoiceOption = (SingleChoiceOption) option;
        List optionValues = singleChoiceOption.getOptionValues();
        if (CollectionUtils.isEmpty(optionValues)) {
            throw new OptionValidationException(
                    "These options(%s) are SingleChoiceOption, the optionValues must not be null.",
                    getOptionKeys(Collections.singletonList(singleChoiceOption)));
        }

        Object o = singleChoiceOption.defaultValue();
        if (o != null && !optionValues.contains(o)) {
            throw new OptionValidationException(
                    "These options(%s) are SingleChoiceOption, the defaultValue(%s) must be one of the optionValues(%s).",
                    getOptionKeys(Collections.singletonList(singleChoiceOption)), o, optionValues);
        }

        Object value = config.get(option);
        if (value != null && !optionValues.contains(value)) {
            throw new OptionValidationException(
                    "These options(%s) are SingleChoiceOption, the value(%s) must be one of the optionValues(%s).",
                    getOptionKeys(Collections.singletonList(singleChoiceOption)),
                    value,
                    optionValues);
        }
    }

    String checkRequiredOption(RequiredOption requiredOption, Expression expression) {
        if (requiredOption instanceof RequiredOption.AbsolutelyRequiredOptions) {
            return checkAbsolutelyRequired(
                    (RequiredOption.AbsolutelyRequiredOptions) requiredOption, expression);
        }
        if (requiredOption instanceof RequiredOption.BundledRequiredOptions) {
            return checkBundled((RequiredOption.BundledRequiredOptions) requiredOption);
        }
        if (requiredOption instanceof RequiredOption.ExclusiveRequiredOptions) {
            return checkExclusive((RequiredOption.ExclusiveRequiredOptions) requiredOption);
        }
        if (isConditionOption(requiredOption)) {
            return checkConditional((RequiredOption.ConditionalRequiredOptions) requiredOption);
        }
        throw new UnsupportedOperationException(
                String.format(
                        "This type option(%s) of validation is not supported",
                        requiredOption.getClass()));
    }

    private List<Option<?>> getAbsentOptions(List<Option<?>> requiredOption) {
        List<Option<?>> absent = new ArrayList<>();
        for (Option<?> option : requiredOption) {
            if (!hasOption(option) && option.defaultValue() == null) {
                absent.add(option);
            }
        }
        return absent;
    }

    String checkAbsolutelyRequired(
            RequiredOption.AbsolutelyRequiredOptions requiredOption, Expression expression) {
        List<Option<?>> absentOptions = getAbsentOptions(requiredOption.getRequiredOption());
        if (absentOptions.isEmpty()) {
            return null;
        }
        String hint = expression == null ? "" : " when [" + expression + "]";
        return String.format(
                "option: %s\n      type: required\n      constraint: required option is not configured%s",
                getOptionKeys(absentOptions), hint);
    }

    boolean hasOption(Option<?> option) {
        return config.getOptional(option).isPresent();
    }

    String checkBundled(RequiredOption.BundledRequiredOptions bundledRequiredOptions) {
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
        if (present.size() == bundledOptions.size() || absent.size() == bundledOptions.size()) {
            return null;
        }
        return String.format(
                "options: %s\n      type: bundled\n      constraint: bundled options must be present or absent together (present: [%s], absent: [%s])",
                getOptionKeys(bundledOptions), getOptionKeys(present), getOptionKeys(absent));
    }

    String checkExclusive(RequiredOption.ExclusiveRequiredOptions exclusiveRequiredOptions) {
        List<Option<?>> presentOptions = new ArrayList<>();
        for (Option<?> option : exclusiveRequiredOptions.getExclusiveOptions()) {
            if (hasOption(option)) {
                presentOptions.add(option);
            }
        }
        int count = presentOptions.size();
        if (count == 1) {
            return null;
        }
        if (count == 0) {
            return String.format(
                    "options: %s\n      type: exclusive\n      constraint: exactly one option must be set, but none are configured",
                    getOptionKeys(exclusiveRequiredOptions.getExclusiveOptions()));
        }
        return String.format(
                "options: %s\n      type: exclusive\n      constraint: mutually exclusive, but multiple are set: [%s]",
                getOptionKeys(exclusiveRequiredOptions.getExclusiveOptions()),
                getOptionKeys(presentOptions));
    }

    String checkConditional(RequiredOption.ConditionalRequiredOptions conditionalRequiredOptions) {
        boolean match = matchCondition(conditionalRequiredOptions);
        if (!match) {
            return null;
        }
        List<Option<?>> absentOptions =
                getAbsentOptions(conditionalRequiredOptions.getRequiredOption());
        if (absentOptions.isEmpty()) {
            return null;
        }
        return String.format(
                "option: %s\n      type: conditional\n      constraint: required because [%s] is true",
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

    /**
     * Evaluates a condition chain with standard boolean precedence: AND binds tighter than OR. The
     * chain {@code A.and(B).or(C)} evaluates as {@code (A && B) || C}, matching the output of
     * {@link Condition#toString()}.
     */
    private <T> boolean validate(Condition<T> condition) {
        Condition<?> cur = condition;
        while (cur != null) {
            boolean andGroupResult = true;
            while (cur != null) {
                andGroupResult = andGroupResult && ConditionEvaluators.evaluate(cur, config);
                if (!cur.hasNext()) {
                    cur = null;
                    break;
                }
                if (Boolean.TRUE.equals(cur.and())) {
                    cur = cur.getNext();
                } else {
                    cur = cur.getNext();
                    break;
                }
            }
            if (andGroupResult) {
                return true;
            }
        }
        return false;
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
