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

import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import static org.apache.seatunnel.api.configuration.util.OptionUtil.getOptionKeys;

public class ConfigValidator {
    private final ReadonlyConfig config;

    private ConfigValidator(ReadonlyConfig config) {
        this.config = config;
    }

    public static ConfigValidator of(ReadonlyConfig config) {
        return new ConfigValidator(config);
    }

    public void validate(OptionRule rule) {
        List<RequiredOption> requiredOptions = rule.getRequiredOptions();
        for (RequiredOption requiredOption : requiredOptions) {
            validate(requiredOption);
            requiredOption
                    .getOptions()
                    .forEach(
                            option -> {
                                if (SingleChoiceOption.class.isAssignableFrom(option.getClass())) {
                                    validateSingleChoice(option);
                                }
                            });
        }

        for (Option option : rule.getOptionalOptions()) {
            if (SingleChoiceOption.class.isAssignableFrom(option.getClass())) {
                validateSingleChoice(option);
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
                    "These options(%s) are SingleChoiceOption, the defaultValue(%s) must be one of the optionValues.",
                    getOptionKeys(Arrays.asList(singleChoiceOption)), o);
        }

        Object value = config.get(option);
        if (value != null && !optionValues.contains(value)) {
            throw new OptionValidationException(
                    "These options(%s) are SingleChoiceOption, the value(%s) must be one of the optionValues.",
                    getOptionKeys(Arrays.asList(singleChoiceOption)), value);
        }
    }

    void validate(RequiredOption requiredOption) {
        if (requiredOption instanceof RequiredOption.AbsolutelyRequiredOptions) {
            validate((RequiredOption.AbsolutelyRequiredOptions) requiredOption);
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
        if (requiredOption instanceof RequiredOption.ConditionalRequiredOptions) {
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

    void validate(RequiredOption.AbsolutelyRequiredOptions requiredOption) {
        List<Option<?>> absentOptions = getAbsentOptions(requiredOption.getRequiredOption());
        if (absentOptions.size() == 0) {
            return;
        }
        throw new OptionValidationException(
                "There are unconfigured options, the options(%s) are required.",
                getOptionKeys(absentOptions));
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
        Expression expression = conditionalRequiredOptions.getExpression();
        boolean match = validate(expression);
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
                getOptionKeys(absentOptions), expression.toString());
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
}
