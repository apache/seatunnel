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

import static org.apache.seatunnel.api.configuration.util.OptionUtil.getOptionKeys;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

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
        throw new UnsupportedOperationException(String.format("This type option(%s) of validation is not supported", requiredOption.getClass()));
    }

    private List<Option<?>> getAbsentOptions(List<Option<?>> requiredOption) {
        List<Option<?>> absent = new ArrayList<>();
        for (Option<?> option : requiredOption) {
            if (!hasOption(option)) {
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
        throw new OptionValidationException("There are unconfigured options, the options(%s) are required.", getOptionKeys(absentOptions));
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
        throw new OptionValidationException("These options(%s) are bundled, must be present or absent together. The options present are: %s. The options absent are %s.",
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
            throw new OptionValidationException("There are unconfigured options, these options(%s) are mutually exclusive, allowing only one set(\"[] for a set\") of options to be configured.",
                getOptionKeys(exclusiveRequiredOptions.getExclusiveOptions()));
        }
        if (count > 1) {
            throw new OptionValidationException("These options(%s) are mutually exclusive, allowing only one set(\"[] for a set\") of options to be configured.",
                getOptionKeys(presentOptions));
        }
    }

    void validate(RequiredOption.ConditionalRequiredOptions conditionalRequiredOptions) {
        Expression expression = conditionalRequiredOptions.getExpression();
        boolean match = validate(expression);
        if (!match) {
            return;
        }
        List<Option<?>> absentOptions = getAbsentOptions(conditionalRequiredOptions.getRequiredOption());
        if (absentOptions.size() == 0) {
            return;
        }
        throw new OptionValidationException("There are unconfigured options, the options(%s) are required because [%s] is true.",
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
