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

import lombok.NonNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Validation rule for {@link Option}.
 * <p>
 * The option rule is typically built in one of the following pattern:
 *
 * <pre>{@code
 * // simple rule
 * OptionRule simpleRule = OptionRule.builder()
 *     .optional(POLL_TIMEOUT, POLL_INTERVAL)
 *     .required(CLIENT_SERVICE_URL)
 *     .build();
 *
 * // basic full rule
 * OptionRule fullRule = OptionRule.builder()
 *     .optional(POLL_TIMEOUT, POLL_INTERVAL, CURSOR_STARTUP_MODE)
 *     .required(CLIENT_SERVICE_URL, ADMIN_SERVICE_URL)
 *     .exclusive(TOPIC_PATTERN, TOPIC)
 *     .conditional(CURSOR_STARTUP_MODE, StartMode.TIMESTAMP, CURSOR_STARTUP_TIMESTAMP)
 *     .build();
 *
 * // complex conditional rule
 * // moot expression
 * Expression expression = Expression.of(TOPIC_DISCOVERY_INTERVAL, 200)
 *     .and(Expression.of(Condition.of(CURSOR_STARTUP_MODE, StartMode.EARLIEST)
 *         .or(CURSOR_STARTUP_MODE, StartMode.LATEST)))
 *     .or(Expression.of(Condition.of(TOPIC_DISCOVERY_INTERVAL, 100)))
 *
 * OptionRule complexRule = OptionRule.builder()
 *     .optional(POLL_TIMEOUT, POLL_INTERVAL, CURSOR_STARTUP_MODE)
 *     .required(CLIENT_SERVICE_URL, ADMIN_SERVICE_URL)
 *     .exclusive(TOPIC_PATTERN, TOPIC)
 *     .conditional(expression, CURSOR_RESET_MODE)
 *     .build();
 * }</pre>
 */
public class OptionRule {

    /**
     * Optional options with default value.
     *
     * <p> This options will not be validated.
     * <p> This is used by the web-UI to show what options are available.
     */
    private final List<Option<?>> optionalOptions;

    /**
     * Required options with no default value.
     *
     * <p> Verify that the option is valid through the defined rules.
     */
    private final List<RequiredOption> requiredOptions;

    OptionRule(List<Option<?>> optionalOptions, List<RequiredOption> requiredOptions) {
        this.optionalOptions = optionalOptions;
        this.requiredOptions = requiredOptions;
    }

    public List<Option<?>> getOptionalOptions() {
        return optionalOptions;
    }

    public List<RequiredOption> getRequiredOptions() {
        return requiredOptions;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof OptionRule)) {
            return false;
        }
        OptionRule that = (OptionRule) o;
        return Objects.equals(optionalOptions, that.optionalOptions)
            && Objects.equals(requiredOptions, that.requiredOptions);
    }

    @Override
    public int hashCode() {
        return Objects.hash(optionalOptions, requiredOptions);
    }

    public static OptionRule.Builder builder() {
        return new OptionRule.Builder();
    }

    /**
     * Builder for {@link OptionRule}.
     */
    public static class Builder {
        private final List<Option<?>> optionalOptions = new ArrayList<>();
        private final List<RequiredOption> requiredOptions = new ArrayList<>();

        private Builder() {
        }

        /**
         * Optional options
         *
         * <p> This options will not be validated.
         * <p> This is used by the web-UI to show what options are available.
         */
        public Builder optional(@NonNull Option<?>... options) {
            for (Option<?> option : options) {
                verifyDuplicate(option, "OptionsOption");
            }
            this.optionalOptions.addAll(Arrays.asList(options));
            return this;
        }

        /**
         * Absolutely required options without any constraints.
         */
        public Builder required(@NonNull Option<?>... options) {
            for (Option<?> option : options) {
                verifyDuplicate(option, "RequiredOption");
                verifyRequiredOptionDefaultValue(option);
            }
            this.requiredOptions.add(RequiredOption.AbsolutelyRequiredOptions.of(options));
            return this;
        }

        /**
         * Exclusive options, only one of the options needs to be configured.
         */
        public Builder exclusive(@NonNull Option<?>... options) {
            if (options.length <= 1) {
                throw new OptionValidationException("The number of exclusive options must be greater than 1.");
            }
            for (Option<?> option : options) {
                verifyDuplicate(option, "ExclusiveOption");
                verifyRequiredOptionDefaultValue(option);
            }
            this.requiredOptions.add(RequiredOption.ExclusiveRequiredOptions.of(options));
            return this;
        }

        public <T> Builder conditional(@NonNull Option<T> conditionalOption, @NonNull List<T> expectValues, @NonNull Option<?>... requiredOptions) {
            for (Option<?> o : requiredOptions) {
                verifyDuplicate(o, "ConditionalOption");
                verifyRequiredOptionDefaultValue(o);
            }

            verifyConditionalExists(conditionalOption);

            if (expectValues.size() == 0) {
                throw new OptionValidationException(
                    String.format("conditional option '%s' must have expect values .", conditionalOption.key()));
            }

            /**
             * Each parameter can only be controlled by one other parameter
             */
            Expression expression = Expression.of(Condition.of(conditionalOption, expectValues.get(0)));
            for (int i = 0; i < expectValues.size(); i++) {
                if (i != 0) {
                    expression = expression.or(Expression.of(Condition.of(conditionalOption, expectValues.get(i))));
                }
            }

            this.requiredOptions.add(RequiredOption.ConditionalRequiredOptions.of(expression,
                new ArrayList<>(Arrays.asList(requiredOptions))));
            return this;
        }

        public <T> Builder conditional(@NonNull Option<T> conditionalOption, @NonNull T expectValue, @NonNull Option<?>... requiredOptions) {
            for (Option<?> o : requiredOptions) {
                verifyDuplicate(o, "ConditionalOption");
                verifyRequiredOptionDefaultValue(o);
            }

            verifyConditionalExists(conditionalOption);

            /**
             * Each parameter can only be controlled by one other parameter
             */
            Expression expression = Expression.of(Condition.of(conditionalOption, expectValue));

            this.requiredOptions.add(RequiredOption.ConditionalRequiredOptions.of(expression,
                new ArrayList<>(Arrays.asList(requiredOptions))));
            return this;
        }

        /**
         * Bundled options, must be present or absent together.
         */
        public Builder bundled(@NonNull Option<?>... requiredOptions) {
            for (Option<?> option : requiredOptions) {
                verifyDuplicate(option, "BundledOption");
            }
            this.requiredOptions.add(RequiredOption.BundledRequiredOptions.of(requiredOptions));
            return this;
        }

        public OptionRule build() {
            return new OptionRule(optionalOptions, requiredOptions);
        }

        private void verifyRequiredOptionDefaultValue(@NonNull Option<?> option) {
            if (option.defaultValue() != null) {
                throw new OptionValidationException(
                    String.format("Required option '%s' should have no default value.", option.key()));
            }
        }

        private void verifyDuplicate(@NonNull Option<?> option, @NonNull String currentOptionType) {
            if (optionalOptions.contains(option)) {
                throw new OptionValidationException(
                    String.format("%s '%s' duplicate in option options.", currentOptionType, option.key()));
            }

            requiredOptions.forEach(requiredOption -> {
                if (requiredOption.getOptions().contains(option)) {
                    throw new OptionValidationException(
                        String.format("%s '%s' duplicate in '%s'.", currentOptionType, option.key(), requiredOption.getClass().getName()));
                }
            });
        }

        private void verifyConditionalExists(@NonNull Option<?> option) {
            boolean inOptions = optionalOptions.contains(option);
            AtomicBoolean inRequired = new AtomicBoolean(false);
            requiredOptions.forEach(requiredOption -> {
                if (requiredOption.getOptions().contains(option)) {
                    inRequired.set(true);
                }
            });

            if (!inOptions && !inRequired.get()) {
                throw new OptionValidationException(
                    String.format("Conditional '%s' not found in options.", option.key()));
            }
        }
    }
}
