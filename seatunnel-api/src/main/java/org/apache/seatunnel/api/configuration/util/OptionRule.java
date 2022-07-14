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

import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/**
 * Validation rule for {@link Option}.
 *
 * The option rule is typically built in one of the following pattern:
 *
 * <pre>{code
 *
 * }</pre>
 */
public class OptionRule {

    private final Set<Option<?>> optionalOptions;

    private final Set<RequiredOption> requiredOptions;

    OptionRule(Set<Option<?>> optionalOptions, Set<RequiredOption> requiredOptions) {
        this.optionalOptions = optionalOptions;
        this.requiredOptions = requiredOptions;
    }

    public Set<Option<?>> getOptionalOptions() {
        return optionalOptions;
    }

    public Set<RequiredOption> getRequiredOptions() {
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

    public static class Builder {
        private final Set<Option<?>> optionalOptions = new HashSet<>();
        private final Set<RequiredOption> requiredOptions = new HashSet<>();

        private Builder() {
        }

        public Builder optional(Option<?>... options) {
            for (Option<?> option : options) {
                if (option.getDefaultValue() == null) {
                    throw new OptionValidationException(String.format("Optional option '%s' should have default value.", option.getKey()));
                }
            }
            this.optionalOptions.addAll(Arrays.asList(options));
            return this;
        }

        public Builder required(Option<?>... options) {
            for (Option<?> option : options) {
                verifyRequiredOptionDefaultValue(option);
                this.requiredOptions.add(RequiredOption.AbsolutelyRequiredOption.of(option));
            }
            return this;
        }

        public Builder exclusive(Option<?>... options) {
            if (options.length <= 1) {
                throw new OptionValidationException("The number of exclusive options must be greater than 1.");
            }
            for (Option<?> option : options) {
                verifyRequiredOptionDefaultValue(option);
            }
            this.requiredOptions.add(RequiredOption.ExclusiveRequiredOptions.of(options));
            return this;
        }

        public <T> Builder conditional(Option<T> option, T expectValue, Option<?>... requiredOptions) {
            return conditional(Expression.of(Condition.of(option, expectValue)), requiredOptions);
        }

        public <T> Builder conditional(Expression expression, Option<?>... requiredOptions) {
            for (Option<?> o : requiredOptions) {
                verifyRequiredOptionDefaultValue(o);
            }
            this.requiredOptions.add(RequiredOption.ConditionalRequiredOptions.of(expression, new HashSet<>(Arrays.asList(requiredOptions))));
            return this;
        }

        public OptionRule build() {
            return new OptionRule(optionalOptions, requiredOptions);
        }

        private void verifyRequiredOptionDefaultValue(Option<?> option) {
            if (option.getDefaultValue() != null) {
                throw new OptionValidationException(String.format("Required option '%s' should have no default value.", option.getKey()));
            }
        }
    }
}
