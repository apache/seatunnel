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

import lombok.Getter;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

public interface RequiredOption {

    /**
     * These options are mutually exclusive, allowing only one set of options to be configured.
     */
    @Getter
    class ExclusiveRequiredOptions implements RequiredOption {
        private final Set<BundledRequiredOptions> exclusiveBundledOptions;
        private final Set<Option<?>> exclusiveOptions;

        ExclusiveRequiredOptions(Set<BundledRequiredOptions> exclusiveBundledOptions, Set<Option<?>> exclusiveOptions) {
            this.exclusiveBundledOptions = exclusiveBundledOptions;
            this.exclusiveOptions = exclusiveOptions;
        }

        public static ExclusiveRequiredOptions of(Option<?>... exclusiveOptions) {
            return ExclusiveRequiredOptions.of(new HashSet<>(), exclusiveOptions);
        }

        public static ExclusiveRequiredOptions of(Set<BundledRequiredOptions> exclusiveBundledOptions, Option<?>... exclusiveOptions) {
            return new ExclusiveRequiredOptions(exclusiveBundledOptions, new HashSet<>(Arrays.asList(exclusiveOptions)));
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof ExclusiveRequiredOptions)) {
                return false;
            }
            ExclusiveRequiredOptions that = (ExclusiveRequiredOptions) obj;
            return Objects.equals(this.exclusiveOptions, that.exclusiveOptions);
        }

        @Override
        public int hashCode() {
            return Objects.hash(exclusiveBundledOptions, exclusiveOptions);
        }

        @Override
        public String toString() {
            return String.format("Exclusive required set options: %s", getOptionKeys(exclusiveOptions, exclusiveBundledOptions));
        }
    }

    /**
     * The option is required.
     */
    class AbsolutelyRequiredOption implements RequiredOption {
        @Getter
        private final Set<Option<?>> requiredOption;

        AbsolutelyRequiredOption(Set<Option<?>> requiredOption) {
            this.requiredOption = requiredOption;
        }

        public static AbsolutelyRequiredOption of(Option<?>... requiredOption) {
            return new AbsolutelyRequiredOption(new HashSet<>(Arrays.asList(requiredOption)));
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof AbsolutelyRequiredOption)) {
                return false;
            }
            AbsolutelyRequiredOption that = (AbsolutelyRequiredOption) obj;
            return Objects.equals(this.requiredOption, that.requiredOption);
        }

        @Override
        public int hashCode() {
            return this.requiredOption.hashCode();
        }

        @Override
        public String toString() {
            return String.format("Absolutely required options: '%s'", getOptionKeys(requiredOption));
        }
    }

    class ConditionalRequiredOptions implements RequiredOption {
        private final Expression expression;
        private final Set<Option<?>> requiredOption;

        ConditionalRequiredOptions(Expression expression, Set<Option<?>> requiredOption) {
            this.expression = expression;
            this.requiredOption = requiredOption;
        }

        public static ConditionalRequiredOptions of(Expression expression, Set<Option<?>> requiredOption) {
            return new ConditionalRequiredOptions(expression, requiredOption);
        }

        public static ConditionalRequiredOptions of(Condition<?> condition, Set<Option<?>> requiredOption) {
            return new ConditionalRequiredOptions(Expression.of(condition), requiredOption);
        }

        public Expression getExpression() {
            return expression;
        }

        public Set<Option<?>> getRequiredOption() {
            return requiredOption;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof ConditionalRequiredOptions)) {
                return false;
            }
            ConditionalRequiredOptions that = (ConditionalRequiredOptions) obj;
            return Objects.equals(this.expression, that.expression) && Objects.equals(this.requiredOption, that.requiredOption);
        }

        @Override
        public int hashCode() {
            return this.requiredOption.hashCode();
        }

        @Override
        public String toString() {
            return String.format("Condition expression: %s, Required options: %s", expression, getOptionKeys(requiredOption));
        }
    }

    /**
     * These options are bundled, must be present or absent together.
     */
    class BundledRequiredOptions implements RequiredOption {
        private final Set<Option<?>> requiredOption;

        BundledRequiredOptions(Set<Option<?>> requiredOption) {
            this.requiredOption = requiredOption;
        }

        public static BundledRequiredOptions of(Option<?>... requiredOption) {
            return new BundledRequiredOptions(new HashSet<>(Arrays.asList(requiredOption)));
        }

        public static BundledRequiredOptions of(Set<Option<?>> requiredOption) {
            return new BundledRequiredOptions(requiredOption);
        }

        public Set<Option<?>> getRequiredOption() {
            return requiredOption;
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof BundledRequiredOptions)) {
                return false;
            }
            BundledRequiredOptions that = (BundledRequiredOptions) obj;
            return Objects.equals(this.requiredOption, that.requiredOption);
        }

        @Override
        public int hashCode() {
            return this.requiredOption.hashCode();
        }

        @Override
        public String toString() {
            return String.format("Bundled Required options: %s", getOptionKeys(requiredOption));
        }
    }
}
