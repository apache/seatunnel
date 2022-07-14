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

public interface RequiredOption {
    class ExclusiveRequiredOptions implements RequiredOption {
        private final Set<Option<?>> exclusiveOptions;

        ExclusiveRequiredOptions(Set<Option<?>> exclusiveOptions) {
            this.exclusiveOptions = exclusiveOptions;
        }

        public static ExclusiveRequiredOptions of(Option<?>... exclusiveOptions) {
            return new ExclusiveRequiredOptions(new HashSet<>(Arrays.asList(exclusiveOptions)));
        }

        public Set<Option<?>> getExclusiveOptions() {
            return exclusiveOptions;
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
            return Objects.hash(exclusiveOptions);
        }

        static String getOptionKeys(Set<Option<?>> options) {
            StringBuilder builder = new StringBuilder();
            int i = 0;
            for (Option<?> option : options) {
                if (i > 0) {
                    builder.append(", ");
                }
                builder.append("'")
                    .append(option.getKey())
                    .append("'");
                i++;
            }
            return builder.toString();
        }
        @Override
        public String toString() {
            return String.format("Exclusive required options: %s", getOptionKeys(exclusiveOptions));
        }
    }

    class AbsolutelyRequiredOption<T> implements RequiredOption {
        private final Option<T> requiredOption;

        AbsolutelyRequiredOption(Option<T> requiredOption) {
            this.requiredOption = requiredOption;
        }

        public static <T> AbsolutelyRequiredOption<T> of(Option<T> requiredOption) {
            return new AbsolutelyRequiredOption<>(requiredOption);
        }

        @Override
        public boolean equals(Object obj) {
            if (this == obj) {
                return true;
            }
            if (!(obj instanceof AbsolutelyRequiredOption)) {
                return false;
            }
            AbsolutelyRequiredOption<?> that = (AbsolutelyRequiredOption<?>) obj;
            return Objects.equals(this.requiredOption, that.requiredOption);
        }

        @Override
        public int hashCode() {
            return this.requiredOption.hashCode();
        }

        @Override
        public String toString() {
            return String.format("Absolutely required option: '%s'", requiredOption.getKey());
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
            return String.format("Condition expression: %s, Required options: %s", expression, ExclusiveRequiredOptions.getOptionKeys(requiredOption));
        }
    }
}
