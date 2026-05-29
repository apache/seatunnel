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

import java.util.Objects;

public class Condition<T> {
    private final Option<T> option;
    private final T expectValue;
    private final ConditionOperator operator;
    private final Option<?> compareOption;
    private Boolean and = null;
    private Condition<?> next = null;

    Condition(Option<T> option, T expectValue) {
        this(option, ConditionOperator.EQUAL, expectValue, null);
    }

    Condition(
            Option<T> option, ConditionOperator operator, T expectValue, Option<?> compareOption) {
        if (option == null) {
            throw new IllegalArgumentException("Condition option must not be null");
        }
        if (operator == null) {
            throw new IllegalArgumentException("Condition operator must not be null");
        }
        if (operator.getSource() == ConditionOperator.Source.FIELD && compareOption == null) {
            throw new IllegalArgumentException(
                    String.format(
                            "Operator %s requires a compareOption (cross-field comparison), but compareOption is null",
                            operator.name()));
        }
        if (operator.getArity() == ConditionOperator.Arity.BINARY
                && operator.getSource() == ConditionOperator.Source.LITERAL
                && expectValue == null) {
            throw new IllegalArgumentException(
                    String.format(
                            "Operator %s requires an expectValue, but expectValue is null",
                            operator.name()));
        }
        this.option = option;
        this.operator = operator;
        this.expectValue = expectValue;
        this.compareOption = compareOption;
    }

    // ==================== Equality (backward-compatible) ====================

    public static <T> Condition<T> of(Option<T> option, T expectValue) {
        return new Condition<>(option, expectValue);
    }

    public static <T> Condition<T> of(Option<T> option, ConditionOperator op, T expectValue) {
        return new Condition<>(option, op, expectValue, null);
    }

    // ==================== Numeric comparison ====================

    public static <T> Condition<T> greaterThan(Option<T> option, T value) {
        return new Condition<>(option, ConditionOperator.GREATER_THAN, value, null);
    }

    public static <T> Condition<T> greaterOrEqual(Option<T> option, T value) {
        return new Condition<>(option, ConditionOperator.GREATER_OR_EQUAL, value, null);
    }

    public static <T> Condition<T> lessThan(Option<T> option, T value) {
        return new Condition<>(option, ConditionOperator.LESS_THAN, value, null);
    }

    public static <T> Condition<T> lessOrEqual(Option<T> option, T value) {
        return new Condition<>(option, ConditionOperator.LESS_OR_EQUAL, value, null);
    }

    // ==================== String validation ====================

    public static <T> Condition<T> notBlank(Option<T> option) {
        return new Condition<>(option, ConditionOperator.NOT_BLANK, null, null);
    }

    public static <T> Condition<T> startsWith(Option<T> option, T prefix) {
        return new Condition<>(option, ConditionOperator.STARTS_WITH, prefix, null);
    }

    public static Condition<String> startsWithIgnoreCase(Option<String> option, String prefix) {
        return new Condition<>(option, ConditionOperator.STARTS_WITH_IGNORE_CASE, prefix, null);
    }

    public static <T> Condition<T> contains(Option<T> option, T substring) {
        return new Condition<>(option, ConditionOperator.CONTAINS, substring, null);
    }

    public static <T> Condition<T> matches(Option<T> option, T regex) {
        return new Condition<>(option, ConditionOperator.MATCHES, regex, null);
    }

    public static <T> Condition<T> upperCase(Option<T> option) {
        return new Condition<>(option, ConditionOperator.UPPER_CASE, null, null);
    }

    public static <T> Condition<T> lowerCase(Option<T> option) {
        return new Condition<>(option, ConditionOperator.LOWER_CASE, null, null);
    }

    // ==================== String length ====================

    public static Condition<String> lengthEqual(Option<String> option, int length) {
        return new Condition(option, ConditionOperator.LENGTH_EQUAL, length, null);
    }

    public static Condition<String> lengthGreaterOrEqual(Option<String> option, int length) {
        return new Condition(option, ConditionOperator.LENGTH_GREATER_OR_EQUAL, length, null);
    }

    public static Condition<String> lengthLessOrEqual(Option<String> option, int length) {
        return new Condition(option, ConditionOperator.LENGTH_LESS_OR_EQUAL, length, null);
    }

    // ==================== String suffix ====================

    public static <T> Condition<T> endsWith(Option<T> option, T suffix) {
        return new Condition<>(option, ConditionOperator.ENDS_WITH, suffix, null);
    }

    public static Condition<String> endsWithIgnoreCase(Option<String> option, String suffix) {
        return new Condition<>(option, ConditionOperator.ENDS_WITH_IGNORE_CASE, suffix, null);
    }

    // ==================== Collection validation ====================

    public static <T> Condition<T> notEmpty(Option<T> option) {
        return new Condition<>(option, ConditionOperator.NOT_EMPTY, null, null);
    }

    public static <T> Condition<T> unique(Option<T> option) {
        return new Condition<>(option, ConditionOperator.COLLECTION_UNIQUE, null, null);
    }

    public static <T> Condition<T> sizeEqual(Option<T> option, int size) {
        return new Condition(option, ConditionOperator.COLLECTION_SIZE_EQUAL, size, null);
    }

    public static <T> Condition<T> sizeGreaterOrEqual(Option<T> option, int size) {
        return new Condition(
                option, ConditionOperator.COLLECTION_SIZE_GREATER_OR_EQUAL, size, null);
    }

    public static <T> Condition<T> sizeLessOrEqual(Option<T> option, int size) {
        return new Condition(option, ConditionOperator.COLLECTION_SIZE_LESS_OR_EQUAL, size, null);
    }

    // ==================== Cross-field comparison ====================

    public static <T> Condition<T> lessThanField(Option<T> option, Option<T> other) {
        return new Condition<>(option, ConditionOperator.FIELD_LESS_THAN, null, other);
    }

    public static <T> Condition<T> lessOrEqualField(Option<T> option, Option<T> other) {
        return new Condition<>(option, ConditionOperator.FIELD_LESS_OR_EQUAL, null, other);
    }

    public static <T> Condition<T> greaterThanField(Option<T> option, Option<T> other) {
        return new Condition<>(option, ConditionOperator.FIELD_GREATER_THAN, null, other);
    }

    public static <T> Condition<T> greaterOrEqualField(Option<T> option, Option<T> other) {
        return new Condition<>(option, ConditionOperator.FIELD_GREATER_OR_EQUAL, null, other);
    }

    public static <T> Condition<T> equalField(Option<T> option, Option<T> other) {
        return new Condition<>(option, ConditionOperator.FIELD_EQUAL, null, other);
    }

    public static <T> Condition<T> notEqualField(Option<T> option, Option<T> other) {
        return new Condition<>(option, ConditionOperator.FIELD_NOT_EQUAL, null, other);
    }

    public static <T> Condition<T> sizeEqualField(Option<T> option, Option<?> other) {
        return new Condition<>(option, ConditionOperator.FIELD_SIZE_EQUAL, null, other);
    }

    // ==================== Chain operations (existing API, unchanged) ====================

    public <E> Condition<T> and(Option<E> option, E expectValue) {
        return and(of(option, expectValue));
    }

    public <E> Condition<T> or(Option<E> option, E expectValue) {
        return or(of(option, expectValue));
    }

    public Condition<T> and(Condition<?> next) {
        addCondition(true, next);
        return this;
    }

    public Condition<T> or(Condition<?> next) {
        addCondition(false, next);
        return this;
    }

    private void addCondition(boolean and, Condition<?> next) {
        // Check: next chain must not contain any node already in this chain
        Condition<?> cur = next;
        while (cur != null) {
            Condition<?> self = this;
            while (self != null) {
                if (self == cur) {
                    throw new IllegalArgumentException(
                            "Circular condition chain detected: '"
                                    + cur.option.key()
                                    + "' already exists in the chain");
                }
                self = self.next;
            }
            cur = cur.next;
        }
        Condition<?> tail = getTailCondition();
        tail.and = and;
        tail.next = next;
    }

    protected int getCount() {
        int i = 1;
        Condition<?> cur = this;
        while (cur.hasNext()) {
            i++;
            cur = cur.next;
        }
        return i;
    }

    Condition<?> getTailCondition() {
        return hasNext() ? this.next.getTailCondition() : this;
    }

    // ==================== Accessors ====================

    public boolean hasNext() {
        return this.next != null;
    }

    public Condition<?> getNext() {
        return this.next;
    }

    public Option<T> getOption() {
        return option;
    }

    public T getExpectValue() {
        return expectValue;
    }

    public ConditionOperator getOperator() {
        return operator;
    }

    public Option<?> getCompareOption() {
        return compareOption;
    }

    public Boolean and() {
        return this.and;
    }

    // ==================== equals / hashCode ====================

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof Condition)) {
            return false;
        }
        Condition<?> that = (Condition<?>) obj;
        return Objects.equals(this.option, that.option)
                && Objects.equals(this.expectValue, that.expectValue)
                && Objects.equals(this.operator, that.operator)
                && Objects.equals(this.compareOption, that.compareOption)
                && Objects.equals(this.and, that.and)
                && Objects.equals(this.next, that.next);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
                this.option,
                this.expectValue,
                this.operator,
                this.compareOption,
                this.and,
                this.next);
    }

    // ==================== toString ====================

    @Override
    public String toString() {
        Condition<?> cur = this;
        StringBuilder builder = new StringBuilder();
        boolean bracket = false;
        do {
            builder.append(conditionToString(cur));
            if (bracket) {
                builder = new StringBuilder(String.format("(%s)", builder));
                bracket = false;
            }
            if (cur.hasNext()) {
                if (cur.next.hasNext() && !cur.and.equals(cur.next.and)) {
                    bracket = true;
                }
                builder.append(cur.and ? " && " : " || ");
            }
            cur = cur.next;
        } while (cur != null);
        return builder.toString();
    }

    private static String conditionToString(Condition<?> cond) {
        ConditionOperator op = cond.operator;
        String key = "'" + cond.option.key() + "'";

        if (op.getSource() == ConditionOperator.Source.FIELD) {
            return key + " " + op.getDisplaySymbol() + " '" + cond.compareOption.key() + "'";
        }
        if (op.getArity() == ConditionOperator.Arity.UNARY) {
            return key + " " + op.getSymbol();
        }
        return key + " " + op.getDisplaySymbol() + " " + cond.expectValue;
    }
}
