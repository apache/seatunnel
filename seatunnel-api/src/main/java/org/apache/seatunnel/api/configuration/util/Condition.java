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
            return key + " " + op.getSymbol() + " '" + cond.compareOption.key() + "'";
        }
        if (op.getArity() == ConditionOperator.Arity.UNARY) {
            return key + " " + op.getSymbol();
        }
        return key + " " + op.getSymbol() + " " + cond.expectValue;
    }
}
