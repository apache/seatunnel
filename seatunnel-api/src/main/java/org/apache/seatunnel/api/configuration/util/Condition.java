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

import lombok.Getter;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

@Getter
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

    public static <T> Condition<T> of(Option<T> option, T expectValue) {
        return new Condition<>(option, expectValue);
    }

    public static <T> Condition<T> of(Option<T> option, ConditionOperator op, T expectValue) {
        return new Condition<>(option, op, expectValue, null);
    }

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

    public boolean hasNext() {
        return this.next != null;
    }

    public Boolean and() {
        return this.and;
    }

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

    /**
     * Renders this condition chain as a human-readable string using AND-first precedence grouping,
     * consistent with the evaluation semantics in {@code ConfigValidator}.
     *
     * <p>Multi-node AND segments are wrapped in parentheses when mixed with OR: {@code A || (B &&
     * C)} rather than {@code (A || B) && C}.
     */
    @Override
    public String toString() {
        List<String> orSegments = new ArrayList<>();
        List<Integer> orSegmentSizes = new ArrayList<>();
        Condition<?> cur = this;
        while (cur != null) {
            StringBuilder segment = new StringBuilder();
            int count = 0;
            while (cur != null) {
                if (count > 0) {
                    segment.append(" && ");
                }
                segment.append(conditionToString(cur));
                count++;
                if (!cur.hasNext()) {
                    cur = null;
                    break;
                }
                if (Boolean.TRUE.equals(cur.and)) {
                    cur = cur.next;
                } else {
                    cur = cur.next;
                    break;
                }
            }
            orSegments.add(segment.toString());
            orSegmentSizes.add(count);
        }
        if (orSegments.size() == 1) {
            return orSegments.get(0);
        }
        StringBuilder result = new StringBuilder();
        for (int i = 0; i < orSegments.size(); i++) {
            if (i > 0) {
                result.append(" || ");
            }
            if (orSegmentSizes.get(i) > 1) {
                result.append("(").append(orSegments.get(i)).append(")");
            } else {
                result.append(orSegments.get(i));
            }
        }
        return result.toString();
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
