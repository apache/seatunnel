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

import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.Objects;
import java.util.Set;

/**
 * A compatibility wrapper for composing a chain of {@link Condition conditions}.
 *
 * @deprecated {@link Condition} provides the preferred condition-chain composition API through
 *     {@link Condition#and(Condition)} and {@link Condition#or(Condition)}. Use {@link Condition}
 *     directly for new code. Rule constructors and factories snapshot expression operands into a
 *     single condition chain in declaration order. AND binds more tightly than OR across the entire
 *     chain, including links between expression nodes.
 */
@Deprecated
public class Expression {
    private final Condition<?> condition;
    private Boolean and = null;
    private Expression next = null;

    Expression(Condition<?> condition) {
        this.condition = condition;
    }

    public static <T> Expression of(Option<T> option, T expectValue) {
        return new Expression(Condition.of(option, expectValue));
    }

    public static Expression of(Condition<?> condition) {
        return new Expression(condition);
    }

    /** Flattens the legacy wrapper links without appending to any caller-owned condition. */
    Condition<?> toCondition() {
        Condition<?> head = Objects.requireNonNull(condition, "condition").copyChain();
        Condition<?> tail = head;
        Set<Expression> visited = Collections.newSetFromMap(new IdentityHashMap<>());
        visited.add(this);
        for (Expression node = this; node.hasNext(); node = node.next) {
            if (!visited.add(node.next)) {
                throw new IllegalArgumentException("Circular expression chain");
            }
            Condition<?> operand =
                    Objects.requireNonNull(node.next.condition, "condition").copyChain();
            tail = tail.getTailCondition();
            if (Boolean.TRUE.equals(node.and)) {
                tail.and(operand);
            } else {
                tail.or(operand);
            }
            tail = operand;
        }
        return head;
    }

    /** Presents a detached legacy expression with one node for each OR-separated AND group. */
    static Expression fromCondition(Condition<?> condition) {
        Condition<?> tail = copyOperand(condition);
        Expression head = of(tail);
        Expression group = head;
        for (Condition<?> node = condition; node.hasNext(); node = node.getNext()) {
            Condition<?> operand = copyOperand(node.getNext());
            if (Boolean.TRUE.equals(node.and())) {
                tail.and(operand);
            } else {
                group.and = false;
                group.next = of(operand);
                group = group.next;
            }
            tail = operand;
        }
        return head;
    }

    private static <T> Condition<T> copyOperand(Condition<T> condition) {
        return new Condition<>(
                condition.getOption(),
                condition.getOperator(),
                condition.getExpectValue(),
                condition.getCompareOption(),
                condition.getExtension());
    }

    public Expression and(Expression next) {
        addExpression(true, next);
        return this;
    }

    public Expression or(Expression next) {
        addExpression(false, next);
        return this;
    }

    private void addExpression(boolean and, Expression next) {
        Expression tail = getTailExpression();
        tail.and = and;
        tail.next = next;
    }

    private Expression getTailExpression() {
        return hasNext() ? this.next.getTailExpression() : this;
    }

    public Condition<?> getCondition() {
        return condition;
    }

    public boolean hasNext() {
        return this.next != null;
    }

    public Expression getNext() {
        return this.next;
    }

    public Boolean and() {
        return this.and;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof Expression)) {
            return false;
        }
        Expression that = (Expression) obj;
        return Objects.equals(this.condition, that.condition)
                && Objects.equals(this.and, that.and)
                && Objects.equals(this.next, that.next);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.condition, this.and, this.next);
    }

    @Override
    public String toString() {
        Expression cur = this;
        StringBuilder builder = new StringBuilder();
        boolean bracket = false;
        do {
            if (cur.condition.getCount() > 1) {
                builder.append("(").append(cur.condition).append(")");
            } else {
                builder.append(cur.condition);
            }
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
}
