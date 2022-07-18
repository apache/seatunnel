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
    private Boolean and = null;
    private Condition<?> next = null;

    Condition(Option<T> option, T expectValue) {
        this.option = option;
        this.expectValue = expectValue;
    }

    public static <T> Condition<T> of(Option<T> option, T expectValue) {
        return new Condition<>(option, expectValue);
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

    public Condition<?> getNext() {
        return this.next;
    }

    public Option<T> getOption() {
        return option;
    }

    public T getExpectValue() {
        return expectValue;
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
            && Objects.equals(this.and, that.and)
            && Objects.equals(this.next, that.next);
    }

    @Override
    public int hashCode() {
        return Objects.hash(this.option, this.expectValue, this.and, this.next);
    }

    @Override
    public String toString() {
        Condition<?> cur = this;
        StringBuilder builder = new StringBuilder();
        boolean bracket = false;
        do {
            builder.append("'")
                .append(cur.option.key())
                // TODO: support another condition
                .append("' == ")
                .append(cur.expectValue);
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
