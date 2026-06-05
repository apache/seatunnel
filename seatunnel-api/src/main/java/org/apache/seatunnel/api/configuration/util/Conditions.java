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

/**
 * Unified factory for creating {@link Condition} instances.
 *
 * <p>Usage example:
 *
 * <pre>{@code
 * static *
 * OptionRule.builder()
 *     .required(PORT, greaterOrEqual(PORT, 1).and(lessOrEqual(PORT, 65535)))
 *     .required(HOST, notBlank(HOST))
 *     .required(START_TS, END_TS, lessThanField(START_TS, END_TS))
 *     .build();
 * }</pre>
 *
 * <p>Currently supported operators (17 total, 4 categories):
 *
 * <ul>
 *   <li><b>Numeric</b>: {@code greaterThan}, {@code greaterOrEqual}, {@code lessThan}, {@code
 *       lessOrEqual}
 *   <li><b>String</b>: {@code notBlank}, {@code startsWith}, {@code contains}, {@code matches},
 *       {@code upperCase}, {@code lowerCase}
 *   <li><b>Collection</b>: {@code notEmpty}, {@code unique}
 *   <li><b>Cross-field</b>: {@code lessThanField}, {@code lessOrEqualField}, {@code
 *       greaterThanField}, {@code greaterOrEqualField}
 * </ul>
 *
 * <p>Additionally, equality checks are available via {@link Condition#of(Option, Object)} (EQUAL)
 * and {@link Condition#of(Option, ConditionOperator, Object)} (NOT_EQUAL).
 */
public final class Conditions {

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

    public static <T> Condition<T> matches(Option<T> option, T regex) {
        return new Condition<>(option, ConditionOperator.MATCHES, regex, null);
    }

    public static <T> Condition<T> contains(Option<T> option, T substring) {
        return new Condition<>(option, ConditionOperator.CONTAINS, substring, null);
    }

    public static <T> Condition<T> upperCase(Option<T> option) {
        return new Condition<>(option, ConditionOperator.UPPER_CASE, null, null);
    }

    public static <T> Condition<T> lowerCase(Option<T> option) {
        return new Condition<>(option, ConditionOperator.LOWER_CASE, null, null);
    }

    // ==================== Collection validation ====================

    public static <T> Condition<T> notEmpty(Option<T> option) {
        return new Condition<>(option, ConditionOperator.NOT_EMPTY, null, null);
    }

    public static <T> Condition<T> unique(Option<T> option) {
        return new Condition<>(option, ConditionOperator.COLLECTION_UNIQUE, null, null);
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
}
