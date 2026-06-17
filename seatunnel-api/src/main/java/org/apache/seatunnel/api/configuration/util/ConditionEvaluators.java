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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import java.math.BigDecimal;
import java.util.Collection;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Objects;

/**
 * Registry of per-{@link ConditionOperator} evaluation logic. Stateless; every evaluator is a pure
 * function of (value, condition, config).
 */
public final class ConditionEvaluators {

    @FunctionalInterface
    interface Evaluator {
        boolean evaluate(Object value, Condition<?> condition, ReadonlyConfig config);
    }

    private static final Map<ConditionOperator, Evaluator> REGISTRY = createRegistry();

    static boolean evaluate(Condition<?> condition, ReadonlyConfig config) {
        ConditionOperator operator = condition.getOperator();
        if (operator == null) {
            throw new OptionValidationException(
                    "Condition for option '%s' has a null operator", condition.getOption().key());
        }
        try {
            Object value = config.get(condition.getOption());
            Evaluator evaluator = REGISTRY.get(operator);
            return evaluator.evaluate(value, condition, config);
        } catch (OptionValidationException e) {
            throw new OptionValidationException(
                    "Failed to evaluate constraint '%s' on option '%s': %s",
                    condition.toString(), condition.getOption().key(), e.getRawMessage());
        }
    }

    @SuppressWarnings({"rawtypes"})
    private static Map<ConditionOperator, Evaluator> createRegistry() {
        Map<ConditionOperator, Evaluator> m = new EnumMap<>(ConditionOperator.class);

        // Equality
        m.put(ConditionOperator.EQUAL, (v, c, cfg) -> Objects.equals(c.getExpectValue(), v));
        m.put(ConditionOperator.NOT_EQUAL, (v, c, cfg) -> !Objects.equals(c.getExpectValue(), v));

        // Numeric (null value -> false, preserving or() short-circuit)
        m.put(
                ConditionOperator.GREATER_THAN,
                (v, c, cfg) -> v != null && compareNumbers(v, c.getExpectValue()) > 0);
        m.put(
                ConditionOperator.GREATER_OR_EQUAL,
                (v, c, cfg) -> v != null && compareNumbers(v, c.getExpectValue()) >= 0);
        m.put(
                ConditionOperator.LESS_THAN,
                (v, c, cfg) -> v != null && compareNumbers(v, c.getExpectValue()) < 0);
        m.put(
                ConditionOperator.LESS_OR_EQUAL,
                (v, c, cfg) -> v != null && compareNumbers(v, c.getExpectValue()) <= 0);

        // String
        m.put(
                ConditionOperator.NOT_BLANK,
                (v, c, cfg) -> v instanceof String && !((String) v).trim().isEmpty());
        m.put(
                ConditionOperator.STARTS_WITH,
                (v, c, cfg) ->
                        v instanceof String
                                && ((String) v).startsWith(String.valueOf(c.getExpectValue())));
        m.put(
                ConditionOperator.CONTAINS,
                (v, c, cfg) ->
                        v instanceof String
                                && ((String) v).contains(String.valueOf(c.getExpectValue())));
        m.put(
                ConditionOperator.MATCHES,
                (v, c, cfg) ->
                        v instanceof String
                                && ((String) v).matches(String.valueOf(c.getExpectValue())));
        m.put(
                ConditionOperator.UPPER_CASE,
                (v, c, cfg) -> v instanceof String && v.equals(((String) v).toUpperCase()));
        m.put(
                ConditionOperator.LOWER_CASE,
                (v, c, cfg) -> v instanceof String && v.equals(((String) v).toLowerCase()));

        // Collection
        m.put(
                ConditionOperator.NOT_EMPTY,
                (v, c, cfg) -> v instanceof Collection && !((Collection) v).isEmpty());
        m.put(
                ConditionOperator.COLLECTION_UNIQUE,
                (v, c, cfg) -> {
                    if (v instanceof Collection) {
                        Collection col = (Collection) v;
                        return col.size() == new HashSet<>(col).size();
                    }
                    return false;
                });

        // Map
        m.put(
                ConditionOperator.MAP_NOT_EMPTY,
                (v, c, cfg) -> v instanceof Map && !((Map) v).isEmpty());
        m.put(
                ConditionOperator.MAP_CONTAINS_KEY,
                (v, c, cfg) -> v instanceof Map && ((Map) v).containsKey(c.getExpectValue()));
        m.put(
                ConditionOperator.MAP_CONTAINS_KEYS,
                (v, c, cfg) -> {
                    if (!(v instanceof Map)) return false;
                    Object expect = c.getExpectValue();
                    if (!(expect instanceof Collection)) return false;
                    return ((Map) v).keySet().containsAll((Collection) expect);
                });

        // Cross-field (null on either side -> false, preserving or() short-circuit)
        m.put(
                ConditionOperator.FIELD_LESS_THAN,
                (v, c, cfg) -> {
                    if (v == null) return false;
                    Object other = cfg.get(c.getCompareOption());
                    if (other == null) return false;
                    return compareNumbers(v, other) < 0;
                });
        m.put(
                ConditionOperator.FIELD_LESS_OR_EQUAL,
                (v, c, cfg) -> {
                    if (v == null) return false;
                    Object other = cfg.get(c.getCompareOption());
                    if (other == null) return false;
                    return compareNumbers(v, other) <= 0;
                });
        m.put(
                ConditionOperator.FIELD_GREATER_THAN,
                (v, c, cfg) -> {
                    if (v == null) return false;
                    Object other = cfg.get(c.getCompareOption());
                    if (other == null) return false;
                    return compareNumbers(v, other) > 0;
                });
        m.put(
                ConditionOperator.FIELD_GREATER_OR_EQUAL,
                (v, c, cfg) -> {
                    if (v == null) return false;
                    Object other = cfg.get(c.getCompareOption());
                    if (other == null) return false;
                    return compareNumbers(v, other) >= 0;
                });

        // Extension (custom logic delegated to ConditionExtension)
        m.put(
                ConditionOperator.EXTENSION,
                (v, c, cfg) -> {
                    ConditionExtension<Object> ext = (ConditionExtension<Object>) c.getExtension();
                    return ext.evaluate(cfg, v);
                });

        for (ConditionOperator op : ConditionOperator.values()) {
            if (!m.containsKey(op)) {
                throw new IllegalStateException(
                        "Missing evaluator for ConditionOperator." + op.name());
            }
        }
        return Collections.unmodifiableMap(m);
    }

    @SuppressWarnings({"rawtypes"})
    static int compareNumbers(Object a, Object b) {
        if (a == null || b == null) {
            throw new OptionValidationException(
                    "Cannot compare null values in numeric comparison: leftPresent=%s, rightPresent=%s",
                    a != null, b != null);
        }
        if (a instanceof Number && b instanceof Number) {
            return compareNumberValues((Number) a, (Number) b);
        }
        if (a instanceof Comparable && b instanceof Comparable) {
            return ((Comparable) a).compareTo(b);
        }
        throw new OptionValidationException(
                "Cannot compare values of type %s and %s",
                a.getClass().getSimpleName(), b.getClass().getSimpleName());
    }

    private static int compareNumberValues(Number a, Number b) {
        if (a instanceof BigDecimal || b instanceof BigDecimal) {
            BigDecimal bdA =
                    a instanceof BigDecimal ? (BigDecimal) a : new BigDecimal(a.toString());
            BigDecimal bdB =
                    b instanceof BigDecimal ? (BigDecimal) b : new BigDecimal(b.toString());
            return bdA.compareTo(bdB);
        }
        if (a instanceof Double
                || a instanceof Float
                || b instanceof Double
                || b instanceof Float) {
            return Double.compare(a.doubleValue(), b.doubleValue());
        }
        return Long.compare(a.longValue(), b.longValue());
    }
}
