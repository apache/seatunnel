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

package org.apache.seatunnel.connectors.seatunnel.jdbc;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcCommonOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSinkOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.sink.JdbcSinkFactory;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.JdbcSourceFactory;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Every applicable Jdbc option must be declared in its source or sink factory option rule.
 *
 * <p>{@code --check}, {@code --dry-run} and the Flink/Spark config validation reject any key that
 * is not declared in the rule ({@link ConfigValidator#validateUnknownKeys}), so a defined but
 * undeclared option makes a valid config fail validation. The options are collected by reflection,
 * so an option added later is covered without changing this test.
 *
 * <p>The source and sink factories each own the options that are applicable to their own side, so a
 * field declared in the shared {@link JdbcCommonOptions} is not automatically a member of both
 * rules: the source reads some options the sink never consumes and vice versa. {@link
 * #NOT_SOURCE_OPTIONS} and {@link #NOT_SINK_OPTIONS} hold exactly those documented scoped
 * exceptions, so every other option visible from an options class stays declared by the matching
 * factory.
 */
class JdbcOptionRuleCompletenessTest {

    /** Sink-only options that the source does not read. */
    private static final Set<String> NOT_SOURCE_OPTIONS =
            Collections.unmodifiableSet(
                    new HashSet<>(Arrays.asList(JdbcCommonOptions.SCHEMA.key())));

    /** Source-only options that the sink does not read. */
    private static final Set<String> NOT_SINK_OPTIONS =
            Collections.unmodifiableSet(
                    new HashSet<>(
                            Arrays.asList(JdbcCommonOptions.NUMERIC_SPECIAL_VALUE_AS_NULL.key())));

    @Test
    void testSourceRuleDeclaresAllSourceOptions() {
        assertAllDeclared(
                new JdbcSourceFactory().optionRule(),
                collectOptionKeys(JdbcSourceOptions.class, NOT_SOURCE_OPTIONS));
    }

    @Test
    void testSinkRuleDeclaresAllSinkOptions() {
        assertAllDeclared(
                new JdbcSinkFactory().optionRule(),
                collectOptionKeys(JdbcSinkOptions.class, NOT_SINK_OPTIONS));
    }

    /** The source-only key must remain accepted by the source and rejected by the sink. */
    @Test
    void testSourceOnlyOptionRejectedBySinkRule() {
        String key = JdbcCommonOptions.NUMERIC_SPECIAL_VALUE_AS_NULL.key();
        // Only key membership matters here.
        Map<String, Object> config = Collections.singletonMap(key, true);
        Assertions.assertDoesNotThrow(
                () ->
                        ConfigValidator.validateUnknownKeys(
                                ReadonlyConfig.fromMap(config),
                                new JdbcSourceFactory().optionRule(),
                                "Jdbc"));
        OptionValidationException exception =
                Assertions.assertThrows(
                        OptionValidationException.class,
                        () ->
                                ConfigValidator.validateUnknownKeys(
                                        ReadonlyConfig.fromMap(config),
                                        new JdbcSinkFactory().optionRule(),
                                        "Jdbc"));
        Assertions.assertTrue(
                exception.getMessage().contains(key),
                "Sink rejection must name '" + key + "', but was: " + exception.getMessage());
    }

    private static void assertAllDeclared(OptionRule rule, Set<String> optionKeys) {
        Map<String, Object> config = new HashMap<>();
        for (String key : optionKeys) {
            // Only the key matters for the unknown-key check, not the value
            config.put(key, "value");
        }
        Assertions.assertDoesNotThrow(
                () ->
                        ConfigValidator.validateUnknownKeys(
                                ReadonlyConfig.fromMap(config), rule, "Jdbc"));
    }

    /**
     * Returns the keys of all public static {@link Option} fields of the given options class and
     * its superclasses (such as {@link JdbcCommonOptions}), minus the excluded keys.
     */
    private static Set<String> collectOptionKeys(Class<?> optionsClass, Set<String> excluded) {
        Set<String> keys = new HashSet<>();
        for (Class<?> clazz = optionsClass; clazz != null; clazz = clazz.getSuperclass()) {
            for (Field field : clazz.getDeclaredFields()) {
                int modifiers = field.getModifiers();
                if (Modifier.isPublic(modifiers)
                        && Modifier.isStatic(modifiers)
                        && Option.class.isAssignableFrom(field.getType())) {
                    keys.add(readOption(field).key());
                }
            }
        }
        keys.removeAll(excluded);
        Assertions.assertFalse(keys.isEmpty(), "No option found in " + optionsClass.getName());
        return keys;
    }

    private static Option<?> readOption(Field field) {
        try {
            return (Option<?>) field.get(null);
        } catch (IllegalAccessException e) {
            throw new IllegalStateException("Cannot read option field " + field.getName(), e);
        }
    }
}
