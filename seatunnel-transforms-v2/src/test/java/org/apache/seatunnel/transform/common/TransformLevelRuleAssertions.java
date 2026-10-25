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

package org.apache.seatunnel.transform.common;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.Condition;
import org.apache.seatunnel.api.configuration.util.ConditionRule;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.RequiredOption;
import org.apache.seatunnel.api.table.factory.TableTransformFactory;
import org.apache.seatunnel.api.table.factory.TableTransformFactoryContext;
import org.apache.seatunnel.api.transform.SeaTunnelTransform;

import org.junit.jupiter.api.Assertions;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Guards the duplication between the per-table rule declared by a transform factory and the {@link
 * AbstractMultiCatalogTransform#hasTransformLevelRuleConfigured(ReadonlyConfig)} override of its
 * multi-catalog transform. The override has to detect exactly the mandatory options of the factory
 * rule, otherwise a configured transform level rule is silently ignored, or an incomplete one is
 * applied to the tables matched by {@code table_match_regex}.
 */
public final class TransformLevelRuleAssertions {

    private TransformLevelRuleAssertions() {}

    /**
     * Verifies that the multi-catalog transform of the given factory detects a transform level rule
     * exactly from the mandatory options of its per-table rule.
     *
     * @param factory the transform factory under test
     * @param tableRuleOptions the mandatory options of its per-table rule
     */
    public static void assertMatchesFactoryRule(
            TableTransformFactory factory, Option<?>... tableRuleOptions) {
        String identifier = factory.factoryIdentifier();
        OptionRule tableRule = perTableRuleOf(factory.optionRule());
        Assertions.assertNotNull(
                tableRule, identifier + ": the factory does not declare a per-table rule");

        Set<String> declaredMandatoryOptions = mandatoryOptionKeys(tableRule);
        Set<String> expectedMandatoryOptions = optionKeys(tableRuleOptions);
        Assertions.assertEquals(
                expectedMandatoryOptions,
                declaredMandatoryOptions,
                identifier + ": the mandatory options of the per-table rule changed");

        AbstractMultiCatalogTransform transform = createTransform(factory);

        Assertions.assertFalse(
                transform.hasTransformLevelRuleConfigured(emptyConfig()),
                identifier + ": an empty config must not be detected as a transform level rule");

        for (Option<?> option : tableRuleOptions) {
            Assertions.assertTrue(
                    transform.hasTransformLevelRuleConfigured(configWith(option)),
                    identifier
                            + ": the mandatory option '"
                            + option.key()
                            + "' has to be detected as a transform level rule");
        }

        for (Option<?> option : declaredOptions(factory.optionRule())) {
            if (expectedMandatoryOptions.contains(option.key())) {
                continue;
            }
            Assertions.assertFalse(
                    transform.hasTransformLevelRuleConfigured(configWith(option)),
                    identifier
                            + ": '"
                            + option.key()
                            + "' is not a mandatory option of the per-table rule, it must not be"
                            + " detected as a transform level rule");
        }
    }

    private static AbstractMultiCatalogTransform createTransform(TableTransformFactory factory) {
        TableTransformFactoryContext context =
                new TableTransformFactoryContext(
                        Collections.emptyList(),
                        emptyConfig(),
                        factory.getClass().getClassLoader());
        SeaTunnelTransform<?> transform = factory.createTransform(context).createTransform();
        return (AbstractMultiCatalogTransform) transform;
    }

    /** The conditional rule that applies while no per-table rule is configured. */
    private static OptionRule perTableRuleOf(OptionRule factoryRule) {
        for (ConditionRule conditionRule : factoryRule.getConditionRules()) {
            Condition<?> condition = conditionRule.getExpression().getCondition();
            if (TransformCommonOptions.MULTI_TABLES.key().equals(condition.getOption().key())) {
                return conditionRule.getOptionRule();
            }
        }
        return null;
    }

    private static Set<String> mandatoryOptionKeys(OptionRule rule) {
        Set<String> keys = new TreeSet<>();
        for (RequiredOption requiredOption : rule.getRequiredOptions()) {
            // Conditional options are only mandatory for a given value of another option.
            if (requiredOption instanceof RequiredOption.ConditionalRequiredOptions) {
                continue;
            }
            for (Option<?> option : requiredOption.getOptions()) {
                keys.add(option.key());
            }
        }
        return keys;
    }

    /** All options of a rule, including the ones declared in nested rules. */
    private static List<Option<?>> declaredOptions(OptionRule rule) {
        List<Option<?>> options = new ArrayList<>(rule.getOptionalOptions());
        for (RequiredOption requiredOption : rule.getRequiredOptions()) {
            options.addAll(requiredOption.getOptions());
        }
        for (ConditionRule conditionRule : rule.getConditionRules()) {
            options.addAll(declaredOptions(conditionRule.getOptionRule()));
        }
        return options;
    }

    private static Set<String> optionKeys(Option<?>... options) {
        Set<String> keys = new TreeSet<>();
        for (Option<?> option : options) {
            keys.add(option.key());
        }
        return keys;
    }

    private static ReadonlyConfig emptyConfig() {
        return ReadonlyConfig.fromMap(new HashMap<>());
    }

    private static ReadonlyConfig configWith(Option<?> option) {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(option.key(), sampleValue(option));
        return ReadonlyConfig.fromMap(configMap);
    }

    private static Object sampleValue(Option<?> option) {
        if (option.defaultValue() != null) {
            return option.defaultValue();
        }
        return sampleValue(option.typeReference().getType(), option.key());
    }

    /** A value of the declared type, only used to check whether the option is present. */
    private static Object sampleValue(Type type, String optionKey) {
        if (type instanceof Class) {
            Class<?> clazz = (Class<?>) type;
            if (clazz.isEnum()) {
                return clazz.getEnumConstants()[0].toString();
            }
            if (Boolean.class.equals(clazz)) {
                return true;
            }
            if (Integer.class.equals(clazz)) {
                return 1;
            }
            if (Long.class.equals(clazz)) {
                return 1L;
            }
            if (Double.class.equals(clazz)) {
                return 1.0d;
            }
            if (String.class.equals(clazz)) {
                return "value";
            }
            // Object options (POJOs) are built from an empty object.
            return Collections.emptyMap();
        }
        ParameterizedType parameterizedType = (ParameterizedType) type;
        Class<?> rawType = (Class<?>) parameterizedType.getRawType();
        if (List.class.isAssignableFrom(rawType)) {
            return Collections.singletonList(
                    sampleValue(parameterizedType.getActualTypeArguments()[0], optionKey));
        }
        if (Map.class.isAssignableFrom(rawType)) {
            return Collections.singletonMap("key", "value");
        }
        throw new IllegalArgumentException(
                "Add a sample value for the option '" + optionKey + "' of type " + type);
    }
}
