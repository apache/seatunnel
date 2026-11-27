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
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import org.junit.jupiter.api.Test;

import java.lang.invoke.MethodHandles;
import java.util.List;
import java.util.Map;

import static java.lang.invoke.MethodType.methodType;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SuppressWarnings("deprecation")
class ExpressionCompatibilityTest {
    private static final Option<String> MODE = Options.key("mode").stringType().noDefaultValue();
    private static final Option<String> TOKEN = Options.key("token").stringType().noDefaultValue();

    @Test
    void publishedExpressionEntryPointsRetainTheirJvmDescriptors()
            throws ReflectiveOperationException {
        MethodHandles.Lookup lookup = MethodHandles.publicLookup();
        Class<RequiredOption.ConditionalRequiredOptions> required =
                RequiredOption.ConditionalRequiredOptions.class;
        lookup.findConstructor(
                ConditionRule.class, methodType(void.class, Expression.class, OptionRule.class));
        lookup.findVirtual(ConditionRule.class, "getExpression", methodType(Expression.class));
        lookup.findStatic(required, "of", methodType(required, Expression.class, List.class));
        lookup.findVirtual(required, "getExpression", methodType(Expression.class));
        lookup.findVirtual(
                ConfigValidator.class,
                "validate",
                methodType(void.class, OptionRule.class, Expression.class));
    }

    @Test
    void legacyRulesSnapshotTheTriggerAndUseConditionPrecedence() {
        Condition<?> alternatives = Condition.of(MODE, "b").or(MODE, "c");
        Expression trigger = Expression.of(MODE, "a").and(Expression.of(alternatives));
        OptionRule nested = OptionRule.builder().required(TOKEN).build();
        ConditionRule conditionalRule = new ConditionRule(trigger, nested);
        RequiredOption.ConditionalRequiredOptions required =
                RequiredOption.ConditionalRequiredOptions.of(trigger, List.of(TOKEN));

        Condition<?> expected = Condition.of(MODE, "a").and(MODE, "b").or(MODE, "c");
        assertEquals(Condition.of(MODE, "a"), trigger.getCondition());
        alternatives.and(MODE, "d");
        trigger.or(Expression.of(MODE, "e"));
        assertEquals(expected, conditionalRule.getCondition());
        assertEquals(expected, required.getCondition());

        OptionRule rule =
                new OptionRule(
                        List.of(MODE), List.of(required), List.of(conditionalRule), List.of());
        ConfigValidator validator = ConfigValidator.of(ReadonlyConfig.fromMap(Map.of("mode", "c")));
        String message =
                assertThrows(OptionValidationException.class, () -> validator.validate(rule))
                        .getMessage();
        assertTrue(message.contains("required because [" + expected + "] is true"), message);
        assertTrue(message.contains("when [" + expected + "]"), message);
    }

    @Test
    void legacyGettersExposeDetachedOrAlternativesFromBuilderRules() {
        OptionRule nested = OptionRule.builder().required(TOKEN).build();
        OptionRule rule =
                OptionRule.builder()
                        .optional(MODE)
                        .conditional(MODE, List.of("a", "b"), TOKEN)
                        .conditionalRule(MODE, List.of("a", "b"), nested)
                        .build();
        RequiredOption.ConditionalRequiredOptions required =
                (RequiredOption.ConditionalRequiredOptions) rule.getRequiredOptions().get(0);
        ConditionRule conditionalRule = rule.getConditionRules().get(0);
        for (Expression view : List.of(required.getExpression(), conditionalRule.getExpression())) {
            assertEquals(Condition.of(MODE, "a"), view.getCondition());
            assertEquals(Boolean.FALSE, view.and());
            assertEquals(Condition.of(MODE, "b"), view.getNext().getCondition());
            assertFalse(view.getNext().hasNext());
            view.getCondition().and(MODE, "c");
            view.or(Expression.of(MODE, "d"));
        }
        Condition<?> expected = Condition.of(MODE, "a").or(MODE, "b");
        assertEquals(expected, required.getCondition());
        assertEquals(expected, conditionalRule.getCondition());
    }

    @Test
    void legacyValidationHintIsOptionalAndDoesNotGateValidation() {
        ConfigValidator validator = ConfigValidator.of(ReadonlyConfig.fromMap(Map.of()));
        OptionRule rule = OptionRule.builder().required(TOKEN).build();
        String plain =
                assertThrows(OptionValidationException.class, () -> validator.validate(rule))
                        .getMessage();
        assertEquals(
                plain,
                assertThrows(OptionValidationException.class, () -> validator.validate(rule, null))
                        .getMessage());

        Expression hint = Expression.of(MODE, "a").or(Expression.of(MODE, "b"));
        String message =
                assertThrows(OptionValidationException.class, () -> validator.validate(rule, hint))
                        .getMessage();
        assertTrue(message.contains("when [" + hint + "]"), message);
    }
}
