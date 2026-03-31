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

package org.apache.seatunnel.engine.server.rest.service;

import org.apache.seatunnel.api.common.PluginIdentifier;
import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.SingleChoiceOption;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.engine.server.rest.response.OptionRuleResponse;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class OptionRulesServiceTest {

    private final OptionRulesService service = new OptionRulesService(null);

    @Test
    void shouldReturnRuntimeSourceOptionRules() {
        OptionRuleResponse response = service.getOptionRules("source", "FakeSource");

        assertEquals("seatunnel", response.getEngineType());
        assertEquals("source", response.getPluginType());
        assertEquals("FakeSource", response.getPluginName());
        assertFalse(response.getOptionRule().getOptionalOptions().isEmpty());
        assertTrue(
                response.getOptionRule().getOptionalOptions().stream()
                        .anyMatch(option -> "row.num".equals(option.getKey())));
        assertTrue(
                response.getOptionRule().getRequiredOptions().stream()
                        .anyMatch(
                                option ->
                                        option.getRuleType()
                                                == OptionRuleResponse.RuleType.EXCLUSIVE));

        OptionRuleResponse.RequiredOptionMetadata conditionalRule =
                response.getOptionRule().getRequiredOptions().stream()
                        .filter(
                                option ->
                                        option.getRuleType()
                                                == OptionRuleResponse.RuleType.CONDITIONAL)
                        .findFirst()
                        .orElseThrow(AssertionError::new);
        assertNotNull(conditionalRule.getExpression());
        assertNotNull(conditionalRule.getExpressionTree());
        assertTrue(response.getOptionRule().getConditionRules().isEmpty());
    }

    @Test
    void shouldReturnRuntimeSinkOptionRules() {
        OptionRuleResponse response = service.getOptionRules("sink", "Console");

        assertEquals("seatunnel", response.getEngineType());
        assertEquals("sink", response.getPluginType());
        assertEquals("Console", response.getPluginName());
        assertTrue(
                response.getOptionRule().getOptionalOptions().stream()
                        .anyMatch(option -> "log.print.data".equals(option.getKey())));
        assertTrue(response.getOptionRule().getConditionRules().isEmpty());
    }

    @Test
    void shouldTrimPluginTypeAndPluginName() {
        OptionRuleResponse response = service.getOptionRules(" source ", " FakeSource ");

        assertEquals("source", response.getPluginType());
        assertEquals("FakeSource", response.getPluginName());
    }

    @Test
    void shouldPreserveBundledConditionalAndChoiceMetadata() {
        SingleChoiceOption<AuthMode> authMode =
                Options.key("auth.mode")
                        .singleChoice(
                                AuthMode.class, Arrays.asList(AuthMode.PASSWORD, AuthMode.TOKEN))
                        .defaultValue(AuthMode.PASSWORD)
                        .withDescription("Authentication mode");
        Option<String> username =
                Options.key("username")
                        .stringType()
                        .noDefaultValue()
                        .withDescription("Username")
                        .withFallbackKeys("user");
        Option<String> password =
                Options.key("password").stringType().noDefaultValue().withDescription("Password");
        Option<String> token =
                Options.key("token").stringType().noDefaultValue().withDescription("Access token");

        OptionRule optionRule =
                OptionRule.builder()
                        .optional(authMode)
                        .bundled(username, password)
                        .conditional(
                                authMode, Arrays.asList(AuthMode.PASSWORD, AuthMode.TOKEN), token)
                        .build();

        OptionRuleResponse response =
                service.buildResponse(
                        PluginIdentifier.of("seatunnel", "source", "CustomSource"), optionRule);

        OptionRuleResponse.OptionMetadata authModeMetadata =
                response.getOptionRule().getOptionalOptions().stream()
                        .filter(option -> "auth.mode".equals(option.getKey()))
                        .findFirst()
                        .orElseThrow(AssertionError::new);
        assertEquals(2, authModeMetadata.getOptionValues().size());

        OptionRuleResponse.RequiredOptionMetadata bundledRule =
                response.getOptionRule().getRequiredOptions().stream()
                        .filter(
                                option ->
                                        option.getRuleType() == OptionRuleResponse.RuleType.BUNDLED)
                        .findFirst()
                        .orElseThrow(AssertionError::new);
        assertTrue(
                bundledRule.getOptions().stream()
                        .anyMatch(option -> option.getFallbackKeys().contains("user")));

        OptionRuleResponse.RequiredOptionMetadata conditionalRule =
                response.getOptionRule().getRequiredOptions().stream()
                        .filter(
                                option ->
                                        option.getRuleType()
                                                == OptionRuleResponse.RuleType.CONDITIONAL)
                        .findFirst()
                        .orElseThrow(AssertionError::new);
        assertEquals(
                OptionRuleResponse.LogicalOperator.OR,
                conditionalRule.getExpressionTree().getOperator());
        assertNotNull(conditionalRule.getExpressionTree().getNext());
        assertTrue(response.getOptionRule().getConditionRules().isEmpty());
    }

    @Test
    void shouldPreserveNestedConditionRuleMetadata() {
        SingleChoiceOption<AuthMode> authMode =
                Options.key("auth.mode")
                        .singleChoice(
                                AuthMode.class, Arrays.asList(AuthMode.PASSWORD, AuthMode.TOKEN))
                        .defaultValue(AuthMode.PASSWORD)
                        .withDescription("Authentication mode");
        Option<String> username =
                Options.key("username").stringType().noDefaultValue().withDescription("Username");
        Option<String> password =
                Options.key("password").stringType().noDefaultValue().withDescription("Password");

        OptionRule adminRule = OptionRule.builder().required(password).build();
        OptionRule passwordAuthRule =
                OptionRule.builder()
                        .required(username)
                        .conditionalRule(username, "admin", adminRule)
                        .build();
        OptionRule optionRule =
                OptionRule.builder()
                        .optional(authMode)
                        .conditionalRule(authMode, AuthMode.PASSWORD, passwordAuthRule)
                        .build();

        OptionRuleResponse response =
                service.buildResponse(
                        PluginIdentifier.of("seatunnel", "source", "NestedSource"), optionRule);

        assertTrue(response.getOptionRule().getRequiredOptions().isEmpty());
        assertEquals(1, response.getOptionRule().getConditionRules().size());

        OptionRuleResponse.ConditionRuleMetadata rootConditionRule =
                response.getOptionRule().getConditionRules().get(0);
        assertEquals("'auth.mode' == PASSWORD", rootConditionRule.getExpression());
        assertNotNull(rootConditionRule.getExpressionTree());
        assertEquals(
                "username",
                rootConditionRule
                        .getOptionRule()
                        .getRequiredOptions()
                        .get(0)
                        .getOptions()
                        .get(0)
                        .getKey());

        assertEquals(1, rootConditionRule.getOptionRule().getConditionRules().size());
        OptionRuleResponse.ConditionRuleMetadata nestedConditionRule =
                rootConditionRule.getOptionRule().getConditionRules().get(0);
        assertEquals("'username' == admin", nestedConditionRule.getExpression());
        assertNotNull(nestedConditionRule.getExpressionTree());
        assertEquals(
                "password",
                nestedConditionRule
                        .getOptionRule()
                        .getRequiredOptions()
                        .get(0)
                        .getOptions()
                        .get(0)
                        .getKey());
        assertTrue(nestedConditionRule.getOptionRule().getConditionRules().isEmpty());
    }

    @Test
    void shouldClearCachedOptionRules() {
        OptionRuleResponse cachedResponse = service.getOptionRules("source", "FakeSource");
        OptionRuleResponse sameCachedResponse = service.getOptionRules("source", "FakeSource");

        assertSame(cachedResponse, sameCachedResponse);

        service.clearCache();

        OptionRuleResponse refreshedResponse = service.getOptionRules("source", "FakeSource");
        assertNotSame(cachedResponse, refreshedResponse);
        assertEquals("FakeSource", refreshedResponse.getPluginName());
    }

    @Test
    void shouldReturnConsistentResponsesForConcurrentRequests() throws Exception {
        int threadCount = 8;
        ExecutorService executorService = Executors.newFixedThreadPool(threadCount);
        CountDownLatch ready = new CountDownLatch(threadCount);
        CountDownLatch start = new CountDownLatch(1);
        List<Future<OptionRuleResponse>> futures = new ArrayList<>();
        try {
            for (int i = 0; i < threadCount; i++) {
                futures.add(
                        executorService.submit(
                                () -> {
                                    ready.countDown();
                                    assertTrue(start.await(10, TimeUnit.SECONDS));
                                    return service.getOptionRules("source", "FakeSource");
                                }));
            }

            assertTrue(ready.await(10, TimeUnit.SECONDS));
            start.countDown();

            OptionRuleResponse firstResponse = futures.get(0).get(30, TimeUnit.SECONDS);
            for (Future<OptionRuleResponse> future : futures) {
                assertSame(firstResponse, future.get(30, TimeUnit.SECONDS));
            }
        } finally {
            executorService.shutdownNow();
        }
    }

    @Test
    void shouldRejectInvalidType() {
        IllegalArgumentException error =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> service.getOptionRules("transform", "Replace"));
        assertTrue(error.getMessage().contains("Unsupported plugin type"));
    }

    @Test
    void shouldRejectBlankPluginName() {
        IllegalArgumentException error =
                assertThrows(
                        IllegalArgumentException.class,
                        () -> service.getOptionRules("source", " "));
        assertTrue(error.getMessage().contains("Parameter 'plugin' cannot be empty"));
    }

    @Test
    void shouldThrowWhenPluginDoesNotExist() {
        assertThrows(
                NoSuchElementException.class,
                () -> service.getOptionRules("source", "MissingPlugin"));
    }

    private enum AuthMode {
        PASSWORD,
        TOKEN
    }
}
