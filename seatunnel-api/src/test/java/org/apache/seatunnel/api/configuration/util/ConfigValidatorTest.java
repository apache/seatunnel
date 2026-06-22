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

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.type.TypeReference;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.OptionTest;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.seatunnel.api.configuration.OptionTest.TEST_MODE;
import static org.apache.seatunnel.api.configuration.util.Conditions.contains;
import static org.apache.seatunnel.api.configuration.util.Conditions.greaterOrEqual;
import static org.apache.seatunnel.api.configuration.util.Conditions.greaterOrEqualField;
import static org.apache.seatunnel.api.configuration.util.Conditions.greaterThan;
import static org.apache.seatunnel.api.configuration.util.Conditions.greaterThanField;
import static org.apache.seatunnel.api.configuration.util.Conditions.lessOrEqual;
import static org.apache.seatunnel.api.configuration.util.Conditions.lessOrEqualField;
import static org.apache.seatunnel.api.configuration.util.Conditions.lessThan;
import static org.apache.seatunnel.api.configuration.util.Conditions.lessThanField;
import static org.apache.seatunnel.api.configuration.util.Conditions.lowerCase;
import static org.apache.seatunnel.api.configuration.util.Conditions.mapContainsKey;
import static org.apache.seatunnel.api.configuration.util.Conditions.mapContainsKeys;
import static org.apache.seatunnel.api.configuration.util.Conditions.mapNotEmpty;
import static org.apache.seatunnel.api.configuration.util.Conditions.matches;
import static org.apache.seatunnel.api.configuration.util.Conditions.notBlank;
import static org.apache.seatunnel.api.configuration.util.Conditions.notEmpty;
import static org.apache.seatunnel.api.configuration.util.Conditions.startsWith;
import static org.apache.seatunnel.api.configuration.util.Conditions.unique;
import static org.apache.seatunnel.api.configuration.util.Conditions.upperCase;
import static org.apache.seatunnel.api.configuration.util.OptionRuleTest.TEST_PORTS;
import static org.apache.seatunnel.api.configuration.util.OptionRuleTest.TEST_TIMESTAMP;
import static org.apache.seatunnel.api.configuration.util.OptionRuleTest.TEST_TOPIC;
import static org.apache.seatunnel.api.configuration.util.OptionRuleTest.TEST_TOPIC_PATTERN;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class ConfigValidatorTest {
    public static final Option<String> KEY_USERNAME =
            Options.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("username of the Neo4j");

    public static final Option<String> KEY_PASSWORD =
            Options.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("password of the Neo4j");

    public static final Option<String> KEY_BEARER_TOKEN =
            Options.key("bearer-token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("base64 encoded bearer token of the Neo4j. for Auth.");

    public static final Option<String> KEY_KERBEROS_TICKET =
            Options.key("kerberos-ticket")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("base64 encoded kerberos ticket of the Neo4j. for Auth.");

    public static final Option<String> SINGLE_CHOICE_TEST =
            Options.key("single_choice_test")
                    .singleChoice(String.class, Arrays.asList("A", "B", "C"))
                    .defaultValue("M")
                    .withDescription("test single choice error");

    public static final Option<String> SINGLE_CHOICE_VALUE_TEST =
            Options.key("single_choice_test")
                    .singleChoice(String.class, Arrays.asList("A", "B", "C"))
                    .defaultValue("A")
                    .withDescription("test single choice value");

    void validate(Map<String, Object> config, OptionRule rule) {
        ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(rule);
    }

    @Test
    public void testAbsolutelyRequiredOption() {
        OptionRule rule =
                OptionRule.builder().required(TEST_PORTS, KEY_USERNAME, KEY_PASSWORD).build();
        Map<String, Object> config = new HashMap<>();
        Executable executable = () -> validate(config, rule);

        // absent
        config.put(TEST_PORTS.key(), "[9090]");
        String msg = assertThrows(OptionValidationException.class, executable).getMessage();
        Assertions.assertTrue(msg.contains("1 error)"));
        Assertions.assertTrue(msg.contains("[1] option: 'username', 'password'"));
        Assertions.assertTrue(msg.contains("constraint: required option is not configured"));

        config.put(KEY_USERNAME.key(), "asuka");
        msg = assertThrows(OptionValidationException.class, executable).getMessage();
        Assertions.assertTrue(msg.contains("[1] option: 'password'"));
        Assertions.assertTrue(msg.contains("constraint: required option is not configured"));

        // all present
        config.put(KEY_PASSWORD.key(), "saitou");
        Assertions.assertDoesNotThrow(executable);
    }

    @Test
    public void testBundledRequiredOptions() {
        OptionRule rule = OptionRule.builder().bundled(KEY_USERNAME, KEY_PASSWORD).build();
        Map<String, Object> config = new HashMap<>();
        Executable executable = () -> validate(config, rule);

        // case1: all absent
        Assertions.assertDoesNotThrow(executable);

        // case2: some present
        config.put(KEY_USERNAME.key(), "asuka");
        String msg = assertThrows(OptionValidationException.class, executable).getMessage();
        Assertions.assertTrue(msg.contains("[1] options: 'username', 'password'"));
        Assertions.assertTrue(msg.contains("bundled options must be present or absent together"));
        Assertions.assertTrue(msg.contains("present: ['username']"));
        Assertions.assertTrue(msg.contains("absent: ['password']"));

        // case3: all present
        config.put(KEY_PASSWORD.key(), "saitou");
        Assertions.assertDoesNotThrow(executable);
    }

    @Test
    public void testSimpleExclusiveRequiredOptions() {
        OptionRule rule = OptionRule.builder().exclusive(TEST_TOPIC_PATTERN, TEST_TOPIC).build();
        Map<String, Object> config = new HashMap<>();
        Executable executable = () -> validate(config, rule);

        // all absent
        String msg = assertThrows(OptionValidationException.class, executable).getMessage();
        Assertions.assertTrue(msg.contains("'option.topic-pattern', 'option.topic'"));
        Assertions.assertTrue(
                msg.contains("exactly one option must be set, but none are configured"));

        // only one present
        config.put(TEST_TOPIC_PATTERN.key(), "asuka");
        Assertions.assertDoesNotThrow(executable);

        // present > 1
        config.put(TEST_TOPIC.key(), "[\"saitou\"]");
        msg = assertThrows(OptionValidationException.class, executable).getMessage();
        Assertions.assertTrue(msg.contains("mutually exclusive, but multiple are set"));
    }

    @Test
    public void testComplexExclusiveRequiredOptions() {
        OptionRule rule =
                OptionRule.builder().exclusive(KEY_BEARER_TOKEN, KEY_KERBEROS_TICKET).build();

        Map<String, Object> config = new HashMap<>();
        Executable executable = () -> validate(config, rule);

        // all absent
        String msg = assertThrows(OptionValidationException.class, executable).getMessage();
        Assertions.assertTrue(msg.contains("'bearer-token', 'kerberos-ticket'"));
        Assertions.assertTrue(
                msg.contains("exactly one option must be set, but none are configured"));

        // set one
        config.put(KEY_BEARER_TOKEN.key(), "ashulin");
        Assertions.assertDoesNotThrow(executable);

        // all set
        config.put(KEY_KERBEROS_TICKET.key(), "zongwen");
        msg = assertThrows(OptionValidationException.class, executable).getMessage();
        Assertions.assertTrue(msg.contains("mutually exclusive, but multiple are set"));
    }

    @Test
    public void testSimpleConditionalRequiredOptionsWithDefaultValue() {
        OptionRule rule =
                OptionRule.builder()
                        .optional(TEST_MODE)
                        .conditional(TEST_MODE, OptionTest.TestMode.TIMESTAMP, TEST_TIMESTAMP)
                        .build();
        Map<String, Object> config = new HashMap<>();
        Executable executable = () -> validate(config, rule);

        // Expression mismatch
        Assertions.assertDoesNotThrow(executable);

        // Expression match, and required options absent
        config.put(TEST_MODE.key(), "timestamp");
        String msg = assertThrows(OptionValidationException.class, executable).getMessage();
        Assertions.assertTrue(msg.contains("option: 'option.timestamp'"));
        Assertions.assertTrue(
                msg.contains("required because ['option.mode' == TIMESTAMP] is true"));

        // Expression match, and required options all present
        config.put(TEST_TIMESTAMP.key(), "564231238596789");
        Assertions.assertDoesNotThrow(executable);

        // Expression mismatch
        config.put(TEST_MODE.key(), "EARLIEST");
        Assertions.assertDoesNotThrow(executable);
    }

    @Test
    public void testSimpleConditionalRequiredOptionsWithoutDefaultValue() {
        OptionRule rule =
                OptionRule.builder()
                        .optional(KEY_USERNAME)
                        .conditional(KEY_USERNAME, "ashulin", TEST_TIMESTAMP)
                        .build();
        Map<String, Object> config = new HashMap<>();
        Executable executable = () -> validate(config, rule);

        // Expression mismatch
        Assertions.assertDoesNotThrow(executable);

        // Expression match, and required options absent
        config.put(KEY_USERNAME.key(), "ashulin");
        String msg = assertThrows(OptionValidationException.class, executable).getMessage();
        Assertions.assertTrue(msg.contains("option: 'option.timestamp'"));
        Assertions.assertTrue(msg.contains("required because ['username' == ashulin] is true"));

        // Expression match, and required options all present
        config.put(TEST_TIMESTAMP.key(), "564231238596789");
        Assertions.assertDoesNotThrow(executable);

        // Expression mismatch
        config.put(KEY_USERNAME.key(), "asuka");
        Assertions.assertDoesNotThrow(executable);
    }

    @Test
    public void testComplexConditionalRequiredOptions() {
        OptionRule rule =
                OptionRule.builder()
                        .optional(KEY_USERNAME)
                        .conditional(
                                KEY_USERNAME, Arrays.asList("ashulin", "asuka"), TEST_TIMESTAMP)
                        .build();
        Map<String, Object> config = new HashMap<>();
        Executable executable = () -> validate(config, rule);

        // Expression mismatch
        Assertions.assertDoesNotThrow(executable);

        // 'username' == ashulin, and required options absent
        config.put(KEY_USERNAME.key(), "ashulin");
        String msg = assertThrows(OptionValidationException.class, executable).getMessage();
        Assertions.assertTrue(msg.contains("option: 'option.timestamp'"));
        Assertions.assertTrue(
                msg.contains(
                        "required because ['username' == ashulin || 'username' == asuka] is true"));

        // 'username' == asuka, and required options absent
        config.put(KEY_USERNAME.key(), "asuka");
        msg = assertThrows(OptionValidationException.class, executable).getMessage();
        Assertions.assertTrue(msg.contains("option: 'option.timestamp'"));
        Assertions.assertTrue(
                msg.contains(
                        "required because ['username' == ashulin || 'username' == asuka] is true"));

        // Expression match, and required options all present
        config.put(TEST_TIMESTAMP.key(), "564231238596789");
        Assertions.assertDoesNotThrow(executable);

        // Expression mismatch
        config.put(KEY_USERNAME.key(), "asuka111");
        Assertions.assertDoesNotThrow(executable);
    }

    @Test
    public void testSingleChoiceOptionDefaultValueValidator() {
        OptionRule optionRule = OptionRule.builder().required(SINGLE_CHOICE_TEST).build();
        Map<String, Object> config = new HashMap<>();
        config.put(SINGLE_CHOICE_TEST.key(), "A");
        Executable executable = () -> validate(config, optionRule);
        OptionValidationException ex = assertThrows(OptionValidationException.class, executable);
        String msg = ex.getMessage();
        Assertions.assertTrue(
                msg.contains("single_choice_test"), "Should mention option key: " + msg);
        Assertions.assertTrue(
                msg.contains("defaultValue(M) must be one of"),
                "Should mention invalid defaultValue: " + msg);
    }

    @Test
    public void testSingleChoiceOptionValueValidator() {
        OptionRule optionRule = OptionRule.builder().required(SINGLE_CHOICE_VALUE_TEST).build();
        Map<String, Object> config = new HashMap<>();
        config.put(SINGLE_CHOICE_VALUE_TEST.key(), "A");
        Executable executable = () -> validate(config, optionRule);
        Assertions.assertDoesNotThrow(executable);

        config.put(SINGLE_CHOICE_VALUE_TEST.key(), "N");
        executable = () -> validate(config, optionRule);
        OptionValidationException ex = assertThrows(OptionValidationException.class, executable);
        String msg = ex.getMessage();
        Assertions.assertTrue(
                msg.contains("single_choice_test"), "Should mention option key: " + msg);
        Assertions.assertTrue(
                msg.contains("value(N) must be one of"), "Should mention invalid value: " + msg);
    }

    @Test
    public void testNestedOptionRule() {
        Option<String> test_key =
                Options.key("test_key").stringType().noDefaultValue().withDescription("for test");
        OptionRule adminUserOption = OptionRule.builder().required(test_key).build();
        OptionRule subOption1 =
                OptionRule.builder()
                        .required(KEY_USERNAME, KEY_PASSWORD)
                        .conditionalRule(KEY_USERNAME, "admin", adminUserOption)
                        .build();
        OptionRule subOption2 = OptionRule.builder().required(KEY_BEARER_TOKEN).build();

        // the final rule is :
        // key_kerberos_ticket is required
        // single_choice_test is optional (the default value is A)
        // when single_choice_test == A, username and password are required, and when username ==
        // admin, test_key is required
        // when single_choice_test == B, bearer_token is required
        // when single_choice_test == C, no extra options are required
        OptionRule optionRule =
                OptionRule.builder()
                        .required(KEY_KERBEROS_TICKET)
                        .optional(SINGLE_CHOICE_VALUE_TEST)
                        .conditionalRule(SINGLE_CHOICE_VALUE_TEST, "A", subOption1)
                        .conditionalRule(SINGLE_CHOICE_VALUE_TEST, "B", subOption2)
                        .build();

        Map<String, Object> config = new HashMap<>();
        config.put(KEY_KERBEROS_TICKET.key(), "A");
        config.put(SINGLE_CHOICE_VALUE_TEST.key(), "C");
        Executable executable = () -> validate(config, optionRule);
        Assertions.assertDoesNotThrow(executable);

        config.put(SINGLE_CHOICE_VALUE_TEST.key(), "A");
        executable = () -> validate(config, optionRule);
        String msg = assertThrows(OptionValidationException.class, executable).getMessage();
        Assertions.assertTrue(msg.contains("option: 'username', 'password'"));
        Assertions.assertTrue(msg.contains("type: required"));
        Assertions.assertTrue(
                msg.contains("required option is not configured when ['single_choice_test' == A]"));

        config.put(KEY_USERNAME.key(), "root");
        config.put(KEY_PASSWORD.key(), "111");
        executable = () -> validate(config, optionRule);
        Assertions.assertDoesNotThrow(executable);

        config.put(KEY_USERNAME.key(), "admin");
        executable = () -> validate(config, optionRule);
        msg = assertThrows(OptionValidationException.class, executable).getMessage();
        Assertions.assertTrue(msg.contains("option: 'test_key'"));
        Assertions.assertTrue(msg.contains("type: required"));
        Assertions.assertTrue(
                msg.contains("required option is not configured when ['username' == admin]"));

        config.put(test_key.key(), "111");
        executable = () -> validate(config, optionRule);
        Assertions.assertDoesNotThrow(executable);
    }

    @Test
    public void testEmptyNestedOption() {
        OptionRule emptyRule = OptionRule.builder().build();
        Executable executable =
                () ->
                        OptionRule.builder()
                                .optional(SINGLE_CHOICE_VALUE_TEST)
                                .conditionalRule(SINGLE_CHOICE_VALUE_TEST, "A", emptyRule)
                                .build();
        assertEquals(
                "ErrorCode:[API-02], ErrorDescription:[Option item validate failed] - conditional option rule for 'single_choice_test' must have options.",
                assertThrows(OptionValidationException.class, executable).getMessage());
    }

    @Test
    public void testDuplicatedNestedOption() {
        OptionRule subOption1 = OptionRule.builder().required(KEY_USERNAME).build();
        OptionRule subOption2 = OptionRule.builder().required(KEY_PASSWORD).build();
        Executable executable =
                () ->
                        OptionRule.builder()
                                .required(KEY_KERBEROS_TICKET)
                                .optional(SINGLE_CHOICE_VALUE_TEST)
                                .conditionalRule(SINGLE_CHOICE_VALUE_TEST, "A", subOption1)
                                .conditionalRule(SINGLE_CHOICE_VALUE_TEST, "A", subOption2)
                                .build();
        assertEquals(
                "ErrorCode:[API-02], ErrorDescription:[Option item validate failed] - conditional option rule for 'single_choice_test' with expression ''single_choice_test' == A' already exists.",
                assertThrows(OptionValidationException.class, executable).getMessage());
    }

    @Test
    public void testMultipleValueNestedRule() {
        OptionRule subOption1 = OptionRule.builder().required(KEY_USERNAME, KEY_PASSWORD).build();
        OptionRule optionRule =
                OptionRule.builder()
                        .optional(SINGLE_CHOICE_VALUE_TEST)
                        .conditionalRule(
                                SINGLE_CHOICE_VALUE_TEST, Arrays.asList("A", "B"), subOption1)
                        .build();

        Map<String, Object> config = new HashMap<>();
        config.put(KEY_KERBEROS_TICKET.key(), "A");
        config.put(SINGLE_CHOICE_VALUE_TEST.key(), "B");
        Executable executable = () -> validate(config, optionRule);
        String msg = assertThrows(OptionValidationException.class, executable).getMessage();
        Assertions.assertTrue(msg.contains("option: 'username', 'password'"));
        Assertions.assertTrue(msg.contains("type: required"));
        Assertions.assertTrue(
                msg.contains(
                        "required option is not configured when ['single_choice_test' == A || 'single_choice_test' == B]"));

        config.put(SINGLE_CHOICE_VALUE_TEST.key(), "B");
        executable = () -> validate(config, optionRule);
        msg = assertThrows(OptionValidationException.class, executable).getMessage();
        Assertions.assertTrue(msg.contains("option: 'username', 'password'"));
        Assertions.assertTrue(msg.contains("type: required"));
        Assertions.assertTrue(
                msg.contains(
                        "required option is not configured when ['single_choice_test' == A || 'single_choice_test' == B]"));
    }

    // ==================== Validation Rule Tests ====================

    public static final Option<Integer> PORT =
            Options.key("port").intType().noDefaultValue().withDescription("port number");

    public static final Option<Double> RATIO =
            Options.key("ratio").doubleType().noDefaultValue().withDescription("ratio");

    public static final Option<String> HOST =
            Options.key("host").stringType().noDefaultValue().withDescription("host name");

    public static final Option<String> ENDPOINT =
            Options.key("endpoint").stringType().noDefaultValue().withDescription("endpoint");

    public static final Option<String> DB_NAME =
            Options.key("db_name").stringType().noDefaultValue().withDescription("database name");

    public static final Option<Long> START_TS =
            Options.key("start_ts").longType().noDefaultValue().withDescription("start timestamp");

    public static final Option<Long> END_TS =
            Options.key("end_ts").longType().noDefaultValue().withDescription("end timestamp");

    public static final Option<Boolean> ENABLE_TX =
            Options.key("enable_tx")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("enable transaction");

    public static final Option<String> FILE_EXPR =
            Options.key("file_expr")
                    .stringType()
                    .defaultValue("default")
                    .withDescription("file name expression");

    public static final Option<String> MODE =
            Options.key("mode").stringType().defaultValue("batch").withDescription("run mode");

    public static final Option<List<String>> TAGS =
            Options.key("tags").listType().noDefaultValue().withDescription("tag list");

    @Test
    public void testGreaterThanValidation() {
        OptionRule rule = OptionRule.builder().required(PORT, greaterThan(PORT, 0)).build();

        Map<String, Object> config = new HashMap<>();
        config.put(PORT.key(), 8080);
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(PORT.key(), 0);
        assertThrows(OptionValidationException.class, () -> validate(config, rule));

        config.put(PORT.key(), -1);
        assertThrows(OptionValidationException.class, () -> validate(config, rule));
    }

    @Test
    public void testGreaterOrEqualValidation() {
        OptionRule rule = OptionRule.builder().required(PORT, greaterOrEqual(PORT, 0)).build();

        Map<String, Object> config = new HashMap<>();
        config.put(PORT.key(), 0);
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(PORT.key(), 100);
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(PORT.key(), -1);
        assertThrows(OptionValidationException.class, () -> validate(config, rule));
    }

    @Test
    public void testRangeValidation() {
        OptionRule rule =
                OptionRule.builder()
                        .required(PORT, greaterOrEqual(PORT, 1).and(lessOrEqual(PORT, 65535)))
                        .build();

        Map<String, Object> config = new HashMap<>();
        config.put(PORT.key(), 1);
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(PORT.key(), 65535);
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(PORT.key(), 8080);
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(PORT.key(), 0);
        assertThrows(OptionValidationException.class, () -> validate(config, rule));

        config.put(PORT.key(), 65536);
        assertThrows(OptionValidationException.class, () -> validate(config, rule));
    }

    @Test
    public void testHalfOpenIntervalValidation() {
        OptionRule rule =
                OptionRule.builder()
                        .required(RATIO, greaterThan(RATIO, 0.0).and(lessOrEqual(RATIO, 1.0)))
                        .build();

        Map<String, Object> config = new HashMap<>();
        config.put(RATIO.key(), 0.5);
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(RATIO.key(), 1.0);
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(RATIO.key(), 0.0);
        assertThrows(OptionValidationException.class, () -> validate(config, rule));

        config.put(RATIO.key(), 1.1);
        assertThrows(OptionValidationException.class, () -> validate(config, rule));
    }

    @Test
    public void testNotBlankValidation() {
        OptionRule rule = OptionRule.builder().required(HOST, notBlank(HOST)).build();

        Map<String, Object> config = new HashMap<>();
        config.put(HOST.key(), "localhost");
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(HOST.key(), "");
        assertThrows(OptionValidationException.class, () -> validate(config, rule));

        config.put(HOST.key(), "   ");
        assertThrows(OptionValidationException.class, () -> validate(config, rule));
    }

    @Test
    public void testStartsWithValidation() {
        OptionRule rule =
                OptionRule.builder()
                        .required(ENDPOINT, startsWith(ENDPOINT, "jdbc:databend://"))
                        .build();

        Map<String, Object> config = new HashMap<>();
        config.put(ENDPOINT.key(), "jdbc:databend://localhost:8123");
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(ENDPOINT.key(), "jdbc:mysql://localhost:3306");
        assertThrows(OptionValidationException.class, () -> validate(config, rule));
    }

    @Test
    public void testContainsValidation() {
        OptionRule rule =
                OptionRule.builder()
                        .optional(FILE_EXPR, contains(FILE_EXPR, "#{transactionId}"))
                        .build();

        Map<String, Object> config = new HashMap<>();
        config.put(FILE_EXPR.key(), "data_#{transactionId}.csv");
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(FILE_EXPR.key(), "data_output.csv");
        assertThrows(OptionValidationException.class, () -> validate(config, rule));
    }

    @Test
    public void testMatchesValidation() {
        OptionRule rule =
                OptionRule.builder().required(ENDPOINT, matches(ENDPOINT, "^[^:]+:\\d+$")).build();

        Map<String, Object> config = new HashMap<>();
        config.put(ENDPOINT.key(), "localhost:8080");
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(ENDPOINT.key(), "invalid-format");
        assertThrows(OptionValidationException.class, () -> validate(config, rule));
    }

    @Test
    public void testUpperCaseValidation() {
        OptionRule rule = OptionRule.builder().required(DB_NAME, upperCase(DB_NAME)).build();

        Map<String, Object> config = new HashMap<>();
        config.put(DB_NAME.key(), "ORACLE_DB");
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(DB_NAME.key(), "Oracle_DB");
        assertThrows(OptionValidationException.class, () -> validate(config, rule));
    }

    @Test
    public void testLowerCaseValidation() {
        OptionRule rule = OptionRule.builder().required(DB_NAME, lowerCase(DB_NAME)).build();

        Map<String, Object> config = new HashMap<>();
        config.put(DB_NAME.key(), "my_database");
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(DB_NAME.key(), "My_Database");
        assertThrows(OptionValidationException.class, () -> validate(config, rule));
    }

    @Test
    public void testCrossFieldComparison() {
        OptionRule rule =
                OptionRule.builder()
                        .required(START_TS, END_TS, lessThanField(START_TS, END_TS))
                        .build();

        Map<String, Object> config = new HashMap<>();
        config.put(START_TS.key(), 100L);
        config.put(END_TS.key(), 200L);
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(START_TS.key(), 200L);
        config.put(END_TS.key(), 100L);
        assertThrows(OptionValidationException.class, () -> validate(config, rule));

        config.put(START_TS.key(), 100L);
        config.put(END_TS.key(), 100L);
        assertThrows(OptionValidationException.class, () -> validate(config, rule));
    }

    @Test
    public void testCrossFieldLessOrEqual() {
        OptionRule rule =
                OptionRule.builder()
                        .required(START_TS, END_TS, lessOrEqualField(START_TS, END_TS))
                        .build();

        Map<String, Object> config = new HashMap<>();
        config.put(START_TS.key(), 100L);
        config.put(END_TS.key(), 100L);
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(START_TS.key(), 50L);
        config.put(END_TS.key(), 100L);
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(START_TS.key(), 200L);
        config.put(END_TS.key(), 100L);
        assertThrows(OptionValidationException.class, () -> validate(config, rule));
    }

    @Test
    public void testNotEmptyCollectionValidation() {
        OptionRule rule = OptionRule.builder().required(TAGS, notEmpty(TAGS)).build();

        Map<String, Object> config = new HashMap<>();
        config.put(TAGS.key(), Arrays.asList("tag1", "tag2"));
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(TAGS.key(), Collections.emptyList());
        assertThrows(OptionValidationException.class, () -> validate(config, rule));
    }

    @Test
    public void testUniqueCollectionValidation() {
        OptionRule rule = OptionRule.builder().required(TAGS, unique(TAGS)).build();

        Map<String, Object> config = new HashMap<>();
        config.put(TAGS.key(), Arrays.asList("a", "b", "c"));
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(TAGS.key(), Arrays.asList("a", "b", "a"));
        assertThrows(OptionValidationException.class, () -> validate(config, rule));
    }

    @Test
    public void testOrChainAtLeastOneNotBlank() {
        OptionRule rule =
                OptionRule.builder()
                        .optional(HOST, notBlank(HOST).or(notBlank(ENDPOINT)))
                        .optional(ENDPOINT)
                        .build();

        Map<String, Object> config = new HashMap<>();
        config.put(HOST.key(), "localhost");
        config.put(ENDPOINT.key(), "");
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(HOST.key(), "");
        config.put(ENDPOINT.key(), "my-endpoint");
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(HOST.key(), "");
        config.put(ENDPOINT.key(), "");
        assertThrows(OptionValidationException.class, () -> validate(config, rule));
    }

    @Test
    public void testValidationSkippedForAbsentOptional() {
        OptionRule rule =
                OptionRule.builder().optional(ENDPOINT, matches(ENDPOINT, "^[^:]+:\\d+$")).build();

        Map<String, Object> config = new HashMap<>();
        Assertions.assertDoesNotThrow(() -> validate(config, rule));
    }

    @Test
    public void testConditionToString() {
        assertEquals("'port' > 0", greaterThan(PORT, 0).toString());
        assertEquals(
                "'port' >= 1 && 'port' <= 65535",
                greaterOrEqual(PORT, 1).and(lessOrEqual(PORT, 65535)).toString());
        assertEquals("'host' is not blank", notBlank(HOST).toString());
        assertEquals("'start_ts' < 'end_ts'", lessThanField(START_TS, END_TS).toString());
        assertEquals("'db_name' is uppercase", upperCase(DB_NAME).toString());
        assertEquals("'tags' has unique elements", unique(TAGS).toString());
    }

    @Test
    public void testMultipleValidationRules() {
        OptionRule rule =
                OptionRule.builder()
                        .required(PORT, greaterOrEqual(PORT, 1).and(lessOrEqual(PORT, 65535)))
                        .required(HOST, notBlank(HOST))
                        .required(DB_NAME, upperCase(DB_NAME))
                        .build();

        Map<String, Object> config = new HashMap<>();
        config.put(PORT.key(), 8080);
        config.put(HOST.key(), "localhost");
        config.put(DB_NAME.key(), "ORACLE");
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(DB_NAME.key(), "oracle");
        assertThrows(OptionValidationException.class, () -> validate(config, rule));
    }

    @Test
    public void testBackwardCompatibility() {
        OptionRule rule =
                OptionRule.builder()
                        .optional(OptionTest.TEST_MODE)
                        .conditional(
                                OptionTest.TEST_MODE, OptionTest.TestMode.TIMESTAMP, TEST_TIMESTAMP)
                        .build();
        Map<String, Object> config = new HashMap<>();
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(OptionTest.TEST_MODE.key(), "timestamp");
        config.put(TEST_TIMESTAMP.key(), "564231238596789");
        Assertions.assertDoesNotThrow(() -> validate(config, rule));
    }

    @Test
    public void testNotEqualOperator() {
        OptionRule rule =
                OptionRule.builder()
                        .required(HOST, Condition.of(HOST, ConditionOperator.NOT_EQUAL, ""))
                        .build();

        Map<String, Object> config = new HashMap<>();
        config.put(HOST.key(), "localhost");
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(HOST.key(), "");
        assertThrows(OptionValidationException.class, () -> validate(config, rule));
    }

    // ==================== Missing Operator Coverage ====================

    @Test
    public void testLessThanValidation() {
        OptionRule rule = OptionRule.builder().required(PORT, lessThan(PORT, 100)).build();

        Map<String, Object> config = new HashMap<>();
        config.put(PORT.key(), 50);
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(PORT.key(), 99);
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(PORT.key(), 100);
        assertThrows(OptionValidationException.class, () -> validate(config, rule));

        config.put(PORT.key(), 200);
        assertThrows(OptionValidationException.class, () -> validate(config, rule));
    }

    @Test
    public void testFieldGreaterThanValidation() {
        OptionRule rule =
                OptionRule.builder()
                        .required(END_TS, START_TS, greaterThanField(END_TS, START_TS))
                        .build();

        Map<String, Object> config = new HashMap<>();
        config.put(END_TS.key(), 200L);
        config.put(START_TS.key(), 100L);
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(END_TS.key(), 100L);
        config.put(START_TS.key(), 100L);
        assertThrows(OptionValidationException.class, () -> validate(config, rule));

        config.put(END_TS.key(), 50L);
        config.put(START_TS.key(), 100L);
        assertThrows(OptionValidationException.class, () -> validate(config, rule));
    }

    @Test
    public void testFieldGreaterOrEqualValidation() {
        OptionRule rule =
                OptionRule.builder()
                        .required(END_TS, START_TS, greaterOrEqualField(END_TS, START_TS))
                        .build();

        Map<String, Object> config = new HashMap<>();
        config.put(END_TS.key(), 200L);
        config.put(START_TS.key(), 100L);
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(END_TS.key(), 100L);
        config.put(START_TS.key(), 100L);
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(END_TS.key(), 50L);
        config.put(START_TS.key(), 100L);
        assertThrows(OptionValidationException.class, () -> validate(config, rule));
    }

    // ==================== Builder Overload Coverage ====================

    @Test
    public void testConditionalWithValueConstraint() {
        OptionRule rule =
                OptionRule.builder()
                        .optional(TEST_MODE)
                        .conditional(
                                TEST_MODE,
                                OptionTest.TestMode.TIMESTAMP,
                                greaterThan(TEST_TIMESTAMP, 0L))
                        .build();

        Map<String, Object> config = new HashMap<>();
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(TEST_MODE.key(), "timestamp");
        config.put(TEST_TIMESTAMP.key(), 100L);
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(TEST_TIMESTAMP.key(), 0L);
        assertThrows(OptionValidationException.class, () -> validate(config, rule));

        config.put(TEST_TIMESTAMP.key(), -1L);
        assertThrows(OptionValidationException.class, () -> validate(config, rule));

        config.put(TEST_MODE.key(), "EARLIEST");
        config.put(TEST_TIMESTAMP.key(), -1L);
        Assertions.assertDoesNotThrow(() -> validate(config, rule));
    }

    @Test
    public void testConditionalWithMultiFieldConstraint() {
        OptionRule rule =
                OptionRule.builder()
                        .optional(ENABLE_TX)
                        .conditional(
                                ENABLE_TX, true, START_TS, END_TS, lessThanField(START_TS, END_TS))
                        .build();

        Map<String, Object> config = new HashMap<>();
        config.put(ENABLE_TX.key(), false);
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(ENABLE_TX.key(), true);
        config.put(START_TS.key(), 100L);
        config.put(END_TS.key(), 200L);
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(START_TS.key(), 300L);
        config.put(END_TS.key(), 200L);
        assertThrows(OptionValidationException.class, () -> validate(config, rule));
    }

    @Test
    public void testOptionalWithValueConstraint() {
        OptionRule rule = OptionRule.builder().optional(PORT, greaterOrEqual(PORT, 1)).build();

        Map<String, Object> config = new HashMap<>();
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(PORT.key(), 8080);
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(PORT.key(), 0);
        assertThrows(OptionValidationException.class, () -> validate(config, rule));
    }

    @Test
    public void testOptionalWithMultiFieldConstraint() {
        OptionRule rule =
                OptionRule.builder()
                        .optional(START_TS, END_TS, lessThanField(START_TS, END_TS))
                        .build();

        Map<String, Object> config = new HashMap<>();
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(START_TS.key(), 100L);
        config.put(END_TS.key(), 200L);
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(START_TS.key(), 200L);
        config.put(END_TS.key(), 100L);
        assertThrows(OptionValidationException.class, () -> validate(config, rule));
    }

    @Test
    public void testNotEmptyAndUniqueChain() {
        OptionRule rule =
                OptionRule.builder().required(TAGS, notEmpty(TAGS).and(unique(TAGS))).build();

        Map<String, Object> config = new HashMap<>();
        config.put(TAGS.key(), Arrays.asList("a", "b", "c"));
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(TAGS.key(), Collections.emptyList());
        assertThrows(OptionValidationException.class, () -> validate(config, rule));

        config.put(TAGS.key(), Arrays.asList("a", "a", "b"));
        assertThrows(OptionValidationException.class, () -> validate(config, rule));
    }

    // ==================== toString Coverage for All Operators ====================

    @Test
    public void testAllOperatorToString() {
        // Core operators
        assertEquals("'port' < 100", lessThan(PORT, 100).toString());
        assertEquals("'port' <= 100", lessOrEqual(PORT, 100).toString());
        assertEquals("'port' > 0", greaterThan(PORT, 0).toString());
        assertEquals("'port' >= 0", greaterOrEqual(PORT, 0).toString());
        assertEquals("'host' is not blank", notBlank(HOST).toString());
        assertEquals("'endpoint' starts with jdbc:", startsWith(ENDPOINT, "jdbc:").toString());
        assertEquals("'endpoint' matches ^\\d+$", matches(ENDPOINT, "^\\d+$").toString());
        assertEquals("'tags' is not empty", notEmpty(TAGS).toString());
        assertEquals("'tags' has unique elements", unique(TAGS).toString());

        // Extended operators
        assertEquals("'endpoint' contains ://", contains(ENDPOINT, "://").toString());
        assertEquals("'db_name' is uppercase", upperCase(DB_NAME).toString());
        assertEquals("'db_name' is lowercase", lowerCase(DB_NAME).toString());
        assertEquals("'start_ts' < 'end_ts'", lessThanField(START_TS, END_TS).toString());
        assertEquals("'start_ts' <= 'end_ts'", lessOrEqualField(START_TS, END_TS).toString());
        assertEquals("'end_ts' > 'start_ts'", greaterThanField(END_TS, START_TS).toString());
        assertEquals("'end_ts' >= 'start_ts'", greaterOrEqualField(END_TS, START_TS).toString());
    }

    @Test
    public void testCircularConditionChainDetected() {
        Condition<Integer> a = greaterThan(PORT, 0);
        assertThrows(IllegalArgumentException.class, () -> a.and(a));
    }

    @Test
    public void testCircularConditionChainIndirect() {
        Condition<Integer> a = greaterThan(PORT, 0);
        Condition<Integer> b = lessThan(PORT, 100);
        a.and(b);
        assertThrows(IllegalArgumentException.class, () -> b.and(a));
    }

    @Test
    public void testCircularConditionChainDuplicateAppend() {
        Condition<Integer> a = greaterThan(PORT, 0);
        Condition<Integer> b = lessThan(PORT, 100);
        a.and(b);
        assertThrows(IllegalArgumentException.class, () -> a.and(b));
    }

    @Test
    public void testNullOperatorRejected() {
        assertThrows(IllegalArgumentException.class, () -> Condition.of(PORT, null, 0));
    }

    @Test
    public void testFieldOperatorWithoutCompareOptionRejected() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new Condition<>(PORT, ConditionOperator.FIELD_LESS_THAN, null, null));
    }

    @Test
    public void testBinaryLiteralOperatorWithoutExpectValueRejected() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new Condition<>(PORT, ConditionOperator.GREATER_THAN, null, null));
    }

    @Test
    public void testUnknownKeysDoesNotRejectValueConstraintOptions() {
        OptionRule rule =
                OptionRule.builder()
                        .required(PORT, greaterOrEqual(PORT, 1))
                        .required(START_TS, END_TS, lessThanField(START_TS, END_TS))
                        .build();

        Map<String, Object> config = new HashMap<>();
        config.put(PORT.key(), 8080);
        config.put(START_TS.key(), 100L);
        config.put(END_TS.key(), 200L);

        Assertions.assertDoesNotThrow(
                () ->
                        ConfigValidator.validateUnknownKeys(
                                ReadonlyConfig.fromMap(config), rule, "TestConnector"));
    }

    @Test
    public void testUnknownKeysRejectsUndeclaredKey() {
        OptionRule rule = OptionRule.builder().required(PORT, greaterOrEqual(PORT, 1)).build();

        Map<String, Object> config = new HashMap<>();
        config.put(PORT.key(), 8080);
        config.put("typo_key", "oops");

        assertThrows(
                OptionValidationException.class,
                () ->
                        ConfigValidator.validateUnknownKeys(
                                ReadonlyConfig.fromMap(config), rule, "TestConnector"));
    }

    @Test
    public void testUnknownKeysRecognizesChainedConditionOptions() {
        OptionRule rule =
                OptionRule.builder()
                        .required(PORT, greaterOrEqual(PORT, 1).and(lessOrEqual(PORT, 65535)))
                        .build();

        Map<String, Object> config = new HashMap<>();
        config.put(PORT.key(), 443);

        Assertions.assertDoesNotThrow(
                () ->
                        ConfigValidator.validateUnknownKeys(
                                ReadonlyConfig.fromMap(config), rule, "TestConnector"));
    }

    @Test
    public void testLargeTimestampComparisonPrecision() {
        long tsA = (1L << 53) + 1;
        long tsB = (1L << 53) + 2;

        OptionRule rule =
                OptionRule.builder()
                        .required(START_TS, END_TS, lessThanField(START_TS, END_TS))
                        .build();

        Map<String, Object> config = new HashMap<>();
        config.put(START_TS.key(), tsA);
        config.put(END_TS.key(), tsB);
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(START_TS.key(), tsB);
        config.put(END_TS.key(), tsA);
        assertThrows(OptionValidationException.class, () -> validate(config, rule));
    }

    @Test
    public void testLargeLongLiteralComparison() {
        long bigValue = Long.MAX_VALUE - 1;

        OptionRule rule =
                OptionRule.builder().required(START_TS, lessThan(START_TS, Long.MAX_VALUE)).build();

        Map<String, Object> config = new HashMap<>();
        config.put(START_TS.key(), bigValue);
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(START_TS.key(), Long.MAX_VALUE);
        assertThrows(OptionValidationException.class, () -> validate(config, rule));
    }

    @Test
    public void testOptionalCrossFieldOnlyStartPresent() {
        OptionRule rule =
                OptionRule.builder()
                        .optional(START_TS, END_TS, lessThanField(START_TS, END_TS))
                        .build();

        Map<String, Object> config = new HashMap<>();
        config.put(START_TS.key(), 100L);
        Assertions.assertDoesNotThrow(() -> validate(config, rule));
    }

    @Test
    public void testOptionalCrossFieldOnlyEndPresent() {
        OptionRule rule =
                OptionRule.builder()
                        .optional(START_TS, END_TS, lessThanField(START_TS, END_TS))
                        .build();

        Map<String, Object> config = new HashMap<>();
        config.put(END_TS.key(), 200L);
        Assertions.assertDoesNotThrow(() -> validate(config, rule));
    }

    @Test
    public void testOrChainNumericNullWithStringFallback() {
        OptionRule rule =
                OptionRule.builder()
                        .optional(PORT, greaterOrEqual(PORT, 1).or(notBlank(HOST)))
                        .optional(HOST)
                        .build();

        // PORT absent, HOST present -> or chain should pass
        Map<String, Object> config = new HashMap<>();
        config.put(HOST.key(), "localhost");
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        // PORT present and valid, HOST absent -> pass
        config.clear();
        config.put(PORT.key(), 8080);
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        // PORT present but invalid, HOST present -> or chain second branch saves it
        config.clear();
        config.put(PORT.key(), 0);
        config.put(HOST.key(), "localhost");
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        // PORT present but invalid, HOST blank -> both branches fail
        config.clear();
        config.put(PORT.key(), 0);
        config.put(HOST.key(), "");
        assertThrows(OptionValidationException.class, () -> validate(config, rule));
    }

    @Test
    public void testOrChainAllOptionalAllAbsent() {
        OptionRule rule =
                OptionRule.builder()
                        .optional(PORT, greaterOrEqual(PORT, 1).or(notBlank(HOST)))
                        .optional(HOST)
                        .build();

        Map<String, Object> config = new HashMap<>();
        Assertions.assertDoesNotThrow(() -> validate(config, rule));
    }

    @Test
    public void testNumericTypeMismatchIntVsLong() {
        OptionRule rule =
                OptionRule.builder().required(START_TS, greaterThan(START_TS, 0L)).build();

        Map<String, Object> config = new HashMap<>();
        config.put(START_TS.key(), 100L);
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(START_TS.key(), 0L);
        assertThrows(OptionValidationException.class, () -> validate(config, rule));
    }

    @Test
    public void testNumericTypeMismatchDoubleVsInt() {
        OptionRule rule = OptionRule.builder().required(RATIO, lessOrEqual(RATIO, 100.0)).build();

        Map<String, Object> config = new HashMap<>();
        // Integer in config vs Double in constraint (parser may return Integer for whole numbers)
        config.put(RATIO.key(), 50);
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(RATIO.key(), 100);
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        // Double in config vs Double in constraint (fractional values)
        config.put(RATIO.key(), 50.5);
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(RATIO.key(), 100.0);
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(RATIO.key(), 100.1);
        assertThrows(OptionValidationException.class, () -> validate(config, rule));

        // Integer exceeding constraint
        config.put(RATIO.key(), 101);
        assertThrows(OptionValidationException.class, () -> validate(config, rule));
    }

    @Test
    public void testConditionalNestedValueConstraint() {
        OptionRule rule =
                OptionRule.builder()
                        .optional(MODE)
                        .conditional(
                                MODE, "stream", START_TS, END_TS, lessThanField(START_TS, END_TS))
                        .build();

        // Mode is "stream", both timestamps present and valid
        Map<String, Object> config = new HashMap<>();
        config.put(MODE.key(), "stream");
        config.put(START_TS.key(), 100L);
        config.put(END_TS.key(), 200L);
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        // Mode is "stream", timestamps violate constraint
        config.put(START_TS.key(), 300L);
        config.put(END_TS.key(), 100L);
        assertThrows(OptionValidationException.class, () -> validate(config, rule));

        // Mode is not "stream", constraint should not apply
        config.clear();
        config.put(MODE.key(), "batch");
        Assertions.assertDoesNotThrow(() -> validate(config, rule));
    }

    @Test
    public void testConditionWithNullExpectValueThrows() {
        assertThrows(IllegalArgumentException.class, () -> Condition.of(HOST, null));
        assertThrows(
                IllegalArgumentException.class,
                () -> Condition.of(HOST, ConditionOperator.NOT_EQUAL, null));
        assertThrows(IllegalArgumentException.class, () -> greaterThan(PORT, null));
        assertThrows(IllegalArgumentException.class, () -> startsWith(HOST, null));
    }

    // ==================== Supplementary Operator & Edge-case Coverage ====================

    @Test
    public void testEqualOperatorValidation() {
        OptionRule rule =
                OptionRule.builder().required(HOST, Condition.of(HOST, "expected_host")).build();

        Map<String, Object> config = new HashMap<>();
        config.put(HOST.key(), "expected_host");
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(HOST.key(), "other_host");
        assertThrows(OptionValidationException.class, () -> validate(config, rule));
    }

    @Test
    public void testLessOrEqualValidationStandalone() {
        OptionRule rule = OptionRule.builder().required(PORT, lessOrEqual(PORT, 1024)).build();

        Map<String, Object> config = new HashMap<>();
        config.put(PORT.key(), 1024);
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(PORT.key(), 80);
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(PORT.key(), 1025);
        assertThrows(OptionValidationException.class, () -> validate(config, rule));
    }

    @Test
    public void testNotEqualToString() {
        assertEquals(
                "'host' != blocked",
                Condition.of(HOST, ConditionOperator.NOT_EQUAL, "blocked").toString());
    }

    @Test
    public void testEqualToString() {
        assertEquals("'host' == localhost", Condition.of(HOST, "localhost").toString());
    }

    @Test
    public void testAndChainShortCircuit() {
        OptionRule rule =
                OptionRule.builder()
                        .required(PORT, greaterOrEqual(PORT, 1).and(lessOrEqual(PORT, 100)))
                        .build();

        Map<String, Object> config = new HashMap<>();
        config.put(PORT.key(), 0);
        assertThrows(OptionValidationException.class, () -> validate(config, rule));

        config.put(PORT.key(), 50);
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(PORT.key(), 101);
        assertThrows(OptionValidationException.class, () -> validate(config, rule));
    }

    @Test
    public void testOrChainShortCircuitFirstTrue() {
        OptionRule rule =
                OptionRule.builder()
                        .required(HOST, notBlank(HOST).or(notBlank(ENDPOINT)))
                        .optional(ENDPOINT)
                        .build();

        Map<String, Object> config = new HashMap<>();
        config.put(HOST.key(), "valid-host");
        Assertions.assertDoesNotThrow(() -> validate(config, rule));
    }

    @Test
    public void testOrChainShortCircuitFirstFalseSecondTrue() {
        OptionRule rule =
                OptionRule.builder()
                        .required(HOST, notBlank(HOST).or(notBlank(ENDPOINT)))
                        .optional(ENDPOINT)
                        .build();

        Map<String, Object> config = new HashMap<>();
        config.put(HOST.key(), "");
        config.put(ENDPOINT.key(), "valid-endpoint");
        Assertions.assertDoesNotThrow(() -> validate(config, rule));
    }

    @Test
    public void testOptionalWithDefaultValueAndConstraint() {
        OptionRule rule = OptionRule.builder().optional(FILE_EXPR, notBlank(FILE_EXPR)).build();

        Map<String, Object> config = new HashMap<>();
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(FILE_EXPR.key(), "my_file.csv");
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(FILE_EXPR.key(), "   ");
        assertThrows(OptionValidationException.class, () -> validate(config, rule));
    }

    @Test
    public void testThreeArgConditionFactory() {
        Condition<Integer> cond = Condition.of(PORT, ConditionOperator.GREATER_THAN, 0);
        OptionRule rule = OptionRule.builder().required(PORT, cond).build();

        Map<String, Object> config = new HashMap<>();
        config.put(PORT.key(), 10);
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(PORT.key(), 0);
        assertThrows(OptionValidationException.class, () -> validate(config, rule));
    }

    @Test
    public void testUnknownKeysWithFallbackKeys() {
        Option<String> hostWithFallback =
                Options.key("host")
                        .stringType()
                        .noDefaultValue()
                        .withFallbackKeys("hostname")
                        .withDescription("host");

        OptionRule rule =
                OptionRule.builder().required(hostWithFallback, notBlank(hostWithFallback)).build();

        Map<String, Object> config = new HashMap<>();
        config.put("hostname", "localhost");

        Assertions.assertDoesNotThrow(
                () ->
                        ConfigValidator.validateUnknownKeys(
                                ReadonlyConfig.fromMap(config), rule, "TestConnector"));
    }

    @Test
    public void testRequiredCrossFieldBothEqual() {
        OptionRule rule =
                OptionRule.builder()
                        .required(START_TS, END_TS, lessOrEqualField(START_TS, END_TS))
                        .build();

        Map<String, Object> config = new HashMap<>();
        config.put(START_TS.key(), 500L);
        config.put(END_TS.key(), 500L);
        Assertions.assertDoesNotThrow(() -> validate(config, rule));
    }

    @Test
    public void testAndChainToString() {
        assertEquals(
                "'port' >= 1 && 'port' <= 65535",
                greaterOrEqual(PORT, 1).and(lessOrEqual(PORT, 65535)).toString());
    }

    @Test
    public void testOrChainToString() {
        assertEquals(
                "'host' is not blank || 'endpoint' is not blank",
                notBlank(HOST).or(notBlank(ENDPOINT)).toString());
    }

    @Test
    public void testMixedAndOrChainToString() {
        assertEquals(
                "('port' >= 1 && 'port' <= 65535) || 'host' is not blank",
                greaterOrEqual(PORT, 1)
                        .and(lessOrEqual(PORT, 65535))
                        .or(notBlank(HOST))
                        .toString());
    }

    @Test
    public void testOptionalCrossFieldBothPresent() {
        OptionRule rule =
                OptionRule.builder()
                        .optional(START_TS, END_TS, lessThanField(START_TS, END_TS))
                        .build();

        Map<String, Object> config = new HashMap<>();
        config.put(START_TS.key(), 100L);
        config.put(END_TS.key(), 200L);
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(START_TS.key(), 300L);
        assertThrows(OptionValidationException.class, () -> validate(config, rule));
    }

    @Test
    public void testOptionalCrossFieldNonePresent() {
        OptionRule rule =
                OptionRule.builder()
                        .optional(START_TS, END_TS, lessThanField(START_TS, END_TS))
                        .build();

        Map<String, Object> config = new HashMap<>();
        Assertions.assertDoesNotThrow(() -> validate(config, rule));
    }

    @Test
    public void testLongThreeNodeAndChain() {
        OptionRule rule =
                OptionRule.builder()
                        .required(
                                PORT,
                                greaterOrEqual(PORT, 1)
                                        .and(lessOrEqual(PORT, 65535))
                                        .and(Condition.of(PORT, ConditionOperator.NOT_EQUAL, 22)))
                        .build();

        Map<String, Object> config = new HashMap<>();
        config.put(PORT.key(), 8080);
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(PORT.key(), 22);
        assertThrows(OptionValidationException.class, () -> validate(config, rule));

        config.put(PORT.key(), 0);
        assertThrows(OptionValidationException.class, () -> validate(config, rule));
    }

    @Test
    public void testCollectionNotEmptyWithScalarValue() {
        OptionRule rule = OptionRule.builder().required(TAGS, notEmpty(TAGS)).build();

        Map<String, Object> config = new HashMap<>();
        config.put(TAGS.key(), "not_a_list");
        Assertions.assertDoesNotThrow(
                () -> validate(config, rule),
                "ReadonlyConfig normalizes scalar to single-element list, which is non-empty");
    }

    @Test
    public void testCollectionUniqueWithScalarValue() {
        OptionRule rule = OptionRule.builder().required(TAGS, unique(TAGS)).build();

        Map<String, Object> config = new HashMap<>();
        config.put(TAGS.key(), "not_a_list");
        Assertions.assertDoesNotThrow(
                () -> validate(config, rule),
                "ReadonlyConfig normalizes scalar to single-element list, which is unique");
    }

    @Test
    public void testNotBlankWithNumericValue() {
        OptionRule rule = OptionRule.builder().required(HOST, notBlank(HOST)).build();

        Map<String, Object> config = new HashMap<>();
        config.put(HOST.key(), 12345);
        Assertions.assertDoesNotThrow(
                () -> validate(config, rule),
                "ReadonlyConfig converts integer to string '12345', which is not blank");
    }

    @Test
    public void testStartsWithNonMatch() {
        OptionRule rule =
                OptionRule.builder().required(ENDPOINT, startsWith(ENDPOINT, "https://")).build();

        Map<String, Object> config = new HashMap<>();
        config.put(ENDPOINT.key(), "http://example.com");
        assertThrows(OptionValidationException.class, () -> validate(config, rule));

        config.put(ENDPOINT.key(), "https://example.com");
        Assertions.assertDoesNotThrow(() -> validate(config, rule));
    }

    @Test
    public void testMatchesNonMatch() {
        OptionRule rule = OptionRule.builder().required(HOST, matches(HOST, "^[a-z]+$")).build();

        Map<String, Object> config = new HashMap<>();
        config.put(HOST.key(), "localhost");
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(HOST.key(), "Local-Host");
        assertThrows(OptionValidationException.class, () -> validate(config, rule));
    }

    // ==================== isConstraintApplicable — partial optional coverage ====================

    @Test
    public void testOptionalCrossFieldOnlyStartPresentNoFalsePositive() {
        OptionRule rule =
                OptionRule.builder()
                        .optional(START_TS, END_TS, lessThanField(START_TS, END_TS))
                        .build();

        Map<String, Object> config = new HashMap<>();
        config.put(START_TS.key(), 999L);
        Assertions.assertDoesNotThrow(
                () -> validate(config, rule), "partial cross-field must not cause false-positive");
    }

    @Test
    public void testOptionalCrossFieldOnlyEndPresentNoFalsePositive() {
        OptionRule rule =
                OptionRule.builder()
                        .optional(START_TS, END_TS, lessThanField(START_TS, END_TS))
                        .build();

        Map<String, Object> config = new HashMap<>();
        config.put(END_TS.key(), 999L);
        Assertions.assertDoesNotThrow(
                () -> validate(config, rule), "partial cross-field must not cause false-positive");
    }

    @Test
    public void testOptionalSingleFieldAbsentSkipsConstraint() {
        OptionRule rule = OptionRule.builder().optional(PORT, greaterOrEqual(PORT, 1)).build();

        Map<String, Object> config = new HashMap<>();
        Assertions.assertDoesNotThrow(
                () -> validate(config, rule), "absent optional should skip constraint");
    }

    @Test
    public void testOptionalSingleFieldPresentEnforcesConstraint() {
        OptionRule rule = OptionRule.builder().optional(PORT, greaterOrEqual(PORT, 1)).build();

        Map<String, Object> config = new HashMap<>();
        config.put(PORT.key(), 0);
        assertThrows(
                OptionValidationException.class,
                () -> validate(config, rule),
                "present optional should enforce constraint");
    }

    @Test
    public void testOrChainFirstSegmentValidSecondAbsent() {
        OptionRule rule =
                OptionRule.builder()
                        .optional(PORT, greaterOrEqual(PORT, 1).or(notBlank(HOST)))
                        .optional(HOST)
                        .build();

        Map<String, Object> config = new HashMap<>();
        config.put(PORT.key(), 8080);
        Assertions.assertDoesNotThrow(
                () -> validate(config, rule), "first OR segment present and valid -> pass");
    }

    @Test
    public void testOrChainFirstSegmentInvalidSecondAbsentFails() {
        OptionRule rule =
                OptionRule.builder()
                        .optional(PORT, greaterOrEqual(PORT, 1).or(notBlank(HOST)))
                        .optional(HOST)
                        .build();

        Map<String, Object> config = new HashMap<>();
        config.put(PORT.key(), 0);
        assertThrows(
                OptionValidationException.class,
                () -> validate(config, rule),
                "first OR segment invalid + second absent -> fail");
    }

    @Test
    public void testOrChainFirstAbsentSecondPresent() {
        OptionRule rule =
                OptionRule.builder()
                        .optional(PORT, greaterOrEqual(PORT, 1).or(notBlank(HOST)))
                        .optional(HOST)
                        .build();

        Map<String, Object> config = new HashMap<>();
        config.put(HOST.key(), "localhost");
        Assertions.assertDoesNotThrow(
                () -> validate(config, rule),
                "first OR segment absent + second present and valid -> pass");
    }

    @Test
    public void testOrChainFirstAbsentSecondInvalidFails() {
        OptionRule rule =
                OptionRule.builder()
                        .optional(PORT, greaterOrEqual(PORT, 1).or(notBlank(HOST)))
                        .optional(HOST)
                        .build();

        Map<String, Object> config = new HashMap<>();
        config.put(HOST.key(), "   ");
        assertThrows(
                OptionValidationException.class,
                () -> validate(config, rule),
                "first OR segment absent + second present but invalid -> fail");
    }

    @Test
    public void testOrChainCrossFieldOrLiteral() {
        OptionRule rule =
                OptionRule.builder()
                        .optional(
                                START_TS,
                                END_TS,
                                lessThanField(START_TS, END_TS).or(greaterOrEqual(START_TS, 0L)))
                        .build();

        Map<String, Object> config = new HashMap<>();
        config.put(START_TS.key(), 100L);
        Assertions.assertDoesNotThrow(
                () -> validate(config, rule),
                "cross-field OR absent + literal segment valid -> pass");

        config.put(START_TS.key(), -1L);
        assertThrows(
                OptionValidationException.class,
                () -> validate(config, rule),
                "cross-field OR absent + literal segment invalid -> fail");

        config.put(START_TS.key(), 200L);
        config.put(END_TS.key(), 300L);
        Assertions.assertDoesNotThrow(
                () -> validate(config, rule), "cross-field segment present and valid -> pass");
    }

    @Test
    public void testOrChainBothPresentFirstFails() {
        OptionRule rule =
                OptionRule.builder()
                        .optional(PORT, greaterOrEqual(PORT, 1).or(notBlank(HOST)))
                        .optional(HOST)
                        .build();

        Map<String, Object> config = new HashMap<>();
        config.put(PORT.key(), 0);
        config.put(HOST.key(), "localhost");
        Assertions.assertDoesNotThrow(
                () -> validate(config, rule), "OR chain should pass when second branch succeeds");
    }

    @Test
    public void testOrChainBothPresentBothFail() {
        OptionRule rule =
                OptionRule.builder()
                        .optional(PORT, greaterOrEqual(PORT, 1).or(notBlank(HOST)))
                        .optional(HOST)
                        .build();

        Map<String, Object> config = new HashMap<>();
        config.put(PORT.key(), 0);
        config.put(HOST.key(), "   ");
        assertThrows(
                OptionValidationException.class,
                () -> validate(config, rule),
                "OR chain should fail when both branches fail");
    }

    @Test
    public void testRequiredCrossFieldOneAbsentFailsRequired() {
        OptionRule rule =
                OptionRule.builder()
                        .required(START_TS, END_TS, lessThanField(START_TS, END_TS))
                        .build();

        Map<String, Object> config = new HashMap<>();
        config.put(START_TS.key(), 100L);
        assertThrows(
                OptionValidationException.class,
                () -> validate(config, rule),
                "required cross-field should fail when one option is absent");
    }

    // ==================== Error message quality & aggregation ====================

    @Test
    public void testSingleConstraintErrorMessageContainsActualValue() {
        OptionRule rule =
                OptionRule.builder()
                        .required(PORT, greaterOrEqual(PORT, 1).and(lessOrEqual(PORT, 65535)))
                        .build();

        Map<String, Object> config = new HashMap<>();
        config.put(PORT.key(), -1);
        OptionValidationException ex =
                assertThrows(OptionValidationException.class, () -> validate(config, rule));
        String msg = ex.getMessage();
        Assertions.assertTrue(msg.contains("option: port"), "should contain option label");
        Assertions.assertTrue(msg.contains("constraint:"), "should contain constraint label");
        Assertions.assertTrue(msg.contains(">= 1"), "should contain constraint expression");
    }

    @Test
    public void testMultipleConstraintErrorsAggregated() {
        OptionRule rule =
                OptionRule.builder()
                        .required(PORT, greaterOrEqual(PORT, 1))
                        .required(HOST, notBlank(HOST))
                        .build();

        Map<String, Object> config = new HashMap<>();
        config.put(PORT.key(), -1);
        config.put(HOST.key(), "   ");
        OptionValidationException ex =
                assertThrows(OptionValidationException.class, () -> validate(config, rule));
        String msg = ex.getMessage();
        Assertions.assertTrue(msg.contains("2 errors"), "should report 2 errors");
        Assertions.assertTrue(msg.contains("[1] option: port"), "should have numbered port");
        Assertions.assertTrue(msg.contains("[2] option: host"), "should have numbered host");
    }

    @Test
    public void testThreeConstraintErrorsAggregated() {
        OptionRule rule =
                OptionRule.builder()
                        .required(PORT, greaterOrEqual(PORT, 1).and(lessOrEqual(PORT, 65535)))
                        .required(HOST, notBlank(HOST))
                        .required(ENDPOINT, matches(ENDPOINT, "^[^:]+:\\d+$"))
                        .build();

        Map<String, Object> config = new HashMap<>();
        config.put(PORT.key(), 99999);
        config.put(HOST.key(), "");
        config.put(ENDPOINT.key(), "no-port-here");
        OptionValidationException ex =
                assertThrows(OptionValidationException.class, () -> validate(config, rule));
        String msg = ex.getMessage();
        Assertions.assertTrue(msg.contains("3 errors"), "should report 3 errors");
        Assertions.assertTrue(msg.contains("[1] option: port"));
        Assertions.assertTrue(msg.contains("[2] option: host"));
        Assertions.assertTrue(msg.contains("[3] option: endpoint"));
    }

    @Test
    public void testMixedNumericStringCollectionErrors() {
        OptionRule rule =
                OptionRule.builder()
                        .required(RATIO, greaterThan(RATIO, 0.0).and(lessOrEqual(RATIO, 1.0)))
                        .required(DB_NAME, notBlank(DB_NAME))
                        .required(TAGS, notEmpty(TAGS))
                        .build();

        Map<String, Object> config = new HashMap<>();
        config.put(RATIO.key(), -0.5);
        config.put(DB_NAME.key(), "  ");
        config.put(TAGS.key(), Collections.emptyList());
        OptionValidationException ex =
                assertThrows(OptionValidationException.class, () -> validate(config, rule));
        String msg = ex.getMessage();
        Assertions.assertTrue(msg.contains("3 errors"), "should report 3 errors");
        Assertions.assertTrue(msg.contains("option: ratio"));
        Assertions.assertTrue(msg.contains("option: db_name"));
        Assertions.assertTrue(msg.contains("option: tags"));
    }

    @Test
    public void testCrossFieldAndLiteralErrorsAggregated() {
        OptionRule rule =
                OptionRule.builder()
                        .required(START_TS, END_TS, lessThanField(START_TS, END_TS))
                        .required(PORT, greaterOrEqual(PORT, 1))
                        .build();

        Map<String, Object> config = new HashMap<>();
        config.put(START_TS.key(), 2000L);
        config.put(END_TS.key(), 1000L);
        config.put(PORT.key(), 0);
        OptionValidationException ex =
                assertThrows(OptionValidationException.class, () -> validate(config, rule));
        String msg = ex.getMessage();
        Assertions.assertTrue(msg.contains("2 errors"), "should report 2 errors");
        Assertions.assertTrue(msg.contains("option: start_ts"));
        Assertions.assertTrue(msg.contains("option: port"));
    }

    @Test
    public void testPassingConstraintsNoAggregation() {
        OptionRule rule =
                OptionRule.builder()
                        .required(PORT, greaterOrEqual(PORT, 1).and(lessOrEqual(PORT, 65535)))
                        .required(HOST, notBlank(HOST))
                        .required(TAGS, notEmpty(TAGS).and(unique(TAGS)))
                        .build();

        Map<String, Object> config = new HashMap<>();
        config.put(PORT.key(), 8080);
        config.put(HOST.key(), "localhost");
        config.put(TAGS.key(), Arrays.asList("a", "b", "c"));
        Assertions.assertDoesNotThrow(() -> validate(config, rule));
    }

    @Test
    public void testSingleErrorNoPlural() {
        OptionRule rule = OptionRule.builder().required(PORT, greaterOrEqual(PORT, 1)).build();

        Map<String, Object> config = new HashMap<>();
        config.put(PORT.key(), 0);
        OptionValidationException ex =
                assertThrows(OptionValidationException.class, () -> validate(config, rule));
        String msg = ex.getMessage();
        Assertions.assertTrue(msg.contains("1 error)"), "single error should not be plural");
        Assertions.assertFalse(msg.contains("1 errors"), "should not say '1 errors'");
    }

    @Test
    public void testUnknownKeysWithConditionalValueConstraint() {
        OptionRule rule =
                OptionRule.builder()
                        .optional(MODE)
                        .conditional(MODE, "stream", greaterThan(START_TS, 0L))
                        .build();

        Map<String, Object> config = new HashMap<>();
        config.put(MODE.key(), "stream");
        config.put(START_TS.key(), 100L);

        Assertions.assertDoesNotThrow(
                () ->
                        ConfigValidator.validateUnknownKeys(
                                ReadonlyConfig.fromMap(config), rule, "TestConnector"),
                "conditional value constraint option should be in declared keys");
    }

    @Test
    public void testUnknownKeysWithConditionalMultiFieldConstraint() {
        OptionRule rule =
                OptionRule.builder()
                        .optional(ENABLE_TX)
                        .conditional(
                                ENABLE_TX, true, START_TS, END_TS, lessThanField(START_TS, END_TS))
                        .build();

        Map<String, Object> config = new HashMap<>();
        config.put(ENABLE_TX.key(), true);
        config.put(START_TS.key(), 100L);
        config.put(END_TS.key(), 200L);

        Assertions.assertDoesNotThrow(
                () ->
                        ConfigValidator.validateUnknownKeys(
                                ReadonlyConfig.fromMap(config), rule, "TestConnector"),
                "conditional multi-field constraint options should be in declared keys");
    }

    @Test
    public void testUnknownKeysWithOrChainMultipleOptions() {
        OptionRule rule =
                OptionRule.builder()
                        .optional(HOST, notBlank(HOST).or(notBlank(ENDPOINT)))
                        .optional(ENDPOINT)
                        .build();

        Map<String, Object> config = new HashMap<>();
        config.put(HOST.key(), "localhost");
        config.put(ENDPOINT.key(), "my-endpoint");

        Assertions.assertDoesNotThrow(
                () ->
                        ConfigValidator.validateUnknownKeys(
                                ReadonlyConfig.fromMap(config), rule, "TestConnector"),
                "OR chain options should all be in declared keys");
    }

    // ==================== Additional OR / AND chain edge cases ====================

    @Test
    public void testThreeWayOrChain() {
        OptionRule rule =
                OptionRule.builder()
                        .optional(HOST, notBlank(HOST).or(notBlank(ENDPOINT)).or(notBlank(DB_NAME)))
                        .optional(ENDPOINT)
                        .optional(DB_NAME)
                        .build();

        Map<String, Object> config = new HashMap<>();
        config.put(DB_NAME.key(), "mydb");
        Assertions.assertDoesNotThrow(
                () -> validate(config, rule), "third OR segment present and valid -> pass");

        config.clear();
        config.put(HOST.key(), "");
        config.put(ENDPOINT.key(), "");
        config.put(DB_NAME.key(), "");
        assertThrows(
                OptionValidationException.class,
                () -> validate(config, rule),
                "all three OR branches blank -> fail");

        config.clear();
        Assertions.assertDoesNotThrow(() -> validate(config, rule), "all three absent -> skip");
    }

    @Test
    public void testAndOrMixedChainAndBindsTighter() {
        // A.and(B).or(C) evaluates as (A && B) || C — AND has higher precedence than OR
        OptionRule rule =
                OptionRule.builder()
                        .optional(
                                START_TS,
                                END_TS,
                                lessThanField(START_TS, END_TS)
                                        .and(greaterOrEqual(START_TS, 0L))
                                        .or(notBlank(HOST)))
                        .optional(HOST)
                        .build();

        // (A && B) = true -> pass
        Map<String, Object> config = new HashMap<>();
        config.put(START_TS.key(), 100L);
        config.put(END_TS.key(), 200L);
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        // (A && B) both fail, C = true -> OR rescues
        config.clear();
        config.put(START_TS.key(), 300L);
        config.put(END_TS.key(), 100L);
        config.put(HOST.key(), "fallback");
        Assertions.assertDoesNotThrow(
                () -> validate(config, rule), "AND group fails but OR fallback rescues");

        // Only HOST present and valid -> C alone passes via OR
        config.clear();
        config.put(HOST.key(), "fallback");
        Assertions.assertDoesNotThrow(
                () -> validate(config, rule), "only OR fallback present and valid -> pass");

        // (A && B) fail, C blank -> both OR branches fail
        config.clear();
        config.put(START_TS.key(), 300L);
        config.put(END_TS.key(), 100L);
        config.put(HOST.key(), "");
        assertThrows(
                OptionValidationException.class,
                () -> validate(config, rule),
                "AND group fails + OR fallback blank -> fail");

        // All absent -> constraint skipped
        config.clear();
        Assertions.assertDoesNotThrow(() -> validate(config, rule));
    }

    @Test
    public void testMultipleVarargsConstraints() {
        OptionRule rule =
                OptionRule.builder()
                        .required(PORT, greaterOrEqual(PORT, 1), lessOrEqual(PORT, 65535))
                        .build();

        Map<String, Object> config = new HashMap<>();
        config.put(PORT.key(), 8080);
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(PORT.key(), 0);
        assertThrows(OptionValidationException.class, () -> validate(config, rule));

        config.put(PORT.key(), 70000);
        assertThrows(OptionValidationException.class, () -> validate(config, rule));
    }

    @Test
    public void testConditionalValueOnlyConstraintSkipsWhenTargetAbsent() {
        OptionRule rule =
                OptionRule.builder()
                        .optional(MODE)
                        .conditional(MODE, "stream", greaterThan(START_TS, 0L))
                        .build();

        Map<String, Object> config = new HashMap<>();
        config.put(MODE.key(), "stream");
        Assertions.assertDoesNotThrow(
                () -> validate(config, rule),
                "value-only conditional with target absent -> constraint skipped by design");
    }

    @Test
    public void testConditionalValueOnlyConstraintEnforcesWhenTargetPresent() {
        OptionRule rule =
                OptionRule.builder()
                        .optional(MODE)
                        .conditional(MODE, "stream", greaterThan(START_TS, 0L))
                        .build();

        Map<String, Object> config = new HashMap<>();
        config.put(MODE.key(), "stream");
        config.put(START_TS.key(), 100L);
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(START_TS.key(), 0L);
        assertThrows(OptionValidationException.class, () -> validate(config, rule));
    }

    @Test
    public void testEmptyStringTreatedAsPresent() {
        OptionRule rule = OptionRule.builder().optional(HOST, notBlank(HOST)).build();

        Map<String, Object> config = new HashMap<>();
        config.put(HOST.key(), "");
        assertThrows(
                OptionValidationException.class,
                () -> validate(config, rule),
                "empty string is present but blank -> constraint should fail");
    }

    @Test
    public void testRequiredPrimaryWithAbsentOptionalCompareField() {
        OptionRule rule =
                OptionRule.builder()
                        .required(START_TS, lessThanField(START_TS, END_TS))
                        .optional(END_TS)
                        .build();

        Map<String, Object> config = new HashMap<>();
        config.put(START_TS.key(), 100L);
        assertThrows(
                OptionValidationException.class,
                () -> validate(config, rule),
                "required primary -> constraint applicable; compare field null -> evaluator returns false -> fail");
    }

    @Test
    public void testRequiredPrimaryWithCompareFieldBothPresent() {
        OptionRule rule =
                OptionRule.builder()
                        .required(START_TS, lessThanField(START_TS, END_TS))
                        .optional(END_TS)
                        .build();

        Map<String, Object> config = new HashMap<>();
        config.put(START_TS.key(), 100L);
        config.put(END_TS.key(), 200L);
        Assertions.assertDoesNotThrow(() -> validate(config, rule));
    }

    @Test
    public void testOptionalHeadWithRequiredCompareFieldHeadAbsent() {
        OptionRule rule =
                OptionRule.builder()
                        .optional(START_TS, lessThanField(START_TS, END_TS))
                        .required(END_TS)
                        .build();

        Map<String, Object> config = new HashMap<>();
        config.put(END_TS.key(), 200L);
        Assertions.assertDoesNotThrow(
                () -> validate(config, rule),
                "head (START_TS) is optional and absent -> constraint skipped regardless of required compareField");
    }

    @Test
    public void testOptionalHeadWithRequiredCompareFieldBothPresent() {
        OptionRule rule =
                OptionRule.builder()
                        .optional(START_TS, lessThanField(START_TS, END_TS))
                        .required(END_TS)
                        .build();

        Map<String, Object> config = new HashMap<>();
        config.put(START_TS.key(), 100L);
        config.put(END_TS.key(), 200L);
        Assertions.assertDoesNotThrow(() -> validate(config, rule));
    }

    @Test
    public void testOptionalHeadWithRequiredCompareFieldViolation() {
        OptionRule rule =
                OptionRule.builder()
                        .optional(START_TS, lessThanField(START_TS, END_TS))
                        .required(END_TS)
                        .build();

        Map<String, Object> config = new HashMap<>();
        config.put(START_TS.key(), 300L);
        config.put(END_TS.key(), 200L);
        assertThrows(
                OptionValidationException.class,
                () -> validate(config, rule),
                "both present but start > end -> constraint fails");
    }

    @Test
    public void testOrThenAndChainEvaluation() {
        // A.or(B).and(C) evaluates as A || (B && C)
        OptionRule rule =
                OptionRule.builder()
                        .required(
                                PORT,
                                notBlank(HOST)
                                        .or(greaterOrEqual(PORT, 1).and(lessOrEqual(PORT, 100))))
                        .required(HOST)
                        .build();

        Map<String, Object> config = new HashMap<>();
        config.put(HOST.key(), "localhost");
        config.put(PORT.key(), 200);
        Assertions.assertDoesNotThrow(
                () -> validate(config, rule),
                "A (notBlank HOST) true -> OR short-circuits, (B && C) skipped");

        config.put(HOST.key(), "");
        config.put(PORT.key(), 50);
        Assertions.assertDoesNotThrow(
                () -> validate(config, rule),
                "A false, (B && C) = (50>=1 && 50<=100) = true -> pass");

        config.put(HOST.key(), "");
        config.put(PORT.key(), 200);
        assertThrows(
                OptionValidationException.class,
                () -> validate(config, rule),
                "A false, (B && C) = (200>=1 && 200<=100) = false -> fail");
    }

    @Test
    public void testInvalidRegexPatternThrowsPatternSyntaxException() {
        OptionRule rule = OptionRule.builder().required(HOST, matches(HOST, "[invalid")).build();

        Map<String, Object> config = new HashMap<>();
        config.put(HOST.key(), "test");
        assertThrows(
                java.util.regex.PatternSyntaxException.class,
                () -> validate(config, rule),
                "invalid regex escapes as PatternSyntaxException from String.matches()");
    }

    @Test
    public void testNullOptionRejected() {
        assertThrows(
                IllegalArgumentException.class,
                () -> Condition.of(null, "value"),
                "null option should throw IAE");
    }

    @Test
    public void testCircularChainViaOr() {
        Condition<Integer> a = greaterThan(PORT, 0);
        Condition<String> b = notBlank(HOST);
        a.or(b);
        assertThrows(
                IllegalArgumentException.class,
                () -> b.or(a),
                "circular chain via or() should be detected");
    }

    @Test
    public void testUpperCaseEmptyString() {
        OptionRule rule = OptionRule.builder().required(DB_NAME, upperCase(DB_NAME)).build();

        Map<String, Object> config = new HashMap<>();
        config.put(DB_NAME.key(), "");
        Assertions.assertDoesNotThrow(
                () -> validate(config, rule), "empty string equals its uppercase form");
    }

    @Test
    public void testLowerCaseEmptyString() {
        OptionRule rule = OptionRule.builder().required(DB_NAME, lowerCase(DB_NAME)).build();

        Map<String, Object> config = new HashMap<>();
        config.put(DB_NAME.key(), "");
        Assertions.assertDoesNotThrow(
                () -> validate(config, rule), "empty string equals its lowercase form");
    }

    @Test
    public void testStructuralAndConstraintErrorsAggregated() {
        OptionRule rule =
                OptionRule.builder()
                        .required(PORT, greaterOrEqual(PORT, 1))
                        .required(HOST, notBlank(HOST))
                        .build();

        Map<String, Object> config = new HashMap<>();
        config.put(PORT.key(), 0);
        OptionValidationException ex =
                assertThrows(OptionValidationException.class, () -> validate(config, rule));
        String msg = ex.getMessage();
        Assertions.assertTrue(msg.contains("2 errors"), "should aggregate both errors");
        Assertions.assertTrue(
                msg.contains("[1] option: 'host'"), "structural error for host should come first");
        Assertions.assertTrue(msg.contains("type: required"));
        Assertions.assertTrue(
                msg.contains("required option is not configured"),
                "should describe the structural rule");
        Assertions.assertTrue(
                msg.contains("[2] option: port"), "constraint error for port should come second");
        Assertions.assertTrue(msg.contains("type: value"));
        Assertions.assertTrue(msg.contains("'port' >= 1"), "should include constraint expression");
        Assertions.assertFalse(
                msg.contains("[3]"),
                "host constraint should be suppressed since host is structurally absent");
    }

    @Test
    public void testMergedConditionalConstraints() {
        OptionRule rule =
                OptionRule.builder()
                        .optional(MODE)
                        .conditional(MODE, "stream", greaterThan(START_TS, 0L))
                        .conditional(MODE, "stream", greaterThan(END_TS, 0L))
                        .build();

        Map<String, Object> config = new HashMap<>();
        config.put(MODE.key(), "stream");
        config.put(START_TS.key(), 100L);
        config.put(END_TS.key(), 200L);
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(START_TS.key(), 0L);
        config.put(END_TS.key(), 0L);
        OptionValidationException ex =
                assertThrows(OptionValidationException.class, () -> validate(config, rule));
        Assertions.assertTrue(
                ex.getMessage().contains("2 errors"),
                "merged conditional constraints should aggregate both failures");
    }

    @Test
    public void testConstraintErrorMessageHasErrorCodePrefix() {
        OptionRule rule = OptionRule.builder().required(PORT, greaterOrEqual(PORT, 1)).build();

        Map<String, Object> config = new HashMap<>();
        config.put(PORT.key(), 0);
        OptionValidationException ex =
                assertThrows(OptionValidationException.class, () -> validate(config, rule));
        Assertions.assertTrue(
                ex.getMessage().startsWith("ErrorCode:[API-02]"),
                "constraint error should carry standard ErrorCode prefix");
    }

    @Test
    public void testThreeWayOrChainToString() {
        assertEquals(
                "'host' is not blank || 'endpoint' is not blank || 'db_name' is not blank",
                notBlank(HOST).or(notBlank(ENDPOINT)).or(notBlank(DB_NAME)).toString());
    }

    @Test
    public void testOrThenAndChainToString() {
        // AND-first precedence grouping, consistent with evaluation semantics:
        // notBlank(HOST) -or-> greaterOrEqual(PORT,1) -and-> lessOrEqual(PORT,100)
        // evaluates as: HOST || (PORT>=1 && PORT<=100)
        assertEquals(
                "'host' is not blank || ('port' >= 1 && 'port' <= 100)",
                notBlank(HOST).or(greaterOrEqual(PORT, 1).and(lessOrEqual(PORT, 100))).toString());
    }

    @Test
    public void testContainsEmptySubstring() {
        OptionRule rule = OptionRule.builder().required(HOST, contains(HOST, "")).build();

        Map<String, Object> config = new HashMap<>();
        config.put(HOST.key(), "anything");
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(HOST.key(), "");
        Assertions.assertDoesNotThrow(() -> validate(config, rule));
    }

    @Test
    public void testStartsWithEmptyPrefix() {
        OptionRule rule = OptionRule.builder().required(ENDPOINT, startsWith(ENDPOINT, "")).build();

        Map<String, Object> config = new HashMap<>();
        config.put(ENDPOINT.key(), "anything");
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(ENDPOINT.key(), "");
        Assertions.assertDoesNotThrow(() -> validate(config, rule));
    }

    @Test
    public void testUpperCaseWithDigitsAndSpecialChars() {
        OptionRule rule = OptionRule.builder().required(DB_NAME, upperCase(DB_NAME)).build();

        Map<String, Object> config = new HashMap<>();
        config.put(DB_NAME.key(), "ABC123");
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(DB_NAME.key(), "DB_NAME_01");
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(DB_NAME.key(), "ABC-123_OK");
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(DB_NAME.key(), "Abc123");
        assertThrows(OptionValidationException.class, () -> validate(config, rule));
    }

    @Test
    public void testLowerCaseWithDigitsAndSpecialChars() {
        OptionRule rule = OptionRule.builder().required(DB_NAME, lowerCase(DB_NAME)).build();

        Map<String, Object> config = new HashMap<>();
        config.put(DB_NAME.key(), "abc123");
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(DB_NAME.key(), "db_name_01");
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(DB_NAME.key(), "abc-123_ok");
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(DB_NAME.key(), "Abc123");
        assertThrows(OptionValidationException.class, () -> validate(config, rule));
    }

    @Test
    public void testUniqueEmptyCollection() {
        OptionRule rule = OptionRule.builder().required(TAGS, unique(TAGS)).build();

        Map<String, Object> config = new HashMap<>();
        config.put(TAGS.key(), Collections.emptyList());
        Assertions.assertDoesNotThrow(() -> validate(config, rule));
    }

    @Test
    public void testUniqueSingleElement() {
        OptionRule rule = OptionRule.builder().required(TAGS, unique(TAGS)).build();

        Map<String, Object> config = new HashMap<>();
        config.put(TAGS.key(), Collections.singletonList("only"));
        Assertions.assertDoesNotThrow(() -> validate(config, rule));
    }

    @Test
    public void testNotEmptySingleElement() {
        OptionRule rule = OptionRule.builder().required(TAGS, notEmpty(TAGS)).build();

        Map<String, Object> config = new HashMap<>();
        config.put(TAGS.key(), Collections.singletonList("one"));
        Assertions.assertDoesNotThrow(() -> validate(config, rule));
    }

    @Test
    public void testRequiredMissingErrorFormat() {
        OptionRule rule = OptionRule.builder().required(HOST).build();

        Map<String, Object> config = new HashMap<>();
        OptionValidationException ex =
                assertThrows(OptionValidationException.class, () -> validate(config, rule));
        String msg = ex.getMessage();
        Assertions.assertTrue(msg.contains("Option validation failed (1 error):"));
        Assertions.assertTrue(msg.contains("[1] option: 'host'"));
        Assertions.assertTrue(msg.contains("type: required"));
        Assertions.assertTrue(msg.contains("constraint: required option is not configured"));
    }

    @Test
    public void testBundledErrorFormat() {
        OptionRule rule = OptionRule.builder().bundled(KEY_USERNAME, KEY_PASSWORD).build();

        Map<String, Object> config = new HashMap<>();
        config.put(KEY_USERNAME.key(), "admin");
        OptionValidationException ex =
                assertThrows(OptionValidationException.class, () -> validate(config, rule));
        String msg = ex.getMessage();
        Assertions.assertTrue(msg.contains("Option validation failed (1 error):"));
        Assertions.assertTrue(msg.contains("[1] options: 'username', 'password'"));
        Assertions.assertTrue(msg.contains("type: bundled"));
        Assertions.assertTrue(
                msg.contains("constraint: bundled options must be present or absent together"));
        Assertions.assertTrue(msg.contains("present: ['username']"));
        Assertions.assertTrue(msg.contains("absent: ['password']"));
    }

    @Test
    public void testExclusiveNoneSetErrorFormat() {
        OptionRule rule = OptionRule.builder().exclusive(KEY_USERNAME, KEY_BEARER_TOKEN).build();

        Map<String, Object> config = new HashMap<>();
        OptionValidationException ex =
                assertThrows(OptionValidationException.class, () -> validate(config, rule));
        String msg = ex.getMessage();
        Assertions.assertTrue(msg.contains("Option validation failed (1 error):"));
        Assertions.assertTrue(msg.contains("[1] options: 'username', 'bearer-token'"));
        Assertions.assertTrue(msg.contains("type: exclusive"));
        Assertions.assertTrue(
                msg.contains(
                        "constraint: exactly one option must be set, but none are configured"));
    }

    @Test
    public void testExclusiveMultipleSetErrorFormat() {
        OptionRule rule = OptionRule.builder().exclusive(KEY_USERNAME, KEY_BEARER_TOKEN).build();

        Map<String, Object> config = new HashMap<>();
        config.put(KEY_USERNAME.key(), "admin");
        config.put(KEY_BEARER_TOKEN.key(), "token123");
        OptionValidationException ex =
                assertThrows(OptionValidationException.class, () -> validate(config, rule));
        String msg = ex.getMessage();
        Assertions.assertTrue(msg.contains("Option validation failed (1 error):"));
        Assertions.assertTrue(msg.contains("type: exclusive"));
        Assertions.assertTrue(msg.contains("mutually exclusive, but multiple are set"));
    }

    @Test
    public void testConditionalMissingErrorFormat() {
        OptionRule rule =
                OptionRule.builder()
                        .optional(TEST_MODE)
                        .conditional(TEST_MODE, OptionTest.TestMode.TIMESTAMP, TEST_TIMESTAMP)
                        .build();

        Map<String, Object> config = new HashMap<>();
        config.put(TEST_MODE.key(), "timestamp");
        OptionValidationException ex =
                assertThrows(OptionValidationException.class, () -> validate(config, rule));
        String msg = ex.getMessage();
        Assertions.assertTrue(msg.contains("Option validation failed (1 error):"));
        Assertions.assertTrue(msg.contains("[1] option: 'option.timestamp'"));
        Assertions.assertTrue(msg.contains("type: conditional"));
        Assertions.assertTrue(
                msg.contains("constraint: required because ['option.mode' == TIMESTAMP] is true"));
    }

    @Test
    public void testMultipleStructuralErrorsAggregated() {
        OptionRule rule =
                OptionRule.builder().required(HOST).required(PORT).required(DB_NAME).build();

        Map<String, Object> config = new HashMap<>();
        OptionValidationException ex =
                assertThrows(OptionValidationException.class, () -> validate(config, rule));
        String msg = ex.getMessage();
        Assertions.assertTrue(msg.contains("3 errors"), "should report all 3 missing");
        Assertions.assertTrue(msg.contains("[1] option: 'host'"));
        Assertions.assertTrue(msg.contains("[2] option: 'port'"));
        Assertions.assertTrue(msg.contains("[3] option: 'db_name'"));
    }

    @Test
    public void testBundledAndConstraintAggregated() {
        OptionRule rule =
                OptionRule.builder()
                        .bundled(KEY_USERNAME, KEY_PASSWORD)
                        .required(PORT, greaterOrEqual(PORT, 1))
                        .build();

        Map<String, Object> config = new HashMap<>();
        config.put(KEY_USERNAME.key(), "admin");
        config.put(PORT.key(), 0);
        OptionValidationException ex =
                assertThrows(OptionValidationException.class, () -> validate(config, rule));
        String msg = ex.getMessage();
        Assertions.assertTrue(msg.contains("2 errors"));
        Assertions.assertTrue(msg.contains("[1] options: 'username', 'password'"));
        Assertions.assertTrue(msg.contains("bundled"));
        Assertions.assertTrue(msg.contains("[2] option: port"));
        Assertions.assertTrue(msg.contains("'port' >= 1"));
    }

    @Test
    public void testExclusiveAndRequiredAggregated() {
        OptionRule rule =
                OptionRule.builder()
                        .exclusive(KEY_USERNAME, KEY_BEARER_TOKEN)
                        .required(HOST)
                        .build();

        Map<String, Object> config = new HashMap<>();
        config.put(KEY_USERNAME.key(), "admin");
        config.put(KEY_BEARER_TOKEN.key(), "token");
        OptionValidationException ex =
                assertThrows(OptionValidationException.class, () -> validate(config, rule));
        String msg = ex.getMessage();
        Assertions.assertTrue(msg.contains("2 errors"));
        Assertions.assertTrue(msg.contains("mutually exclusive"));
        Assertions.assertTrue(msg.contains("required option is not configured"));
    }

    @Test
    public void testAbsentRequiredSuppressesValueConstraint() {
        OptionRule rule = OptionRule.builder().required(HOST, notBlank(HOST)).build();

        Map<String, Object> config = new HashMap<>();
        OptionValidationException ex =
                assertThrows(OptionValidationException.class, () -> validate(config, rule));
        String msg = ex.getMessage();
        Assertions.assertTrue(msg.contains("1 error)"), "should only report 1 error");
        Assertions.assertTrue(msg.contains("type: required"));
        Assertions.assertTrue(msg.contains("required option is not configured"));
        Assertions.assertFalse(
                msg.contains("type: value"),
                "value constraint should be suppressed for absent required option");
    }

    @Test
    public void testAllStructuralTypesAggregated() {
        OptionRule rule =
                OptionRule.builder()
                        .required(HOST)
                        .bundled(KEY_USERNAME, KEY_PASSWORD)
                        .exclusive(KEY_BEARER_TOKEN, KEY_KERBEROS_TICKET)
                        .build();

        Map<String, Object> config = new HashMap<>();
        config.put(KEY_USERNAME.key(), "admin");
        config.put(KEY_BEARER_TOKEN.key(), "token");
        config.put(KEY_KERBEROS_TICKET.key(), "ticket");
        OptionValidationException ex =
                assertThrows(OptionValidationException.class, () -> validate(config, rule));
        String msg = ex.getMessage();
        Assertions.assertTrue(msg.contains("3 errors"));
        Assertions.assertTrue(msg.contains("required option is not configured"));
        Assertions.assertTrue(msg.contains("bundled"));
        Assertions.assertTrue(msg.contains("mutually exclusive"));
    }

    @Test
    public void testConditionalAbsentSuppressesValueConstraint() {
        OptionRule nestedRule =
                OptionRule.builder()
                        .required(TEST_TIMESTAMP, greaterThan(TEST_TIMESTAMP, 0L))
                        .build();
        OptionRule rule =
                OptionRule.builder()
                        .optional(TEST_MODE)
                        .conditionalRule(TEST_MODE, OptionTest.TestMode.TIMESTAMP, nestedRule)
                        .build();

        Map<String, Object> config = new HashMap<>();
        config.put(TEST_MODE.key(), "timestamp");
        OptionValidationException ex =
                assertThrows(OptionValidationException.class, () -> validate(config, rule));
        String msg = ex.getMessage();
        Assertions.assertTrue(msg.contains("1 error)"));
        Assertions.assertTrue(msg.contains("type: required"));
        Assertions.assertFalse(
                msg.contains("> 0"),
                "value constraint should be suppressed for absent conditional option");
    }

    @Test
    public void testErrorCodePrefixInUnifiedFormat() {
        OptionRule rule = OptionRule.builder().required(HOST).build();

        Map<String, Object> config = new HashMap<>();
        OptionValidationException ex =
                assertThrows(OptionValidationException.class, () -> validate(config, rule));
        Assertions.assertTrue(
                ex.getMessage().startsWith("ErrorCode:[API-02]"),
                "unified format should still carry standard ErrorCode prefix");
    }

    private static final Option<Map<String, String>> MAP_OPTION =
            Options.key("properties").mapType().noDefaultValue().withDescription("test map option");

    @Test
    public void testMapNotEmpty() {
        OptionRule rule =
                OptionRule.builder().required(MAP_OPTION, mapNotEmpty(MAP_OPTION)).build();
        Map<String, Object> config = new HashMap<>();

        // empty map -> fail
        config.put(MAP_OPTION.key(), Collections.emptyMap());
        String msg =
                assertThrows(OptionValidationException.class, () -> validate(config, rule))
                        .getMessage();
        Assertions.assertTrue(msg.contains("properties"));
        Assertions.assertTrue(msg.contains("is not empty"));

        // non-empty map -> pass
        Map<String, String> props = new HashMap<>();
        props.put("k1", "v1");
        config.put(MAP_OPTION.key(), props);
        Assertions.assertDoesNotThrow(() -> validate(config, rule));
    }

    @Test
    public void testMapContainsKey() {
        OptionRule rule =
                OptionRule.builder()
                        .required(MAP_OPTION, mapContainsKey(MAP_OPTION, "bootstrap.servers"))
                        .build();
        Map<String, Object> config = new HashMap<>();

        // map without required key -> fail
        Map<String, String> props = new HashMap<>();
        props.put("group.id", "test-group");
        config.put(MAP_OPTION.key(), props);
        String msg =
                assertThrows(OptionValidationException.class, () -> validate(config, rule))
                        .getMessage();
        Assertions.assertTrue(msg.contains("properties"));
        Assertions.assertTrue(msg.contains("contains key"));

        // map with required key -> pass
        props.put("bootstrap.servers", "localhost:9092");
        config.put(MAP_OPTION.key(), props);
        Assertions.assertDoesNotThrow(() -> validate(config, rule));
    }

    @Test
    public void testMapContainsKeys() {
        OptionRule rule =
                OptionRule.builder()
                        .required(
                                MAP_OPTION, mapContainsKeys(MAP_OPTION, "host", "port", "database"))
                        .build();
        Map<String, Object> config = new HashMap<>();

        // map missing some keys -> fail
        Map<String, String> props = new HashMap<>();
        props.put("host", "localhost");
        props.put("port", "3306");
        config.put(MAP_OPTION.key(), props);
        String msg =
                assertThrows(OptionValidationException.class, () -> validate(config, rule))
                        .getMessage();
        Assertions.assertTrue(msg.contains("properties"));
        Assertions.assertTrue(msg.contains("contains keys"));

        // map with all required keys -> pass
        props.put("database", "mydb");
        config.put(MAP_OPTION.key(), props);
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        // map with extra keys beyond required -> still pass
        props.put("username", "root");
        config.put(MAP_OPTION.key(), props);
        Assertions.assertDoesNotThrow(() -> validate(config, rule));
    }

    @Test
    public void testMapContainsKeyWithNullValue() {
        OptionRule rule =
                OptionRule.builder()
                        .required(MAP_OPTION, mapContainsKey(MAP_OPTION, "token"))
                        .build();
        Map<String, Object> config = new HashMap<>();

        // non-map value -> fail
        config.put(MAP_OPTION.key(), "not-a-map");
        assertThrows(Exception.class, () -> validate(config, rule));

        // map with the key but null value -> still pass (containsKey only checks key presence)
        Map<String, String> props = new HashMap<>();
        props.put("token", null);
        config.put(MAP_OPTION.key(), props);
        Assertions.assertDoesNotThrow(() -> validate(config, rule));
    }

    @Test
    public void testMapNotEmptyAndContainsKeyCombined() {
        OptionRule rule =
                OptionRule.builder()
                        .required(
                                MAP_OPTION,
                                mapNotEmpty(MAP_OPTION)
                                        .and(mapContainsKey(MAP_OPTION, "bootstrap.servers")))
                        .build();
        Map<String, Object> config = new HashMap<>();

        // empty map -> fail (mapNotEmpty)
        config.put(MAP_OPTION.key(), Collections.emptyMap());
        assertThrows(OptionValidationException.class, () -> validate(config, rule));

        // non-empty map without required key -> fail (containsKey)
        Map<String, String> props = new HashMap<>();
        props.put("group.id", "test");
        config.put(MAP_OPTION.key(), props);
        assertThrows(OptionValidationException.class, () -> validate(config, rule));

        // non-empty map with required key -> pass
        props.put("bootstrap.servers", "localhost:9092");
        config.put(MAP_OPTION.key(), props);
        Assertions.assertDoesNotThrow(() -> validate(config, rule));
    }

    @Test
    public void testExclusiveWithOptionalValueConstraint() {
        OptionRule rule =
                OptionRule.builder()
                        .exclusive(TEST_TOPIC_PATTERN, TEST_TOPIC)
                        .optional(TEST_TOPIC_PATTERN, Conditions.notBlank(TEST_TOPIC_PATTERN))
                        .optional(TEST_TOPIC, notEmpty(TEST_TOPIC))
                        .build();

        // neither present -> fails exclusive check
        Map<String, Object> config1 = new HashMap<>();
        assertThrows(OptionValidationException.class, () -> validate(config1, rule));

        // one present with valid value -> pass
        Map<String, Object> config2 = new HashMap<>();
        config2.put(TEST_TOPIC_PATTERN.key(), "pattern.*");
        Assertions.assertDoesNotThrow(() -> validate(config2, rule));

        // one present with empty value -> fails value constraint
        Map<String, Object> config3 = new HashMap<>();
        config3.put(TEST_TOPIC_PATTERN.key(), "");
        assertThrows(OptionValidationException.class, () -> validate(config3, rule));

        // both present -> fails exclusive check
        Map<String, Object> config4 = new HashMap<>();
        config4.put(TEST_TOPIC_PATTERN.key(), "pattern.*");
        config4.put(TEST_TOPIC.key(), Arrays.asList("t1", "t2"));
        assertThrows(OptionValidationException.class, () -> validate(config4, rule));

        // list option present with empty list -> fails value constraint
        Map<String, Object> config5 = new HashMap<>();
        config5.put(TEST_TOPIC.key(), Collections.emptyList());
        assertThrows(OptionValidationException.class, () -> validate(config5, rule));

        // list option present with valid list -> pass
        Map<String, Object> config6 = new HashMap<>();
        config6.put(TEST_TOPIC.key(), Collections.singletonList("topic1"));
        Assertions.assertDoesNotThrow(() -> validate(config6, rule));
    }

    @Test
    public void testBundledWithOptionalValueConstraint() {
        OptionRule rule =
                OptionRule.builder()
                        .bundled(TEST_TOPIC_PATTERN, TEST_TOPIC)
                        .optional(TEST_TOPIC_PATTERN, Conditions.notBlank(TEST_TOPIC_PATTERN))
                        .optional(TEST_TOPIC, notEmpty(TEST_TOPIC))
                        .build();

        // neither present -> pass (bundled options are optional as a group)
        Map<String, Object> config1 = new HashMap<>();
        Assertions.assertDoesNotThrow(() -> validate(config1, rule));

        // both present with valid values -> pass
        Map<String, Object> config2 = new HashMap<>();
        config2.put(TEST_TOPIC_PATTERN.key(), "pattern.*");
        config2.put(TEST_TOPIC.key(), Collections.singletonList("t1"));
        Assertions.assertDoesNotThrow(() -> validate(config2, rule));

        // only one present -> fails bundled check
        Map<String, Object> config3 = new HashMap<>();
        config3.put(TEST_TOPIC_PATTERN.key(), "pattern.*");
        assertThrows(OptionValidationException.class, () -> validate(config3, rule));

        // both present but one has empty value -> fails value constraint
        Map<String, Object> config4 = new HashMap<>();
        config4.put(TEST_TOPIC_PATTERN.key(), "pattern.*");
        config4.put(TEST_TOPIC.key(), Collections.emptyList());
        assertThrows(OptionValidationException.class, () -> validate(config4, rule));
    }

    static final Option<Integer> EXT_PORT =
            Options.key("ext.port").intType().noDefaultValue().withDescription("port");

    static final Option<Integer> OPT_PORT =
            Options.key("opt.port").intType().defaultValue(8080).withDescription("optional port");

    static class PortRangeExtension implements ConditionExtension<Integer> {
        @Override
        public String description() {
            return "must be between 1 and 65535";
        }

        @Override
        public boolean evaluate(ReadonlyConfig config, Integer value)
                throws OptionValidationException {
            return value != null && value >= 1 && value <= 65535;
        }
    }

    @Test
    public void testExtensionRequiredPass() {
        OptionRule rule =
                OptionRule.builder()
                        .required(
                                EXT_PORT, Conditions.extension(EXT_PORT, new PortRangeExtension()))
                        .build();
        Map<String, Object> config = new HashMap<>();
        config.put("ext.port", 8080);
        Assertions.assertDoesNotThrow(() -> validate(config, rule));
    }

    @Test
    public void testExtensionRequiredFail() {
        OptionRule rule =
                OptionRule.builder()
                        .required(
                                EXT_PORT, Conditions.extension(EXT_PORT, new PortRangeExtension()))
                        .build();
        Map<String, Object> config = new HashMap<>();
        config.put("ext.port", 99999);
        assertThrows(OptionValidationException.class, () -> validate(config, rule));
    }

    @Test
    public void testExtensionOptionalAbsentSkip() {
        OptionRule rule =
                OptionRule.builder()
                        .optional(
                                OPT_PORT, Conditions.extension(OPT_PORT, new PortRangeExtension()))
                        .build();
        Map<String, Object> config = new HashMap<>();
        Assertions.assertDoesNotThrow(() -> validate(config, rule));
    }

    @Test
    public void testExtensionOptionalPresentPass() {
        OptionRule rule =
                OptionRule.builder()
                        .optional(
                                OPT_PORT, Conditions.extension(OPT_PORT, new PortRangeExtension()))
                        .build();
        Map<String, Object> config = new HashMap<>();
        config.put("opt.port", 443);
        Assertions.assertDoesNotThrow(() -> validate(config, rule));
    }

    @Test
    public void testExtensionOptionalPresentFail() {
        OptionRule rule =
                OptionRule.builder()
                        .optional(
                                OPT_PORT, Conditions.extension(OPT_PORT, new PortRangeExtension()))
                        .build();
        Map<String, Object> config = new HashMap<>();
        config.put("opt.port", 0);
        assertThrows(OptionValidationException.class, () -> validate(config, rule));
    }

    @Test
    public void testExtensionAndBuiltinCombined() {
        Condition<Integer> combined =
                greaterOrEqual(EXT_PORT, 1)
                        .and(Conditions.extension(EXT_PORT, new PortRangeExtension()));
        OptionRule rule = OptionRule.builder().required(EXT_PORT, combined).build();

        Map<String, Object> pass = new HashMap<>();
        pass.put("ext.port", 80);
        Assertions.assertDoesNotThrow(() -> validate(pass, rule));

        Map<String, Object> fail = new HashMap<>();
        fail.put("ext.port", 70000);
        assertThrows(OptionValidationException.class, () -> validate(fail, rule));
    }

    @Test
    public void testExtensionOrBuiltinCombined() {
        ConditionExtension<Integer> isWellKnown =
                new ConditionExtension<Integer>() {
                    @Override
                    public String description() {
                        return "is a well-known port (1-1023)";
                    }

                    @Override
                    public boolean evaluate(ReadonlyConfig config, Integer value)
                            throws OptionValidationException {
                        return value != null && value >= 1 && value <= 1023;
                    }
                };
        Condition<Integer> combined =
                Conditions.extension(EXT_PORT, isWellKnown)
                        .or(Condition.of(EXT_PORT, ConditionOperator.EQUAL, 8080));
        OptionRule rule = OptionRule.builder().required(EXT_PORT, combined).build();

        Map<String, Object> passWellKnown = new HashMap<>();
        passWellKnown.put("ext.port", 443);
        Assertions.assertDoesNotThrow(() -> validate(passWellKnown, rule));

        Map<String, Object> passExact = new HashMap<>();
        passExact.put("ext.port", 8080);
        Assertions.assertDoesNotThrow(() -> validate(passExact, rule));

        Map<String, Object> fail = new HashMap<>();
        fail.put("ext.port", 5000);
        assertThrows(OptionValidationException.class, () -> validate(fail, rule));
    }

    @Test
    public void testExtensionConditionToString() {
        Condition<Integer> cond = Conditions.extension(EXT_PORT, new PortRangeExtension());
        assertEquals("'ext.port' must be between 1 and 65535", cond.toString());
    }

    @Test
    public void testExtensionConditionEquals() {
        Condition<Integer> a = Conditions.extension(EXT_PORT, new PortRangeExtension());
        Condition<Integer> b = Conditions.extension(EXT_PORT, new PortRangeExtension());
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());

        Condition<Integer> c =
                Conditions.extension(
                        EXT_PORT,
                        new ConditionExtension<Integer>() {
                            @Override
                            public String description() {
                                return "different impl";
                            }

                            @Override
                            public boolean evaluate(ReadonlyConfig config, Integer value) {
                                return value != null;
                            }
                        });
        assertEquals(a, c);
        assertEquals(a.hashCode(), c.hashCode());
    }

    static final Option<List<Map<String, Object>>> LIST_MAP_OPTION =
            Options.key("rules")
                    .type(new TypeReference<List<Map<String, Object>>>() {})
                    .noDefaultValue()
                    .withDescription("list of rule maps");

    static final Option<Map<String, List<Map<String, Object>>>> NESTED_MAP_OPTION =
            Options.key("nested.config")
                    .type(new TypeReference<Map<String, List<Map<String, Object>>>>() {})
                    .noDefaultValue()
                    .withDescription("nested map of list of maps");

    static class ListMapStructureExtension
            implements ConditionExtension<List<Map<String, Object>>> {
        @Override
        public String description() {
            return "each rule must contain 'field' and 'type' keys";
        }

        @Override
        public boolean evaluate(ReadonlyConfig config, List<Map<String, Object>> value)
                throws OptionValidationException {
            if (value == null || value.isEmpty()) return false;
            for (Map<String, Object> rule : value) {
                if (!rule.containsKey("field") || !rule.containsKey("type")) {
                    return false;
                }
            }
            return true;
        }
    }

    static class NestedMapExtension
            implements ConditionExtension<Map<String, List<Map<String, Object>>>> {
        @Override
        public String description() {
            return "each group must have non-empty rules with 'name' key";
        }

        @Override
        public boolean evaluate(ReadonlyConfig config, Map<String, List<Map<String, Object>>> value)
                throws OptionValidationException {
            if (value == null || value.isEmpty()) return false;
            for (Map.Entry<String, List<Map<String, Object>>> entry : value.entrySet()) {
                List<Map<String, Object>> rules = entry.getValue();
                if (rules == null || rules.isEmpty()) return false;
                for (Map<String, Object> rule : rules) {
                    if (!rule.containsKey("name")) return false;
                }
            }
            return true;
        }
    }

    @Test
    public void testExtensionListMapValidStructure() {
        OptionRule rule =
                OptionRule.builder()
                        .required(
                                LIST_MAP_OPTION,
                                Conditions.extension(
                                        LIST_MAP_OPTION, new ListMapStructureExtension()))
                        .build();

        Map<String, Object> ruleItem1 = new HashMap<>();
        ruleItem1.put("field", "name");
        ruleItem1.put("type", "string");
        Map<String, Object> ruleItem2 = new HashMap<>();
        ruleItem2.put("field", "age");
        ruleItem2.put("type", "int");

        Map<String, Object> config = new HashMap<>();
        config.put("rules", Arrays.asList(ruleItem1, ruleItem2));
        Assertions.assertDoesNotThrow(() -> validate(config, rule));
    }

    @Test
    public void testExtensionListMapMissingKey() {
        OptionRule rule =
                OptionRule.builder()
                        .required(
                                LIST_MAP_OPTION,
                                Conditions.extension(
                                        LIST_MAP_OPTION, new ListMapStructureExtension()))
                        .build();

        Map<String, Object> ruleItem = new HashMap<>();
        ruleItem.put("field", "name");

        Map<String, Object> config = new HashMap<>();
        config.put("rules", Collections.singletonList(ruleItem));
        assertThrows(OptionValidationException.class, () -> validate(config, rule));
    }

    @Test
    public void testExtensionListMapEmptyList() {
        OptionRule rule =
                OptionRule.builder()
                        .required(
                                LIST_MAP_OPTION,
                                Conditions.extension(
                                        LIST_MAP_OPTION, new ListMapStructureExtension()))
                        .build();

        Map<String, Object> config = new HashMap<>();
        config.put("rules", Collections.emptyList());
        assertThrows(OptionValidationException.class, () -> validate(config, rule));
    }

    @Test
    public void testExtensionListMapNullValue() {
        OptionRule rule =
                OptionRule.builder()
                        .required(
                                LIST_MAP_OPTION,
                                Conditions.extension(
                                        LIST_MAP_OPTION, new ListMapStructureExtension()))
                        .build();

        Map<String, Object> config = new HashMap<>();
        config.put("rules", null);
        assertThrows(OptionValidationException.class, () -> validate(config, rule));
    }

    @Test
    public void testExtensionListMapSingleItemValid() {
        OptionRule rule =
                OptionRule.builder()
                        .required(
                                LIST_MAP_OPTION,
                                Conditions.extension(
                                        LIST_MAP_OPTION, new ListMapStructureExtension()))
                        .build();

        Map<String, Object> item = new HashMap<>();
        item.put("field", "id");
        item.put("type", "long");

        Map<String, Object> config = new HashMap<>();
        config.put("rules", Collections.singletonList(item));
        Assertions.assertDoesNotThrow(() -> validate(config, rule));
    }

    @Test
    public void testExtensionListMapPartialInvalid() {
        OptionRule rule =
                OptionRule.builder()
                        .required(
                                LIST_MAP_OPTION,
                                Conditions.extension(
                                        LIST_MAP_OPTION, new ListMapStructureExtension()))
                        .build();

        Map<String, Object> good = new HashMap<>();
        good.put("field", "id");
        good.put("type", "long");
        Map<String, Object> bad = new HashMap<>();
        bad.put("field", "name");

        Map<String, Object> config = new HashMap<>();
        config.put("rules", Arrays.asList(good, bad));
        assertThrows(OptionValidationException.class, () -> validate(config, rule));
    }

    @Test
    public void testExtensionNestedMapValid() {
        OptionRule rule =
                OptionRule.builder()
                        .required(
                                NESTED_MAP_OPTION,
                                Conditions.extension(NESTED_MAP_OPTION, new NestedMapExtension()))
                        .build();

        Map<String, Object> item1 = new HashMap<>();
        item1.put("name", "rule1");
        Map<String, Object> item2 = new HashMap<>();
        item2.put("name", "rule2");

        Map<String, Object> nested = new HashMap<>();
        nested.put("group_a", Arrays.asList(item1, item2));

        Map<String, Object> config = new HashMap<>();
        config.put("nested.config", nested);
        Assertions.assertDoesNotThrow(() -> validate(config, rule));
    }

    @Test
    public void testExtensionNestedMapEmptyGroup() {
        OptionRule rule =
                OptionRule.builder()
                        .required(
                                NESTED_MAP_OPTION,
                                Conditions.extension(NESTED_MAP_OPTION, new NestedMapExtension()))
                        .build();

        Map<String, Object> nested = new HashMap<>();
        nested.put("group_a", Collections.emptyList());

        Map<String, Object> config = new HashMap<>();
        config.put("nested.config", nested);
        assertThrows(OptionValidationException.class, () -> validate(config, rule));
    }

    @Test
    public void testExtensionNestedMapMissingNameKey() {
        OptionRule rule =
                OptionRule.builder()
                        .required(
                                NESTED_MAP_OPTION,
                                Conditions.extension(NESTED_MAP_OPTION, new NestedMapExtension()))
                        .build();

        Map<String, Object> item = new HashMap<>();
        item.put("value", "something");
        Map<String, Object> nested = new HashMap<>();
        nested.put("group_a", Collections.singletonList(item));

        Map<String, Object> config = new HashMap<>();
        config.put("nested.config", nested);
        assertThrows(OptionValidationException.class, () -> validate(config, rule));
    }

    @Test
    public void testExtensionNestedMapEmptyOuter() {
        OptionRule rule =
                OptionRule.builder()
                        .required(
                                NESTED_MAP_OPTION,
                                Conditions.extension(NESTED_MAP_OPTION, new NestedMapExtension()))
                        .build();

        Map<String, Object> config = new HashMap<>();
        config.put("nested.config", Collections.emptyMap());
        assertThrows(OptionValidationException.class, () -> validate(config, rule));
    }

    @Test
    public void testExtensionNestedMapMultiGroupPartialInvalid() {
        OptionRule rule =
                OptionRule.builder()
                        .required(
                                NESTED_MAP_OPTION,
                                Conditions.extension(NESTED_MAP_OPTION, new NestedMapExtension()))
                        .build();

        Map<String, Object> good = new HashMap<>();
        good.put("name", "ok");
        Map<String, Object> bad = new HashMap<>();
        bad.put("other", "missing name");
        Map<String, Object> nested = new HashMap<>();
        nested.put("group_a", Collections.singletonList(good));
        nested.put("group_b", Collections.singletonList(bad));

        Map<String, Object> config = new HashMap<>();
        config.put("nested.config", nested);
        assertThrows(OptionValidationException.class, () -> validate(config, rule));
    }

    @Test
    public void testExtensionThrowsOptionValidationException() {
        OptionRule rule =
                OptionRule.builder()
                        .required(
                                EXT_PORT,
                                Conditions.extension(
                                        EXT_PORT,
                                        new ConditionExtension<Integer>() {
                                            @Override
                                            public String description() {
                                                return "must be even";
                                            }

                                            @Override
                                            public boolean evaluate(
                                                    ReadonlyConfig config, Integer value)
                                                    throws OptionValidationException {
                                                if (value != null && value % 2 != 0) {
                                                    throw new OptionValidationException(
                                                            "Value %d is odd, must be even", value);
                                                }
                                                return value != null;
                                            }
                                        }))
                        .build();

        Map<String, Object> pass = new HashMap<>();
        pass.put("ext.port", 80);
        Assertions.assertDoesNotThrow(() -> validate(pass, rule));

        Map<String, Object> fail = new HashMap<>();
        fail.put("ext.port", 81);
        OptionValidationException ex =
                assertThrows(OptionValidationException.class, () -> validate(fail, rule));
        Assertions.assertTrue(ex.getMessage().contains("odd"));
    }

    @Test
    public void testExtensionListMapOptionalAbsent() {
        OptionRule rule =
                OptionRule.builder()
                        .optional(
                                LIST_MAP_OPTION,
                                Conditions.extension(
                                        LIST_MAP_OPTION, new ListMapStructureExtension()))
                        .build();

        Map<String, Object> config = new HashMap<>();
        Assertions.assertDoesNotThrow(() -> validate(config, rule));
    }

    @Test
    public void testExtensionListMapToString() {
        Condition<List<Map<String, Object>>> cond =
                Conditions.extension(LIST_MAP_OPTION, new ListMapStructureExtension());
        assertEquals("'rules' each rule must contain 'field' and 'type' keys", cond.toString());
    }

    @Test
    public void testExtensionNestedMapToString() {
        Condition<Map<String, List<Map<String, Object>>>> cond =
                Conditions.extension(NESTED_MAP_OPTION, new NestedMapExtension());
        assertEquals(
                "'nested.config' each group must have non-empty rules with 'name' key",
                cond.toString());
    }

    @Test
    public void testExtensionListMapAndBuiltinChain() {
        Condition<List<Map<String, Object>>> combined =
                notEmpty(LIST_MAP_OPTION)
                        .and(
                                Conditions.extension(
                                        LIST_MAP_OPTION, new ListMapStructureExtension()));
        OptionRule rule = OptionRule.builder().required(LIST_MAP_OPTION, combined).build();

        Map<String, Object> config = new HashMap<>();
        config.put("rules", Collections.emptyList());
        assertThrows(OptionValidationException.class, () -> validate(config, rule));

        Map<String, Object> good = new HashMap<>();
        good.put("field", "x");
        good.put("type", "y");
        Map<String, Object> config2 = new HashMap<>();
        config2.put("rules", Collections.singletonList(good));
        Assertions.assertDoesNotThrow(() -> validate(config2, rule));
    }

    @Test
    public void testExtensionAndExtensionChain() {
        ConditionExtension<Integer> positiveExt =
                new ConditionExtension<Integer>() {
                    @Override
                    public String description() {
                        return "must be positive";
                    }

                    @Override
                    public boolean evaluate(ReadonlyConfig config, Integer value) {
                        return value != null && value > 0;
                    }
                };
        ConditionExtension<Integer> evenExt =
                new ConditionExtension<Integer>() {
                    @Override
                    public String description() {
                        return "must be even";
                    }

                    @Override
                    public boolean evaluate(ReadonlyConfig config, Integer value) {
                        return value != null && value % 2 == 0;
                    }
                };
        Condition<Integer> combined =
                Conditions.extension(EXT_PORT, positiveExt)
                        .and(Conditions.extension(EXT_PORT, evenExt));
        OptionRule rule = OptionRule.builder().required(EXT_PORT, combined).build();

        Map<String, Object> pass = new HashMap<>();
        pass.put("ext.port", 80);
        Assertions.assertDoesNotThrow(() -> validate(pass, rule));

        // positive but odd -> fail
        Map<String, Object> failOdd = new HashMap<>();
        failOdd.put("ext.port", 81);
        assertThrows(OptionValidationException.class, () -> validate(failOdd, rule));

        // even but negative -> fail
        Map<String, Object> failNeg = new HashMap<>();
        failNeg.put("ext.port", -2);
        assertThrows(OptionValidationException.class, () -> validate(failNeg, rule));
    }

    @Test
    public void testExtensionOrExtensionChain() {
        ConditionExtension<Integer> wellKnownExt =
                new ConditionExtension<Integer>() {
                    @Override
                    public String description() {
                        return "well-known port (1-1023)";
                    }

                    @Override
                    public boolean evaluate(ReadonlyConfig config, Integer value) {
                        return value != null && value >= 1 && value <= 1023;
                    }
                };
        ConditionExtension<Integer> highPortExt =
                new ConditionExtension<Integer>() {
                    @Override
                    public String description() {
                        return "high port (49152-65535)";
                    }

                    @Override
                    public boolean evaluate(ReadonlyConfig config, Integer value) {
                        return value != null && value >= 49152 && value <= 65535;
                    }
                };
        Condition<Integer> combined =
                Conditions.extension(EXT_PORT, wellKnownExt)
                        .or(Conditions.extension(EXT_PORT, highPortExt));
        OptionRule rule = OptionRule.builder().required(EXT_PORT, combined).build();

        Map<String, Object> passWellKnown = new HashMap<>();
        passWellKnown.put("ext.port", 443);
        Assertions.assertDoesNotThrow(() -> validate(passWellKnown, rule));

        Map<String, Object> passHigh = new HashMap<>();
        passHigh.put("ext.port", 50000);
        Assertions.assertDoesNotThrow(() -> validate(passHigh, rule));

        // middle range -> fail both
        Map<String, Object> fail = new HashMap<>();
        fail.put("ext.port", 8080);
        assertThrows(OptionValidationException.class, () -> validate(fail, rule));
    }

    @Test
    public void testExtensionWithExclusiveOptional() {
        ConditionExtension<String> patternExt =
                new ConditionExtension<String>() {
                    @Override
                    public String description() {
                        return "must start with a letter";
                    }

                    @Override
                    public boolean evaluate(ReadonlyConfig config, String value) {
                        return value != null
                                && !value.isEmpty()
                                && Character.isLetter(value.charAt(0));
                    }
                };
        OptionRule rule =
                OptionRule.builder()
                        .exclusive(TEST_TOPIC_PATTERN, TEST_TOPIC)
                        .optional(
                                TEST_TOPIC_PATTERN,
                                Conditions.extension(TEST_TOPIC_PATTERN, patternExt))
                        .optional(TEST_TOPIC, notEmpty(TEST_TOPIC))
                        .build();

        // neither present -> fails exclusive
        Map<String, Object> config1 = new HashMap<>();
        assertThrows(OptionValidationException.class, () -> validate(config1, rule));

        // pattern present with valid value -> pass
        Map<String, Object> config2 = new HashMap<>();
        config2.put(TEST_TOPIC_PATTERN.key(), "topic.*");
        Assertions.assertDoesNotThrow(() -> validate(config2, rule));

        // pattern present but starts with digit -> fails extension
        Map<String, Object> config3 = new HashMap<>();
        config3.put(TEST_TOPIC_PATTERN.key(), "123topic");
        assertThrows(OptionValidationException.class, () -> validate(config3, rule));

        // both present -> fails exclusive
        Map<String, Object> config4 = new HashMap<>();
        config4.put(TEST_TOPIC_PATTERN.key(), "topic.*");
        config4.put(TEST_TOPIC.key(), Collections.singletonList("t1"));
        assertThrows(OptionValidationException.class, () -> validate(config4, rule));

        // topic present with valid list -> pass
        Map<String, Object> config5 = new HashMap<>();
        config5.put(TEST_TOPIC.key(), Arrays.asList("t1", "t2"));
        Assertions.assertDoesNotThrow(() -> validate(config5, rule));
    }

    @Test
    public void testExtensionWithBundledOptional() {
        ConditionExtension<String> patternExt =
                new ConditionExtension<String>() {
                    @Override
                    public String description() {
                        return "must contain a wildcard";
                    }

                    @Override
                    public boolean evaluate(ReadonlyConfig config, String value) {
                        return value != null && value.contains("*");
                    }
                };
        OptionRule rule =
                OptionRule.builder()
                        .bundled(TEST_TOPIC_PATTERN, TEST_TOPIC)
                        .optional(
                                TEST_TOPIC_PATTERN,
                                Conditions.extension(TEST_TOPIC_PATTERN, patternExt))
                        .optional(TEST_TOPIC, notEmpty(TEST_TOPIC))
                        .build();

        // neither present -> pass (bundled group absent)
        Map<String, Object> config1 = new HashMap<>();
        Assertions.assertDoesNotThrow(() -> validate(config1, rule));

        // both present with valid values -> pass
        Map<String, Object> config2 = new HashMap<>();
        config2.put(TEST_TOPIC_PATTERN.key(), "topic.*");
        config2.put(TEST_TOPIC.key(), Collections.singletonList("t1"));
        Assertions.assertDoesNotThrow(() -> validate(config2, rule));

        // only one present -> fails bundled
        Map<String, Object> config3 = new HashMap<>();
        config3.put(TEST_TOPIC_PATTERN.key(), "topic.*");
        assertThrows(OptionValidationException.class, () -> validate(config3, rule));

        // both present but pattern has no wildcard -> fails extension
        Map<String, Object> config4 = new HashMap<>();
        config4.put(TEST_TOPIC_PATTERN.key(), "topic-fixed");
        config4.put(TEST_TOPIC.key(), Collections.singletonList("t1"));
        assertThrows(OptionValidationException.class, () -> validate(config4, rule));

        // both present but topic is empty -> fails notEmpty
        Map<String, Object> config5 = new HashMap<>();
        config5.put(TEST_TOPIC_PATTERN.key(), "topic.*");
        config5.put(TEST_TOPIC.key(), Collections.emptyList());
        assertThrows(OptionValidationException.class, () -> validate(config5, rule));
    }

    // ==================== collectErrors contract tests ====================

    @Test
    public void testMultipleSingleChoiceErrorsCollected() {
        Option<String> choice1 =
                Options.key("mode1")
                        .singleChoice(String.class, Arrays.asList("A", "B", "C"))
                        .defaultValue("A")
                        .withDescription("mode1");
        Option<String> choice2 =
                Options.key("mode2")
                        .singleChoice(String.class, Arrays.asList("X", "Y", "Z"))
                        .defaultValue("X")
                        .withDescription("mode2");

        OptionRule rule = OptionRule.builder().required(choice1, choice2).build();
        Map<String, Object> config = new HashMap<>();
        config.put("mode1", "INVALID1");
        config.put("mode2", "INVALID2");

        OptionValidationException ex =
                assertThrows(OptionValidationException.class, () -> validate(config, rule));
        String msg = ex.getMessage();
        Assertions.assertTrue(msg.contains("mode1"), "Should report mode1 error: " + msg);
        Assertions.assertTrue(msg.contains("mode2"), "Should report mode2 error: " + msg);
        Assertions.assertTrue(
                msg.contains("INVALID1") && msg.contains("INVALID2"),
                "Should report both invalid values: " + msg);
    }

    @Test
    public void testMixedErrorTypesAllCollected() {
        Option<String> choice =
                Options.key("format")
                        .singleChoice(String.class, Arrays.asList("json", "csv", "avro"))
                        .defaultValue("json")
                        .withDescription("format");
        Option<String> host =
                Options.key("host").stringType().noDefaultValue().withDescription("host");
        Option<Integer> port =
                Options.key("port").intType().noDefaultValue().withDescription("port");

        OptionRule rule =
                OptionRule.builder()
                        .required(choice, host)
                        .required(port, greaterOrEqual(port, 1))
                        .build();

        Map<String, Object> config = new HashMap<>();
        config.put("format", "xml");
        config.put("port", 0);

        OptionValidationException ex =
                assertThrows(OptionValidationException.class, () -> validate(config, rule));
        String msg = ex.getMessage();
        Assertions.assertTrue(msg.contains("host"), "Should report missing host: " + msg);
        Assertions.assertTrue(
                msg.contains("format") && msg.contains("singleChoice"),
                "Should report single_choice error: " + msg);
        Assertions.assertTrue(
                msg.contains("port") && msg.contains("value"),
                "Should report value constraint error: " + msg);
    }

    @Test
    public void testExtensionExceptionIsAggregatedNotFailFast() {
        ConditionExtension<Integer> throwingExtension =
                new ConditionExtension<Integer>() {
                    @Override
                    public String description() {
                        return "must be positive";
                    }

                    @Override
                    public boolean evaluate(ReadonlyConfig config, Integer value)
                            throws OptionValidationException {
                        if (value != null && value <= 0) {
                            throw new OptionValidationException(
                                    "port value %d is not positive", value);
                        }
                        return true;
                    }
                };

        OptionRule rule =
                OptionRule.builder()
                        .required(HOST, notBlank(HOST))
                        .required(PORT, Conditions.extension(PORT, throwingExtension))
                        .build();

        Map<String, Object> config = new HashMap<>();
        config.put(HOST.key(), "");
        config.put(PORT.key(), -1);

        OptionValidationException ex =
                Assertions.assertThrows(
                        OptionValidationException.class,
                        () -> ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(rule));
        String msg = ex.getMessage();
        Assertions.assertTrue(msg.contains("2 errors"), "both errors should be aggregated: " + msg);
        Assertions.assertTrue(
                msg.contains("host") && msg.contains("is not blank"),
                "host notBlank error should appear: " + msg);
        Assertions.assertTrue(
                msg.contains("port value -1 is not positive"),
                "extension exception message should be preserved: " + msg);
    }
}
