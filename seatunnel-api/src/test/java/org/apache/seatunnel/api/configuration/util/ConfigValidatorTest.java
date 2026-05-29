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
        assertEquals(
                "ErrorCode:[API-02], ErrorDescription:[Option item validate failed] - There are unconfigured options, the options('username', 'password') are required.",
                assertThrows(OptionValidationException.class, executable).getMessage());

        config.put(KEY_USERNAME.key(), "asuka");
        assertEquals(
                "ErrorCode:[API-02], ErrorDescription:[Option item validate failed] - There are unconfigured options, the options('password') are required.",
                assertThrows(OptionValidationException.class, executable).getMessage());

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
        assertEquals(
                "ErrorCode:[API-02], ErrorDescription:[Option item validate failed] - These options('username', 'password') are bundled, must be present or absent together."
                        + " The options present are: 'username'. The options absent are 'password'.",
                assertThrows(OptionValidationException.class, executable).getMessage());

        // case2: all present
        config.put(KEY_PASSWORD.key(), "saitou");
        Assertions.assertDoesNotThrow(executable);
    }

    @Test
    public void testSimpleExclusiveRequiredOptions() {
        OptionRule rule = OptionRule.builder().exclusive(TEST_TOPIC_PATTERN, TEST_TOPIC).build();
        Map<String, Object> config = new HashMap<>();
        Executable executable = () -> validate(config, rule);

        // all absent
        assertEquals(
                "ErrorCode:[API-02], ErrorDescription:[Option item validate failed] - There are unconfigured options, these options('option.topic-pattern', 'option.topic') are mutually exclusive,"
                        + " allowing only one set(\"[] for a set\") of options to be configured.",
                assertThrows(OptionValidationException.class, executable).getMessage());

        // only one present
        config.put(TEST_TOPIC_PATTERN.key(), "asuka");
        Assertions.assertDoesNotThrow(executable);

        // present > 1
        config.put(TEST_TOPIC.key(), "[\"saitou\"]");
        assertEquals(
                "ErrorCode:[API-02], ErrorDescription:[Option item validate failed] - These options('option.topic-pattern', 'option.topic') are mutually exclusive, "
                        + "allowing only one set(\"[] for a set\") of options to be configured.",
                assertThrows(OptionValidationException.class, executable).getMessage());
    }

    @Test
    public void testComplexExclusiveRequiredOptions() {
        OptionRule rule =
                OptionRule.builder().exclusive(KEY_BEARER_TOKEN, KEY_KERBEROS_TICKET).build();

        Map<String, Object> config = new HashMap<>();
        Executable executable = () -> validate(config, rule);

        // all absent
        assertEquals(
                "ErrorCode:[API-02], ErrorDescription:[Option item validate failed] - There are unconfigured options, these options('bearer-token', 'kerberos-ticket') are mutually exclusive,"
                        + " allowing only one set(\"[] for a set\") of options to be configured.",
                assertThrows(OptionValidationException.class, executable).getMessage());

        // set one
        config.put(KEY_BEARER_TOKEN.key(), "ashulin");
        Assertions.assertDoesNotThrow(executable);

        // all set
        config.put(KEY_KERBEROS_TICKET.key(), "zongwen");
        assertEquals(
                "ErrorCode:[API-02], ErrorDescription:[Option item validate failed] - These options('bearer-token', 'kerberos-ticket') are mutually exclusive,"
                        + " allowing only one set(\"[] for a set\") of options to be configured.",
                assertThrows(OptionValidationException.class, executable).getMessage());
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
        assertEquals(
                "ErrorCode:[API-02], ErrorDescription:[Option item validate failed] - There are unconfigured options, the options('option.timestamp') are required"
                        + " because ['option.mode' == TIMESTAMP] is true.",
                assertThrows(OptionValidationException.class, executable).getMessage());

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
        assertEquals(
                "ErrorCode:[API-02], ErrorDescription:[Option item validate failed] - There are unconfigured options, the options('option.timestamp') are required"
                        + " because ['username' == ashulin] is true.",
                assertThrows(OptionValidationException.class, executable).getMessage());

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
        assertEquals(
                "ErrorCode:[API-02], ErrorDescription:[Option item validate failed] - There are unconfigured options, the options('option.timestamp') are required"
                        + " because ['username' == ashulin || 'username' == asuka] is true.",
                assertThrows(OptionValidationException.class, executable).getMessage());

        // 'username' == asuka, and required options absent
        config.put(KEY_USERNAME.key(), "asuka");
        assertEquals(
                "ErrorCode:[API-02], ErrorDescription:[Option item validate failed] - There are unconfigured options, the options('option.timestamp') are required"
                        + " because ['username' == ashulin || 'username' == asuka] is true.",
                assertThrows(OptionValidationException.class, executable).getMessage());

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
        assertEquals(
                "ErrorCode:[API-02], ErrorDescription:[Option item validate failed] - These options('single_choice_test') are SingleChoiceOption, the defaultValue(M) must be one of the optionValues([A, B, C]).",
                assertThrows(OptionValidationException.class, executable).getMessage());
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
        assertEquals(
                "ErrorCode:[API-02], ErrorDescription:[Option item validate failed] - These options('single_choice_test') are SingleChoiceOption, the value(N) must be one of the optionValues([A, B, C]).",
                assertThrows(OptionValidationException.class, executable).getMessage());
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
        assertEquals(
                "ErrorCode:[API-02], ErrorDescription:[Option item validate failed] - There are unconfigured options, the options('username', 'password') are required when ['single_choice_test' == A].",
                assertThrows(OptionValidationException.class, executable).getMessage());

        config.put(KEY_USERNAME.key(), "root");
        config.put(KEY_PASSWORD.key(), "111");
        executable = () -> validate(config, optionRule);
        Assertions.assertDoesNotThrow(executable);

        config.put(KEY_USERNAME.key(), "admin");
        executable = () -> validate(config, optionRule);
        assertEquals(
                "ErrorCode:[API-02], ErrorDescription:[Option item validate failed] - There are unconfigured options, the options('test_key') are required when ['username' == admin].",
                assertThrows(OptionValidationException.class, executable).getMessage());

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
        OptionRule subOption2 = OptionRule.builder().required(KEY_BEARER_TOKEN).build();
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
        assertEquals(
                "ErrorCode:[API-02], ErrorDescription:[Option item validate failed] - There are unconfigured options, the options('username', 'password') are required when ['single_choice_test' == A || 'single_choice_test' == B].",
                assertThrows(OptionValidationException.class, executable).getMessage());

        config.put(SINGLE_CHOICE_VALUE_TEST.key(), "B");
        executable = () -> validate(config, optionRule);
        assertEquals(
                "ErrorCode:[API-02], ErrorDescription:[Option item validate failed] - There are unconfigured options, the options('username', 'password') are required when ['single_choice_test' == A || 'single_choice_test' == B].",
                assertThrows(OptionValidationException.class, executable).getMessage());
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

    public static final Option<String> DELIMITER =
            Options.key("delimiter").stringType().noDefaultValue().withDescription("delimiter");

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

    public static final Option<List<String>> TAGS =
            Options.key("tags").listType().noDefaultValue().withDescription("tag list");

    @Test
    public void testGreaterThanValidation() {
        OptionRule rule =
                OptionRule.builder().required(PORT, Condition.greaterThan(PORT, 0)).build();

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
        OptionRule rule =
                OptionRule.builder().required(PORT, Condition.greaterOrEqual(PORT, 0)).build();

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
                        .required(
                                PORT,
                                Condition.greaterOrEqual(PORT, 1)
                                        .and(Condition.lessOrEqual(PORT, 65535)))
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
                        .required(
                                RATIO,
                                Condition.greaterThan(RATIO, 0.0)
                                        .and(Condition.lessOrEqual(RATIO, 1.0)))
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
        OptionRule rule = OptionRule.builder().required(HOST, Condition.notBlank(HOST)).build();

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
                        .required(ENDPOINT, Condition.startsWith(ENDPOINT, "jdbc:databend://"))
                        .build();

        Map<String, Object> config = new HashMap<>();
        config.put(ENDPOINT.key(), "jdbc:databend://localhost:8123");
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(ENDPOINT.key(), "jdbc:mysql://localhost:3306");
        assertThrows(OptionValidationException.class, () -> validate(config, rule));
    }

    @Test
    public void testStartsWithIgnoreCaseValidation() {
        Option<String> WHERE =
                Options.key("where").stringType().noDefaultValue().withDescription("where clause");
        OptionRule rule =
                OptionRule.builder()
                        .required(WHERE, Condition.startsWithIgnoreCase(WHERE, "where"))
                        .build();

        Map<String, Object> config = new HashMap<>();
        config.put(WHERE.key(), "WHERE id > 10");
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(WHERE.key(), "where name = 'test'");
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(WHERE.key(), "SELECT * FROM t");
        assertThrows(OptionValidationException.class, () -> validate(config, rule));
    }

    @Test
    public void testContainsValidation() {
        OptionRule rule =
                OptionRule.builder()
                        .optional(FILE_EXPR, Condition.contains(FILE_EXPR, "#{transactionId}"))
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
                OptionRule.builder()
                        .required(ENDPOINT, Condition.matches(ENDPOINT, "^[^:]+:\\d+$"))
                        .build();

        Map<String, Object> config = new HashMap<>();
        config.put(ENDPOINT.key(), "localhost:8080");
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(ENDPOINT.key(), "invalid-format");
        assertThrows(OptionValidationException.class, () -> validate(config, rule));
    }

    @Test
    public void testUpperCaseValidation() {
        OptionRule rule =
                OptionRule.builder().required(DB_NAME, Condition.upperCase(DB_NAME)).build();

        Map<String, Object> config = new HashMap<>();
        config.put(DB_NAME.key(), "ORACLE_DB");
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(DB_NAME.key(), "Oracle_DB");
        assertThrows(OptionValidationException.class, () -> validate(config, rule));
    }

    @Test
    public void testLowerCaseValidation() {
        OptionRule rule =
                OptionRule.builder().required(DB_NAME, Condition.lowerCase(DB_NAME)).build();

        Map<String, Object> config = new HashMap<>();
        config.put(DB_NAME.key(), "my_database");
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(DB_NAME.key(), "My_Database");
        assertThrows(OptionValidationException.class, () -> validate(config, rule));
    }

    @Test
    public void testLengthEqualValidation() {
        OptionRule rule =
                OptionRule.builder()
                        .required(DELIMITER, Condition.lengthEqual(DELIMITER, 1))
                        .build();

        Map<String, Object> config = new HashMap<>();
        config.put(DELIMITER.key(), ",");
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(DELIMITER.key(), "||");
        assertThrows(OptionValidationException.class, () -> validate(config, rule));
    }

    @Test
    public void testCrossFieldComparison() {
        OptionRule rule =
                OptionRule.builder()
                        .required(START_TS, END_TS, Condition.lessThanField(START_TS, END_TS))
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
                        .required(START_TS, END_TS, Condition.lessOrEqualField(START_TS, END_TS))
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
        OptionRule rule = OptionRule.builder().required(TAGS, Condition.notEmpty(TAGS)).build();

        Map<String, Object> config = new HashMap<>();
        config.put(TAGS.key(), Arrays.asList("tag1", "tag2"));
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(TAGS.key(), Collections.emptyList());
        assertThrows(OptionValidationException.class, () -> validate(config, rule));
    }

    @Test
    public void testUniqueCollectionValidation() {
        OptionRule rule = OptionRule.builder().required(TAGS, Condition.unique(TAGS)).build();

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
                        .optional(HOST, Condition.notBlank(HOST).or(Condition.notBlank(ENDPOINT)))
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
                OptionRule.builder()
                        .optional(ENDPOINT, Condition.matches(ENDPOINT, "^[^:]+:\\d+$"))
                        .build();

        Map<String, Object> config = new HashMap<>();
        Assertions.assertDoesNotThrow(() -> validate(config, rule));
    }

    @Test
    public void testConditionToString() {
        assertEquals("'port' > 0", Condition.greaterThan(PORT, 0).toString());
        assertEquals(
                "'port' >= 1 && 'port' <= 65535",
                Condition.greaterOrEqual(PORT, 1)
                        .and(Condition.lessOrEqual(PORT, 65535))
                        .toString());
        assertEquals("'host' is not blank", Condition.notBlank(HOST).toString());
        assertEquals("'start_ts' < 'end_ts'", Condition.lessThanField(START_TS, END_TS).toString());
        assertEquals("'db_name' is uppercase", Condition.upperCase(DB_NAME).toString());
        assertEquals("'tags' has unique elements", Condition.unique(TAGS).toString());
    }

    @Test
    public void testMultipleValidationRules() {
        OptionRule rule =
                OptionRule.builder()
                        .required(
                                PORT,
                                Condition.greaterOrEqual(PORT, 1)
                                        .and(Condition.lessOrEqual(PORT, 65535)))
                        .required(HOST, Condition.notBlank(HOST))
                        .required(DB_NAME, Condition.upperCase(DB_NAME))
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

    // ==================== New Operator Tests ====================

    @Test
    public void testEndsWithValidation() {
        OptionRule rule =
                OptionRule.builder()
                        .required(ENDPOINT, Condition.endsWith(ENDPOINT, "/v0"))
                        .build();

        Map<String, Object> config = new HashMap<>();
        config.put(ENDPOINT.key(), "https://api.airtable.com/v0");
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(ENDPOINT.key(), "https://api.airtable.com/v1");
        assertThrows(OptionValidationException.class, () -> validate(config, rule));
    }

    @Test
    public void testEndsWithIgnoreCaseValidation() {
        OptionRule rule =
                OptionRule.builder()
                        .required(ENDPOINT, Condition.endsWithIgnoreCase(ENDPOINT, ".csv"))
                        .build();

        Map<String, Object> config = new HashMap<>();
        config.put(ENDPOINT.key(), "data.CSV");
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(ENDPOINT.key(), "data.csv");
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(ENDPOINT.key(), "data.json");
        assertThrows(OptionValidationException.class, () -> validate(config, rule));
    }

    @Test
    public void testCollectionSizeEqual() {
        OptionRule rule = OptionRule.builder().required(TAGS, Condition.sizeEqual(TAGS, 3)).build();

        Map<String, Object> config = new HashMap<>();
        config.put(TAGS.key(), Arrays.asList("a", "b", "c"));
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(TAGS.key(), Arrays.asList("a", "b"));
        assertThrows(OptionValidationException.class, () -> validate(config, rule));
    }

    @Test
    public void testCollectionSizeFixedOne() {
        OptionRule rule = OptionRule.builder().required(TAGS, Condition.sizeEqual(TAGS, 1)).build();

        Map<String, Object> config = new HashMap<>();
        config.put(TAGS.key(), Collections.singletonList("only_one"));
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(TAGS.key(), Arrays.asList("a", "b"));
        assertThrows(OptionValidationException.class, () -> validate(config, rule));
    }

    public static final Option<List<String>> COLLECTIONS =
            Options.key("collections")
                    .listType()
                    .noDefaultValue()
                    .withDescription("collection list");

    @Test
    public void testFieldSizeEqual() {
        OptionRule rule =
                OptionRule.builder()
                        .required(TAGS, COLLECTIONS, Condition.sizeEqualField(TAGS, COLLECTIONS))
                        .build();

        Map<String, Object> config = new HashMap<>();
        config.put(TAGS.key(), Arrays.asList("a", "b", "c"));
        config.put(COLLECTIONS.key(), Arrays.asList("x", "y", "z"));
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(COLLECTIONS.key(), Arrays.asList("x", "y"));
        assertThrows(OptionValidationException.class, () -> validate(config, rule));
    }

    public static final Option<String> SCHEMA_TABLE =
            Options.key("schema_table")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("schema table name");

    public static final Option<String> COLLECTION_NAME =
            Options.key("collection_name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("collection name");

    @Test
    public void testFieldEqualValidation() {
        OptionRule rule =
                OptionRule.builder()
                        .required(
                                SCHEMA_TABLE,
                                COLLECTION_NAME,
                                Condition.equalField(SCHEMA_TABLE, COLLECTION_NAME))
                        .build();

        Map<String, Object> config = new HashMap<>();
        config.put(SCHEMA_TABLE.key(), "db.users");
        config.put(COLLECTION_NAME.key(), "db.users");
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(COLLECTION_NAME.key(), "db.orders");
        assertThrows(OptionValidationException.class, () -> validate(config, rule));
    }

    @Test
    public void testFieldNotEqualValidation() {
        OptionRule rule =
                OptionRule.builder()
                        .required(
                                SCHEMA_TABLE,
                                COLLECTION_NAME,
                                Condition.notEqualField(SCHEMA_TABLE, COLLECTION_NAME))
                        .build();

        Map<String, Object> config = new HashMap<>();
        config.put(SCHEMA_TABLE.key(), "source_db");
        config.put(COLLECTION_NAME.key(), "target_db");
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(COLLECTION_NAME.key(), "source_db");
        assertThrows(OptionValidationException.class, () -> validate(config, rule));
    }

    @Test
    public void testNewOperatorToString() {
        assertEquals("'endpoint' ends with /v0", Condition.endsWith(ENDPOINT, "/v0").toString());
        assertEquals("'tags' size == 3", Condition.sizeEqual(TAGS, 3).toString());
        assertEquals(
                "'tags' size == 'collections'",
                Condition.sizeEqualField(TAGS, COLLECTIONS).toString());
        assertEquals(
                "'schema_table' == 'collection_name'",
                Condition.equalField(SCHEMA_TABLE, COLLECTION_NAME).toString());
        assertEquals(
                "'schema_table' != 'collection_name'",
                Condition.notEqualField(SCHEMA_TABLE, COLLECTION_NAME).toString());
    }

    // ==================== Missing Operator Coverage ====================

    @Test
    public void testLessThanValidation() {
        OptionRule rule =
                OptionRule.builder().required(PORT, Condition.lessThan(PORT, 100)).build();

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
    public void testLengthGreaterOrEqualValidation() {
        OptionRule rule =
                OptionRule.builder()
                        .required(HOST, Condition.lengthGreaterOrEqual(HOST, 3))
                        .build();

        Map<String, Object> config = new HashMap<>();
        config.put(HOST.key(), "abc");
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(HOST.key(), "localhost");
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(HOST.key(), "ab");
        assertThrows(OptionValidationException.class, () -> validate(config, rule));
    }

    @Test
    public void testLengthLessOrEqualValidation() {
        OptionRule rule =
                OptionRule.builder()
                        .required(DELIMITER, Condition.lengthLessOrEqual(DELIMITER, 2))
                        .build();

        Map<String, Object> config = new HashMap<>();
        config.put(DELIMITER.key(), ",");
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(DELIMITER.key(), "||");
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(DELIMITER.key(), "|||");
        assertThrows(OptionValidationException.class, () -> validate(config, rule));
    }

    @Test
    public void testLengthRangeValidation() {
        OptionRule rule =
                OptionRule.builder()
                        .required(
                                HOST,
                                Condition.lengthGreaterOrEqual(HOST, 1)
                                        .and(Condition.lengthLessOrEqual(HOST, 255)))
                        .build();

        Map<String, Object> config = new HashMap<>();
        config.put(HOST.key(), "a");
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(HOST.key(), "");
        assertThrows(OptionValidationException.class, () -> validate(config, rule));
    }

    @Test
    public void testCollectionSizeGreaterOrEqualValidation() {
        OptionRule rule =
                OptionRule.builder().required(TAGS, Condition.sizeGreaterOrEqual(TAGS, 2)).build();

        Map<String, Object> config = new HashMap<>();
        config.put(TAGS.key(), Arrays.asList("a", "b"));
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(TAGS.key(), Arrays.asList("a", "b", "c"));
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(TAGS.key(), Collections.singletonList("a"));
        assertThrows(OptionValidationException.class, () -> validate(config, rule));
    }

    @Test
    public void testCollectionSizeLessOrEqualValidation() {
        OptionRule rule =
                OptionRule.builder().required(TAGS, Condition.sizeLessOrEqual(TAGS, 3)).build();

        Map<String, Object> config = new HashMap<>();
        config.put(TAGS.key(), Arrays.asList("a", "b", "c"));
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(TAGS.key(), Arrays.asList("a", "b"));
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(TAGS.key(), Arrays.asList("a", "b", "c", "d"));
        assertThrows(OptionValidationException.class, () -> validate(config, rule));
    }

    @Test
    public void testFieldGreaterThanValidation() {
        OptionRule rule =
                OptionRule.builder()
                        .required(END_TS, START_TS, Condition.greaterThanField(END_TS, START_TS))
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
                        .required(END_TS, START_TS, Condition.greaterOrEqualField(END_TS, START_TS))
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
                                Condition.greaterThan(TEST_TIMESTAMP, 0L))
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
                                ENABLE_TX,
                                true,
                                START_TS,
                                END_TS,
                                Condition.lessThanField(START_TS, END_TS))
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
        OptionRule rule =
                OptionRule.builder().optional(PORT, Condition.greaterOrEqual(PORT, 1)).build();

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
                        .optional(START_TS, END_TS, Condition.lessThanField(START_TS, END_TS))
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

    // ==================== Condition Chain Coverage ====================

    @Test
    public void testNotEmptyAndUniqueChain() {
        OptionRule rule =
                OptionRule.builder()
                        .required(TAGS, Condition.notEmpty(TAGS).and(Condition.unique(TAGS)))
                        .build();

        Map<String, Object> config = new HashMap<>();
        config.put(TAGS.key(), Arrays.asList("a", "b", "c"));
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(TAGS.key(), Collections.emptyList());
        assertThrows(OptionValidationException.class, () -> validate(config, rule));

        config.put(TAGS.key(), Arrays.asList("a", "a", "b"));
        assertThrows(OptionValidationException.class, () -> validate(config, rule));
    }

    @Test
    public void testCollectionSizeRangeChain() {
        OptionRule rule =
                OptionRule.builder()
                        .required(
                                TAGS,
                                Condition.sizeGreaterOrEqual(TAGS, 1)
                                        .and(Condition.sizeLessOrEqual(TAGS, 5)))
                        .build();

        Map<String, Object> config = new HashMap<>();
        config.put(TAGS.key(), Arrays.asList("a"));
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(TAGS.key(), Arrays.asList("a", "b", "c", "d", "e"));
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(TAGS.key(), Collections.emptyList());
        assertThrows(OptionValidationException.class, () -> validate(config, rule));

        config.put(TAGS.key(), Arrays.asList("1", "2", "3", "4", "5", "6"));
        assertThrows(OptionValidationException.class, () -> validate(config, rule));
    }

    @Test
    public void testMultipleConditionsVarargs() {
        OptionRule rule =
                OptionRule.builder()
                        .required(
                                TAGS,
                                Condition.notEmpty(TAGS),
                                Condition.unique(TAGS),
                                Condition.sizeLessOrEqual(TAGS, 10))
                        .build();

        Map<String, Object> config = new HashMap<>();
        config.put(TAGS.key(), Arrays.asList("a", "b", "c"));
        Assertions.assertDoesNotThrow(() -> validate(config, rule));

        config.put(TAGS.key(), Collections.emptyList());
        assertThrows(OptionValidationException.class, () -> validate(config, rule));

        config.put(TAGS.key(), Arrays.asList("a", "a"));
        assertThrows(OptionValidationException.class, () -> validate(config, rule));
    }

    // ==================== toString Coverage for All Operators ====================

    @Test
    public void testAllOperatorToString() {
        assertEquals("'port' < 100", Condition.lessThan(PORT, 100).toString());
        assertEquals("'port' <= 100", Condition.lessOrEqual(PORT, 100).toString());
        assertEquals("'port' > 0", Condition.greaterThan(PORT, 0).toString());
        assertEquals("'port' >= 0", Condition.greaterOrEqual(PORT, 0).toString());
        assertEquals("'host' is not blank", Condition.notBlank(HOST).toString());
        assertEquals(
                "'endpoint' starts with jdbc:", Condition.startsWith(ENDPOINT, "jdbc:").toString());
        assertEquals(
                "'endpoint' starts with (ignore case) jdbc:",
                Condition.startsWithIgnoreCase(ENDPOINT, "jdbc:").toString());
        assertEquals("'endpoint' ends with .csv", Condition.endsWith(ENDPOINT, ".csv").toString());
        assertEquals(
                "'endpoint' ends with (ignore case) .csv",
                Condition.endsWithIgnoreCase(ENDPOINT, ".csv").toString());
        assertEquals("'endpoint' contains ://", Condition.contains(ENDPOINT, "://").toString());
        assertEquals("'endpoint' matches ^\\d+$", Condition.matches(ENDPOINT, "^\\d+$").toString());
        assertEquals("'db_name' is uppercase", Condition.upperCase(DB_NAME).toString());
        assertEquals("'db_name' is lowercase", Condition.lowerCase(DB_NAME).toString());
        assertEquals("'delimiter' length == 1", Condition.lengthEqual(DELIMITER, 1).toString());
        assertEquals("'host' length >= 3", Condition.lengthGreaterOrEqual(HOST, 3).toString());
        assertEquals("'host' length <= 255", Condition.lengthLessOrEqual(HOST, 255).toString());
        assertEquals("'tags' is not empty", Condition.notEmpty(TAGS).toString());
        assertEquals("'tags' has unique elements", Condition.unique(TAGS).toString());
        assertEquals("'tags' size == 3", Condition.sizeEqual(TAGS, 3).toString());
        assertEquals("'tags' size >= 1", Condition.sizeGreaterOrEqual(TAGS, 1).toString());
        assertEquals("'tags' size <= 10", Condition.sizeLessOrEqual(TAGS, 10).toString());
        assertEquals("'start_ts' < 'end_ts'", Condition.lessThanField(START_TS, END_TS).toString());
        assertEquals(
                "'start_ts' <= 'end_ts'", Condition.lessOrEqualField(START_TS, END_TS).toString());
        assertEquals(
                "'end_ts' > 'start_ts'", Condition.greaterThanField(END_TS, START_TS).toString());
        assertEquals(
                "'end_ts' >= 'start_ts'",
                Condition.greaterOrEqualField(END_TS, START_TS).toString());
        assertEquals(
                "'schema_table' == 'collection_name'",
                Condition.equalField(SCHEMA_TABLE, COLLECTION_NAME).toString());
        assertEquals(
                "'schema_table' != 'collection_name'",
                Condition.notEqualField(SCHEMA_TABLE, COLLECTION_NAME).toString());
        assertEquals(
                "'tags' size == 'collections'",
                Condition.sizeEqualField(TAGS, COLLECTIONS).toString());
    }

    @Test
    public void testCircularConditionChainDetected() {
        Condition<Integer> a = Condition.greaterThan(PORT, 0);
        assertThrows(IllegalArgumentException.class, () -> a.and(a));
    }

    @Test
    public void testCircularConditionChainIndirect() {
        Condition<Integer> a = Condition.greaterThan(PORT, 0);
        Condition<Integer> b = Condition.lessThan(PORT, 100);
        a.and(b);
        assertThrows(IllegalArgumentException.class, () -> b.and(a));
    }

    @Test
    public void testCircularConditionChainDuplicateAppend() {
        Condition<Integer> a = Condition.greaterThan(PORT, 0);
        Condition<Integer> b = Condition.lessThan(PORT, 100);
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
}
