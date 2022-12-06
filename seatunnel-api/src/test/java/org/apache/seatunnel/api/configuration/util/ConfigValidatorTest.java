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

import static org.apache.seatunnel.api.configuration.OptionTest.TEST_MODE;
import static org.apache.seatunnel.api.configuration.util.OptionRuleTest.TEST_PORTS;
import static org.apache.seatunnel.api.configuration.util.OptionRuleTest.TEST_TIMESTAMP;
import static org.apache.seatunnel.api.configuration.util.OptionRuleTest.TEST_TOPIC;
import static org.apache.seatunnel.api.configuration.util.OptionRuleTest.TEST_TOPIC_PATTERN;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.OptionTest;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

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

    void validate(Map<String, Object> config, OptionRule rule) {
        ConfigValidator.of(ReadonlyConfig.fromMap(config)).validate(rule);
    }

    @Test
    public void testAbsolutelyRequiredOption() {
        OptionRule rule = OptionRule.builder()
            .required(TEST_PORTS, KEY_USERNAME, KEY_PASSWORD)
            .build();
        Map<String, Object> config = new HashMap<>();
        Executable executable = () -> validate(config, rule);

        // absent
        config.put(TEST_PORTS.key(), "[9090]");
        assertEquals("ErrorCode:[API-02], ErrorDescription:[Option item validate failed] - There are unconfigured options, the options('username', 'password') are required.",
            assertThrows(OptionValidationException.class, executable).getMessage());

        config.put(KEY_USERNAME.key(), "asuka");
        assertEquals("ErrorCode:[API-02], ErrorDescription:[Option item validate failed] - There are unconfigured options, the options('password') are required.",
            assertThrows(OptionValidationException.class, executable).getMessage());

        // all present
        config.put(KEY_PASSWORD.key(), "saitou");
        Assertions.assertDoesNotThrow(executable);
    }

    @Test
    public void testBundledRequiredOptions() {
        OptionRule rule = OptionRule.builder()
            .bundled(KEY_USERNAME, KEY_PASSWORD).build();
        Map<String, Object> config = new HashMap<>();
        Executable executable = () -> validate(config, rule);

        // case1: all absent
        Assertions.assertDoesNotThrow(executable);

        // case2: some present
        config.put(KEY_USERNAME.key(), "asuka");
        assertEquals("ErrorCode:[API-02], ErrorDescription:[Option item validate failed] - These options('username', 'password') are bundled, must be present or absent together." +
                " The options present are: 'username'. The options absent are 'password'.",
            assertThrows(OptionValidationException.class, executable).getMessage());

        // case2: all present
        config.put(KEY_PASSWORD.key(), "saitou");
        Assertions.assertDoesNotThrow(executable);
    }

    @Test
    public void testSimpleExclusiveRequiredOptions() {
        OptionRule rule = OptionRule.builder()
            .exclusive(TEST_TOPIC_PATTERN, TEST_TOPIC)
            .build();
        Map<String, Object> config = new HashMap<>();
        Executable executable = () -> validate(config, rule);

        // all absent
        assertEquals("ErrorCode:[API-02], ErrorDescription:[Option item validate failed] - There are unconfigured options, these options('option.topic-pattern', 'option.topic') are mutually exclusive," +
                " allowing only one set(\"[] for a set\") of options to be configured.",
            assertThrows(OptionValidationException.class, executable).getMessage());

        // only one present
        config.put(TEST_TOPIC_PATTERN.key(), "asuka");
        Assertions.assertDoesNotThrow(executable);

        // present > 1
        config.put(TEST_TOPIC.key(), "[\"saitou\"]");
        assertEquals("ErrorCode:[API-02], ErrorDescription:[Option item validate failed] - These options('option.topic-pattern', 'option.topic') are mutually exclusive, " +
                "allowing only one set(\"[] for a set\") of options to be configured.",
            assertThrows(OptionValidationException.class, executable).getMessage());
    }

    @Test
    public void testComplexExclusiveRequiredOptions() {
        OptionRule rule = OptionRule.builder()
            .exclusive(KEY_BEARER_TOKEN, KEY_KERBEROS_TICKET)
            .build();

        Map<String, Object> config = new HashMap<>();
        Executable executable = () -> validate(config, rule);

        // all absent
        assertEquals("ErrorCode:[API-02], ErrorDescription:[Option item validate failed] - There are unconfigured options, these options('bearer-token', 'kerberos-ticket') are mutually exclusive," +
                " allowing only one set(\"[] for a set\") of options to be configured.",
            assertThrows(OptionValidationException.class, executable).getMessage());

        // set one
        config.put(KEY_BEARER_TOKEN.key(), "ashulin");
        Assertions.assertDoesNotThrow(executable);

        // all set
        config.put(KEY_KERBEROS_TICKET.key(), "zongwen");
        assertEquals("ErrorCode:[API-02], ErrorDescription:[Option item validate failed] - These options('bearer-token', 'kerberos-ticket') are mutually exclusive," +
                " allowing only one set(\"[] for a set\") of options to be configured.",
            assertThrows(OptionValidationException.class, executable).getMessage());
    }

    @Test
    public void testSimpleConditionalRequiredOptionsWithDefaultValue() {
        OptionRule rule = OptionRule.builder()
            .optional(TEST_MODE)
            .conditional(TEST_MODE, OptionTest.TestMode.TIMESTAMP, TEST_TIMESTAMP)
            .build();
        Map<String, Object> config = new HashMap<>();
        Executable executable = () -> validate(config, rule);

        // Expression mismatch
        Assertions.assertDoesNotThrow(executable);

        // Expression match, and required options absent
        config.put(TEST_MODE.key(), "timestamp");
        assertEquals("ErrorCode:[API-02], ErrorDescription:[Option item validate failed] - There are unconfigured options, the options('option.timestamp') are required" +
                " because ['option.mode' == TIMESTAMP] is true.",
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
        OptionRule rule = OptionRule.builder()
            .optional(KEY_USERNAME)
            .conditional(KEY_USERNAME, "ashulin", TEST_TIMESTAMP)
            .build();
        Map<String, Object> config = new HashMap<>();
        Executable executable = () -> validate(config, rule);

        // Expression mismatch
        Assertions.assertDoesNotThrow(executable);

        // Expression match, and required options absent
        config.put(KEY_USERNAME.key(), "ashulin");
        assertEquals("ErrorCode:[API-02], ErrorDescription:[Option item validate failed] - There are unconfigured options, the options('option.timestamp') are required" +
                " because ['username' == ashulin] is true.",
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
        OptionRule rule = OptionRule.builder()
            .optional(KEY_USERNAME)
            .conditional(KEY_USERNAME, Arrays.asList("ashulin", "asuka"), TEST_TIMESTAMP)
            .build();
        Map<String, Object> config = new HashMap<>();
        Executable executable = () -> validate(config, rule);

        // Expression mismatch
        Assertions.assertDoesNotThrow(executable);

        // 'username' == ashulin, and required options absent
        config.put(KEY_USERNAME.key(), "ashulin");
        assertEquals("ErrorCode:[API-02], ErrorDescription:[Option item validate failed] - There are unconfigured options, the options('option.timestamp') are required" +
                " because ['username' == ashulin || 'username' == asuka] is true.",
            assertThrows(OptionValidationException.class, executable).getMessage());

        // 'username' == asuka, and required options absent
        config.put(KEY_USERNAME.key(), "asuka");
        assertEquals("ErrorCode:[API-02], ErrorDescription:[Option item validate failed] - There are unconfigured options, the options('option.timestamp') are required" +
                " because ['username' == ashulin || 'username' == asuka] is true.",
            assertThrows(OptionValidationException.class, executable).getMessage());

        // Expression match, and required options all present
        config.put(TEST_TIMESTAMP.key(), "564231238596789");
        Assertions.assertDoesNotThrow(executable);

        // Expression mismatch
        config.put(KEY_USERNAME.key(), "asuka111");
        Assertions.assertDoesNotThrow(executable);
    }
}
