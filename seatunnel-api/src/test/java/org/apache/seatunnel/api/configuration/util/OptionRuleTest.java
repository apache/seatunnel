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
import static org.apache.seatunnel.api.configuration.OptionTest.TEST_NUM;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.OptionTest;
import org.apache.seatunnel.api.configuration.Options;

import com.fasterxml.jackson.core.type.TypeReference;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;

import java.util.List;

public class OptionRuleTest {
    public static final Option<Long> TEST_TIMESTAMP = Options.key("option.timestamp")
        .longType()
        .noDefaultValue()
        .withDescription("test long timestamp");

    public static final Option<String> TEST_TOPIC_PATTERN = Options.key("option.topic-pattern")
        .stringType()
        .noDefaultValue()
        .withDescription("test string type");

    public static final Option<List<String>> TEST_TOPIC = Options.key("option.topic")
        .listType()
        .noDefaultValue()
        .withDescription("test list string type");

    public static final Option<List<Integer>> TEST_PORTS = Options.key("option.ports")
        .type(new TypeReference<List<Integer>>() {
        })
        .noDefaultValue()
        .withDescription("test list int type");

    public static final Option<String> TEST_REQUIRED_HAVE_DEFAULT_VALUE = Options.key("option.required-have-default")
        .stringType()
        .defaultValue("11")
        .withDescription("test string type");

    public static final Option<String> TEST_DUPLICATE = Options.key("option.test-duplicate")
        .stringType()
        .noDefaultValue()
        .withDescription("test string type");

    @Test
    public void testBuildSuccess() {
        OptionRule rule = OptionRule.builder()
            .optional(TEST_NUM, TEST_MODE)
            .required(TEST_PORTS)
            .exclusive(TEST_TOPIC_PATTERN, TEST_TOPIC)
            .conditional(TEST_MODE, OptionTest.TestMode.TIMESTAMP, TEST_TIMESTAMP)
            .build();
        Assertions.assertNotNull(rule);
    }

    @Test
    public void testVerify() {
        Executable executable = () -> {
            OptionRule.builder()
                .optional(TEST_NUM, TEST_MODE)
                .required(TEST_PORTS, TEST_REQUIRED_HAVE_DEFAULT_VALUE)
                .exclusive(TEST_TOPIC_PATTERN, TEST_TOPIC)
                .conditional(TEST_MODE, OptionTest.TestMode.TIMESTAMP, TEST_TIMESTAMP)
                .build();
        };

        // test required option have no default value
        assertEquals(
            "ErrorCode:[API-02], ErrorDescription:[Option item validate failed] - Required option 'option.required-have-default' should have no default value.",
            assertThrows(OptionValidationException.class, executable).getMessage());

        executable = () -> {
            OptionRule.builder()
                .optional(TEST_NUM, TEST_MODE, TEST_REQUIRED_HAVE_DEFAULT_VALUE)
                .required(TEST_PORTS, TEST_REQUIRED_HAVE_DEFAULT_VALUE)
                .exclusive(TEST_TOPIC_PATTERN, TEST_TOPIC)
                .conditional(TEST_MODE, OptionTest.TestMode.TIMESTAMP, TEST_TIMESTAMP)
                .build();
        };

        // test duplicate
        assertEquals(
            "ErrorCode:[API-02], ErrorDescription:[Option item validate failed] - RequiredOption 'option.required-have-default' duplicate in option options.",
            assertThrows(OptionValidationException.class, executable).getMessage());

        executable = () -> {
            OptionRule.builder()
                .optional(TEST_NUM, TEST_MODE)
                .exclusive(TEST_TOPIC_PATTERN, TEST_TOPIC, TEST_DUPLICATE)
                .required(TEST_PORTS, TEST_DUPLICATE)
                .conditional(TEST_MODE, OptionTest.TestMode.TIMESTAMP, TEST_TIMESTAMP)
                .build();
        };

        // test duplicate in RequiredOption$ExclusiveRequiredOptions
        assertEquals(
            "ErrorCode:[API-02], ErrorDescription:[Option item validate failed] - RequiredOption 'option.test-duplicate' duplicate in 'org.apache.seatunnel.api.configuration.util.RequiredOption$ExclusiveRequiredOptions'.",
            assertThrows(OptionValidationException.class, executable).getMessage());

        executable = () -> {
            OptionRule.builder()
                .optional(TEST_NUM)
                .exclusive(TEST_TOPIC_PATTERN, TEST_TOPIC)
                .required(TEST_PORTS)
                .conditional(TEST_MODE, OptionTest.TestMode.TIMESTAMP, TEST_TIMESTAMP)
                .build();
        };

        // test conditional not found in other options
        assertEquals(
            "ErrorCode:[API-02], ErrorDescription:[Option item validate failed] - Conditional 'option.mode' not found in options.",
            assertThrows(OptionValidationException.class, executable).getMessage());

        executable = () -> {
            OptionRule.builder()
                .optional(TEST_NUM, TEST_MODE)
                .exclusive(TEST_TOPIC_PATTERN, TEST_TOPIC)
                .required(TEST_PORTS)
                .conditional(TEST_MODE, OptionTest.TestMode.TIMESTAMP, TEST_TIMESTAMP)
                .conditional(TEST_NUM, 100, TEST_TIMESTAMP)
                .build();
        };

        // test parameter can only be controlled by one other parameter
        assertEquals(
            "ErrorCode:[API-02], ErrorDescription:[Option item validate failed] - ConditionalOption 'option.timestamp' duplicate in 'org.apache.seatunnel.api.configuration.util.RequiredOption$ConditionalRequiredOptions'.",
            assertThrows(OptionValidationException.class, executable).getMessage());
    }

    @Test
    public void testEquals() {
        OptionRule rule1 = OptionRule.builder()
            .optional(TEST_NUM, TEST_MODE)
            .required(TEST_PORTS)
            .exclusive(TEST_TOPIC_PATTERN, TEST_TOPIC)
            .conditional(TEST_MODE, OptionTest.TestMode.TIMESTAMP, TEST_TIMESTAMP)
            .build();
        OptionRule rule2 = OptionRule.builder()
            .optional(TEST_NUM)
            .optional(TEST_MODE)
            .required(TEST_PORTS)
            .exclusive(TEST_TOPIC_PATTERN, TEST_TOPIC)
            .conditional(TEST_MODE, OptionTest.TestMode.TIMESTAMP, TEST_TIMESTAMP)
            .build();
        Assertions.assertEquals(rule1, rule2);
    }
}
