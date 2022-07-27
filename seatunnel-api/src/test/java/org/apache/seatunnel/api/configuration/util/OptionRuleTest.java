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

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.OptionTest;
import org.apache.seatunnel.api.configuration.Options;

import com.fasterxml.jackson.core.type.TypeReference;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

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
    public void testOptionalException() {
        Assertions.assertThrows(OptionValidationException.class,
            () -> OptionRule.builder().optional(TEST_NUM, TEST_MODE, TEST_PORTS).build(),
            "Optional option 'option.ports' should have default value.");
    }

    @Test
    public void testRequiredException() {
        Assertions.assertThrows(OptionValidationException.class,
            () -> OptionRule.builder().required(TEST_NUM, TEST_MODE, TEST_PORTS).build(),
            "Required option 'option.num' should have no default value.");
    }

    @Test
    public void testExclusiveException() {
        Assertions.assertThrows(OptionValidationException.class,
            () -> OptionRule.builder().exclusive(TEST_TOPIC_PATTERN, TEST_TOPIC, TEST_MODE, TEST_PORTS).build(),
            "Required option 'option.mode' should have no default value.");
        Assertions.assertThrows(OptionValidationException.class,
            () -> OptionRule.builder().exclusive(TEST_TOPIC_PATTERN).build(),
            "The number of exclusive options must be greater than 1.");
    }

    @Test
    public void testConditionalException() {
        Assertions.assertThrows(OptionValidationException.class,
            () -> OptionRule.builder().conditional(TEST_MODE, OptionTest.TestMode.TIMESTAMP, TEST_NUM).build(),
            "Required option 'option.num' should have no default value.");
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
