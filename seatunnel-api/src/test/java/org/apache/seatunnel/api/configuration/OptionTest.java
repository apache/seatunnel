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

package org.apache.seatunnel.api.configuration;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class OptionTest {
    public static final Option<Integer> TEST_NUM =
            Options.key("option.num")
                    .intType()
                    .defaultValue(100)
                    .withDescription("test int option");

    public static final Option<TestMode> TEST_MODE =
            Options.key("option.mode")
                    .enumType(TestMode.class)
                    .defaultValue(TestMode.LATEST)
                    .withDescription("test enum option");

    public enum TestMode {
        EARLIEST,
        LATEST,
        TIMESTAMP,
    }

    @Test
    public void testEquals() {
        Assertions.assertEquals(TEST_NUM, Options.key("option.num").intType().defaultValue(100));
        Assertions.assertEquals(
                TEST_MODE,
                Options.key("option.mode").enumType(TestMode.class).defaultValue(TestMode.LATEST));
        Assertions.assertEquals(
                TEST_NUM.withFallbackKeys("option.numeric"),
                Options.key("option.num")
                        .intType()
                        .defaultValue(100)
                        .withFallbackKeys("option.numeric"));
    }
}
