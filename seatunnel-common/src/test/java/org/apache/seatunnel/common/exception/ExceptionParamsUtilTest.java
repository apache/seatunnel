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

package org.apache.seatunnel.common.exception;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ExceptionParamsUtilTest {

    @Test
    void testGetParamsForDescription() {
        String description = "test description with param <key1>, <key2> and <key3>.";
        Assertions.assertIterableEquals(
                Arrays.asList("key1", "key2", "key3"), ExceptionParamsUtil.getParams(description));
        String description2 = "test description with no param.";
        Assertions.assertIterableEquals(
                Collections.emptyList(), ExceptionParamsUtil.getParams(description2));
        String description3 = "test description with wrong param <>, <, >, < >.";
        Assertions.assertIterableEquals(
                Collections.emptyList(), ExceptionParamsUtil.getParams(description3));
    }

    @Test
    void testGetDescriptionForTemplate() {
        String description = "test description with param <key1>, <key2> and <key3>.";
        Map<String, String> params = new HashMap<>();
        params.put("key1", "value1");
        params.put("key2", "value2");
        params.put("key3", "value3");
        Assertions.assertEquals(
                "test description with param value1, value2 and value3.",
                ExceptionParamsUtil.getDescription(description, params));

        params.remove("key2");
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> ExceptionParamsUtil.getDescription(description, params));
    }

    @Test
    void testAssertParamsMatchWithDescription() {
        String description = "test description with param <key1>, <key2> and <key3>.";
        Map<String, String> params = new HashMap<>();
        params.put("key1", "value1");
        params.put("key2", "value2");
        params.put("key3", "value3");
        ExceptionParamsUtil.assertParamsMatchWithDescription(description, params);

        params.remove("key2");
        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> ExceptionParamsUtil.assertParamsMatchWithDescription(description, params));
    }
}
