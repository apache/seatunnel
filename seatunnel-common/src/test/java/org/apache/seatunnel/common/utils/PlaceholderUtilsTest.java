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

package org.apache.seatunnel.common.utils;

import org.junit.jupiter.api.Test;

import java.util.LinkedHashMap;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PlaceholderUtilsTest {

    @Test
    void testMultiplePlaceholdersWithDefault() {
        String input = "select * from ${resName:fake_test}_table where name = '${nameValForEnv}'";

        String result =
                PlaceholderUtils.processPlaceholders(
                        input,
                        key -> false,
                        new LinkedHashMap<String, String>(),
                        new LinkedHashMap<String, String>());
        assertEquals("select * from fake_test_table where name = '${nameValForEnv}'", result);
    }
}
