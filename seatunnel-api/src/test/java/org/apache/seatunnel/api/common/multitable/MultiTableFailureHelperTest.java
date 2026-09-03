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

package org.apache.seatunnel.api.common.multitable;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

class MultiTableFailureHelperTest {

    @Test
    void testMergeOptionsPreservesSpecialCharacterKeys() {
        Map<String, Object> primaryKeys = new HashMap<>();
        primaryKeys.put("^t_nova_.*$", Arrays.asList("${primary_key}", "DATA_SOURCE"));

        Map<String, Object> multiTableConfig = new HashMap<>();
        multiTableConfig.put("primary_keys", primaryKeys);

        Map<String, Object> primaryMap = new HashMap<>();
        primaryMap.put("multi-table_config", multiTableConfig);
        primaryMap.put("url", "jdbc:mysql://localhost:3306/primary");

        Map<String, Object> fallbackMap = new HashMap<>();
        fallbackMap.put("url", "jdbc:mysql://localhost:3306/fallback");
        fallbackMap.put("username", "u");

        ReadonlyConfig merged =
                MultiTableFailureHelper.mergeOptions(
                        ReadonlyConfig.fromMap(primaryMap), ReadonlyConfig.fromMap(fallbackMap));

        Map<String, Object> mergedMap = merged.getSourceMap();
        Assertions.assertEquals("jdbc:mysql://localhost:3306/primary", mergedMap.get("url"));
        Assertions.assertEquals("u", mergedMap.get("username"));

        Map<String, Object> mergedMultiTableConfig =
                (Map<String, Object>) mergedMap.get("multi-table_config");
        Assertions.assertNotNull(mergedMultiTableConfig);
        Map<String, Object> mergedPrimaryKeys =
                (Map<String, Object>) mergedMultiTableConfig.get("primary_keys");
        Assertions.assertNotNull(mergedPrimaryKeys);
        Assertions.assertTrue(mergedPrimaryKeys.containsKey("^t_nova_.*$"));
        Assertions.assertEquals(
                Arrays.asList("${primary_key}", "DATA_SOURCE"),
                mergedPrimaryKeys.get("^t_nova_.*$"));
    }
}
