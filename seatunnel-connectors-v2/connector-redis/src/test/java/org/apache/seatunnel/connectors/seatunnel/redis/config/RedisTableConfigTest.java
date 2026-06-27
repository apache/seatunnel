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

package org.apache.seatunnel.connectors.seatunnel.redis.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class RedisTableConfigTest {

    private static ReadonlyConfig config(Map<String, Object> map) {
        return ReadonlyConfig.fromMap(map);
    }

    @Test
    void singleTableBuildsOneTable() {
        Map<String, Object> map = new HashMap<>();
        map.put("keys", "key_test*");
        map.put("data_type", "string");

        List<RedisTableConfig> tables = RedisTableConfig.of(config(map));

        Assertions.assertEquals(1, tables.size());
        Assertions.assertEquals("key_test*", tables.get(0).getKeys());
        Assertions.assertEquals(RedisDataType.STRING, tables.get(0).getDataType());
    }

    @Test
    void singleTableMissingDataTypeThrows() {
        Map<String, Object> map = new HashMap<>();
        map.put("keys", "key_test*");

        Assertions.assertThrows(
                IllegalArgumentException.class, () -> RedisTableConfig.of(config(map)));
    }

    @Test
    void singleTableBlankKeysThrows() {
        Map<String, Object> map = new HashMap<>();
        map.put("keys", "   ");
        map.put("data_type", "string");

        Assertions.assertThrows(
                IllegalArgumentException.class, () -> RedisTableConfig.of(config(map)));
    }

    @Test
    void multiTableAppliesRequiredFieldValidationPerItem() {
        Map<String, Object> tableItem = new HashMap<>();
        tableItem.put("keys", "key_test*");
        Map<String, Object> map = new HashMap<>();
        map.put("tables_configs", Collections.singletonList(tableItem));

        Assertions.assertThrows(
                IllegalArgumentException.class, () -> RedisTableConfig.of(config(map)));
    }

    @Test
    void multiTableBuildsEachItem() {
        Map<String, Object> t1 = new HashMap<>();
        t1.put("keys", "k1*");
        t1.put("data_type", "string");
        Map<String, Object> t2 = new HashMap<>();
        t2.put("keys", "k2*");
        t2.put("data_type", "hash");
        Map<String, Object> map = new HashMap<>();
        map.put("tables_configs", Arrays.asList(t1, t2));

        List<RedisTableConfig> tables = RedisTableConfig.of(config(map));

        Assertions.assertEquals(2, tables.size());
        Assertions.assertEquals(RedisDataType.STRING, tables.get(0).getDataType());
        Assertions.assertEquals(RedisDataType.HASH, tables.get(1).getDataType());
    }
}
