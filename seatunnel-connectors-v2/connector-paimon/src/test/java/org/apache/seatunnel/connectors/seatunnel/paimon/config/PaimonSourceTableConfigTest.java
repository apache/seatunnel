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

package org.apache.seatunnel.connectors.seatunnel.paimon.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import org.apache.curator.shaded.com.google.common.collect.Lists;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class PaimonSourceTableConfigTest {

    @Test
    public void testSingleTableConfig() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("warehouse", "file:///tmp/paimon");
        configMap.put("database", "test_db");
        configMap.put("table", "test_table");
        configMap.put("query", "SELECT * FROM test_table");

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        List<PaimonSourceTableConfig> tableConfigs = PaimonSourceTableConfig.of(config);

        assertEquals(1, tableConfigs.size());
        PaimonSourceTableConfig tableConfig = tableConfigs.get(0);
        assertEquals("test_db", tableConfig.getDatabase());
        assertEquals("test_table", tableConfig.getTable());
        assertEquals("SELECT * FROM test_table", tableConfig.getQuery());
    }

    @Test
    public void testMultiTableConfig() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("warehouse", "file:///tmp/paimon");

        Map<String, Object> table1 = new HashMap<>();
        table1.put("database", "test_db");
        table1.put("table", "table1");
        table1.put("query", "SELECT * FROM table1");

        Map<String, Object> table2 = new HashMap<>();
        table2.put("database", "test_db");
        table2.put("table", "table2");

        configMap.put("table_list", Lists.newArrayList(table1, table2));

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        List<PaimonSourceTableConfig> tableConfigs = PaimonSourceTableConfig.of(config);

        assertEquals(2, tableConfigs.size());

        PaimonSourceTableConfig config1 = tableConfigs.get(0);
        assertEquals("test_db", config1.getDatabase());
        assertEquals("table1", config1.getTable());
        assertEquals("SELECT * FROM table1", config1.getQuery());

        PaimonSourceTableConfig config2 = tableConfigs.get(1);
        assertEquals("test_db", config2.getDatabase());
        assertEquals("table2", config2.getTable());
        assertEquals(null, config2.getQuery());
    }
}
