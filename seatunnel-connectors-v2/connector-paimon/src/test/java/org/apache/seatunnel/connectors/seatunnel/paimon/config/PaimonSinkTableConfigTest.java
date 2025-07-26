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
import org.apache.seatunnel.api.sink.DataSaveMode;
import org.apache.seatunnel.api.sink.SchemaSaveMode;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class PaimonSinkTableConfigTest {

    @Test
    public void testSingleTableConfig() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("warehouse", "file:///tmp/paimon");
        configMap.put("database", "test_db");
        configMap.put("table", "test_table");
        configMap.put("schema_save_mode", "CREATE_SCHEMA_WHEN_NOT_EXIST");
        configMap.put("data_save_mode", "APPEND_DATA");

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        List<PaimonSinkTableConfig> tableConfigs = PaimonSinkTableConfig.of(config);

        assertEquals(1, tableConfigs.size());
        PaimonSinkTableConfig tableConfig = tableConfigs.get(0);
        assertEquals("test_db", tableConfig.getDatabase());
        assertEquals("test_table", tableConfig.getTable());
        assertEquals(SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST, tableConfig.getSchemaSaveMode());
        assertEquals(DataSaveMode.APPEND_DATA, tableConfig.getDataSaveMode());
        assertNotNull(tableConfig.getCatalogTable());
    }

    @Test
    public void testMultiTableConfig() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("warehouse", "file:///tmp/paimon");

        Map<String, Object> table1 = new HashMap<>();
        table1.put("database", "test_db");
        table1.put("table", "table1");
        table1.put("schema_save_mode", "CREATE_SCHEMA_WHEN_NOT_EXIST");
        table1.put("data_save_mode", "APPEND_DATA");

        Map<String, Object> table2 = new HashMap<>();
        table2.put("database", "test_db");
        table2.put("table", "table2");
        table2.put("schema_save_mode", "RECREATE_SCHEMA");
        table2.put("data_save_mode", "DROP_DATA");

        configMap.put("table_list", List.of(table1, table2));

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        List<PaimonSinkTableConfig> tableConfigs = PaimonSinkTableConfig.of(config);

        assertEquals(2, tableConfigs.size());

        PaimonSinkTableConfig config1 = tableConfigs.get(0);
        assertEquals("test_db", config1.getDatabase());
        assertEquals("table1", config1.getTable());
        assertEquals(SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST, config1.getSchemaSaveMode());
        assertEquals(DataSaveMode.APPEND_DATA, config1.getDataSaveMode());

        PaimonSinkTableConfig config2 = tableConfigs.get(1);
        assertEquals("test_db", config2.getDatabase());
        assertEquals("table2", config2.getTable());
        assertEquals(SchemaSaveMode.RECREATE_SCHEMA, config2.getSchemaSaveMode());
        assertEquals(DataSaveMode.DROP_DATA, config2.getDataSaveMode());
    }
}
