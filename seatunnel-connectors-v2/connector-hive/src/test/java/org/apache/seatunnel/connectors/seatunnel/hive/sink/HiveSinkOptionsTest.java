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

package org.apache.seatunnel.connectors.seatunnel.hive.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SchemaSaveMode;
import org.apache.seatunnel.connectors.seatunnel.hive.config.HiveOptions;

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/** Test for HiveSinkOptions configuration */
public class HiveSinkOptionsTest {

    @Test
    void testSchemaSaveModeOption() {
        assertNotNull(HiveSinkOptions.SCHEMA_SAVE_MODE);
        assertEquals("schema_save_mode", HiveSinkOptions.SCHEMA_SAVE_MODE.key());
        assertEquals(
                SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST,
                HiveSinkOptions.SCHEMA_SAVE_MODE.defaultValue());
    }

    @Test
    void testSaveModeCreateTemplateOption() {
        assertNotNull(HiveSinkOptions.SAVE_MODE_CREATE_TEMPLATE);
        assertEquals("save_mode_create_template", HiveSinkOptions.SAVE_MODE_CREATE_TEMPLATE.key());
        assertNotNull(HiveSinkOptions.SAVE_MODE_CREATE_TEMPLATE);
    }

    @Test
    void testReadSchemaSaveModeFromConfig() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(HiveSinkOptions.SCHEMA_SAVE_MODE.key(), "RECREATE_SCHEMA");

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        SchemaSaveMode saveMode = config.get(HiveSinkOptions.SCHEMA_SAVE_MODE);
        assertEquals(SchemaSaveMode.RECREATE_SCHEMA, saveMode);
    }

    @Test
    void testReadTemplateFromConfig() {
        Map<String, Object> configMap = new HashMap<>();
        String template =
                "CREATE TABLE IF NOT EXISTS `${database}`.`${table}` (${rowtype_fields}) STORED AS PARQUET";
        configMap.put(HiveSinkOptions.SAVE_MODE_CREATE_TEMPLATE.key(), template);

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        String readTemplate = config.get(HiveSinkOptions.SAVE_MODE_CREATE_TEMPLATE);
        assertEquals(template, readTemplate);
    }

    @Test
    void testDefaultValues() {
        Map<String, Object> configMap = new HashMap<>();

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        SchemaSaveMode defaultSaveMode = config.get(HiveSinkOptions.SCHEMA_SAVE_MODE);
        assertEquals(SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST, defaultSaveMode);

        assertFalse(config.getOptional(HiveSinkOptions.SAVE_MODE_CREATE_TEMPLATE).isPresent());
    }

    @Test
    void testOptionalConfiguration() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(HiveOptions.TABLE_NAME.key(), "test_db.test_table");
        configMap.put(HiveOptions.METASTORE_URI.key(), "thrift://localhost:9083");

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        SchemaSaveMode defaultSaveMode = config.get(HiveSinkOptions.SCHEMA_SAVE_MODE);
        assertEquals(SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST, defaultSaveMode);

        assertFalse(config.getOptional(HiveSinkOptions.SAVE_MODE_CREATE_TEMPLATE).isPresent());
    }

    @Test
    void testAllSaveModeValues() {
        SchemaSaveMode[] allModes = {
            SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST,
            SchemaSaveMode.RECREATE_SCHEMA,
            SchemaSaveMode.ERROR_WHEN_SCHEMA_NOT_EXIST,
            SchemaSaveMode.IGNORE
        };

        for (SchemaSaveMode mode : allModes) {
            Map<String, Object> configMap = new HashMap<>();
            configMap.put(HiveSinkOptions.SCHEMA_SAVE_MODE.key(), mode.name());

            ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
            SchemaSaveMode readMode = config.get(HiveSinkOptions.SCHEMA_SAVE_MODE);

            assertEquals(mode, readMode, "Failed to read SaveMode: " + mode);
        }
    }

    @Test
    void testTemplateWithVariables() {
        String[] templateVariables = {
            "${database}",
            "${table}",
            "${rowtype_fields}",
            "${rowtype_partition_fields}",
            "${table_location}"
        };

        String template =
                "CREATE TABLE IF NOT EXISTS `${database}`.`${table}` (${rowtype_fields}) PARTITIONED BY (${rowtype_partition_fields}) STORED AS PARQUET LOCATION '${table_location}'";

        Map<String, Object> configMap = new HashMap<>();
        configMap.put(HiveSinkOptions.SAVE_MODE_CREATE_TEMPLATE.key(), template);

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        String readTemplate = config.get(HiveSinkOptions.SAVE_MODE_CREATE_TEMPLATE);

        for (String variable : templateVariables) {
            assertTrue(
                    readTemplate.contains(variable),
                    "Template should contain variable: " + variable);
        }
    }

    @Test
    void testConfigurationWithExistingHiveOptions() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(HiveOptions.TABLE_NAME.key(), "analytics.user_events");
        configMap.put(HiveOptions.METASTORE_URI.key(), "thrift://hive-metastore:9083");

        configMap.put(HiveSinkOptions.SCHEMA_SAVE_MODE.key(), "RECREATE_SCHEMA");
        String template =
                "CREATE TABLE IF NOT EXISTS `${database}`.`${table}` (\n"
                        + "              ${rowtype_fields}\n"
                        + "            )\n"
                        + "            PARTITIONED BY (\n"
                        + "              year int COMMENT 'Year partition',\n"
                        + "              month int COMMENT 'Month partition'\n"
                        + "            )\n"
                        + "            STORED AS ORC\n"
                        + "            LOCATION '${table_location}'";
        configMap.put(HiveSinkOptions.SAVE_MODE_CREATE_TEMPLATE.key(), template);

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        assertEquals("analytics.user_events", config.get(HiveOptions.TABLE_NAME));
        assertEquals("thrift://hive-metastore:9083", config.get(HiveOptions.METASTORE_URI));
        assertEquals(SchemaSaveMode.RECREATE_SCHEMA, config.get(HiveSinkOptions.SCHEMA_SAVE_MODE));
        assertEquals(template, config.get(HiveSinkOptions.SAVE_MODE_CREATE_TEMPLATE));
    }
}
