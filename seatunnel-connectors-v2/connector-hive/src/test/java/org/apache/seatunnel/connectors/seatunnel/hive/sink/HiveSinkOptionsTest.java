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

        String defaultTemplate = HiveSinkOptions.SAVE_MODE_CREATE_TEMPLATE.defaultValue();
        assertNotNull(defaultTemplate);
        assertTrue(defaultTemplate.contains("${database}"));
        assertTrue(defaultTemplate.contains("${table}"));
        assertTrue(defaultTemplate.contains("${rowtype_fields}"));
        assertTrue(defaultTemplate.contains("${table_location}"));
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
    void testReadCreateTemplateFromConfig() {
        String customTemplate =
                "CREATE TABLE ${database}.${table} (${rowtype_fields}) STORED AS ORC";

        Map<String, Object> configMap = new HashMap<>();
        configMap.put(HiveSinkOptions.SAVE_MODE_CREATE_TEMPLATE.key(), customTemplate);

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        String template = config.get(HiveSinkOptions.SAVE_MODE_CREATE_TEMPLATE);
        assertEquals(customTemplate, template);
    }

    @Test
    void testDefaultValues() {
        Map<String, Object> configMap = new HashMap<>();
        // Empty config - should use defaults

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        // Test default SaveMode
        SchemaSaveMode defaultSaveMode = config.get(HiveSinkOptions.SCHEMA_SAVE_MODE);
        assertEquals(SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST, defaultSaveMode);

        // Test default template
        String defaultTemplate = config.get(HiveSinkOptions.SAVE_MODE_CREATE_TEMPLATE);
        assertNotNull(defaultTemplate);
        assertTrue(defaultTemplate.length() > 0);
    }

    @Test
    void testOptionalConfiguration() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(HiveOptions.TABLE_NAME.key(), "test_db.test_table");
        configMap.put(HiveOptions.METASTORE_URI.key(), "thrift://localhost:9083");
        // No SaveMode options

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        // SaveMode options should use defaults when not specified
        SchemaSaveMode defaultSaveMode = config.get(HiveSinkOptions.SCHEMA_SAVE_MODE);
        assertEquals(SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST, defaultSaveMode);

        String defaultTemplate = config.get(HiveSinkOptions.SAVE_MODE_CREATE_TEMPLATE);
        assertNotNull(defaultTemplate);
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
    void testTemplateVariables() {
        String template = HiveSinkOptions.SAVE_MODE_CREATE_TEMPLATE.defaultValue();

        // Verify all required template variables are present
        assertTrue(template.contains("${database}"), "Template should contain ${database}");
        assertTrue(template.contains("${table}"), "Template should contain ${table}");
        assertTrue(
                template.contains("${rowtype_fields}"),
                "Template should contain ${rowtype_fields}");
        assertTrue(
                template.contains("${table_location}"),
                "Template should contain ${table_location}");
    }

    @Test
    void testComplexTemplateConfiguration() {
        String complexTemplate =
                "CREATE TABLE IF NOT EXISTS `${database}`.`${table}` (\n"
                        + "  ${rowtype_fields}\n"
                        + ") \n"
                        + "PARTITIONED BY (year INT, month INT)\n"
                        + "STORED AS PARQUET\n"
                        + "LOCATION '${table_location}'\n"
                        + "TBLPROPERTIES (\n"
                        + "  'parquet.compression'='SNAPPY',\n"
                        + "  'parquet.enable.dictionary'='true'\n"
                        + ")";

        Map<String, Object> configMap = new HashMap<>();
        configMap.put(HiveSinkOptions.SCHEMA_SAVE_MODE.key(), "CREATE_SCHEMA_WHEN_NOT_EXIST");
        configMap.put(HiveSinkOptions.SAVE_MODE_CREATE_TEMPLATE.key(), complexTemplate);

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        assertEquals(
                SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST,
                config.get(HiveSinkOptions.SCHEMA_SAVE_MODE));
        assertEquals(complexTemplate, config.get(HiveSinkOptions.SAVE_MODE_CREATE_TEMPLATE));
    }

    @Test
    void testConfigurationWithExistingHiveOptions() {
        Map<String, Object> configMap = new HashMap<>();
        // Existing Hive options
        configMap.put(HiveOptions.TABLE_NAME.key(), "analytics.user_events");
        configMap.put(HiveOptions.METASTORE_URI.key(), "thrift://hive-metastore:9083");

        // New SaveMode options
        configMap.put(HiveSinkOptions.SCHEMA_SAVE_MODE.key(), "RECREATE_SCHEMA");
        configMap.put(
                HiveSinkOptions.SAVE_MODE_CREATE_TEMPLATE.key(),
                "CREATE TABLE ${database}.${table} (${rowtype_fields}) STORED AS ORC");

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        // Verify all options are readable
        assertEquals("analytics.user_events", config.get(HiveOptions.TABLE_NAME));
        assertEquals("thrift://hive-metastore:9083", config.get(HiveOptions.METASTORE_URI));
        assertEquals(SchemaSaveMode.RECREATE_SCHEMA, config.get(HiveSinkOptions.SCHEMA_SAVE_MODE));
        assertTrue(config.get(HiveSinkOptions.SAVE_MODE_CREATE_TEMPLATE).contains("STORED AS ORC"));
    }
}
