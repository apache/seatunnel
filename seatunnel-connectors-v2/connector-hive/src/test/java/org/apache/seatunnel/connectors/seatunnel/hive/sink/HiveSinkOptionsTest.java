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

import java.util.Arrays;
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
    void testTableFormatOption() {
        assertNotNull(HiveSinkOptions.TABLE_FORMAT);
        assertEquals("table_format", HiveSinkOptions.TABLE_FORMAT.key());
        assertEquals("PARQUET", HiveSinkOptions.TABLE_FORMAT.defaultValue());
    }

    @Test
    void testPartitionFieldsOption() {
        assertNotNull(HiveSinkOptions.PARTITION_FIELDS);
        assertEquals("partition_fields", HiveSinkOptions.PARTITION_FIELDS.key());
        assertTrue(HiveSinkOptions.PARTITION_FIELDS.defaultValue().isEmpty());
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
    void testReadTableFormatFromConfig() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(HiveSinkOptions.TABLE_FORMAT.key(), "ORC");

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        String format = config.get(HiveSinkOptions.TABLE_FORMAT);
        assertEquals("ORC", format);
    }

    @Test
    void testDefaultValues() {
        Map<String, Object> configMap = new HashMap<>();
        // Empty config - should use defaults

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        // Test default SaveMode
        SchemaSaveMode defaultSaveMode = config.get(HiveSinkOptions.SCHEMA_SAVE_MODE);
        assertEquals(SchemaSaveMode.CREATE_SCHEMA_WHEN_NOT_EXIST, defaultSaveMode);

        // Test default table format
        String defaultFormat = config.get(HiveSinkOptions.TABLE_FORMAT);
        assertEquals("PARQUET", defaultFormat);
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

        String defaultFormat = config.get(HiveSinkOptions.TABLE_FORMAT);
        assertEquals("PARQUET", defaultFormat);
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
    void testTableFormatValues() {
        String[] supportedFormats = {"PARQUET", "ORC", "TEXTFILE", "TEXT"};

        for (String format : supportedFormats) {
            Map<String, Object> configMap = new HashMap<>();
            configMap.put(HiveSinkOptions.TABLE_FORMAT.key(), format);

            ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
            String readFormat = config.get(HiveSinkOptions.TABLE_FORMAT);

            assertEquals(format, readFormat, "Failed to read table format: " + format);
        }
    }

    @Test
    void testConfigurationWithExistingHiveOptions() {
        Map<String, Object> configMap = new HashMap<>();
        // Existing Hive options
        configMap.put(HiveOptions.TABLE_NAME.key(), "analytics.user_events");
        configMap.put(HiveOptions.METASTORE_URI.key(), "thrift://hive-metastore:9083");

        // New SaveMode options
        configMap.put(HiveSinkOptions.SCHEMA_SAVE_MODE.key(), "RECREATE_SCHEMA");
        configMap.put(HiveSinkOptions.TABLE_FORMAT.key(), "ORC");
        configMap.put(HiveSinkOptions.PARTITION_FIELDS.key(), Arrays.asList("year", "month"));

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        // Verify all options are readable
        assertEquals("analytics.user_events", config.get(HiveOptions.TABLE_NAME));
        assertEquals("thrift://hive-metastore:9083", config.get(HiveOptions.METASTORE_URI));
        assertEquals(SchemaSaveMode.RECREATE_SCHEMA, config.get(HiveSinkOptions.SCHEMA_SAVE_MODE));
        assertEquals("ORC", config.get(HiveSinkOptions.TABLE_FORMAT));
    }
}
