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

package org.apache.seatunnel.api.sink;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TablePlaceholderProcessorTest {
    private static final Option<String> DATABASE =
            Options.key("database").stringType().noDefaultValue();
    private static final Option<String> SCHEMA =
            Options.key("schema").stringType().noDefaultValue();
    private static final Option<String> TABLE = Options.key("table").stringType().noDefaultValue();
    private static final Option<String> PRIMARY_KEY =
            Options.key("primary_key").stringType().noDefaultValue();
    private static final Option<List<String>> PRIMARY_KEY_ARRAY =
            Options.key("primary_key_array").listType(String.class).noDefaultValue();
    private static final Option<String> UNIQUE_KEY =
            Options.key("unique_key").stringType().noDefaultValue();
    private static final Option<List<String>> UNIQUE_KEY_ARRAY =
            Options.key("unique_key_array").listType(String.class).noDefaultValue();
    private static final Option<String> FIELD_NAMES =
            Options.key("field_names").stringType().noDefaultValue();
    private static final Option<List<String>> FIELD_NAMES_ARRAY =
            Options.key("field_names_array").listType(String.class).noDefaultValue();

    @Test
    public void testSinkOptions() {
        ReadonlyConfig config = createConfig();
        CatalogTable table = createTestTable();
        ReadonlyConfig newConfig = TablePlaceholderProcessor.replaceTablePlaceholder(config, table);

        Assertions.assertEquals("xyz_my-database_test", newConfig.get(DATABASE));
        Assertions.assertEquals("xyz_my-schema_test", newConfig.get(SCHEMA));
        Assertions.assertEquals("xyz_my-table_test", newConfig.get(TABLE));
        Assertions.assertEquals("f1,f2", newConfig.get(PRIMARY_KEY));
        Assertions.assertEquals("f3,f4", newConfig.get(UNIQUE_KEY));
        Assertions.assertEquals("f1,f2,f3,f4,f5", newConfig.get(FIELD_NAMES));
        Assertions.assertEquals(Arrays.asList("f1", "f2"), newConfig.get(PRIMARY_KEY_ARRAY));
        Assertions.assertEquals(Arrays.asList("f3", "f4"), newConfig.get(UNIQUE_KEY_ARRAY));
        Assertions.assertEquals(
                Arrays.asList("f1", "f2", "f3", "f4", "f5"), newConfig.get(FIELD_NAMES_ARRAY));
    }

    @Test
    public void testSinkOptionsWithNoTablePath() {
        ReadonlyConfig config = createConfig();
        CatalogTable table = createTestTableWithNoDatabaseAndSchemaName();
        ReadonlyConfig newConfig = TablePlaceholderProcessor.replaceTablePlaceholder(config, table);

        Assertions.assertEquals("xyz_default_db_test", newConfig.get(DATABASE));
        Assertions.assertEquals("xyz_default_schema_test", newConfig.get(SCHEMA));
        Assertions.assertEquals("xyz_default_table_test", newConfig.get(TABLE));
        Assertions.assertEquals("f1,f2", newConfig.get(PRIMARY_KEY));
        Assertions.assertEquals("f3,f4", newConfig.get(UNIQUE_KEY));
        Assertions.assertEquals("f1,f2,f3,f4,f5", newConfig.get(FIELD_NAMES));
        Assertions.assertEquals(Arrays.asList("f1", "f2"), newConfig.get(PRIMARY_KEY_ARRAY));
        Assertions.assertEquals(Arrays.asList("f3", "f4"), newConfig.get(UNIQUE_KEY_ARRAY));
        Assertions.assertEquals(
                Arrays.asList("f1", "f2", "f3", "f4", "f5"), newConfig.get(FIELD_NAMES_ARRAY));
    }

    @Test
    public void testSinkOptionsWithExcludeKeys() {
        ReadonlyConfig config = createConfig();
        CatalogTable table = createTestTableWithNoDatabaseAndSchemaName();
        ReadonlyConfig newConfig =
                TablePlaceholderProcessor.replaceTablePlaceholder(
                        config, table, Arrays.asList(DATABASE.key()));

        Assertions.assertEquals("xyz_${database_name: default_db}_test", newConfig.get(DATABASE));
        Assertions.assertEquals("xyz_default_schema_test", newConfig.get(SCHEMA));
        Assertions.assertEquals("xyz_default_table_test", newConfig.get(TABLE));
        Assertions.assertEquals("f1,f2", newConfig.get(PRIMARY_KEY));
        Assertions.assertEquals("f3,f4", newConfig.get(UNIQUE_KEY));
        Assertions.assertEquals("f1,f2,f3,f4,f5", newConfig.get(FIELD_NAMES));
        Assertions.assertEquals(Arrays.asList("f1", "f2"), newConfig.get(PRIMARY_KEY_ARRAY));
        Assertions.assertEquals(Arrays.asList("f3", "f4"), newConfig.get(UNIQUE_KEY_ARRAY));
        Assertions.assertEquals(
                Arrays.asList("f1", "f2", "f3", "f4", "f5"), newConfig.get(FIELD_NAMES_ARRAY));
    }

    @Test
    public void testSinkOptionsWithMultiTable() {
        ReadonlyConfig config = createConfig();
        CatalogTable table1 = createTestTable();
        CatalogTable table2 = createTestTableWithNoDatabaseAndSchemaName();
        ReadonlyConfig newConfig1 =
                TablePlaceholderProcessor.replaceTablePlaceholder(config, table1, Arrays.asList());
        ReadonlyConfig newConfig2 =
                TablePlaceholderProcessor.replaceTablePlaceholder(config, table2, Arrays.asList());

        Assertions.assertEquals("xyz_my-database_test", newConfig1.get(DATABASE));
        Assertions.assertEquals("xyz_my-schema_test", newConfig1.get(SCHEMA));
        Assertions.assertEquals("xyz_my-table_test", newConfig1.get(TABLE));
        Assertions.assertEquals("f1,f2", newConfig1.get(PRIMARY_KEY));
        Assertions.assertEquals("f3,f4", newConfig1.get(UNIQUE_KEY));
        Assertions.assertEquals("f1,f2,f3,f4,f5", newConfig1.get(FIELD_NAMES));
        Assertions.assertEquals(Arrays.asList("f1", "f2"), newConfig1.get(PRIMARY_KEY_ARRAY));
        Assertions.assertEquals(Arrays.asList("f3", "f4"), newConfig1.get(UNIQUE_KEY_ARRAY));
        Assertions.assertEquals(
                Arrays.asList("f1", "f2", "f3", "f4", "f5"), newConfig1.get(FIELD_NAMES_ARRAY));

        Assertions.assertEquals("xyz_default_db_test", newConfig2.get(DATABASE));
        Assertions.assertEquals("xyz_default_schema_test", newConfig2.get(SCHEMA));
        Assertions.assertEquals("xyz_default_table_test", newConfig2.get(TABLE));
        Assertions.assertEquals("f1,f2", newConfig2.get(PRIMARY_KEY));
        Assertions.assertEquals("f3,f4", newConfig2.get(UNIQUE_KEY));
        Assertions.assertEquals("f1,f2,f3,f4,f5", newConfig2.get(FIELD_NAMES));
        Assertions.assertEquals(Arrays.asList("f1", "f2"), newConfig2.get(PRIMARY_KEY_ARRAY));
        Assertions.assertEquals(Arrays.asList("f3", "f4"), newConfig2.get(UNIQUE_KEY_ARRAY));
        Assertions.assertEquals(
                Arrays.asList("f1", "f2", "f3", "f4", "f5"), newConfig2.get(FIELD_NAMES_ARRAY));
    }

    private static ReadonlyConfig createConfig() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(DATABASE.key(), "xyz_${database_name: default_db}_test");
        configMap.put(SCHEMA.key(), "xyz_${schema_name: default_schema}_test");
        configMap.put(TABLE.key(), "xyz_${table_name: default_table}_test");
        configMap.put(PRIMARY_KEY.key(), "${primary_key}");
        configMap.put(UNIQUE_KEY.key(), "${unique_key}");
        configMap.put(FIELD_NAMES.key(), "${field_names}");
        configMap.put(PRIMARY_KEY_ARRAY.key(), Arrays.asList("${primary_key}"));
        configMap.put(UNIQUE_KEY_ARRAY.key(), Arrays.asList("${unique_key}"));
        configMap.put(FIELD_NAMES_ARRAY.key(), Arrays.asList("${field_names}"));
        return ReadonlyConfig.fromMap(configMap);
    }

    private static CatalogTable createTestTableWithNoDatabaseAndSchemaName() {
        TableIdentifier tableId = TableIdentifier.of("my-catalog", null, null, "default_table");
        TableSchema tableSchema =
                TableSchema.builder()
                        .primaryKey(PrimaryKey.of("my-pk", Arrays.asList("f1", "f2")))
                        .constraintKey(
                                ConstraintKey.of(
                                        ConstraintKey.ConstraintType.UNIQUE_KEY,
                                        "my-uk",
                                        Arrays.asList(
                                                ConstraintKey.ConstraintKeyColumn.of(
                                                        "f3", ConstraintKey.ColumnSortType.ASC),
                                                ConstraintKey.ConstraintKeyColumn.of(
                                                        "f4", ConstraintKey.ColumnSortType.ASC))))
                        .column(
                                PhysicalColumn.builder()
                                        .name("f1")
                                        .dataType(BasicType.STRING_TYPE)
                                        .build())
                        .column(
                                PhysicalColumn.builder()
                                        .name("f2")
                                        .dataType(BasicType.STRING_TYPE)
                                        .build())
                        .column(
                                PhysicalColumn.builder()
                                        .name("f3")
                                        .dataType(BasicType.STRING_TYPE)
                                        .build())
                        .column(
                                PhysicalColumn.builder()
                                        .name("f4")
                                        .dataType(BasicType.STRING_TYPE)
                                        .build())
                        .column(
                                PhysicalColumn.builder()
                                        .name("f5")
                                        .dataType(BasicType.STRING_TYPE)
                                        .build())
                        .build();
        return CatalogTable.of(
                tableId, tableSchema, Collections.emptyMap(), Collections.emptyList(), null);
    }

    private static CatalogTable createTestTable() {
        TableIdentifier tableId =
                TableIdentifier.of("my-catalog", "my-database", "my-schema", "my-table");
        TableSchema tableSchema =
                TableSchema.builder()
                        .primaryKey(PrimaryKey.of("my-pk", Arrays.asList("f1", "f2")))
                        .constraintKey(
                                ConstraintKey.of(
                                        ConstraintKey.ConstraintType.UNIQUE_KEY,
                                        "my-uk",
                                        Arrays.asList(
                                                ConstraintKey.ConstraintKeyColumn.of(
                                                        "f3", ConstraintKey.ColumnSortType.ASC),
                                                ConstraintKey.ConstraintKeyColumn.of(
                                                        "f4", ConstraintKey.ColumnSortType.ASC))))
                        .column(
                                PhysicalColumn.builder()
                                        .name("f1")
                                        .dataType(BasicType.STRING_TYPE)
                                        .build())
                        .column(
                                PhysicalColumn.builder()
                                        .name("f2")
                                        .dataType(BasicType.STRING_TYPE)
                                        .build())
                        .column(
                                PhysicalColumn.builder()
                                        .name("f3")
                                        .dataType(BasicType.STRING_TYPE)
                                        .build())
                        .column(
                                PhysicalColumn.builder()
                                        .name("f4")
                                        .dataType(BasicType.STRING_TYPE)
                                        .build())
                        .column(
                                PhysicalColumn.builder()
                                        .name("f5")
                                        .dataType(BasicType.STRING_TYPE)
                                        .build())
                        .build();
        return CatalogTable.of(
                tableId, tableSchema, Collections.emptyMap(), Collections.emptyList(), null);
    }
}
