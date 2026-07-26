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

package org.apache.seatunnel.api.metalake;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.metadata.MetadataConfig;
import org.apache.seatunnel.api.metadata.MetadataProvider;
import org.apache.seatunnel.api.metadata.MetadataProviderManager;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.lang.reflect.Field;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class TableSchemaDiscovererTest {

    private static final String TEST_CATALOG_NAME = "test_catalog";

    /** Create a default MetadataConfig for testing. */
    private MetadataConfig createDefaultMetadataConfig() {
        MetadataConfig config = new MetadataConfig();
        config.setEnabled(true);
        config.setKind("test-mock-provider");
        config.setProperties(new HashMap<>());
        return config;
    }

    @Test
    void testDiscoverTableSchemasWithSingleSchemaFields() throws URISyntaxException {
        Config config = loadConfig("/conf/table_schema_discoverer/single_schema_field.conf");
        ReadonlyConfig sourceOptions = ReadonlyConfig.fromConfig(config);
        MetadataConfig metadataConfig = createDefaultMetadataConfig();
        try (TableSchemaDiscoverer discoverer =
                new TableSchemaDiscoverer(metadataConfig, sourceOptions, TEST_CATALOG_NAME)) {
            Assertions.assertFalse(discoverer.enableMetaLakeClient(sourceOptions));
            List<CatalogTable> result = discoverer.discoverTableSchemas();
            Assertions.assertEquals(1, result.size());
            Assertions.assertEquals(TEST_CATALOG_NAME, result.get(0).getCatalogName());
            Assertions.assertEquals(
                    TablePath.of("default", "default", "default"), result.get(0).getTablePath());
            Assertions.assertEquals(3, result.get(0).getTableSchema().getColumns().size());
        }
    }

    @Test
    void testDiscoverTableSchemasWithSingleSchemaMetadataTableId() throws Exception {
        // Register mock provider before test
        MockMetadataTableProvider mockProvider = new MockMetadataTableProvider();
        registerMockProvider(mockProvider);

        try {
            Config config = loadConfig("/conf/table_schema_discoverer/single_metadata_table.conf");
            ReadonlyConfig sourceOptions = ReadonlyConfig.fromConfig(config);
            MetadataConfig metadataConfig = createDefaultMetadataConfig();
            try (TableSchemaDiscoverer discoverer =
                    new TableSchemaDiscoverer(metadataConfig, sourceOptions, TEST_CATALOG_NAME)) {
                Assertions.assertTrue(discoverer.enableMetaLakeClient(sourceOptions));
                List<CatalogTable> result = discoverer.discoverTableSchemas();

                // Verify result
                Assertions.assertEquals(1, result.size());

                CatalogTable table = result.get(0);
                Assertions.assertEquals(TEST_CATALOG_NAME, table.getCatalogName());
                // TablePath should be constructed from metadata_table_id since no table option is
                // provided
                Assertions.assertEquals(
                        TablePath.of("test_catalog.test_schema.test_table"), table.getTablePath());

                // Verify table schema columns
                List<org.apache.seatunnel.api.table.catalog.Column> columns =
                        table.getTableSchema().getColumns();
                Assertions.assertEquals(4, columns.size());

                // Verify first column (id)
                Assertions.assertEquals("id", columns.get(0).getName());
                Assertions.assertEquals(BasicType.INT_TYPE, columns.get(0).getDataType());
                Assertions.assertFalse(columns.get(0).isNullable());

                // Verify second column (name)
                Assertions.assertEquals("name", columns.get(1).getName());
                Assertions.assertEquals(BasicType.STRING_TYPE, columns.get(1).getDataType());
                Assertions.assertTrue(columns.get(1).isNullable());

                // Verify third column (email)
                Assertions.assertEquals("email", columns.get(2).getName());
                Assertions.assertEquals(BasicType.STRING_TYPE, columns.get(2).getDataType());
                Assertions.assertTrue(columns.get(2).isNullable());

                // Verify fourth column (age)
                Assertions.assertEquals("age", columns.get(3).getName());
                Assertions.assertEquals(BasicType.INT_TYPE, columns.get(3).getDataType());
                Assertions.assertTrue(columns.get(3).isNullable());
            }
        } finally {
            // Clear the cached provider after test
            MetadataProviderManager.clearCache();
        }
    }

    @Test
    void testDiscoverTableSchemasWithMultipleTablesFields() throws URISyntaxException {
        Config config = loadConfig("/conf/table_schema_discoverer/multiple_tables_fields.conf");
        ReadonlyConfig sourceOptions = ReadonlyConfig.fromConfig(config);
        MetadataConfig metadataConfig = createDefaultMetadataConfig();
        try (TableSchemaDiscoverer discoverer =
                new TableSchemaDiscoverer(metadataConfig, sourceOptions, TEST_CATALOG_NAME)) {
            Assertions.assertFalse(discoverer.enableMetaLakeClient(sourceOptions));
            List<CatalogTable> result = discoverer.discoverTableSchemas();
            Assertions.assertEquals(2, result.size());
            Assertions.assertEquals(TEST_CATALOG_NAME, result.get(0).getCatalogName());
            Assertions.assertEquals(
                    TablePath.of("db", null, "table1"), result.get(0).getTablePath());
            Assertions.assertEquals(1, result.get(0).getTableSchema().getColumns().size());
            Assertions.assertEquals(TEST_CATALOG_NAME, result.get(1).getCatalogName());
            Assertions.assertEquals(
                    TablePath.of("db", null, "table2"), result.get(1).getTablePath());
            Assertions.assertEquals(3, result.get(1).getTableSchema().getColumns().size());
        }
    }

    @Test
    void testDiscoverTableSchemasWithMultipleTablesMetadataTableId() throws Exception {
        // Register mock provider before test
        MockMetadataTableProvider mockProvider = new MockMetadataTableProvider();
        registerMockProvider(mockProvider);

        try {
            Config config =
                    loadConfig("/conf/table_schema_discoverer/multiple_tables_metadata_table.conf");
            ReadonlyConfig sourceOptions = ReadonlyConfig.fromConfig(config);
            MetadataConfig metadataConfig = createDefaultMetadataConfig();
            try (TableSchemaDiscoverer discoverer =
                    new TableSchemaDiscoverer(metadataConfig, sourceOptions, TEST_CATALOG_NAME)) {
                Assertions.assertTrue(discoverer.enableMetaLakeClient(sourceOptions));
                List<CatalogTable> result = discoverer.discoverTableSchemas();
                Assertions.assertEquals(2, result.size());

                // Verify first table (table1)
                CatalogTable table1 = result.get(0);
                Assertions.assertEquals(TEST_CATALOG_NAME, table1.getCatalogName());
                Assertions.assertEquals(
                        TablePath.of("test_database.test_schema.test_table1"),
                        table1.getTablePath());
                List<org.apache.seatunnel.api.table.catalog.Column> table1Columns =
                        table1.getTableSchema().getColumns();
                Assertions.assertEquals(2, table1Columns.size());
                Assertions.assertEquals("user_id", table1Columns.get(0).getName());
                Assertions.assertEquals(BasicType.LONG_TYPE, table1Columns.get(0).getDataType());
                Assertions.assertFalse(table1Columns.get(0).isNullable());
                Assertions.assertEquals("username", table1Columns.get(1).getName());
                Assertions.assertEquals(BasicType.STRING_TYPE, table1Columns.get(1).getDataType());
                Assertions.assertTrue(table1Columns.get(1).isNullable());

                // Verify second table (table2)
                CatalogTable table2 = result.get(1);
                Assertions.assertEquals(TEST_CATALOG_NAME, table2.getCatalogName());
                Assertions.assertEquals(
                        TablePath.of("test_catalog.test_schema.table2"), table2.getTablePath());
                List<org.apache.seatunnel.api.table.catalog.Column> table2Columns =
                        table2.getTableSchema().getColumns();
                Assertions.assertEquals(3, table2Columns.size());
                Assertions.assertEquals("order_id", table2Columns.get(0).getName());
                Assertions.assertEquals(BasicType.LONG_TYPE, table2Columns.get(0).getDataType());
                Assertions.assertFalse(table2Columns.get(0).isNullable());
                Assertions.assertEquals("amount", table2Columns.get(1).getName());
                Assertions.assertEquals(BasicType.DOUBLE_TYPE, table2Columns.get(1).getDataType());
                Assertions.assertTrue(table2Columns.get(1).isNullable());
                Assertions.assertEquals("status", table2Columns.get(2).getName());
                Assertions.assertEquals(BasicType.STRING_TYPE, table2Columns.get(2).getDataType());
                Assertions.assertTrue(table2Columns.get(2).isNullable());
            }
        } finally {
            // Clear the cached provider after test
            MetadataProviderManager.clearCache();
        }
    }

    @Test
    void testDiscoverTableSchemasWithMultipleTablesMixedFieldsAndMetadataTableId()
            throws Exception {
        // Register mock provider before test
        MockMetadataTableProvider mockProvider = new MockMetadataTableProvider();
        registerMockProvider(mockProvider);

        try {
            Config config = loadConfig("/conf/table_schema_discoverer/multiple_tables_mixed.conf");
            ReadonlyConfig sourceOptions = ReadonlyConfig.fromConfig(config);
            MetadataConfig metadataConfig = createDefaultMetadataConfig();
            try (TableSchemaDiscoverer discoverer =
                    new TableSchemaDiscoverer(metadataConfig, sourceOptions, TEST_CATALOG_NAME)) {
                Assertions.assertTrue(discoverer.enableMetaLakeClient(sourceOptions));
                List<CatalogTable> result = discoverer.discoverTableSchemas();
                Assertions.assertEquals(2, result.size());

                // Verify first table (from fields config)
                CatalogTable table1 = result.get(0);
                Assertions.assertEquals(TEST_CATALOG_NAME, table1.getCatalogName());
                Assertions.assertEquals(TablePath.of("db.table1"), table1.getTablePath());
                List<org.apache.seatunnel.api.table.catalog.Column> table1Columns =
                        table1.getTableSchema().getColumns();
                Assertions.assertEquals(2, table1Columns.size());
                Assertions.assertEquals("id", table1Columns.get(0).getName());
                Assertions.assertEquals(BasicType.INT_TYPE, table1Columns.get(0).getDataType());
                Assertions.assertEquals("name", table1Columns.get(1).getName());
                Assertions.assertEquals(BasicType.STRING_TYPE, table1Columns.get(1).getDataType());

                // Verify second table (from metadata_table_id)
                CatalogTable table2 = result.get(1);
                Assertions.assertEquals(TEST_CATALOG_NAME, table2.getCatalogName());
                Assertions.assertEquals(
                        TablePath.of("test_catalog.test_schema.table2"), table2.getTablePath());
                List<org.apache.seatunnel.api.table.catalog.Column> table2Columns =
                        table2.getTableSchema().getColumns();
                Assertions.assertEquals(3, table2Columns.size());
                Assertions.assertEquals("order_id", table2Columns.get(0).getName());
                Assertions.assertEquals(BasicType.LONG_TYPE, table2Columns.get(0).getDataType());
                Assertions.assertEquals("amount", table2Columns.get(1).getName());
                Assertions.assertEquals(BasicType.DOUBLE_TYPE, table2Columns.get(1).getDataType());
                Assertions.assertEquals("status", table2Columns.get(2).getName());
                Assertions.assertEquals(BasicType.STRING_TYPE, table2Columns.get(2).getDataType());
            }
        } finally {
            // Clear the cached provider after test
            MetadataProviderManager.clearCache();
        }
    }

    @Test
    void testDiscoverTableSchemaWithSingleParquetNoSchema() throws URISyntaxException {
        Config config = loadConfig("/conf/table_schema_discoverer/single_no_schema.conf");
        ReadonlyConfig sourceOptions = ReadonlyConfig.fromConfig(config);
        MetadataConfig metadataConfig = createDefaultMetadataConfig();
        try (TableSchemaDiscoverer discoverer =
                new TableSchemaDiscoverer(metadataConfig, sourceOptions, TEST_CATALOG_NAME)) {
            Assertions.assertFalse(discoverer.enableMetaLakeClient(sourceOptions));
            List<CatalogTable> result = discoverer.discoverTableSchemas();
            // When no schema is configured, should return a simple text table
            Assertions.assertEquals(1, result.size());
            // Catalog name is "schema" from buildSimpleTextTable()
            Assertions.assertEquals("schema", result.get(0).getCatalogName());
            // TablePath is (database="default", schema=null, tableName="default")
            Assertions.assertEquals(
                    TablePath.of("default", null, "default"), result.get(0).getTablePath());
            Assertions.assertNotNull(result.get(0).getTableSchema());
            Assertions.assertEquals(1, result.get(0).getTableSchema().getColumns().size());
            Assertions.assertEquals(
                    "content", result.get(0).getTableSchema().getColumns().get(0).getName());
        }
    }

    @Test
    void testDiscoverTableSchemasWithMultipleTablesNoSchemaMixedFormat() throws URISyntaxException {
        Config config =
                loadConfig(
                        "/conf/table_schema_discoverer/multiple_tables_no_schema_mixed_format.conf");
        ReadonlyConfig sourceOptions = ReadonlyConfig.fromConfig(config);
        MetadataConfig metadataConfig = createDefaultMetadataConfig();
        try (TableSchemaDiscoverer discoverer =
                new TableSchemaDiscoverer(metadataConfig, sourceOptions, TEST_CATALOG_NAME)) {
            Assertions.assertFalse(discoverer.enableMetaLakeClient(sourceOptions));
            List<CatalogTable> result = discoverer.discoverTableSchemas();
            // Should return 3 tables for parquet, orc, and binary file formats
            Assertions.assertEquals(3, result.size());
            // First table (parquet) - db.parquet_table
            // catalogName is "schema" from buildSimpleTextTable()
            Assertions.assertEquals("schema", result.get(0).getCatalogName());
            Assertions.assertEquals(
                    TablePath.of("db", "parquet_table"), result.get(0).getTablePath());
            Assertions.assertNotNull(result.get(0).getTableSchema());
            Assertions.assertEquals(1, result.get(0).getTableSchema().getColumns().size());
            Assertions.assertEquals(
                    "content", result.get(0).getTableSchema().getColumns().get(0).getName());
            // Second table (orc) - db.orc_table
            Assertions.assertEquals("schema", result.get(1).getCatalogName());
            Assertions.assertEquals(TablePath.of("db", "orc_table"), result.get(1).getTablePath());
            Assertions.assertNotNull(result.get(1).getTableSchema());
            Assertions.assertEquals(1, result.get(1).getTableSchema().getColumns().size());
            Assertions.assertEquals(
                    "content", result.get(1).getTableSchema().getColumns().get(0).getName());
            // Third table (binary) - db.binary_table
            Assertions.assertEquals("schema", result.get(2).getCatalogName());
            Assertions.assertEquals(
                    TablePath.of("db", "binary_table"), result.get(2).getTablePath());
            Assertions.assertNotNull(result.get(2).getTableSchema());
            Assertions.assertEquals(1, result.get(2).getTableSchema().getColumns().size());
            Assertions.assertEquals(
                    "content", result.get(2).getTableSchema().getColumns().get(0).getName());
        }
    }

    /**
     * Load configuration file from test resources.
     *
     * @param configPath the path to the configuration file
     * @return the Config object
     * @throws URISyntaxException if the path is invalid
     */
    private Config loadConfig(String configPath) throws URISyntaxException {
        URL resourceUrl = getClass().getResource(configPath);
        if (resourceUrl == null) {
            throw new IllegalArgumentException("Config file not found: " + configPath);
        }
        File configFile = Paths.get(resourceUrl.toURI()).toFile();
        return ConfigFactory.parseFile(configFile);
    }

    /**
     * Register a mock MetadataProvider for testing table schema discovery from metadata lake.
     *
     * @param provider the mock provider to register
     */
    private void registerMockProvider(MetadataProvider provider) {
        try {
            Field providerField = MetadataProviderManager.class.getDeclaredField("cachedProvider");
            providerField.setAccessible(true);
            providerField.set(null, provider);
        } catch (Exception e) {
            throw new RuntimeException("Failed to register mock provider for testing", e);
        }
    }

    /**
     * Mock MetadataProvider for testing table schema discovery from metadata lake. Returns a
     * predefined table schema with specific columns and types.
     */
    public static class MockMetadataTableProvider implements MetadataProvider {

        @Override
        public String kind() {
            return "test-mock-provider";
        }

        @Override
        public void init(org.apache.seatunnel.shade.com.typesafe.config.Config config) {
            // No-op for testing
        }

        @Override
        public Map<String, Object> datasourceMap(
                String connectorIdentifier, String metaDataDatasourceId) {
            return new HashMap<>();
        }

        @Override
        public Optional<TableSchema> tableSchema(String metaDataTableId) {
            // Return different schemas based on the table ID
            switch (metaDataTableId) {
                case "test_catalog.test_schema.test_table":
                    return Optional.of(
                            TableSchema.builder()
                                    .column(
                                            PhysicalColumn.of(
                                                    "id",
                                                    BasicType.INT_TYPE,
                                                    0,
                                                    false,
                                                    null,
                                                    "Primary key"))
                                    .column(
                                            PhysicalColumn.of(
                                                    "name",
                                                    BasicType.STRING_TYPE,
                                                    1,
                                                    true,
                                                    null,
                                                    "User name"))
                                    .column(
                                            PhysicalColumn.of(
                                                    "email",
                                                    BasicType.STRING_TYPE,
                                                    2,
                                                    true,
                                                    null,
                                                    "User email"))
                                    .column(
                                            PhysicalColumn.of(
                                                    "age",
                                                    BasicType.INT_TYPE,
                                                    3,
                                                    true,
                                                    null,
                                                    "User age"))
                                    .build());

                case "test_catalog.test_schema.table1":
                    return Optional.of(
                            TableSchema.builder()
                                    .column(
                                            PhysicalColumn.of(
                                                    "user_id",
                                                    BasicType.LONG_TYPE,
                                                    0,
                                                    false,
                                                    null,
                                                    "User ID"))
                                    .column(
                                            PhysicalColumn.of(
                                                    "username",
                                                    BasicType.STRING_TYPE,
                                                    1,
                                                    true,
                                                    null,
                                                    "Username"))
                                    .build());

                case "test_catalog.test_schema.table2":
                    return Optional.of(
                            TableSchema.builder()
                                    .column(
                                            PhysicalColumn.of(
                                                    "order_id",
                                                    BasicType.LONG_TYPE,
                                                    0,
                                                    false,
                                                    null,
                                                    "Order ID"))
                                    .column(
                                            PhysicalColumn.of(
                                                    "amount",
                                                    BasicType.DOUBLE_TYPE,
                                                    1,
                                                    true,
                                                    null,
                                                    "Order amount"))
                                    .column(
                                            PhysicalColumn.of(
                                                    "status",
                                                    BasicType.STRING_TYPE,
                                                    2,
                                                    true,
                                                    null,
                                                    "Order status"))
                                    .build());

                default:
                    return Optional.empty();
            }
        }

        @Override
        public void close() {
            // No-op for testing
        }
    }
}
