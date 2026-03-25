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
import org.apache.seatunnel.api.metadata.MetaDataConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;

public class TableSchemaDiscovererTest {

    private static final String TEST_CATALOG_NAME = "test_catalog";

    /** Create a default MetaDataConfig for testing. */
    private MetaDataConfig createDefaultMetaDataConfig() {
        MetaDataConfig config = new MetaDataConfig();
        config.setEnabled(false);
        config.setKind("gravitino");
        config.setProperties(new HashMap<>());
        return config;
    }

    @Test
    void testDiscoverTableSchemasWithSingleSchemaFields() throws URISyntaxException {
        Config config = loadConfig("/conf/table_schema_discoverer/single_schema_field.conf");
        ReadonlyConfig sourceOptions = ReadonlyConfig.fromConfig(config);
        MetaDataConfig metaDataConfig = createDefaultMetaDataConfig();
        try (TableSchemaDiscoverer discoverer =
                new TableSchemaDiscoverer(metaDataConfig, sourceOptions, TEST_CATALOG_NAME)) {
            Assertions.assertFalse(discoverer.enableMetaLakeClient(sourceOptions));
            List<CatalogTable> result = discoverer.discoverTableSchemas();
            Assertions.assertEquals(1, result.size());
            Assertions.assertEquals(TEST_CATALOG_NAME, result.get(0).getCatalogName());
            Assertions.assertEquals(
                    TablePath.of("default", "default", "default"), result.get(0).getTablePath());
            Assertions.assertEquals(3, result.get(0).getTableSchema().getColumns().size());
        }
    }

    @Disabled("Until discoverTableSchemaFromMetaLake is implemented")
    @Test
    void testDiscoverTableSchemasWithSingleSchemaMetadataTableId() throws Exception {
        Config config = loadConfig("/conf/table_schema_discoverer/single_schema_url.conf");
        ReadonlyConfig sourceOptions = ReadonlyConfig.fromConfig(config);
        MetaDataConfig metaDataConfig = createDefaultMetaDataConfig();
        try (TableSchemaDiscoverer discoverer =
                new TableSchemaDiscoverer(metaDataConfig, sourceOptions, TEST_CATALOG_NAME)) {
            Assertions.assertTrue(discoverer.enableMetaLakeClient(sourceOptions));
            List<CatalogTable> result = discoverer.discoverTableSchemas();
            // Currently discoverTableSchemaFromMetaLake returns null, so we expect empty result
            // or the implementation needs to be completed
            Assertions.assertEquals(1, result.size());
            // The result will be null from discoverTableSchemaFromMetaLake, which may cause NPE
            // This test will be updated when the implementation is complete
        }
    }

    @Test
    void testDiscoverTableSchemasWithMultipleTablesFields() throws URISyntaxException {
        Config config = loadConfig("/conf/table_schema_discoverer/multiple_tables_fields.conf");
        ReadonlyConfig sourceOptions = ReadonlyConfig.fromConfig(config);
        MetaDataConfig metaDataConfig = createDefaultMetaDataConfig();
        try (TableSchemaDiscoverer discoverer =
                new TableSchemaDiscoverer(metaDataConfig, sourceOptions, TEST_CATALOG_NAME)) {
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

    @Disabled("Until discoverTableSchemaFromMetaLake is implemented")
    @Test
    void testDiscoverTableSchemasWithMultipleTablesMetadataTableId() throws Exception {
        Config config = loadConfig("/conf/table_schema_discoverer/multiple_tables_schema_url.conf");
        ReadonlyConfig sourceOptions = ReadonlyConfig.fromConfig(config);
        MetaDataConfig metaDataConfig = createDefaultMetaDataConfig();
        try (TableSchemaDiscoverer discoverer =
                new TableSchemaDiscoverer(metaDataConfig, sourceOptions, TEST_CATALOG_NAME)) {
            List<CatalogTable> result = discoverer.discoverTableSchemas();
            Assertions.assertTrue(discoverer.enableMetaLakeClient(sourceOptions));
            // Currently discoverTableSchemaFromMetaLake returns null for both tables
            Assertions.assertEquals(2, result.size());
        }
    }

    @Disabled("Until discoverTableSchemaFromMetaLake is implemented")
    @Test
    void testDiscoverTableSchemasWithMultipleTablesMixedFieldsAndMetadataTableId()
            throws Exception {
        Config config = loadConfig("/conf/table_schema_discoverer/multiple_tables_mixed.conf");
        ReadonlyConfig sourceOptions = ReadonlyConfig.fromConfig(config);
        MetaDataConfig metaDataConfig = createDefaultMetaDataConfig();
        try (TableSchemaDiscoverer discoverer =
                new TableSchemaDiscoverer(metaDataConfig, sourceOptions, TEST_CATALOG_NAME)) {
            Assertions.assertTrue(discoverer.enableMetaLakeClient(sourceOptions));
            List<CatalogTable> result = discoverer.discoverTableSchemas();
            Assertions.assertEquals(2, result.size());
            Assertions.assertEquals(TEST_CATALOG_NAME, result.get(0).getCatalogName());
            Assertions.assertEquals(TablePath.of("db.table1"), result.get(0).getTablePath());
            Assertions.assertEquals(2, result.get(0).getTableSchema().getColumns().size());
            // Second table uses metadata_table_id which currently returns null
        }
    }

    @Test
    void testDiscoverTableSchemaWithSingleParquetNoSchema() throws URISyntaxException {
        Config config = loadConfig("/conf/table_schema_discoverer/single_no_schema.conf");
        ReadonlyConfig sourceOptions = ReadonlyConfig.fromConfig(config);
        MetaDataConfig metaDataConfig = createDefaultMetaDataConfig();
        try (TableSchemaDiscoverer discoverer =
                new TableSchemaDiscoverer(metaDataConfig, sourceOptions, TEST_CATALOG_NAME)) {
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
        MetaDataConfig metaDataConfig = createDefaultMetaDataConfig();
        try (TableSchemaDiscoverer discoverer =
                new TableSchemaDiscoverer(metaDataConfig, sourceOptions, TEST_CATALOG_NAME)) {
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
}
