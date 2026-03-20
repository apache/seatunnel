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

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.metalake.gravitino.GravitinoTableSchemaConvertor;
import org.apache.seatunnel.api.options.EnvCommonOptions;
import org.apache.seatunnel.api.options.table.TableSchemaOptions;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.common.constants.MetaLakeType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class TableSchemaDiscovererTest {

    private static final String TEST_CATALOG_NAME = "test_catalog";

    @Mock private MetalakeClient metalakeClient;
    private final MetaLakeTableSchemaConvertor convertor = new GravitinoTableSchemaConvertor();

    @Test
    void testDiscoverTableSchemasWithSingleSchemaFields() throws URISyntaxException {
        Config config = loadConfig("/conf/table_schema_discoverer/single_schema_field.conf");
        ReadonlyConfig sourceOptions = ReadonlyConfig.fromConfig(config);
        ReadonlyConfig envOptions = ReadonlyConfig.fromMap(new HashMap<>());
        try (TableSchemaDiscoverer discoverer =
                new TableSchemaDiscoverer(
                        envOptions, sourceOptions, TEST_CATALOG_NAME, null, null)) {
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
    void testDiscoverTableSchemasWithSingleSchemaSchemaUrl() throws Exception {
        Config config = loadConfig("/conf/table_schema_discoverer/single_schema_url.conf");
        ReadonlyConfig sourceOptions = ReadonlyConfig.fromConfig(config);
        ReadonlyConfig envOptions = ReadonlyConfig.fromMap(new HashMap<>());
        // Mock setup with real JsonNode structure
        JsonNode schemaNode = createMockTableSchemaNode("test_table");
        String schemaUrl =
                "http://localhost:8090/api/metalakes/test_catalog/schemas/test_schema/tables/test_table";
        when(metalakeClient.getTableSchema(schemaUrl)).thenReturn(schemaNode);
        when(metalakeClient.getTableSchemaPath(schemaUrl))
                .thenReturn(TablePath.of("test_catalog", "test_schema", "test_table"));

        try (TableSchemaDiscoverer discoverer =
                new TableSchemaDiscoverer(
                        envOptions, sourceOptions, TEST_CATALOG_NAME, metalakeClient, convertor)) {
            Assertions.assertTrue(discoverer.enableMetaLakeClient(sourceOptions));
            List<CatalogTable> result = discoverer.discoverTableSchemas();
            Assertions.assertEquals(1, result.size());
            Assertions.assertEquals(TEST_CATALOG_NAME, result.get(0).getCatalogName());
            Assertions.assertEquals(
                    TablePath.of("test_catalog", "test_schema", "test_table"),
                    result.get(0).getTablePath());
            Assertions.assertEquals(2, result.get(0).getTableSchema().getColumns().size());
        }
    }

    @Test
    void testDiscoverTableSchemasWithMultipleTablesFields() throws URISyntaxException {
        Config config = loadConfig("/conf/table_schema_discoverer/multiple_tables_fields.conf");
        ReadonlyConfig sourceOptions = ReadonlyConfig.fromConfig(config);
        ReadonlyConfig envOptions = ReadonlyConfig.fromMap(new HashMap<>());
        try (TableSchemaDiscoverer discoverer =
                new TableSchemaDiscoverer(
                        envOptions, sourceOptions, TEST_CATALOG_NAME, null, null)) {
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
    void testDiscoverTableSchemasWithMultipleTablesSchemaUrl() throws Exception {
        Config config = loadConfig("/conf/table_schema_discoverer/multiple_tables_schema_url.conf");
        ReadonlyConfig sourceOptions = ReadonlyConfig.fromConfig(config);
        ReadonlyConfig envOptions = ReadonlyConfig.fromMap(new HashMap<>());
        // url
        String schemaUrl1 =
                "http://localhost:8090/api/metalakes/test_catalog/schemas/test_schema/tables/table1";
        String schemaUrl2 =
                "http://localhost:8090/api/metalakes/test_catalog/schemas/test_schema/tables/table2";
        // Mock setup with real JsonNode structure
        JsonNode schemaNode1 = createMockTableSchemaNode("table1");
        JsonNode schemaNode2 = createMockTableSchemaNode("table2");
        // json node
        when(metalakeClient.getTableSchema(schemaUrl1)).thenReturn(schemaNode1);
        when(metalakeClient.getTableSchema(schemaUrl2)).thenReturn(schemaNode2);
        when(metalakeClient.getTableSchemaPath(schemaUrl2))
                .thenReturn(TablePath.of("test_catalog", "test_schema", "table2"));
        // discoverer
        try (TableSchemaDiscoverer discoverer =
                new TableSchemaDiscoverer(
                        envOptions, sourceOptions, TEST_CATALOG_NAME, metalakeClient, convertor)) {
            List<CatalogTable> result = discoverer.discoverTableSchemas();
            Assertions.assertTrue(discoverer.enableMetaLakeClient(sourceOptions));
            Assertions.assertEquals(2, result.size());
            Assertions.assertEquals(TEST_CATALOG_NAME, result.get(0).getCatalogName());
            Assertions.assertEquals(
                    TablePath.of("test_database.test_schema.test_table1"),
                    result.get(0).getTablePath());
            Assertions.assertEquals(2, result.get(0).getTableSchema().getColumns().size());
            Assertions.assertEquals(TEST_CATALOG_NAME, result.get(1).getCatalogName());
            Assertions.assertEquals(
                    TablePath.of("test_catalog", "test_schema", "table2"),
                    result.get(1).getTablePath());
            Assertions.assertEquals(2, result.get(1).getTableSchema().getColumns().size());
        }
    }

    @Test
    void testDiscoverTableSchemasWithMultipleTablesMixedFieldsAndSchemaUrl() throws Exception {
        Config config = loadConfig("/conf/table_schema_discoverer/multiple_tables_mixed.conf");
        ReadonlyConfig sourceOptions = ReadonlyConfig.fromConfig(config);
        ReadonlyConfig envOptions = ReadonlyConfig.fromMap(new HashMap<>());
        JsonNode schemaNode2 = createMockTableSchemaNode("table2");
        String url2 =
                "http://localhost:8090/api/metalakes/test_catalog/schemas/test_schema/tables/table2";
        when(metalakeClient.getTableSchema(url2)).thenReturn(schemaNode2);
        when(metalakeClient.getTableSchemaPath(url2))
                .thenReturn(TablePath.of("test_catalog", "test_schema", "table2"));
        try (TableSchemaDiscoverer discoverer =
                new TableSchemaDiscoverer(
                        envOptions, sourceOptions, TEST_CATALOG_NAME, metalakeClient, convertor)) {
            Assertions.assertTrue(discoverer.enableMetaLakeClient(sourceOptions));
            List<CatalogTable> result = discoverer.discoverTableSchemas();
            Assertions.assertEquals(2, result.size());
            Assertions.assertEquals(TEST_CATALOG_NAME, result.get(0).getCatalogName());
            Assertions.assertEquals(TablePath.of("db.table1"), result.get(0).getTablePath());
            Assertions.assertEquals(2, result.get(0).getTableSchema().getColumns().size());
            Assertions.assertEquals(TEST_CATALOG_NAME, result.get(1).getCatalogName());
            Assertions.assertEquals(
                    TablePath.of("test_catalog.test_schema.table2"), result.get(1).getTablePath());
            Assertions.assertEquals(2, result.get(1).getTableSchema().getColumns().size());
        }
    }

    @Test
    void testGetMetaLakeTypeFromSourceOptions() {
        Map<String, Object> sourceConfig = new HashMap<>();
        sourceConfig.put(TableSchemaOptions.METALAKE_TYPE.key(), MetaLakeType.GRAVITINO.name());
        ReadonlyConfig sourceOptions = ReadonlyConfig.fromMap(sourceConfig);
        ReadonlyConfig envOptions = ReadonlyConfig.fromMap(new HashMap<>());
        TableSourceFactoryContext context =
                new TableSourceFactoryContext(
                        sourceOptions, getClass().getClassLoader(), envOptions);
        try (TableSchemaDiscoverer discoverer =
                new TableSchemaDiscoverer(context, TEST_CATALOG_NAME)) {
            MetaLakeType result = discoverer.getMetaLakeType();
            Assertions.assertEquals(MetaLakeType.GRAVITINO, result);
        }
    }

    @Test
    void testGetMetaLakeTypeFromEnvOptions() {
        ReadonlyConfig sourceOptions = ReadonlyConfig.fromMap(new HashMap<>());
        Map<String, Object> envConfig = new HashMap<>();
        envConfig.put(EnvCommonOptions.METALAKE_TYPE.key(), MetaLakeType.GRAVITINO.name());
        ReadonlyConfig envOptions = ReadonlyConfig.fromMap(envConfig);
        TableSourceFactoryContext context =
                new TableSourceFactoryContext(
                        sourceOptions, getClass().getClassLoader(), envOptions);
        try (TableSchemaDiscoverer discoverer =
                new TableSchemaDiscoverer(context, TEST_CATALOG_NAME)) {
            MetaLakeType result = discoverer.getMetaLakeType();
            Assertions.assertEquals(MetaLakeType.GRAVITINO, result);
        }
    }

    @Test
    void testGetMetaLakeTypeFromSystemEnvironment() {
        ReadonlyConfig sourceOptions = ReadonlyConfig.fromMap(new HashMap<>());
        ReadonlyConfig envOptions = ReadonlyConfig.fromMap(new HashMap<>());
        TableSourceFactoryContext context =
                new TableSourceFactoryContext(
                        sourceOptions, getClass().getClassLoader(), envOptions);
        System.setProperty(
                EnvCommonOptions.METALAKE_TYPE.key().toUpperCase(), MetaLakeType.GRAVITINO.name());
        try (TableSchemaDiscoverer discoverer =
                new TableSchemaDiscoverer(context, TEST_CATALOG_NAME)) {
            MetaLakeType result = discoverer.getMetaLakeType();
            Assertions.assertEquals(MetaLakeType.GRAVITINO, result);
        }
    }

    @Test
    void testGetMetaLakeTypeDefaultValue() {
        ReadonlyConfig sourceOptions = ReadonlyConfig.fromMap(new HashMap<>());
        ReadonlyConfig envOptions = ReadonlyConfig.fromMap(new HashMap<>());
        TableSourceFactoryContext context =
                new TableSourceFactoryContext(
                        sourceOptions, getClass().getClassLoader(), envOptions);
        try (TableSchemaDiscoverer discoverer =
                new TableSchemaDiscoverer(context, TEST_CATALOG_NAME)) {
            MetaLakeType result = discoverer.getMetaLakeType();
            Assertions.assertEquals(MetaLakeType.GRAVITINO, result);
        }
    }

    @Test
    void testGetMetaLakeTypePrioritySourceOverEnv() {
        Map<String, Object> sourceConfig = new HashMap<>();
        sourceConfig.put(TableSchemaOptions.METALAKE_TYPE.key(), MetaLakeType.GRAVITINO.name());
        ReadonlyConfig sourceOptions = ReadonlyConfig.fromMap(sourceConfig);
        Map<String, Object> envConfig = new HashMap<>();
        envConfig.put(EnvCommonOptions.METALAKE_TYPE.key(), "other_type");
        ReadonlyConfig envOptions = ReadonlyConfig.fromMap(envConfig);
        TableSourceFactoryContext context =
                new TableSourceFactoryContext(
                        sourceOptions, getClass().getClassLoader(), envOptions);
        try (TableSchemaDiscoverer discoverer =
                new TableSchemaDiscoverer(context, TEST_CATALOG_NAME)) {
            MetaLakeType result = discoverer.getMetaLakeType();
            Assertions.assertEquals(MetaLakeType.GRAVITINO, result);
        }
    }

    @Test
    void testGetMetaLakeTypePriorityEnvOverSystem() {
        ReadonlyConfig sourceOptions = ReadonlyConfig.fromMap(new HashMap<>());
        Map<String, Object> envConfig = new HashMap<>();
        envConfig.put(EnvCommonOptions.METALAKE_TYPE.key(), MetaLakeType.GRAVITINO.name());
        ReadonlyConfig envOptions = ReadonlyConfig.fromMap(envConfig);
        TableSourceFactoryContext context =
                new TableSourceFactoryContext(
                        sourceOptions, getClass().getClassLoader(), envOptions);
        System.setProperty(EnvCommonOptions.METALAKE_TYPE.key().toUpperCase(), "other_type");
        try (TableSchemaDiscoverer discoverer =
                new TableSchemaDiscoverer(context, TEST_CATALOG_NAME)) {
            MetaLakeType result = discoverer.getMetaLakeType();
            Assertions.assertEquals(MetaLakeType.GRAVITINO, result);
        }
    }

    @Test
    void testDiscoverTableSchemaWithSingleParquetNoSchema() throws URISyntaxException {
        Config config = loadConfig("/conf/table_schema_discoverer/single_no_schema.conf");
        ReadonlyConfig sourceOptions = ReadonlyConfig.fromConfig(config);
        ReadonlyConfig envOptions = ReadonlyConfig.fromMap(new HashMap<>());
        try (TableSchemaDiscoverer discoverer =
                new TableSchemaDiscoverer(
                        envOptions, sourceOptions, TEST_CATALOG_NAME, null, null)) {
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
        ReadonlyConfig envOptions = ReadonlyConfig.fromMap(new HashMap<>());
        try (TableSchemaDiscoverer discoverer =
                new TableSchemaDiscoverer(
                        envOptions, sourceOptions, TEST_CATALOG_NAME, null, null)) {
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
     * Create a mock table schema JsonNode for testing. The structure matches Gravitino's table
     * schema format.
     */
    private JsonNode createMockTableSchemaNode(String tableName) {
        ObjectMapper mapper = new ObjectMapper();
        // Create table node
        ObjectNode tableNode = mapper.createObjectNode();
        tableNode.put("name", tableName);

        // Create columns array
        ArrayNode columnsArray = mapper.createArrayNode();

        // Column 1: id (integer, not null)
        ObjectNode column1 = mapper.createObjectNode();
        column1.put("name", "id");
        column1.put("type", "integer");
        column1.put("nullable", false);
        column1.put("autoIncrement", false);
        columnsArray.add(column1);

        // Column 2: big_number (long, nullable, with default value)
        ObjectNode column2 = mapper.createObjectNode();
        column2.put("name", "big_number");
        column2.put("type", "long");
        column2.put("nullable", true);
        column2.put("autoIncrement", false);

        // Default value node
        ObjectNode defaultValue = mapper.createObjectNode();
        defaultValue.put("type", "literal");
        defaultValue.put("dataType", "null");
        defaultValue.put("value", "NULL");
        column2.set("defaultValue", defaultValue);
        columnsArray.add(column2);

        tableNode.set("columns", columnsArray);
        return tableNode;
    }
}
