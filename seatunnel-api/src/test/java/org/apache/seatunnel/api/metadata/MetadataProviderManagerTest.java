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

package org.apache.seatunnel.api.metadata;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.metadata.exception.MetadataProviderException;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import lombok.SneakyThrows;

import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class MetadataProviderManagerTest {

    private static final String TEST_PROVIDER_KIND = "test-provider";

    @BeforeEach
    public void setUp() {
        // Clear the cache before each test
        MetadataProviderManager.clearCache();
    }

    @AfterEach
    public void tearDown() {
        // Clear the cache after each test
        MetadataProviderManager.clearCache();
    }

    @Test
    public void testResolveDataSourceConfigsWithDatasourceId() {
        // Register mock provider
        MockTestMetadataProvider provider = new MockTestMetadataProvider();
        registerProvider(provider);

        // Load config from file with metadata_datasource_id
        Config jobConfig = loadTestConfig();

        // Create DataSourceConfig
        MetadataConfig metaDataConfig = new MetadataConfig();
        metaDataConfig.setEnabled(true);
        metaDataConfig.setKind(TEST_PROVIDER_KIND);

        // Resolve with metadata_datasource_id
        Config resolved =
                MetadataProviderManager.resolveDataSourceConfigs(jobConfig, metaDataConfig);

        assertNotNull(resolved);

        // Verify we have 3 sources and 3 sinks
        List<? extends Config> sources = resolved.getConfigList("source");
        List<? extends Config> sinks = resolved.getConfigList("sink");
        assertEquals(3, sources.size());
        assertEquals(3, sinks.size());

        // Verify first source has datasource config merged (flat structure)
        Config source1 = sources.get(0);
        // In flat structure, values are directly at root level
        assertEquals("jdbc:postgresql://metadata:5432/metadata_db", source1.getString("url"));
        assertEquals("metadata_user", source1.getString("username"));
        assertEquals("metadata_password", source1.getString("password"));
        // The original query should still be there
        assertEquals("select id, name from table1", source1.getString("query"));

        // Verify second source has datasource config merged (flat structure)
        Config source2 = sources.get(1);
        assertEquals("jdbc:postgresql://metadata:5432/metadata_db", source2.getString("url"));
        assertEquals("metadata_user", source2.getString("username"));
        assertEquals("select id, value from table2", source2.getString("query"));

        // Verify third source (Jdbc without metadata_datasource_id) - keep original config
        Config source3 = sources.get(2);
        assertEquals("jdbc:mysql://localhost:3306", source3.getString("url"));
        assertEquals("com.mysql.cj.jdbc.Driver", source3.getString("driver"));
        assertEquals("root", source3.getString("user"));
        assertEquals("123456", source3.getString("password"));
        assertEquals("select id, name from table4", source3.getString("query"));

        // Verify first sink has datasource config merged (flat structure)
        Config sink1 = sinks.get(0);
        assertEquals("jdbc:postgresql://metadata:5432/metadata_db", sink1.getString("url"));
        assertEquals("metadata_user", sink1.getString("username"));
        assertEquals("insert into sink_table1 (id, name) values (?, ?)", sink1.getString("query"));

        // Verify second sink has datasource config merged (flat structure)
        Config sink2 = sinks.get(1);
        assertEquals("jdbc:postgresql://metadata:5432/metadata_db", sink2.getString("url"));
        assertEquals("insert into sink_table2 (id, value) values (?, ?)", sink2.getString("query"));

        // Verify third sink (Jdbc without metadata_datasource_id) - keep original config
        Config sink3 = sinks.get(2);
        assertEquals("jdbc:mysql://localhost:3306", sink3.getString("url"));
        assertEquals("com.mysql.cj.jdbc.Driver", sink3.getString("driver"));
        assertEquals("root", sink3.getString("user"));
        assertEquals("123456", sink3.getString("password"));
        assertEquals("insert into sink_table4 (id, name) values (?, ?)", sink3.getString("query"));
    }

    @Test
    public void testResolveDataSourceConfigsWithNoProvider() {
        // Clear any cached provider
        MetadataProviderManager.clearCache();

        // Load config from file with metadata_datasource_id
        Config jobConfig = loadTestConfig();

        // Create MetadataConfig with unknown provider kind
        MetadataConfig metaDataConfig = new MetadataConfig();
        metaDataConfig.setEnabled(true);
        metaDataConfig.setKind("unknown-provider-kind");

        // Try to resolve with a provider kind that doesn't exist
        MetadataProviderException exception =
                assertThrows(
                        MetadataProviderException.class,
                        () ->
                                MetadataProviderManager.resolveDataSourceConfigs(
                                        jobConfig, metaDataConfig));

        // Verify exception is thrown
        assertNotNull(exception);
        assertNotNull(exception.getMessage());
    }

    /** Helper method to load the test config file. */
    @SneakyThrows
    private Config loadTestConfig() {
        return ConfigFactory.parseFile(
                Paths.get(
                                Objects.requireNonNull(
                                                MetadataProviderManagerTest.class.getResource(
                                                        "/conf/datasource-test.conf"))
                                        .toURI())
                        .toFile());
    }

    /**
     * Helper method to manually register a provider for testing. This is a workaround since we
     * can't easily use SPI in unit tests.
     */
    private void registerProvider(MetadataProvider provider) {
        try {
            java.lang.reflect.Field providerField =
                    MetadataProviderManager.class.getDeclaredField("cachedProvider");
            providerField.setAccessible(true);
            providerField.set(null, provider);
        } catch (Exception e) {
            throw new RuntimeException("Failed to register provider for testing", e);
        }
    }

    /** Mock DataSourceProvider for testing. */
    public static class MockTestMetadataProvider implements MetadataProvider {

        @Override
        public String kind() {
            return TEST_PROVIDER_KIND;
        }

        @Override
        public void init(Config config) {
            // No-op for testing
        }

        @Override
        public Map<String, Object> datasourceMap(
                String connectorIdentifier, String metaDataDatasourceId) {
            // Only support Jdbc connector for testing
            if (!"Jdbc".equalsIgnoreCase(connectorIdentifier)) {
                return new HashMap<>();
            }
            // Simulate fetching connection config from metadata service
            Map<String, Object> config = new HashMap<>();
            config.put("url", "jdbc:postgresql://metadata:5432/metadata_db");
            config.put("driver", "org.postgresql.Driver");
            config.put("username", "metadata_user");
            config.put("password", "metadata_password");
            return config;
        }

        @Override
        public Optional<TableSchema> tableSchema(String metaDataTableId) {
            // Create a simple TableSchema for testing
            TableSchema schema =
                    TableSchema.builder()
                            .columns(
                                    Arrays.asList(
                                            PhysicalColumn.builder()
                                                    .name("id")
                                                    .dataType(BasicType.LONG_TYPE)
                                                    .build(),
                                            PhysicalColumn.builder()
                                                    .name("name")
                                                    .dataType(BasicType.STRING_TYPE)
                                                    .build()))
                            .build();
            return Optional.of(schema);
        }

        @Override
        public void close() {
            // No-op for testing
        }
    }

    @Test
    public void testResolveTableSchemaWithNullConfig() {
        // Test with null metaDataConfig
        Optional<TableSchema> result =
                MetadataProviderManager.resolveTableSchema("test.table", null);

        assertNotNull(result);
        assertFalse(result.isPresent());
    }

    @Test
    public void testResolveTableSchemaWithDisabledConfig() {
        // Test with disabled metaDataConfig
        MetadataConfig metaDataConfig = new MetadataConfig();
        metaDataConfig.setEnabled(false);
        metaDataConfig.setKind(TEST_PROVIDER_KIND);

        Optional<TableSchema> result =
                MetadataProviderManager.resolveTableSchema("test.table", metaDataConfig);

        assertNotNull(result);
        assertFalse(result.isPresent());
    }

    @Test
    public void testResolveTableSchemaWithValidResult() {
        // Register mock provider that returns a valid TableSchema
        MockTestMetadataProvider provider = new MockTestMetadataProvider();
        registerProvider(provider);

        // Create enabled MetadataConfig
        MetadataConfig metaDataConfig = new MetadataConfig();
        metaDataConfig.setEnabled(true);
        metaDataConfig.setKind(TEST_PROVIDER_KIND);

        // The mock provider should return a valid TableSchema
        Optional<TableSchema> result =
                MetadataProviderManager.resolveTableSchema("catalog.db.table", metaDataConfig);

        assertNotNull(result);
        assertTrue(result.isPresent());
        TableSchema schema = result.get();
        assertEquals(2, schema.getColumns().size());
        assertEquals("id", schema.getColumns().get(0).getName());
        assertEquals("name", schema.getColumns().get(1).getName());
    }

    @Test
    public void testResolveTableSchemaWithMultipleCalls() {
        // Register mock provider that returns a valid TableSchema
        MockTestMetadataProvider provider = new MockTestMetadataProvider();
        registerProvider(provider);

        // Create enabled MetadataConfig
        MetadataConfig metaDataConfig = new MetadataConfig();
        metaDataConfig.setEnabled(true);
        metaDataConfig.setKind(TEST_PROVIDER_KIND);

        // First call
        Optional<TableSchema> result1 =
                MetadataProviderManager.resolveTableSchema("catalog.db.table1", metaDataConfig);

        // Second call - should use cached provider
        Optional<TableSchema> result2 =
                MetadataProviderManager.resolveTableSchema("catalog.db.table2", metaDataConfig);

        // Both should return valid results
        assertTrue(result1.isPresent());
        assertTrue(result2.isPresent());
        assertEquals(2, result1.get().getColumns().size());
        assertEquals(2, result2.get().getColumns().size());
    }
}
