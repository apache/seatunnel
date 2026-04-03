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

package org.apache.seatunnel.engine.common.utils;

import org.apache.seatunnel.shade.com.typesafe.config.Config;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.api.datasource.DataSourceProvider;
import org.apache.seatunnel.api.datasource.exception.DataSourceProviderException;
import org.apache.seatunnel.engine.common.config.server.DataSourceConfig;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import lombok.SneakyThrows;

import java.nio.file.Paths;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class DataSourceConfigUtilTest {

    private static final String TEST_PROVIDER_KIND = "test-provider";

    @BeforeEach
    public void setUp() {
        // Clear the cache before each test
        DataSourceConfigResolver.clearCache();
    }

    @AfterEach
    public void tearDown() {
        // Clear the cache after each test
        DataSourceConfigResolver.clearCache();
    }

    @Test
    public void testResolveDataSourceConfigsWithDatasourceId() {
        // Register mock provider
        MockTestDataSourceProvider provider = new MockTestDataSourceProvider();
        registerProvider(provider);

        // Load config from file with datasource_id
        Config jobConfig = loadTestConfig();

        // Create DataSourceConfig
        DataSourceConfig dataSourceConfig = new DataSourceConfig();
        dataSourceConfig.setEnabled(true);
        dataSourceConfig.setKind(TEST_PROVIDER_KIND);

        // Resolve with datasource_id
        Config resolved =
                DataSourceConfigResolver.resolveDataSourceConfigs(jobConfig, dataSourceConfig);

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

        // Verify third source (Jdbc without datasource_id) - keep original config
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

        // Verify third sink (Jdbc without datasource_id) - keep original config
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
        DataSourceConfigResolver.clearCache();

        // Load config from file with datasource_id
        Config jobConfig = loadTestConfig();

        // Create DataSourceConfig with unknown provider kind
        DataSourceConfig dataSourceConfig = new DataSourceConfig();
        dataSourceConfig.setEnabled(true);
        dataSourceConfig.setKind("unknown-provider-kind");

        // Try to resolve with a provider kind that doesn't exist
        DataSourceProviderException exception =
                assertThrows(
                        DataSourceProviderException.class,
                        () ->
                                DataSourceConfigResolver.resolveDataSourceConfigs(
                                        jobConfig, dataSourceConfig));

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
                                                DataSourceConfigUtilTest.class.getResource(
                                                        "/conf/datasource-test.conf"))
                                        .toURI())
                        .toFile());
    }

    /**
     * Helper method to manually register a provider for testing. This is a workaround since we
     * can't easily use SPI in unit tests.
     */
    private void registerProvider(DataSourceProvider provider) {
        try {
            java.lang.reflect.Field providerField =
                    DataSourceConfigResolver.class.getDeclaredField("cachedProvider");
            providerField.setAccessible(true);
            providerField.set(null, provider);
        } catch (Exception e) {
            throw new RuntimeException("Failed to register provider for testing", e);
        }
    }

    /** Mock DataSourceProvider for testing. */
    public static class MockTestDataSourceProvider implements DataSourceProvider {

        @Override
        public String kind() {
            return TEST_PROVIDER_KIND;
        }

        @Override
        public void init(Config config) {
            // No-op for testing
        }

        @Override
        public Map<String, Object> datasourceMap(String connectorIdentifier, String datasourceId) {
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
        public void close() {
            // No-op for testing
        }
    }
}
