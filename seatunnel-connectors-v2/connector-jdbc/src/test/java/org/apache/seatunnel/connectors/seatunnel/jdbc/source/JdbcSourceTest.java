/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.jdbc.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcSourceConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@Disabled("Please Test it in your local environment")
public class JdbcSourceTest {
    private static final String MYSQL_DRIVER_CLASS = "com.mysql.cj.jdbc.Driver";
    private static final String MYSQL_URL = "jdbc:mysql://localhost:3306/test";
    private static final String MYSQL_USERNAME = "root";
    private static final String MYSQL_PASSWORD = "password";

    // Oracle connection details
    private static final String ORACLE_DRIVER_CLASS = "oracle.jdbc.driver.OracleDriver";
    private static final String ORACLE_URL = "jdbc:oracle:thin:@localhost:1521:XE";
    private static final String ORACLE_USERNAME = "system";
    private static final String ORACLE_PASSWORD = "password";

    // PostgreSQL connection details
    private static final String PGSQL_DRIVER_CLASS = "org.postgresql.Driver";
    private static final String PGSQL_URL = "jdbc:postgresql://localhost:5432/postgres";
    private static final String PGSQL_USERNAME = "postgres";
    private static final String PGSQL_PASSWORD = "password";

    @Test
    public void testExactTableMatch() {
        // Create source config with exact table path
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("driver", MYSQL_DRIVER_CLASS);
        configMap.put("url", MYSQL_URL);
        configMap.put("user", MYSQL_USERNAME);
        configMap.put("password", MYSQL_PASSWORD);
        configMap.put("table_path", "test.table1");

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        TableSourceFactoryContext context =
                new TableSourceFactoryContext(
                        config, Thread.currentThread().getContextClassLoader());

        // Create source
        JdbcSource jdbcSource = new JdbcSource(JdbcSourceConfig.of(config));

        // Verify table configuration
        List<CatalogTable> catalogTables = jdbcSource.getProducedCatalogTables();
        Assertions.assertEquals(1, catalogTables.size());
        Assertions.assertEquals("table1", catalogTables.get(0).getTableId().getTableName());
    }

    @Test
    public void testMysqlRegexTableMatch() {
        // Test case 1: Match tables with names containing "table" followed by one or more digits
        testMysqlRegexPattern(
                "test.table\\d+", Arrays.asList("table1", "table2", "table3", "table123"));

        // Test case 2: Match tables with names containing "table" followed by one or more
        // characters
        testMysqlRegexPattern("test.table+", Arrays.asList("tableee"));

        // Test case 3: Match all tables in the "test" database
        testMysqlRegexPattern(
                "test.*",
                Arrays.asList("table1", "table2", "table3", "table123", "tableee", "test1"));

        // Test case 4: Match tables with names matching "table" followed by a digit between 1 and 3
        testMysqlRegexPattern("test.table[1-3]", Arrays.asList("table1", "table2", "table3"));

        // Test case 5: Test pattern that doesn't match any existing tables
        testMysqlRegexPattern("test.nonexistent*", Collections.emptyList());
    }

    /**
     * Helper method to test regex patterns
     *
     * @param pattern The regex pattern to test
     * @param expectedTables List of expected table names
     */
    private void testMysqlRegexPattern(String pattern, List<String> expectedTables) {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("driver", MYSQL_DRIVER_CLASS);
        configMap.put("url", MYSQL_URL);
        configMap.put("user", MYSQL_USERNAME);
        configMap.put("password", MYSQL_PASSWORD);
        configMap.put("table_path", pattern);
        configMap.put("use_regex", true);
        configMap.put("dialect", "mysql");

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        JdbcSource jdbcSource = new JdbcSource(JdbcSourceConfig.of(config));
        List<CatalogTable> catalogTables = jdbcSource.getProducedCatalogTables();

        // Verify number of tables
        Assertions.assertEquals(
                expectedTables.size(),
                catalogTables.size(),
                "Expected " + expectedTables.size() + " tables for pattern: " + pattern);

        if (!expectedTables.isEmpty()) {
            // Verify table names
            Set<String> actualTableNames =
                    catalogTables.stream()
                            .map(table -> table.getTableId().getTableName())
                            .collect(Collectors.toSet());

            for (String expectedTable : expectedTables) {
                Assertions.assertTrue(
                        actualTableNames.contains(expectedTable),
                        "Expected table " + expectedTable + " not found for pattern: " + pattern);
            }
        }
    }

    @Test
    public void testOracleRegexTableMatch() {
        // Test case 1: Match all tables in the TEST1 schema of XE database
        testOracleRegexPattern(
                "XE.TEST1.*",
                Arrays.asList("TEST1", "TEST2", "TEST_DB", "TEST_DB_1", "TEST_DB_2", "TEST_DB_12"));

        // Test case 2: Match tables with names starting with "TEST" followed by a single word
        // character
        testOracleRegexPattern("XE.TEST1.TEST\\w", Arrays.asList("TEST1", "TEST2"));

        // Test case 3: Match tables with names starting with "TEST_" followed by one or more word
        // characters
        testOracleRegexPattern(
                "XE.TEST1.TEST_\\w+",
                Arrays.asList("TEST_DB", "TEST_DB_1", "TEST_DB_2", "TEST_DB_12"));

        // Test case 4: Match table with exact name "TEST_DB_2"
        testOracleRegexPattern("XE.TEST1.TEST_DB_2$", Arrays.asList("TEST_DB_2"));

        // Test case 5: Match tables with names starting with "TEST_DB_" followed by 1 or 2 digits
        testOracleRegexPattern(
                "XE.TEST1.TEST_DB_\\d{1,2}", Arrays.asList("TEST_DB_1", "TEST_DB_2", "TEST_DB_12"));

        // Test case 6: Test pattern that doesn't match any existing tables
        testOracleRegexPattern("XE.TEST1.NONEXISTENT*", Collections.emptyList());
    }

    /**
     * Helper method to test Oracle regex patterns
     *
     * @param pattern The regex pattern to test
     * @param expectedTables List of expected table names
     */
    private void testOracleRegexPattern(String pattern, List<String> expectedTables) {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("driver", ORACLE_DRIVER_CLASS);
        configMap.put("url", ORACLE_URL);
        configMap.put("user", ORACLE_USERNAME);
        configMap.put("password", ORACLE_PASSWORD);
        configMap.put("table_path", pattern);
        configMap.put("use_regex", true);
        configMap.put("dialect", "oracle");

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        JdbcSource jdbcSource = new JdbcSource(JdbcSourceConfig.of(config));
        List<CatalogTable> catalogTables = jdbcSource.getProducedCatalogTables();

        // Verify number of tables
        Assertions.assertEquals(
                expectedTables.size(),
                catalogTables.size(),
                "Expected " + expectedTables.size() + " Oracle tables for pattern: " + pattern);

        if (!expectedTables.isEmpty()) {
            // Verify table names
            Set<String> actualTableNames =
                    catalogTables.stream()
                            .map(table -> table.getTableId().getTableName())
                            .collect(Collectors.toSet());

            for (String expectedTable : expectedTables) {
                Assertions.assertTrue(
                        actualTableNames.contains(expectedTable),
                        "Expected Oracle table "
                                + expectedTable
                                + " not found for pattern: "
                                + pattern);
            }
        }
    }

    @Test
    public void testPostgreSQLRegexTableMatch() {
        // Test case 1: Match tables in public schema with names starting with "test_" followed by
        // word characters
        testPostgreSQLRegexPattern(
                "postgres.public.test_\\w+",
                Arrays.asList(
                        "test_db_10",
                        "test_db_20",
                        "test_db_30",
                        "test_db_10_no_primary",
                        "test_0609"));

        // Test case 2: Match all tables in the public1 schema
        testPostgreSQLRegexPattern(
                "postgres.public1.*", Arrays.asList("test_db_10", "test_db_20", "test_db_30"));

        // Test case 3: Match tables in public schema with names starting with "test_db_" followed
        // by any characters
        testPostgreSQLRegexPattern(
                "postgres.public.test_db_\\.*",
                Arrays.asList("test_db_10", "test_db_20", "test_db_30", "test_db_10_no_primary"));

        // Test case 4: Test pattern that doesn't match any existing tables
        testPostgreSQLRegexPattern("postgres.public.nonexistent*", Collections.emptyList());
    }

    /**
     * Helper method to test PostgreSQL regex patterns
     *
     * @param pattern The regex pattern to test
     * @param expectedTables List of expected table names
     */
    private void testPostgreSQLRegexPattern(String pattern, List<String> expectedTables) {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("driver", PGSQL_DRIVER_CLASS);
        configMap.put("url", PGSQL_URL);
        configMap.put("user", PGSQL_USERNAME);
        configMap.put("password", PGSQL_PASSWORD);
        configMap.put("table_path", pattern);
        configMap.put("use_regex", false);
        configMap.put("dialect", "postgres");

        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        JdbcSource jdbcSource = new JdbcSource(JdbcSourceConfig.of(config));
        List<CatalogTable> catalogTables = jdbcSource.getProducedCatalogTables();

        // Verify number of tables
        Assertions.assertEquals(
                expectedTables.size(),
                catalogTables.size(),
                "Expected " + expectedTables.size() + " PostgreSQL tables for pattern: " + pattern);

        if (!expectedTables.isEmpty()) {
            // Verify table names
            Set<String> actualTableNames =
                    catalogTables.stream()
                            .map(table -> table.getTableId().getTableName())
                            .collect(Collectors.toSet());

            for (String expectedTable : expectedTables) {
                Assertions.assertTrue(
                        actualTableNames.contains(expectedTable),
                        "Expected PostgreSQL table "
                                + expectedTable
                                + " not found for pattern: "
                                + pattern);
            }
        }
    }
}
