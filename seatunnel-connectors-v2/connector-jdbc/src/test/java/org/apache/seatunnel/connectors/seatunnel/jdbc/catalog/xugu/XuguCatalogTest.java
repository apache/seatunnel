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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.xugu;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.util.List;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Disabled("Please Test it in your local environment")
class XuguCatalogTest {

    static XuguCatalog catalog;
    static JdbcUrlUtil.UrlInfo XuguUrlInfo =
            JdbcUrlUtil.getUrlInfo("jdbc:xugu://127.0.0.1:5138/TEST_DATABASE");
    static String DRIVER_CLASS = "com.xugu.cloudjdbc.Driver";

    // Test constants for specific Xugu objects
    private static final String TEST_DATABASE_NAME = "TEST_DATABASE";
    private static final String TEST_SCHEMA_NAME = "SYSDBA";
    private static final String TEST_TABLE_NAME = "XUGU_DATA_TYPES_TEST";

    @BeforeAll
    static void before() {
        catalog = new XuguCatalog("xugu", "SYSDBA", "SYSDBA", XuguUrlInfo, null, DRIVER_CLASS);

        catalog.open();
    }

    @Test
    void testListDatabases() {
        // Test listing databases functionality
        List<String> databases = catalog.listDatabases();
        Assertions.assertNotNull(databases, "Database list should not be null");
        Assertions.assertFalse(databases.isEmpty(), "Database list should not be empty");
    }

    @Test
    void testDatabaseExists() {
        // Test specific database existence with case sensitivity
        Assertions.assertTrue(catalog.databaseExists(TEST_DATABASE_NAME),
            "TEST_DATABASE should exist");
        Assertions.assertTrue(catalog.databaseExists(TEST_DATABASE_NAME.toUpperCase()),
            "Database existence check should be case-insensitive (uppercase)");

        // Test mixed case scenarios for TEST_DATABASE
        Assertions.assertTrue(catalog.databaseExists("test_database"),
            "test_database should exist (lowercase)");
        Assertions.assertTrue(catalog.databaseExists("Test_Database"),
            "Test_Database should exist (mixed case)");


        // Test non-existent database
        Assertions.assertFalse(catalog.databaseExists("NON_EXISTENT_DB"),
            "Non-existent database should return false");
        Assertions.assertFalse(catalog.databaseExists("non_existent_db"),
            "Non-existent database (lowercase) should return false");
    }

    @Test
    void testListTables() {
        // Test listing tables functionality
        List<String> databases = catalog.listDatabases();
        if (!databases.isEmpty()) {
            String databaseName = databases.get(0);
            List<String> tables = catalog.listTables(databaseName);
            Assertions.assertNotNull(tables, "Table list should not be null");
        }
    }

    @Test
    void testTableExists() {
        // Test specific table existence
        TablePath testTablePath = TablePath.of(TEST_DATABASE_NAME, TEST_SCHEMA_NAME, TEST_TABLE_NAME);
        Assertions.assertTrue(catalog.tableExists(testTablePath),
            "XUGU_DATA_TYPES_TEST table should exist in SYSDBA schema");

        // Test case-insensitive database name handling
        TablePath lowerCaseDatabasePath = TablePath.of(TEST_DATABASE_NAME.toLowerCase(), TEST_SCHEMA_NAME, TEST_TABLE_NAME);
        Assertions.assertTrue(catalog.tableExists(lowerCaseDatabasePath),
            "Table existence check should be case-insensitive for database name");

        // Test non-existent table
        TablePath nonExistentTable = TablePath.of(TEST_DATABASE_NAME, TEST_SCHEMA_NAME, "NON_EXISTENT_TABLE");
        Assertions.assertFalse(catalog.tableExists(nonExistentTable),
            "Non-existent table should return false");
    }

    @Test
    void testGetTable() {
        // Test getting specific table metadata
        TablePath testTablePath = TablePath.of(TEST_DATABASE_NAME, TEST_SCHEMA_NAME, TEST_TABLE_NAME);
        CatalogTable table = catalog.getTable(testTablePath);

        Assertions.assertNotNull(table, "XUGU_DATA_TYPES_TEST table metadata should not be null");
        Assertions.assertNotNull(table.getTableSchema(), "Table schema should not be null");
        Assertions.assertEquals(TEST_TABLE_NAME, table.getTableId().getTableName(),
            "Table name should match");
        Assertions.assertEquals(TEST_SCHEMA_NAME, table.getTableId().getSchemaName(),
            "Schema name should match");
        Assertions.assertEquals(TEST_DATABASE_NAME, table.getTableId().getDatabaseName(),
            "Database name should match");

        // Test that table has columns (data types test table should have multiple columns)
        Assertions.assertNotNull(table.getTableSchema().getColumns(),
            "Table should have columns");
        Assertions.assertFalse(table.getTableSchema().getColumns().isEmpty(),
            "XUGU_DATA_TYPES_TEST should have multiple columns for testing data types");
    }

    @Test
    void testXuguCaseInsensitiveDatabaseHandling() {
        // Test Xugu's specific case-insensitive database name handling
        // Xugu forces database names to uppercase internally
        List<String> databases = catalog.listDatabases();
        if (!databases.isEmpty()) {
            String firstDatabase = databases.get(0);

            // Test that all returned database names are uppercase (Xugu behavior)
            Assertions.assertEquals(
                    firstDatabase.toUpperCase(),
                    firstDatabase,
                    "Xugu should return database names in uppercase");

            // Test various case combinations all resolve to the same database
            String[] testCases = {
                firstDatabase,
                firstDatabase.toLowerCase(),
                firstDatabase.toUpperCase(),
                firstDatabase.substring(0, 1).toLowerCase() + firstDatabase.substring(1),
                firstDatabase.substring(0, 1).toUpperCase()
                        + firstDatabase.substring(1).toLowerCase()
            };

            for (String testCase : testCases) {
                Assertions.assertTrue(
                        catalog.databaseExists(testCase),
                        "Database existence check should work for case variant: " + testCase);
            }
        }
    }

    @Test
    void testCreateAndDropTable() {
        // Test table creation and deletion
        List<String> databases = catalog.listDatabases();
        if (!databases.isEmpty()) {
            String databaseName = databases.get(0);
            List<String> tables = catalog.listTables(databaseName);
            if (!tables.isEmpty()) {
                String existingTableName = tables.get(0);
                String[] parts = existingTableName.split("\\.");
                if (parts.length == 2) {
                    TablePath existingTablePath = TablePath.of(databaseName, parts[0], parts[1]);
                    CatalogTable existingTable = catalog.getTable(existingTablePath);

                    // Create test table
                    TablePath testTablePath =
                            TablePath.of(databaseName, parts[0], "TEST_XUGU_TABLE");
                    catalog.createTable(testTablePath, existingTable, false);

                    // Verify table was created
                    Assertions.assertTrue(
                            catalog.tableExists(testTablePath),
                            "Test table should exist after creation");

                    // Drop test table
                    catalog.dropTable(testTablePath, false);

                    // Verify table was dropped
                    Assertions.assertFalse(
                            catalog.tableExists(testTablePath),
                            "Test table should not exist after deletion");
                }
            }
        }
    }
}
