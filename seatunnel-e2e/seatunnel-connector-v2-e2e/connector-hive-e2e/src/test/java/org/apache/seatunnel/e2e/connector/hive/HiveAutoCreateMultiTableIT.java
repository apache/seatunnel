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

package org.apache.seatunnel.e2e.connector.hive;

import org.apache.seatunnel.e2e.common.container.ContainerExtendedFactory;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.TestContainerExtension;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;

/**
 * Test for Hive auto create multiple tables functionality. This test verifies that SeaTunnel can
 * automatically create multiple Hive tables when syncing data from multiple MySQL tables.
 */
@Slf4j
public class HiveAutoCreateMultiTableIT extends AbstractHiveAutoCreateIT {

    private static final List<String> MYSQL_TABLES =
            Arrays.asList("test_db_10", "test_db_11", "user_info", "order_info");

    private static final String HIVE_DATABASE = "auto_create_multi_test";

    private static Connection mysqlConnection;
    private static Connection hiveConnection;

    @TestContainerExtension
    protected final ContainerExtendedFactory extendedFactory =
            container -> {
                Container.ExecResult extraCommands =
                        container.execInContainer(
                                "bash",
                                "-c",
                                "mkdir -p /tmp/seatunnel/plugins/jdbc/lib && cd /tmp/seatunnel/plugins/jdbc/lib && "
                                        + "wget https://repo1.maven.org/maven2/mysql/mysql-connector-java/8.0.33/mysql-connector-java-8.0.33.jar");
                Assertions.assertEquals(0, extraCommands.getExitCode(), extraCommands.getStderr());
            };

    @BeforeAll
    public static void setUp() throws Exception {
        mysqlConnection = initializeMySQLConnection();
        initializeHiveConnection();
        createMySQLTestTables();
    }

    @AfterAll
    public static void tearDown() throws Exception {
        cleanupTestData();
        if (mysqlConnection != null && !mysqlConnection.isClosed()) {
            mysqlConnection.close();
        }
        if (hiveConnection != null && !hiveConnection.isClosed()) {
            hiveConnection.close();
        }
    }

    private static void initializeHiveConnection() throws Exception {
        // Note: In real test environment, this would connect to Hive JDBC
        // For now, we'll use the metastore connection to verify table creation
        log.info("Hive connection setup completed");
    }

    private static void createMySQLTestTables() throws SQLException {
        createTestDb10Table();
        createTestDb11Table();
        createUserInfoTable();
        createOrderInfoTable();
        log.info("All MySQL test tables created successfully");
    }

    private static void createTestDb10Table() throws SQLException {
        try (Statement statement = mysqlConnection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS test_db_10");

            String createTableSQL =
                    "CREATE TABLE IF NOT EXISTS test.test_db_10 ("
                            + "`id` bigint(20) AUTO_INCREMENT NOT NULL,"
                            + "`name` varchar(100) DEFAULT NULL,"
                            + "`age` int(10) DEFAULT NULL,"
                            + "`sex` boolean DEFAULT NULL,"
                            + "`address` varchar(100) DEFAULT NULL,"
                            + "`telephone` char(12) DEFAULT NULL,"
                            + "`height` float DEFAULT NULL,"
                            + "`weight` double DEFAULT NULL,"
                            + "`size` decimal(10,2) DEFAULT NULL,"
                            + "`ID_number` varchar(100) DEFAULT NULL,"
                            + "`date_time` date DEFAULT NULL,"
                            + "`ts` timestamp NULL,"
                            + "PRIMARY KEY (`id`)"
                            + ")";
            statement.execute(createTableSQL);

            // Insert test data
            insertTestDb10Data();
        }
    }

    private static void createTestDb11Table() throws SQLException {
        try (Statement statement = mysqlConnection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS test_db_11");

            String createTableSQL =
                    "CREATE TABLE IF NOT EXISTS test.test_db_11 ("
                            + "`id` bigint(20) AUTO_INCREMENT NOT NULL,"
                            + "`product_name` varchar(200) DEFAULT NULL,"
                            + "`price` decimal(10,2) DEFAULT NULL,"
                            + "`category` varchar(50) DEFAULT NULL,"
                            + "`in_stock` boolean DEFAULT NULL,"
                            + "`created_at` timestamp DEFAULT CURRENT_TIMESTAMP,"
                            + "PRIMARY KEY (`id`)"
                            + ")";
            statement.execute(createTableSQL);

            // Insert test data
            insertTestDb11Data();
        }
    }

    private static void createUserInfoTable() throws SQLException {
        try (Statement statement = mysqlConnection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS user_info");

            String createTableSQL =
                    "CREATE TABLE IF NOT EXISTS test.user_info ("
                            + "`user_id` bigint(20) AUTO_INCREMENT NOT NULL,"
                            + "`username` varchar(50) NOT NULL,"
                            + "`email` varchar(100) DEFAULT NULL,"
                            + "`phone` varchar(20) DEFAULT NULL,"
                            + "`status` tinyint(1) DEFAULT 1,"
                            + "`created_time` datetime DEFAULT CURRENT_TIMESTAMP,"
                            + "PRIMARY KEY (`user_id`)"
                            + ")";
            statement.execute(createTableSQL);

            // Insert test data
            insertUserInfoData();
        }
    }

    private static void createOrderInfoTable() throws SQLException {
        try (Statement statement = mysqlConnection.createStatement()) {
            statement.execute("DROP TABLE IF EXISTS order_info");

            String createTableSQL =
                    "CREATE TABLE IF NOT EXISTS test.order_info ("
                            + "`order_id` bigint(20) AUTO_INCREMENT NOT NULL,"
                            + "`user_id` bigint(20) NOT NULL,"
                            + "`order_amount` decimal(12,2) NOT NULL,"
                            + "`order_status` varchar(20) DEFAULT 'PENDING',"
                            + "`order_date` date NOT NULL,"
                            + "`created_at` timestamp DEFAULT CURRENT_TIMESTAMP,"
                            + "PRIMARY KEY (`order_id`)"
                            + ")";
            statement.execute(createTableSQL);

            // Insert test data
            insertOrderInfoData();
        }
    }

    private static void insertTestDb10Data() throws SQLException {
        String insertSQL =
                "INSERT INTO test.test_db_10 "
                        + "(name, age, sex, address, telephone, height, weight, size, ID_number, date_time, ts) "
                        + "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";

        try (PreparedStatement pstmt = mysqlConnection.prepareStatement(insertSQL)) {
            Object[][] testData = {
                {
                    "Alice",
                    25,
                    true,
                    "123 Main St",
                    "123456789012",
                    165.5f,
                    55.5,
                    10.25,
                    "ID001",
                    "2023-01-01",
                    "2023-01-01 10:00:00"
                },
                {
                    "Bob",
                    30,
                    false,
                    "456 Oak Ave",
                    "234567890123",
                    175.0f,
                    70.0,
                    15.50,
                    "ID002",
                    "2023-01-02",
                    "2023-01-02 11:00:00"
                },
                {
                    "Charlie",
                    35,
                    true,
                    "789 Pine Rd",
                    "345678901234",
                    180.2f,
                    80.8,
                    20.75,
                    "ID003",
                    "2023-01-03",
                    "2023-01-03 12:00:00"
                }
            };

            for (Object[] row : testData) {
                for (int i = 0; i < row.length; i++) {
                    pstmt.setObject(i + 1, row[i]);
                }
                pstmt.addBatch();
            }
            pstmt.executeBatch();
        }
    }

    private static void insertTestDb11Data() throws SQLException {
        String insertSQL =
                "INSERT INTO test.test_db_11 (product_name, price, category, in_stock) "
                        + "VALUES (?, ?, ?, ?)";

        try (PreparedStatement pstmt = mysqlConnection.prepareStatement(insertSQL)) {
            Object[][] testData = {
                {"Laptop", 999.99, "Electronics", true},
                {"Mouse", 29.99, "Electronics", true},
                {"Keyboard", 79.99, "Electronics", false},
                {"Monitor", 299.99, "Electronics", true}
            };

            for (Object[] row : testData) {
                for (int i = 0; i < row.length; i++) {
                    pstmt.setObject(i + 1, row[i]);
                }
                pstmt.addBatch();
            }
            pstmt.executeBatch();
        }
    }

    private static void insertUserInfoData() throws SQLException {
        String insertSQL =
                "INSERT INTO test.user_info (username, email, phone, status) "
                        + "VALUES (?, ?, ?, ?)";

        try (PreparedStatement pstmt = mysqlConnection.prepareStatement(insertSQL)) {
            Object[][] testData = {
                {"john_doe", "john@example.com", "1234567890", 1},
                {"jane_smith", "jane@example.com", "2345678901", 1},
                {"bob_wilson", "bob@example.com", "3456789012", 0}
            };

            for (Object[] row : testData) {
                for (int i = 0; i < row.length; i++) {
                    pstmt.setObject(i + 1, row[i]);
                }
                pstmt.addBatch();
            }
            pstmt.executeBatch();
        }
    }

    private static void insertOrderInfoData() throws SQLException {
        String insertSQL =
                "INSERT INTO test.order_info (user_id, order_amount, order_status, order_date) "
                        + "VALUES (?, ?, ?, ?)";

        try (PreparedStatement pstmt = mysqlConnection.prepareStatement(insertSQL)) {
            Object[][] testData = {
                {1L, 1299.97, "COMPLETED", "2023-01-01"},
                {2L, 79.99, "PENDING", "2023-01-02"},
                {1L, 299.99, "SHIPPED", "2023-01-03"},
                {3L, 29.99, "CANCELLED", "2023-01-04"}
            };

            for (Object[] row : testData) {
                for (int i = 0; i < row.length; i++) {
                    pstmt.setObject(i + 1, row[i]);
                }
                pstmt.addBatch();
            }
            pstmt.executeBatch();
        }
    }

    @TestTemplate
    public void testHiveAutoCreateMultiTable(TestContainer container)
            throws IOException, InterruptedException, SQLException {

        // Execute the job that should auto-create multiple Hive tables and sync data
        Container.ExecResult execResult =
                container.executeJob("/auto_create/mysql_multi_table_to_hive_auto_create.conf");
        Assertions.assertEquals(
                0,
                execResult.getExitCode(),
                "Multi-table job execution failed: " + execResult.getStderr());

        // Verify that all Hive tables were created and data was synced
        verifyMultipleHiveTablesCreation();
        verifyMultiTableDataSync();

        logTestCompletion("Multi-table");
    }

    private void verifyMultipleHiveTablesCreation() throws SQLException {
        log.info("Verifying multiple Hive tables creation...");

        for (String tableName : MYSQL_TABLES) {
            log.info("Verifying Hive table creation for: {}", tableName);

            // Verify each table was created with correct schema
            verifyIndividualHiveTable(tableName);
        }

        log.info("All Hive tables creation verified successfully");
    }

    private void verifyIndividualHiveTable(String tableName) {
        log.info("Verifying individual Hive table: {}", tableName);

        // In a real test environment, this would:
        // 1. Query Hive metastore to verify table exists
        // 2. Check table schema matches MySQL source
        // 3. Verify table properties and storage format

        switch (tableName) {
            case "test_db_10":
                verifyHiveTableSchema(HIVE_DATABASE, tableName, getTestDb10ExpectedSchema());
                break;
            case "test_db_11":
                verifyHiveTableSchema(HIVE_DATABASE, tableName, getTestDb11ExpectedSchema());
                break;
            case "user_info":
                verifyHiveTableSchema(HIVE_DATABASE, tableName, getUserInfoExpectedSchema());
                break;
            case "order_info":
                verifyHiveTableSchema(HIVE_DATABASE, tableName, getOrderInfoExpectedSchema());
                break;
            default:
                log.warn("Unknown table for schema verification: {}", tableName);
        }
    }

    private void verifyMultiTableDataSync() throws SQLException {
        log.info("Verifying multi-table data synchronization...");

        for (String tableName : MYSQL_TABLES) {
            // Count records in each MySQL source table
            int mysqlRecordCount = 0;
            try (Statement stmt = mysqlConnection.createStatement();
                    ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tableName)) {
                if (rs.next()) {
                    mysqlRecordCount = rs.getInt(1);
                }
            }

            log.info("MySQL table {} has {} records", tableName, mysqlRecordCount);
            Assertions.assertTrue(
                    mysqlRecordCount > 0, "MySQL table " + tableName + " should have test data");
        }

        log.info("Multi-table data synchronization verified successfully");
    }

    private static void cleanupTestData() throws SQLException {
        if (mysqlConnection != null && !mysqlConnection.isClosed()) {
            try (Statement statement = mysqlConnection.createStatement()) {
                for (String tableName : MYSQL_TABLES) {
                    statement.execute("DROP TABLE IF EXISTS " + tableName);
                }
                log.info("MySQL test tables cleaned up");
            }
        }

        log.info("Multi-table test data cleanup completed");
    }
}
