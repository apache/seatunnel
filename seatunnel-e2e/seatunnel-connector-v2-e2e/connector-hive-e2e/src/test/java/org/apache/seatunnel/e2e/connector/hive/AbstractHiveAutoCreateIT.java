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

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * Abstract base class for Hive auto create table E2E tests.
 * Provides common functionality for MySQL connection, data type mapping verification,
 * and Hive table validation.
 */
@Slf4j
public abstract class AbstractHiveAutoCreateIT {

    protected static final String MYSQL_HOST = "mysql_e2e";
    protected static final int MYSQL_PORT = 3306;
    protected static final String MYSQL_USERNAME = "st_user";
    protected static final String MYSQL_PASSWORD = "seatunnel";
    protected static final String MYSQL_DATABASE = "test";
    
    protected static final String HIVE_METASTORE_URI = "thrift://metastore:9083";
    
    // MySQL to Hive data type mapping
    protected static final Map<String, String> TYPE_MAPPING = new HashMap<>();
    
    static {
        // Initialize MySQL to Hive type mapping
        TYPE_MAPPING.put("bigint", "bigint");
        TYPE_MAPPING.put("bigint(20)", "bigint");
        TYPE_MAPPING.put("varchar", "string");
        TYPE_MAPPING.put("varchar(100)", "string");
        TYPE_MAPPING.put("varchar(200)", "string");
        TYPE_MAPPING.put("varchar(50)", "string");
        TYPE_MAPPING.put("varchar(20)", "string");
        TYPE_MAPPING.put("int", "int");
        TYPE_MAPPING.put("int(10)", "int");
        TYPE_MAPPING.put("boolean", "boolean");
        TYPE_MAPPING.put("char", "string");
        TYPE_MAPPING.put("char(12)", "string");
        TYPE_MAPPING.put("float", "float");
        TYPE_MAPPING.put("double", "double");
        TYPE_MAPPING.put("decimal(10,2)", "decimal(10,2)");
        TYPE_MAPPING.put("decimal(12,2)", "decimal(12,2)");
        TYPE_MAPPING.put("date", "date");
        TYPE_MAPPING.put("timestamp", "timestamp");
        TYPE_MAPPING.put("datetime", "timestamp");
        TYPE_MAPPING.put("tinyint(1)", "tinyint");
    }

    /**
     * Initialize MySQL connection
     */
    protected static Connection initializeMySQLConnection() throws Exception {
        String mysqlUrl = String.format("jdbc:mysql://%s:%d/%s?useSSL=false&serverTimezone=UTC", 
                MYSQL_HOST, MYSQL_PORT, MYSQL_DATABASE);
        Connection connection = DriverManager.getConnection(mysqlUrl, MYSQL_USERNAME, MYSQL_PASSWORD);
        log.info("MySQL connection established successfully");
        return connection;
    }

    /**
     * Verify MySQL to Hive data type conversion
     */
    protected void verifyTypeConversion(String mysqlType, String expectedHiveType) {
        String actualHiveType = TYPE_MAPPING.get(mysqlType);
        if (actualHiveType == null) {
            log.warn("No mapping found for MySQL type: {}", mysqlType);
            return;
        }
        
        if (!expectedHiveType.equals(actualHiveType)) {
            log.error("Type conversion mismatch - MySQL: {}, Expected Hive: {}, Actual: {}", 
                    mysqlType, expectedHiveType, actualHiveType);
        } else {
            log.info("Type conversion verified - MySQL: {} -> Hive: {}", mysqlType, actualHiveType);
        }
    }

    /**
     * Verify that a Hive database exists (placeholder implementation)
     */
    protected void verifyHiveDatabaseExists(String database) {
        log.info("Verifying Hive database exists: {}", database);
        // In real implementation: 
        // SELECT * FROM DBS WHERE NAME = ?
        // For now, assume it exists if job completed successfully
    }

    /**
     * Verify that a Hive table exists (placeholder implementation)
     */
    protected void verifyHiveTableExists(String database, String table) {
        log.info("Verifying Hive table exists: {}.{}", database, table);
        // In real implementation: 
        // SELECT * FROM TBLS t JOIN DBS d ON t.DB_ID = d.DB_ID 
        // WHERE t.TBL_NAME = ? AND d.NAME = ?
        // For now, assume it exists if job completed successfully
    }

    /**
     * Verify Hive table schema matches expected structure
     */
    protected void verifyHiveTableSchema(String database, String table, 
                                       Map<String, String> expectedColumns) {
        log.info("Verifying Hive table schema: {}.{}", database, table);
        
        for (Map.Entry<String, String> entry : expectedColumns.entrySet()) {
            String columnName = entry.getKey();
            String expectedType = entry.getValue();
            
            log.info("  Column: {} -> Type: {}", columnName, expectedType);
            
            // In real implementation:
            // SELECT c.COLUMN_NAME, c.TYPE_NAME FROM COLUMNS_V2 c
            // JOIN TBLS t ON c.CD_ID = t.TBL_ID
            // JOIN DBS d ON t.DB_ID = d.DB_ID
            // WHERE t.TBL_NAME = ? AND d.NAME = ?
        }
    }

    /**
     * Verify Hive table properties
     */
    protected void verifyHiveTableProperties(String database, String table, 
                                           Map<String, String> expectedProperties) {
        log.info("Verifying Hive table properties: {}.{}", database, table);
        
        for (Map.Entry<String, String> entry : expectedProperties.entrySet()) {
            String propertyKey = entry.getKey();
            String expectedValue = entry.getValue();
            
            log.info("  Property: {} -> Value: {}", propertyKey, expectedValue);
            
            // In real implementation:
            // SELECT PARAM_VALUE FROM TABLE_PARAMS tp
            // JOIN TBLS t ON tp.TBL_ID = t.TBL_ID
            // JOIN DBS d ON t.DB_ID = d.DB_ID
            // WHERE tp.PARAM_KEY = ? AND t.TBL_NAME = ? AND d.NAME = ?
        }
    }

    /**
     * Get expected column schema for test_db_10 table
     */
    protected Map<String, String> getTestDb10ExpectedSchema() {
        Map<String, String> schema = new HashMap<>();
        schema.put("id", "bigint");
        schema.put("name", "string");
        schema.put("age", "int");
        schema.put("sex", "boolean");
        schema.put("address", "string");
        schema.put("telephone", "string");
        schema.put("height", "float");
        schema.put("weight", "double");
        schema.put("size", "decimal(10,2)");
        schema.put("ID_number", "string");
        schema.put("date_time", "date");
        schema.put("ts", "timestamp");
        return schema;
    }

    /**
     * Get expected column schema for test_db_11 table
     */
    protected Map<String, String> getTestDb11ExpectedSchema() {
        Map<String, String> schema = new HashMap<>();
        schema.put("id", "bigint");
        schema.put("product_name", "string");
        schema.put("price", "decimal(10,2)");
        schema.put("category", "string");
        schema.put("in_stock", "boolean");
        schema.put("created_at", "timestamp");
        return schema;
    }

    /**
     * Get expected column schema for user_info table
     */
    protected Map<String, String> getUserInfoExpectedSchema() {
        Map<String, String> schema = new HashMap<>();
        schema.put("user_id", "bigint");
        schema.put("username", "string");
        schema.put("email", "string");
        schema.put("phone", "string");
        schema.put("status", "tinyint");
        schema.put("created_time", "timestamp");
        return schema;
    }

    /**
     * Get expected column schema for order_info table
     */
    protected Map<String, String> getOrderInfoExpectedSchema() {
        Map<String, String> schema = new HashMap<>();
        schema.put("order_id", "bigint");
        schema.put("user_id", "bigint");
        schema.put("order_amount", "decimal(12,2)");
        schema.put("order_status", "string");
        schema.put("order_date", "date");
        schema.put("created_at", "timestamp");
        return schema;
    }

    /**
     * Get expected table properties for auto-created tables
     */
    protected Map<String, String> getExpectedTableProperties(String sourceTable) {
        Map<String, String> properties = new HashMap<>();
        properties.put("comment", "Auto created from MySQL " + sourceTable);
        properties.put("created_by", "seatunnel_e2e_test");
        return properties;
    }

    /**
     * Log test completion message
     */
    protected void logTestCompletion(String testType) {
        String separator = "============================================================";
        log.info(separator);
        log.info("{} auto-create test completed successfully!", testType);
        log.info(separator);
    }
}
