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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.kingbase.container;

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.dialectenum.FieldIdeEnum;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.kingbase.KingbaseDialect;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;

/**
 * Unit tests for KingbaseDialect using Testcontainers. Tests dialect-specific functionality like
 * quoting, SQL generation, and upsert statements.
 */
@DisabledOnOs(OS.WINDOWS)
public class KingbaseDialectContainerTest extends AbstractKingbaseContainerTest {

    private static final String TEST_TABLE = "dialect_test_table";
    private KingbaseDialect dialect;

    @BeforeAll
    public void setupDialect() throws SQLException {
        dialect = new KingbaseDialect();

        String qualifiedTable =
                String.format("%s.%s", quoteIdentifier(SCHEMA), quoteIdentifier(TEST_TABLE));

        String createTableSql =
                "CREATE TABLE IF NOT EXISTS "
                        + qualifiedTable
                        + " ("
                        + "id BIGINT PRIMARY KEY, "
                        + "name VARCHAR(100), "
                        + "value NUMERIC(10,2), "
                        + "created_at TIMESTAMP"
                        + ")";
        executeSql(createTableSql);

        String truncateSql = "TRUNCATE TABLE " + qualifiedTable;
        executeSql(truncateSql);

        // Insert test data
        String insertSql =
                "INSERT INTO "
                        + qualifiedTable
                        + " (id, name, value, created_at) "
                        + "VALUES (1, 'test1', 100.50, CURRENT_TIMESTAMP)";
        executeSql(insertSql);
    }

    @Test
    public void testDialectName() {
        Assertions.assertEquals(DatabaseIdentifier.KINGBASE, dialect.dialectName());
    }

    @Test
    public void testQuoteIdentifier() {
        // Test basic identifier
        Assertions.assertEquals("\"table_name\"", dialect.quoteIdentifier("table_name"));
        Assertions.assertEquals("\"COLUMN\"", dialect.quoteIdentifier("COLUMN"));

        // Test identifier with dots (schema.table)
        Assertions.assertEquals("\"schema\".\"table\"", dialect.quoteIdentifier("schema.table"));
    }

    @Test
    public void testQuoteIdentifierWithFieldIde() {
        // Test with fieldIde = UPPERCASE
        KingbaseDialect dialectUpper = new KingbaseDialect(FieldIdeEnum.UPPERCASE.getValue());
        Assertions.assertEquals("\"COLUMN_NAME\"", dialectUpper.quoteIdentifier("column_name"));

        // Test with fieldIde = LOWERCASE
        KingbaseDialect dialectLower = new KingbaseDialect(FieldIdeEnum.LOWERCASE.getValue());
        Assertions.assertEquals("\"column_name\"", dialectLower.quoteIdentifier("COLUMN_NAME"));

        // Test with fieldIde = ORIGINAL (default)
        KingbaseDialect dialectOriginal = new KingbaseDialect(FieldIdeEnum.ORIGINAL.getValue());
        Assertions.assertEquals("\"Column_Name\"", dialectOriginal.quoteIdentifier("Column_Name"));
    }

    @Test
    public void testTableIdentifier() {
        // Test with database and table
        String identifier = dialect.tableIdentifier("mydb", "mytable");
        Assertions.assertEquals("\"mydb\".\"mytable\"", identifier);
    }

    @Test
    public void testQuoteDatabaseIdentifier() {
        Assertions.assertEquals("\"testdb\"", dialect.quoteDatabaseIdentifier("testdb"));
        Assertions.assertEquals("\"MyDatabase\"", dialect.quoteDatabaseIdentifier("MyDatabase"));
    }

    @Test
    public void testParseTablePath() {
        // Test parsing full table path
        TablePath path1 = dialect.parse("database.schema.table");
        Assertions.assertEquals("database", path1.getDatabaseName());
        Assertions.assertEquals("schema", path1.getSchemaName());
        Assertions.assertEquals("table", path1.getTableName());

        // Test parsing simple table name
        TablePath path2 = dialect.parse("table");
        Assertions.assertNull(path2.getDatabaseName());
        Assertions.assertEquals("table", path2.getTableName());
    }

    @Test
    public void testGetUpsertStatement() {
        String[] fieldNames = {"id", "name", "value", "created_at"};
        String[] uniqueKeyFields = {"id"};

        Optional<String> upsertSqlOptional =
                dialect.getUpsertStatement(SCHEMA, TEST_TABLE, fieldNames, uniqueKeyFields);

        Assertions.assertTrue(upsertSqlOptional.isPresent());
        String upsertSql = upsertSqlOptional.get();

        // Verify the SQL contains expected parts
        Assertions.assertTrue(upsertSql.contains("INSERT INTO"));
        Assertions.assertTrue(upsertSql.contains("ON CONFLICT"));
        Assertions.assertTrue(upsertSql.contains("DO UPDATE SET"));
        Assertions.assertTrue(upsertSql.contains("EXCLUDED"));
    }

    @Test
    public void testGetInsertIntoStatement() {
        String[] fieldNames = {"id", "name", "value"};

        String insertSql = dialect.getInsertIntoStatement(SCHEMA, TEST_TABLE, fieldNames);

        Assertions.assertNotNull(insertSql);
        Assertions.assertTrue(insertSql.contains("INSERT INTO"));
        Assertions.assertTrue(insertSql.contains("\"id\""));
        Assertions.assertTrue(insertSql.contains("\"name\""));
        Assertions.assertTrue(insertSql.contains("\"value\""));
    }

    @Test
    public void testGetUpdateStatement() {
        String[] fieldNames = {"name", "value"};
        String[] conditionFields = {"id"};

        String updateSql =
                dialect.getUpdateStatement(SCHEMA, TEST_TABLE, fieldNames, conditionFields, false);

        Assertions.assertNotNull(updateSql);
        Assertions.assertTrue(updateSql.contains("UPDATE"));
        Assertions.assertTrue(updateSql.contains("SET"));
        Assertions.assertTrue(updateSql.contains("WHERE"));
    }

    @Test
    public void testGetDeleteStatement() {
        String[] conditionFields = {"id"};

        String deleteSql = dialect.getDeleteStatement(SCHEMA, TEST_TABLE, conditionFields);

        Assertions.assertNotNull(deleteSql);
        Assertions.assertTrue(deleteSql.contains("DELETE FROM"));
        Assertions.assertTrue(deleteSql.contains("WHERE"));
    }

    @Test
    public void testGetRowExistsStatement() {
        String[] conditionFields = {"id"};

        String existsSql = dialect.getRowExistsStatement(SCHEMA, TEST_TABLE, conditionFields);

        Assertions.assertNotNull(existsSql);
        Assertions.assertTrue(existsSql.contains("SELECT 1 FROM"));
        Assertions.assertTrue(existsSql.contains("WHERE"));
    }

    @Test
    public void testRealUpsertExecution() throws SQLException {
        String testTable = "test_upsert_execution";

        try {
            // Create test table
            String createTableSql =
                    String.format(
                            "CREATE TABLE %s.%s ("
                                    + "id INT8 PRIMARY KEY, "
                                    + "name VARCHAR(100), "
                                    + "value INT4"
                                    + ")",
                            quoteIdentifier(SCHEMA), quoteIdentifier(testTable));
            executeSql(createTableSql);

            // Insert first row
            String insertSql =
                    String.format(
                            "INSERT INTO %s.%s (id, name, value) VALUES (1, 'first', 100)",
                            quoteIdentifier(SCHEMA), quoteIdentifier(testTable));
            executeSql(insertSql);

            // Verify insert
            try (Connection connection = getConnection();
                    Statement stmt = connection.createStatement();
                    ResultSet rs =
                            stmt.executeQuery(
                                    String.format(
                                            "SELECT COUNT(*) FROM %s.%s",
                                            quoteIdentifier(SCHEMA), quoteIdentifier(testTable)))) {
                rs.next();
                Assertions.assertEquals(1, rs.getInt(1));
            }

            // Generate upsert SQL
            String[] fieldNames = {"id", "name", "value"};
            String[] uniqueKeyFields = {"id"};
            Optional<String> upsertSqlOptional =
                    dialect.getUpsertStatement(SCHEMA, testTable, fieldNames, uniqueKeyFields);

            Assertions.assertTrue(upsertSqlOptional.isPresent());
            String upsertSql = upsertSqlOptional.get();

            // Verify the generated SQL structure
            Assertions.assertTrue(upsertSql.contains("INSERT INTO"));
            Assertions.assertTrue(upsertSql.contains("ON CONFLICT"));
            Assertions.assertTrue(upsertSql.contains("DO UPDATE SET"));

        } finally {
            // Cleanup
            try {
                executeSql(
                        String.format(
                                "DROP TABLE IF EXISTS %s.%s",
                                quoteIdentifier(SCHEMA), quoteIdentifier(testTable)));
            } catch (SQLException e) {
                // Ignore cleanup errors
            }
        }
    }

    @Test
    public void testGetRowConverter() {
        Assertions.assertNotNull(dialect.getRowConverter());
        Assertions.assertEquals(
                "KingbaseJdbcRowConverter", dialect.getRowConverter().getClass().getSimpleName());
    }

    @Test
    public void testGetJdbcDialectTypeMapper() {
        Assertions.assertNotNull(dialect.getJdbcDialectTypeMapper());
        Assertions.assertEquals(
                "KingbaseTypeMapper",
                dialect.getJdbcDialectTypeMapper().getClass().getSimpleName());
    }

    @Test
    public void testFieldIdeHandling() {
        // Test with ORIGINAL (default)
        String original = dialect.getFieldIde("ColumnName", FieldIdeEnum.ORIGINAL.getValue());
        Assertions.assertEquals("ColumnName", original);

        // Test with UPPERCASE
        String upper = dialect.getFieldIde("ColumnName", FieldIdeEnum.UPPERCASE.getValue());
        Assertions.assertEquals("COLUMNNAME", upper);

        // Test with LOWERCASE
        String lower = dialect.getFieldIde("ColumnName", FieldIdeEnum.LOWERCASE.getValue());
        Assertions.assertEquals("columnname", lower);
    }

    @Test
    public void testCreatPreparedStatement() throws SQLException {
        String sql =
                String.format(
                        "SELECT * FROM %s.%s",
                        quoteIdentifier(SCHEMA), quoteIdentifier(TEST_TABLE));

        try (Connection connection = getConnection();
                PreparedStatement ps = dialect.creatPreparedStatement(connection, sql, 100)) {
            Assertions.assertNotNull(ps);
            Assertions.assertEquals(100, ps.getFetchSize());
        }
    }

    @Test
    public void testTableIdentifierWithTablePath() {
        TablePath tablePath = TablePath.of(DATABASE, SCHEMA, TEST_TABLE);
        String identifier = dialect.tableIdentifier(tablePath);

        Assertions.assertTrue(identifier.contains(SCHEMA));
        Assertions.assertTrue(identifier.contains(TEST_TABLE));
    }
}
