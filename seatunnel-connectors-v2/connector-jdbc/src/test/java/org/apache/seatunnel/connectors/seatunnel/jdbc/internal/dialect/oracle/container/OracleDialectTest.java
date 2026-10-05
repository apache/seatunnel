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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.oracle.container;

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.oracle.OracleDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.executor.FieldNamedPreparedStatement;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.JdbcSourceTable;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for OracleDialect using Testcontainers. Tests dialect-specific functionality like
 * quoting, SQL generation, and data sampling.
 */
@DisabledOnOs(OS.WINDOWS)
public class OracleDialectTest extends AbstractOracleContainerTest {

    private static OracleDialect dialect;
    private static final String TEST_TABLE = "DIALECT_TEST_TABLE";

    @BeforeAll
    public static void setupDialect() throws SQLException {
        dialect = new OracleDialect();

        String createTableSql =
                String.format(
                        "CREATE TABLE %s.%s ("
                                + "ID NUMBER(10) PRIMARY KEY, "
                                + "VARCHAR_COL VARCHAR2(100), "
                                + "NUMBER_COL NUMBER(10,2), "
                                + "DATE_COL DATE"
                                + ")",
                        quoteIdentifier(SCHEMA), quoteIdentifier(TEST_TABLE));

        try (Statement stmt = connection.createStatement()) {
            stmt.execute(createTableSql);
        }

        String insertSql =
                String.format(
                        "INSERT INTO %s.%s (ID, VARCHAR_COL, NUMBER_COL, DATE_COL) "
                                + "VALUES (1, 'test1', 100.50, SYSDATE)",
                        quoteIdentifier(SCHEMA), quoteIdentifier(TEST_TABLE));
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(insertSql);
            // Note: Auto-commit is enabled by default, so no explicit commit needed
        }
    }

    @AfterEach
    public void cleanupTestData() throws SQLException {
        // Reset table to initial state (keep only ID=1)
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(
                    String.format(
                            "DELETE FROM %s.%s WHERE ID <> 1",
                            quoteIdentifier(SCHEMA), quoteIdentifier(TEST_TABLE)));
        }
    }

    @Test
    public void testDialectName() {
        Assertions.assertEquals(DatabaseIdentifier.ORACLE, dialect.dialectName());
    }

    @Test
    public void testQuoteIdentifier() {
        Assertions.assertEquals("\"table_name\"", dialect.quoteIdentifier("table_name"));
        Assertions.assertEquals("\"COLUMN\"", dialect.quoteIdentifier("COLUMN"));
    }

    @Test
    public void testTableIdentifierWithSchema() {
        // OracleDialect.tableIdentifier(String, String) ignores database parameter
        String identifier = dialect.tableIdentifier(SCHEMA, TEST_TABLE);
        Assertions.assertTrue(identifier.contains(TEST_TABLE));
    }

    @Test
    public void testTableIdentifierWithTablePath() {
        TablePath tablePath = TablePath.of(null, SCHEMA, TEST_TABLE);
        String identifier = dialect.tableIdentifier(tablePath);
        Assertions.assertTrue(identifier.contains(SCHEMA));
        Assertions.assertTrue(identifier.contains(TEST_TABLE));
    }

    @Test
    public void testParse() {
        TablePath parsedPath = dialect.parse(SCHEMA + "." + TEST_TABLE);
        Assertions.assertEquals(SCHEMA, parsedPath.getSchemaName());
        Assertions.assertEquals(TEST_TABLE, parsedPath.getTableName());

        TablePath singlePath = dialect.parse(TEST_TABLE);
        Assertions.assertEquals(TEST_TABLE, singlePath.getTableName());
    }

    @Test
    public void testHashModForField() {
        String result = dialect.hashModForField("ID", 10);
        Assertions.assertTrue(result.contains("MOD"));
        Assertions.assertTrue(result.contains("ORA_HASH"));
        Assertions.assertTrue(result.contains("\"ID\""));
        Assertions.assertTrue(result.contains("10"));
    }

    @Test
    public void testDualTable() {
        String dualTable = dialect.dualTable();
        Assertions.assertEquals(" FROM dual ", dualTable);
    }

    @Test
    public void testSampleDataFromColumn() throws Exception {
        TablePath tablePath = TablePath.of(DATABASE, SCHEMA, TEST_TABLE);
        JdbcSourceTable table =
                JdbcSourceTable.builder()
                        .tablePath(tablePath)
                        .useSelectCount(false)
                        .skipAnalyze(false)
                        .build();

        Object[] samples = dialect.sampleDataFromColumn(connection, table, "ID", 1, 1024);
        Assertions.assertNotNull(samples);
        Assertions.assertTrue(samples.length > 0);
    }

    @Test
    public void testSampleDataFromColumnWithQuery() throws Exception {
        TablePath tablePath = TablePath.of(DATABASE, SCHEMA, TEST_TABLE);
        JdbcSourceTable table =
                JdbcSourceTable.builder()
                        .tablePath(tablePath)
                        .query(
                                "select * from "
                                        + quoteIdentifier(SCHEMA)
                                        + "."
                                        + quoteIdentifier(TEST_TABLE)
                                        + " where ID = 1")
                        .useSelectCount(false)
                        .skipAnalyze(false)
                        .build();

        Object[] samples = dialect.sampleDataFromColumn(connection, table, "ID", 1, 1024);
        Assertions.assertNotNull(samples);
    }

    @Test
    public void testApproximateRowCntStatement() throws Exception {
        TablePath tablePath = TablePath.of(DATABASE, SCHEMA, TEST_TABLE);
        JdbcSourceTable table =
                JdbcSourceTable.builder()
                        .tablePath(tablePath)
                        .useSelectCount(false)
                        .skipAnalyze(false)
                        .build();

        Long rowCount = dialect.approximateRowCntStatement(connection, table);
        Assertions.assertNotNull(rowCount);
        Assertions.assertTrue(rowCount >= 0);
    }

    @Test
    public void testQueryNextChunkMax() throws Exception {
        TablePath tablePath = TablePath.of(DATABASE, SCHEMA, TEST_TABLE);
        JdbcSourceTable table =
                JdbcSourceTable.builder()
                        .tablePath(tablePath)
                        .useSelectCount(false)
                        .skipAnalyze(false)
                        .build();

        Object maxValue = dialect.queryNextChunkMax(connection, table, "ID", 10, 0);
        Assertions.assertNotNull(maxValue);
        Assertions.assertEquals(1, ((Number) maxValue).intValue());
    }

    @Test
    public void testGetRowConverter() {
        Assertions.assertNotNull(dialect.getRowConverter());
    }

    @Test
    public void testGetTypeConverter() {
        Assertions.assertNotNull(dialect.getTypeConverter());
    }

    @Test
    public void testGetJdbcDialectTypeMapper() {
        Assertions.assertNotNull(dialect.getJdbcDialectTypeMapper());
    }

    @Test
    public void testCreatPreparedStatement() throws Exception {
        try (PreparedStatement preparedStatement =
                dialect.creatPreparedStatement(
                        connection,
                        "SELECT * FROM "
                                + quoteIdentifier(SCHEMA)
                                + "."
                                + quoteIdentifier(TEST_TABLE),
                        128)) {
            Assertions.assertNotNull(preparedStatement);
        }
    }

    @Test
    public void testInsertIntoStatement() throws Exception {
        String insertSql =
                dialect.getInsertIntoStatement(
                        SCHEMA, TEST_TABLE, new String[] {"ID", "VARCHAR_COL"});
        // Oracle dialect ignores database parameter, uses only table name
        Assertions.assertEquals(
                "INSERT INTO \"DIALECT_TEST_TABLE\" (\"ID\", \"VARCHAR_COL\") VALUES (:ID, :VARCHAR_COL)",
                insertSql);

        // Execute the insert and verify
        executeSql(insertSql, params("ID", 100, "VARCHAR_COL", "test_insert"));
        Assertions.assertEquals(2, countRows()); // 1 from setup + 1 new
    }

    @Test
    public void testUpdateStatement() throws Exception {
        String updateSql =
                dialect.getUpdateStatement(
                        SCHEMA,
                        TEST_TABLE,
                        new String[] {"VARCHAR_COL", "NUMBER_COL"},
                        new String[] {"ID"},
                        false);
        Assertions.assertTrue(updateSql.contains("UPDATE"));
        Assertions.assertTrue(updateSql.contains("\"VARCHAR_COL\" = :VARCHAR_COL"));
        Assertions.assertTrue(updateSql.contains("\"NUMBER_COL\" = :NUMBER_COL"));
        Assertions.assertTrue(updateSql.contains("WHERE \"ID\" = :ID"));

        // Execute the update and verify
        executeSql(
                updateSql, params("ID", 1, "VARCHAR_COL", "updated_value", "NUMBER_COL", 200.99));

        try (Statement stmt = connection.createStatement();
                ResultSet rs =
                        stmt.executeQuery(
                                String.format(
                                        "SELECT VARCHAR_COL, NUMBER_COL FROM %s.%s WHERE ID = 1",
                                        quoteIdentifier(SCHEMA), quoteIdentifier(TEST_TABLE)))) {
            Assertions.assertTrue(rs.next());
            Assertions.assertEquals("updated_value", rs.getString("VARCHAR_COL"));
            Assertions.assertEquals(200.99, rs.getDouble("NUMBER_COL"), 0.01);
        }
    }

    @Test
    public void testDeleteStatement() throws Exception {
        // Insert additional row
        String insertSql =
                dialect.getInsertIntoStatement(
                        SCHEMA, TEST_TABLE, new String[] {"ID", "VARCHAR_COL"});
        executeSql(insertSql, params("ID", 99, "VARCHAR_COL", "to_delete"));
        Assertions.assertEquals(2, countRows());

        // Test delete
        String deleteSql = dialect.getDeleteStatement(SCHEMA, TEST_TABLE, new String[] {"ID"});
        // Oracle dialect ignores database parameter, uses only table name
        Assertions.assertEquals("DELETE FROM \"DIALECT_TEST_TABLE\" WHERE \"ID\" = :ID", deleteSql);

        executeSql(deleteSql, params("ID", 99));
        Assertions.assertEquals(1, countRows());
    }

    @Test
    public void testRowExistsStatement() throws Exception {
        String existsSql = dialect.getRowExistsStatement(SCHEMA, TEST_TABLE, new String[] {"ID"});
        // Oracle dialect ignores database parameter, uses only table name
        Assertions.assertEquals(
                "SELECT 1 FROM \"DIALECT_TEST_TABLE\" WHERE \"ID\" = :ID", existsSql);

        // Test with existing row
        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(executableSql(existsSql, params("ID", 1)))) {
            Assertions.assertTrue(rs.next());
        }

        // Test with non-existing row
        try (Statement stmt = connection.createStatement();
                ResultSet rs = stmt.executeQuery(executableSql(existsSql, params("ID", 999)))) {
            Assertions.assertFalse(rs.next());
        }
    }

    @Test
    public void testUpsertStatement() throws Exception {
        String upsertSql =
                dialect.getUpsertStatement(
                                SCHEMA,
                                TEST_TABLE,
                                new String[] {"ID", "VARCHAR_COL", "NUMBER_COL"},
                                new String[] {"ID"})
                        .orElseThrow(() -> new AssertionError("Upsert statement not supported"));

        Assertions.assertTrue(upsertSql.contains("MERGE INTO"));
        Assertions.assertTrue(upsertSql.contains("USING"));
        Assertions.assertTrue(upsertSql.contains("ON"));
        Assertions.assertTrue(upsertSql.contains("WHEN MATCHED"));
        Assertions.assertTrue(upsertSql.contains("WHEN NOT MATCHED"));

        // Test insert via upsert (ID=2 doesn't exist)
        executeSql(upsertSql, params("ID", 2, "VARCHAR_COL", "upsert_insert", "NUMBER_COL", 50.0));
        Assertions.assertEquals(2, countRows());

        // Test update via upsert (ID=2 now exists)
        executeSql(upsertSql, params("ID", 2, "VARCHAR_COL", "upsert_update", "NUMBER_COL", 75.5));
        Assertions.assertEquals(2, countRows());

        // Verify the update worked
        try (Statement stmt = connection.createStatement();
                ResultSet rs =
                        stmt.executeQuery(
                                String.format(
                                        "SELECT VARCHAR_COL, NUMBER_COL FROM %s.%s WHERE ID = 2",
                                        quoteIdentifier(SCHEMA), quoteIdentifier(TEST_TABLE)))) {
            Assertions.assertTrue(rs.next());
            Assertions.assertEquals("upsert_update", rs.getString("VARCHAR_COL"));
            Assertions.assertEquals(75.5, rs.getDouble("NUMBER_COL"), 0.01);
        }
    }

    // Helper methods for DML testing

    private void executeSql(String sqlTemplate, Map<String, Object> params) throws Exception {
        try (Statement statement = connection.createStatement()) {
            // Oracle dialect doesn't include schema in SQL templates, so we need to add it
            String sqlWithSchema =
                    sqlTemplate.replace(
                            quoteIdentifier(TEST_TABLE),
                            quoteIdentifier(SCHEMA) + "." + quoteIdentifier(TEST_TABLE));
            statement.execute(executableSql(sqlWithSchema, params));
        }
    }

    private String executableSql(String sqlTemplate, Map<String, Object> params) {
        String executable = sqlTemplate;
        for (Map.Entry<String, Object> entry : params.entrySet()) {
            executable = executable.replace(":" + entry.getKey(), formatLiteral(entry.getValue()));
        }
        // Add schema qualification for Oracle
        if (!executable.contains(quoteIdentifier(SCHEMA) + ".")) {
            executable =
                    executable.replace(
                            quoteIdentifier(TEST_TABLE),
                            quoteIdentifier(SCHEMA) + "." + quoteIdentifier(TEST_TABLE));
        }
        return executable;
    }

    private Map<String, Object> params(Object... keyValues) {
        Map<String, Object> params = new HashMap<>();
        for (int i = 0; i < keyValues.length; i += 2) {
            params.put(keyValues[i].toString(), keyValues[i + 1]);
        }
        return params;
    }

    private String formatLiteral(Object value) {
        if (value == null) {
            return "NULL";
        }
        if (value instanceof String) {
            return "'" + value.toString().replace("'", "''") + "'";
        }
        return value.toString();
    }

    private int countRows() throws SQLException {
        try (Statement statement = connection.createStatement();
                ResultSet rs =
                        statement.executeQuery(
                                String.format(
                                        "SELECT COUNT(*) FROM %s.%s",
                                        quoteIdentifier(SCHEMA), quoteIdentifier(TEST_TABLE)))) {
            rs.next();
            return rs.getInt(1);
        }
    }

    @Test
    public void testFieldNamedPreparedStatementInsert() throws Exception {
        String insertSql =
                String.format(
                        "INSERT INTO %s.%s (ID, VARCHAR_COL, NUMBER_COL) VALUES (:ID, :VARCHAR_COL, :NUMBER_COL)",
                        quoteIdentifier(SCHEMA), quoteIdentifier(TEST_TABLE));
        String[] fieldNames = new String[] {"ID", "VARCHAR_COL", "NUMBER_COL"};

        try (FieldNamedPreparedStatement pstmt =
                FieldNamedPreparedStatement.prepareStatement(connection, insertSql, fieldNames)) {
            Assertions.assertNotNull(pstmt);

            // Set parameters
            pstmt.setInt(1, 200); // ID
            pstmt.setString(2, "field_named_test"); // VARCHAR_COL
            pstmt.setDouble(3, 99.99); // NUMBER_COL

            int rowsInserted = pstmt.executeUpdate();
            Assertions.assertEquals(1, rowsInserted);

            // Verify insertion
            Assertions.assertEquals(2, countRows());
        }
    }

    @Test
    public void testFieldNamedPreparedStatementUpdate() throws Exception {
        String updateSql =
                String.format(
                        "UPDATE %s.%s SET VARCHAR_COL = :VARCHAR_COL, NUMBER_COL = :NUMBER_COL WHERE ID = :ID",
                        quoteIdentifier(SCHEMA), quoteIdentifier(TEST_TABLE));
        String[] fieldNames = new String[] {"VARCHAR_COL", "NUMBER_COL", "ID"};

        try (FieldNamedPreparedStatement pstmt =
                FieldNamedPreparedStatement.prepareStatement(connection, updateSql, fieldNames)) {
            pstmt.setString(1, "updated_via_named"); // VARCHAR_COL
            pstmt.setDouble(2, 777.77); // NUMBER_COL
            pstmt.setInt(3, 1); // ID

            int rowsUpdated = pstmt.executeUpdate();
            Assertions.assertEquals(1, rowsUpdated);

            // Verify update
            try (Statement stmt = connection.createStatement();
                    ResultSet rs =
                            stmt.executeQuery(
                                    String.format(
                                            "SELECT VARCHAR_COL, NUMBER_COL FROM %s.%s WHERE ID = 1",
                                            quoteIdentifier(SCHEMA),
                                            quoteIdentifier(TEST_TABLE)))) {
                Assertions.assertTrue(rs.next());
                Assertions.assertEquals("updated_via_named", rs.getString("VARCHAR_COL"));
                Assertions.assertEquals(777.77, rs.getDouble("NUMBER_COL"), 0.01);
            }
        }
    }

    @Test
    public void testFieldNamedPreparedStatementWithDuplicateParameters() throws Exception {
        // Insert test data
        String insertSql =
                String.format(
                        "INSERT INTO %s.%s (ID, VARCHAR_COL) VALUES (300, 'duplicate_test')",
                        quoteIdentifier(SCHEMA), quoteIdentifier(TEST_TABLE));
        try (Statement stmt = connection.createStatement()) {
            stmt.execute(insertSql);
        }

        // Test query with duplicate named parameter
        String querySql =
                String.format(
                        "SELECT * FROM %s.%s WHERE ID = :ID OR VARCHAR_COL = :VARCHAR_COL OR ID > :ID",
                        quoteIdentifier(SCHEMA), quoteIdentifier(TEST_TABLE));
        String[] fieldNames = new String[] {"ID", "VARCHAR_COL"};

        try (FieldNamedPreparedStatement pstmt =
                FieldNamedPreparedStatement.prepareStatement(connection, querySql, fieldNames)) {
            pstmt.setInt(1, 300); // ID (used twice in query)
            pstmt.setString(2, "duplicate_test"); // VARCHAR_COL

            try (ResultSet rs = pstmt.executeQuery()) {
                Assertions.assertTrue(rs.next());
                Assertions.assertEquals(300, rs.getInt("ID"));
            }
        }
    }

    @Test
    public void testFieldNamedPreparedStatementParseNamedStatement() {
        Map<String, List<Integer>> paramMap = new HashMap<>();
        String sql = "INSERT INTO table (a, b, c) VALUES (:a, :b, :c)";
        String parsed = FieldNamedPreparedStatement.parseNamedStatement(sql, paramMap);

        Assertions.assertEquals("INSERT INTO table (a, b, c) VALUES (?, ?, ?)", parsed);
        Assertions.assertEquals(3, paramMap.size());
        Assertions.assertTrue(paramMap.containsKey("a"));
        Assertions.assertTrue(paramMap.containsKey("b"));
        Assertions.assertTrue(paramMap.containsKey("c"));
        Assertions.assertEquals(1, paramMap.get("a").get(0));
        Assertions.assertEquals(2, paramMap.get("b").get(0));
        Assertions.assertEquals(3, paramMap.get("c").get(0));
    }

    @Test
    public void testFieldNamedPreparedStatementParseWithDuplicates() {
        Map<String, List<Integer>> paramMap = new HashMap<>();
        String sql = "SELECT * FROM table WHERE id = :id OR id > :id";
        String parsed = FieldNamedPreparedStatement.parseNamedStatement(sql, paramMap);

        Assertions.assertEquals("SELECT * FROM table WHERE id = ? OR id > ?", parsed);
        Assertions.assertEquals(1, paramMap.size());
        Assertions.assertTrue(paramMap.containsKey("id"));
        Assertions.assertEquals(2, paramMap.get("id").size()); // "id" appears twice
        Assertions.assertEquals(1, paramMap.get("id").get(0));
        Assertions.assertEquals(2, paramMap.get("id").get(1));
    }
}
