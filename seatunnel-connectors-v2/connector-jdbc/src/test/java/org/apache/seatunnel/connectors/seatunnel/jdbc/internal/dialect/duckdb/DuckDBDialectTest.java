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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.duckdb;

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.JdbcSourceTable;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class DuckDBDialectTest {

    private static final String TABLE_NAME = "dialect_test";
    private static DuckDBDialect dialect;
    private static Connection connection;
    private static TablePath tablePath;
    private static JdbcSourceTable sourceTable;
    private static String insertTemplate;
    private static final String DB_FILE = "DuckDBDialectTest.db";

    @BeforeAll
    static void setUp() throws Exception {
        dialect = new DuckDBDialect();
        File dbFile = new File(DB_FILE);
        if (dbFile.exists()) {
            dbFile.delete();
        }
        connection = DriverManager.getConnection("jdbc:duckdb:" + dbFile.getAbsolutePath());
        tablePath = TablePath.of("main", "main", TABLE_NAME);
        sourceTable = JdbcSourceTable.builder().tablePath(tablePath).build();
        insertTemplate =
                dialect.getInsertIntoStatement("main", TABLE_NAME, new String[] {"id", "name"});
        try (Statement statement = connection.createStatement()) {
            statement.execute(
                    String.format("CREATE TABLE \"%s\"(id INTEGER, name VARCHAR)", TABLE_NAME));
        }
    }

    @AfterEach
    void cleanTable() throws SQLException {
        try (Statement statement = connection.createStatement()) {
            statement.execute(String.format("DELETE FROM \"%s\"", TABLE_NAME));
        }
    }

    @AfterAll
    static void tearDown() throws Exception {
        if (connection != null) {
            connection.close();
        }
        File dbFile = new File(DB_FILE);
        if (dbFile.exists()) {
            dbFile.delete();
        }
    }

    @Test
    void testInsertStatementExecution() throws Exception {
        Assertions.assertEquals(
                "INSERT INTO \"main\".\"dialect_test\" (\"id\", \"name\") VALUES (:id, :name)",
                insertTemplate);
        executeSql(insertTemplate, params("id", 1, "name", "duck-1"));
        executeSql(insertTemplate, params("id", 2, "name", "duck-2"));
        Assertions.assertEquals(2, countRows());
    }

    @Test
    void testHashModForFieldExecution() throws Exception {
        insertRows(1, 2, 3, 4);
        String hashExpression = dialect.hashModForField("id", 3);
        String sql =
                String.format(
                        "SELECT %s AS bucket FROM %s ORDER BY id",
                        hashExpression, dialect.tableIdentifier(tablePath));
        try (Statement statement = connection.createStatement();
                ResultSet rs = statement.executeQuery(sql)) {
            int rowCount = 0;
            while (rs.next()) {
                rowCount++;
                int bucket = rs.getInt("bucket");
                Assertions.assertTrue(bucket >= 0 && bucket < 3);
            }
            Assertions.assertEquals(4, rowCount);
        }
    }

    @Test
    void testDeleteStatementExecution() throws Exception {
        insertRows(1, 2);
        String delete = dialect.getDeleteStatement("main", TABLE_NAME, new String[] {"id", "name"});
        Assertions.assertEquals(
                "DELETE FROM \"main\".\"dialect_test\" WHERE \"id\" = :id AND \"name\" = :name",
                delete);
        executeSql(delete, params("id", 1, "name", "name-1"));
        Assertions.assertEquals(1, countRows());
    }

    @Test
    void testRowExistsStatementExecution() throws Exception {
        insertRows(5);
        String exists =
                dialect.getRowExistsStatement("main", TABLE_NAME, new String[] {"id", "name"});
        Assertions.assertEquals(
                "SELECT 1 FROM \"main\".\"dialect_test\" WHERE \"id\" = :id AND \"name\" = :name",
                exists);
        try (Statement statement = connection.createStatement();
                ResultSet rs =
                        statement.executeQuery(
                                executableSql(exists, params("id", 5, "name", "name-5")))) {
            Assertions.assertTrue(rs.next());
        }
        try (Statement statement = connection.createStatement();
                ResultSet rs =
                        statement.executeQuery(
                                executableSql(exists, params("id", 9, "name", "name-9")))) {
            Assertions.assertFalse(rs.next());
        }
    }

    @Test
    void testApproximateRowCntStatement() throws Exception {
        insertRows(1, 2, 3, 4, 5);
        Long count = dialect.approximateRowCntStatement(connection, sourceTable);
        Assertions.assertEquals(5L, count);
    }

    @Test
    void testSampleDataFromColumn() throws Exception {
        insertRows(IntStream.rangeClosed(1, 8).boxed().collect(Collectors.toList()).toArray());
        Object[] samples = dialect.sampleDataFromColumn(connection, sourceTable, "id", 2, 100);
        int[] sampleValues =
                Arrays.stream(samples).mapToInt(value -> ((Number) value).intValue()).toArray();
        Assertions.assertArrayEquals(new int[] {2, 4, 6, 8}, sampleValues);
    }

    @Test
    void testQueryNextChunkMax() throws Exception {
        insertRows(IntStream.rangeClosed(1, 10).boxed().collect(Collectors.toList()).toArray());
        Object firstChunkMax = dialect.queryNextChunkMax(connection, sourceTable, "id", 3, 1);
        Assertions.assertEquals(3, ((Number) firstChunkMax).intValue());
        Object secondChunkMax = dialect.queryNextChunkMax(connection, sourceTable, "id", 3, 3);
        Assertions.assertEquals(5, ((Number) secondChunkMax).intValue());
    }

    private void insertRows(Object... ids) throws Exception {
        for (Object id : ids) {
            executeSql(insertTemplate, params("id", id, "name", "name-" + id));
        }
    }

    private void executeSql(String sqlTemplate, Map<String, Object> params) throws Exception {
        try (Statement statement = connection.createStatement()) {
            statement.execute(executableSql(sqlTemplate, params));
        }
    }

    private String executableSql(String sqlTemplate, Map<String, Object> params) {
        String executable = sqlTemplate;
        for (Map.Entry<String, Object> entry : params.entrySet()) {
            executable = executable.replace(":" + entry.getKey(), formatLiteral(entry.getValue()));
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
                                String.format("SELECT COUNT(*) FROM \"%s\"", TABLE_NAME))) {
            rs.next();
            return rs.getInt(1);
        }
    }
}
