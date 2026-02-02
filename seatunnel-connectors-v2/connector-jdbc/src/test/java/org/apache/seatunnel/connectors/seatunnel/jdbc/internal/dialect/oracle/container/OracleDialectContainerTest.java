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
import org.apache.seatunnel.connectors.seatunnel.jdbc.source.JdbcSourceTable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Unit tests for OracleDialect using Testcontainers. Tests dialect-specific functionality like
 * quoting, SQL generation, and data sampling.
 */
public class OracleDialectContainerTest extends AbstractOracleContainerTest {

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
        PreparedStatement preparedStatement =
                dialect.creatPreparedStatement(
                        connection,
                        "SELECT * FROM "
                                + quoteIdentifier(SCHEMA)
                                + "."
                                + quoteIdentifier(TEST_TABLE),
                        128);

        Assertions.assertNotNull(preparedStatement);
        preparedStatement.close();
    }
}
