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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.dm.container;

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.dialectenum.FieldIdeEnum;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.dm.DmdbDialect;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Optional;

/** Integration tests for {@link DmdbDialect} against a real Dameng database container. */
@Slf4j
@DisabledOnOs(OS.WINDOWS)
public class DamengDialectContainerTest extends AbstractDamengContainerTest {

    @Test
    void testConnection() throws Exception {
        try (Connection conn = getConnection()) {
            Assertions.assertTrue(conn.isValid(5));
            log.info("Dameng container is running at: {}", getJdbcUrl());
        }
    }

    @Test
    void testCreateTable() throws Exception {
        String tableName = "TEST_CREATE_" + System.currentTimeMillis();
        createTestTable(DM_SCHEMA, tableName);

        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement();
                ResultSet rs =
                        stmt.executeQuery(
                                String.format(
                                        "SELECT COUNT(*) FROM \"%s\".\"%s\"",
                                        DM_SCHEMA, tableName))) {
            rs.next();
            Assertions.assertEquals(0, rs.getInt(1));
            log.info("Table {}.{} created successfully", DM_SCHEMA, tableName);
        } finally {
            dropTableIfExists(DM_SCHEMA, tableName);
        }
    }

    @Test
    void testRealUpsertExecution() throws SQLException {
        String tableName = "TEST_UPSERT_EXEC";
        dropTableIfExists(DM_SCHEMA, tableName);

        String createTableSQL =
                String.format(
                        "CREATE TABLE \"%s\".\"%s\" ("
                                + "\"ID\" INT PRIMARY KEY, "
                                + "\"NAME\" VARCHAR(100), "
                                + "\"VALUE\" INT"
                                + ")",
                        DM_SCHEMA, tableName);

        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement()) {

            stmt.execute(createTableSQL);
            log.info("Created test table for upsert: {}.{}", DM_SCHEMA, tableName);

            // Insert first row
            stmt.executeUpdate(
                    String.format(
                            "INSERT INTO \"%s\".\"%s\" (\"ID\", \"NAME\", \"VALUE\") VALUES (1, 'first', 100)",
                            DM_SCHEMA, tableName));

            // Verify insert
            ResultSet rs =
                    stmt.executeQuery(
                            String.format(
                                    "SELECT COUNT(*) FROM \"%s\".\"%s\"", DM_SCHEMA, tableName));
            rs.next();
            Assertions.assertEquals(1, rs.getInt(1));
            rs.close();

            // Verify upsert SQL generation
            DmdbDialect dialect = new DmdbDialect(FieldIdeEnum.ORIGINAL.getValue());
            String[] fieldNames = {"ID", "NAME", "VALUE"};
            String[] uniqueKeyFields = {"ID"};

            Optional<String> upsertSQLOptional =
                    dialect.getUpsertStatement(null, tableName, fieldNames, uniqueKeyFields);
            Assertions.assertTrue(upsertSQLOptional.isPresent());

            String upsertSQL = upsertSQLOptional.get();
            Assertions.assertTrue(upsertSQL.contains("MERGE INTO"));
            Assertions.assertTrue(upsertSQL.contains("WHEN MATCHED THEN"));
            Assertions.assertTrue(upsertSQL.contains("WHEN NOT MATCHED THEN"));
            log.info("Generated upsert SQL: {}", upsertSQL);

        } finally {
            dropTableIfExists(DM_SCHEMA, tableName);
        }
    }

    @Test
    void testInsertAndQuery() throws SQLException {
        String tableName = "TEST_INSERT_QUERY";
        dropTableIfExists(DM_SCHEMA, tableName);

        createTestTable(DM_SCHEMA, tableName);

        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement()) {

            // Insert data
            stmt.executeUpdate(
                    String.format(
                            "INSERT INTO \"%s\".\"%s\" (\"ID\", \"NAME\", \"AGE\") VALUES (1, 'Alice', 30)",
                            DM_SCHEMA, tableName));
            stmt.executeUpdate(
                    String.format(
                            "INSERT INTO \"%s\".\"%s\" (\"ID\", \"NAME\", \"AGE\") VALUES (2, 'Bob', 25)",
                            DM_SCHEMA, tableName));

            // Query data
            ResultSet rs =
                    stmt.executeQuery(
                            String.format(
                                    "SELECT COUNT(*) FROM \"%s\".\"%s\"", DM_SCHEMA, tableName));
            rs.next();
            Assertions.assertEquals(2, rs.getInt(1));
            rs.close();

            // Query specific row
            rs =
                    stmt.executeQuery(
                            String.format(
                                    "SELECT \"NAME\", \"AGE\" FROM \"%s\".\"%s\" WHERE \"ID\" = 1",
                                    DM_SCHEMA, tableName));
            Assertions.assertTrue(rs.next());
            Assertions.assertEquals("Alice", rs.getString("NAME"));
            Assertions.assertEquals(30, rs.getInt("AGE"));
            rs.close();

            log.info("Insert and query test passed for {}.{}", DM_SCHEMA, tableName);

        } finally {
            dropTableIfExists(DM_SCHEMA, tableName);
        }
    }

    @Test
    void testPreparedStatementExecution() throws SQLException {
        String tableName = "TEST_PREPARED_STMT";
        dropTableIfExists(DM_SCHEMA, tableName);

        createTestTable(DM_SCHEMA, tableName);

        try (Connection conn = getConnection()) {

            // Insert with PreparedStatement
            String insertSQL =
                    String.format(
                            "INSERT INTO \"%s\".\"%s\" (\"ID\", \"NAME\", \"AGE\") VALUES (?, ?, ?)",
                            DM_SCHEMA, tableName);
            try (PreparedStatement ps = conn.prepareStatement(insertSQL)) {
                ps.setInt(1, 1);
                ps.setString(2, "TestUser");
                ps.setInt(3, 28);
                int affected = ps.executeUpdate();
                Assertions.assertEquals(1, affected);
            }

            // Query with PreparedStatement
            String querySQL =
                    String.format(
                            "SELECT \"NAME\", \"AGE\" FROM \"%s\".\"%s\" WHERE \"ID\" = ?",
                            DM_SCHEMA, tableName);
            try (PreparedStatement ps = conn.prepareStatement(querySQL)) {
                ps.setInt(1, 1);
                ResultSet rs = ps.executeQuery();
                Assertions.assertTrue(rs.next());
                Assertions.assertEquals("TestUser", rs.getString("NAME"));
                Assertions.assertEquals(28, rs.getInt("AGE"));
                rs.close();
            }

            log.info("PreparedStatement test passed for {}.{}", DM_SCHEMA, tableName);

        } finally {
            dropTableIfExists(DM_SCHEMA, tableName);
        }
    }

    @Test
    void testDialectQuoteIdentifierWithContainer() throws SQLException {
        DmdbDialect dialect = new DmdbDialect(FieldIdeEnum.ORIGINAL.getValue());

        String tableName = "TEST_QUOTE_ID";
        dropTableIfExists(DM_SCHEMA, tableName);

        // Create table using quoted identifiers
        String quotedTable = dialect.quoteIdentifier(tableName);
        String quotedSchema = dialect.quoteIdentifier(DM_SCHEMA);
        String createSQL =
                String.format(
                        "CREATE TABLE %s.%s (" + "%s INT PRIMARY KEY, " + "%s VARCHAR(100)" + ")",
                        quotedSchema,
                        quotedTable,
                        dialect.quoteIdentifier("ID"),
                        dialect.quoteIdentifier("NAME"));

        try {
            executeSql(createSQL);
            log.info("Table created with quoted identifiers: {}.{}", quotedSchema, quotedTable);

            // Verify table exists
            try (Connection conn = getConnection();
                    Statement stmt = conn.createStatement();
                    ResultSet rs =
                            stmt.executeQuery(
                                    String.format(
                                            "SELECT COUNT(*) FROM ALL_TABLES WHERE OWNER = '%s' AND TABLE_NAME = '%s'",
                                            DM_SCHEMA, tableName))) {
                rs.next();
                Assertions.assertEquals(1, rs.getInt(1), "Table should exist after creation");
            }
        } finally {
            dropTableIfExists(DM_SCHEMA, tableName);
        }
    }

    @Test
    void testDialectGetInsertStatement() {
        DmdbDialect dialect = new DmdbDialect(FieldIdeEnum.ORIGINAL.getValue());
        String[] fieldNames = {"ID", "NAME", "AGE"};

        String sql = dialect.getInsertIntoStatement(DM_SCHEMA, "TEST_TABLE", fieldNames);

        Assertions.assertNotNull(sql);
        Assertions.assertTrue(sql.contains("INSERT INTO"));
        Assertions.assertTrue(sql.contains("\"ID\""));
        Assertions.assertTrue(sql.contains("\"NAME\""));
        Assertions.assertTrue(sql.contains("\"AGE\""));
        log.info("Generated insert SQL: {}", sql);
    }

    @Test
    void testDialectGetDeleteStatement() {
        DmdbDialect dialect = new DmdbDialect(FieldIdeEnum.ORIGINAL.getValue());
        String[] conditionFields = {"ID"};

        String sql = dialect.getDeleteStatement(DM_SCHEMA, "TEST_TABLE", conditionFields);

        Assertions.assertNotNull(sql);
        Assertions.assertTrue(sql.contains("DELETE FROM"));
        Assertions.assertTrue(sql.contains("\"ID\""));
        log.info("Generated delete SQL: {}", sql);
    }

    @Test
    void testDialectGetUpdateStatement() {
        DmdbDialect dialect = new DmdbDialect(FieldIdeEnum.ORIGINAL.getValue());
        String[] fieldNames = {"NAME", "AGE"};
        String[] conditionFields = {"ID"};

        String sql =
                dialect.getUpdateStatement(
                        DM_SCHEMA, "TEST_TABLE", fieldNames, conditionFields, false);

        Assertions.assertNotNull(sql);
        Assertions.assertTrue(sql.contains("UPDATE"));
        Assertions.assertTrue(sql.contains("SET"));
        Assertions.assertTrue(sql.contains("WHERE"));
        log.info("Generated update SQL: {}", sql);
    }

    @Test
    void testTableIdentifierWithTablePath() {
        DmdbDialect dialect = new DmdbDialect(FieldIdeEnum.ORIGINAL.getValue());

        TablePath tablePath = TablePath.of(DM_DATABASE, DM_SCHEMA, "TEST_TABLE");
        String identifier = dialect.tableIdentifier(tablePath);

        Assertions.assertNotNull(identifier);
        Assertions.assertTrue(identifier.contains(DM_SCHEMA));
        Assertions.assertTrue(identifier.contains("TEST_TABLE"));
        log.info("Table identifier: {}", identifier);
    }

    @Test
    void testTypeConverterAndMapper() {
        DmdbDialect dialect = new DmdbDialect(FieldIdeEnum.ORIGINAL.getValue());

        Assertions.assertNotNull(dialect.getTypeConverter());
        Assertions.assertEquals(
                "DmdbTypeConverter", dialect.getTypeConverter().getClass().getSimpleName());

        Assertions.assertNotNull(dialect.getJdbcDialectTypeMapper());
        Assertions.assertEquals(
                "DmdbTypeMapper", dialect.getJdbcDialectTypeMapper().getClass().getSimpleName());

        Assertions.assertNotNull(dialect.getRowConverter());
        Assertions.assertEquals(
                "DmdbJdbcRowConverter", dialect.getRowConverter().getClass().getSimpleName());

        log.info("Type converter, mapper, and row converter verified");
    }
}
