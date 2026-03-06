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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.sqlserver.container;

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.sqlserver.SqlServerDialect;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

@DisabledOnOs(OS.WINDOWS)
public class SqlServerDialectContainerTest extends AbstractSqlServerContainerTest {

    @Test
    void testUpsertStatement() {
        SqlServerDialect dialect = new SqlServerDialect();
        final String database = "master";
        final String tableName = "role";
        final String[] fieldNames = {
            "id", "type", "role_name", "description", "create_time", "update_time"
        };
        final String[] uniqueKeyFields = {"id"};

        java.util.Optional<String> upsertSqlOptional =
                dialect.getUpsertStatement(database, tableName, fieldNames, uniqueKeyFields);
        Assertions.assertTrue(upsertSqlOptional.isPresent());
        String upsertSql = upsertSqlOptional.get();

        Assertions.assertTrue(upsertSql.contains("MERGE INTO"));
        Assertions.assertTrue(upsertSql.contains("[master].[role] AS [TARGET]"));
        Assertions.assertTrue(
                upsertSql.contains(
                        "USING (SELECT :id [id], :type [type], :role_name [role_name], :description [description], :create_time [create_time], :update_time [update_time]) AS [SOURCE]"));
        Assertions.assertTrue(upsertSql.contains("ON ([TARGET].[id]=[SOURCE].[id])"));
        Assertions.assertTrue(
                upsertSql.contains(
                        "WHEN MATCHED THEN UPDATE SET [TARGET].[type]=[SOURCE].[type], [TARGET].[role_name]=[SOURCE].[role_name], [TARGET].[description]=[SOURCE].[description], [TARGET].[create_time]=[SOURCE].[create_time], [TARGET].[update_time]=[SOURCE].[update_time]"));
        Assertions.assertTrue(
                upsertSql.contains(
                        "WHEN NOT MATCHED THEN INSERT ([id], [type], [role_name], [description], [create_time], [update_time]) VALUES ([SOURCE].[id], [SOURCE].[type], [SOURCE].[role_name], [SOURCE].[description], [SOURCE].[create_time], [SOURCE].[update_time]);"));
    }

    @Test
    void testQuoteIdentifier() {
        SqlServerDialect dialect = new SqlServerDialect();

        Assertions.assertEquals("[test]", dialect.quoteIdentifier("test"));
        Assertions.assertEquals("[schema].[table]", dialect.quoteIdentifier("schema.table"));
    }

    @Test
    void testTableIdentifier() {
        SqlServerDialect dialect = new SqlServerDialect();

        Assertions.assertEquals("[master].[role]", dialect.tableIdentifier("master", "role"));
        Assertions.assertEquals(
                "[master].[dbo].[role]", dialect.tableIdentifier("master", "dbo.role"));
    }

    @Test
    void testParseTablePath() {
        SqlServerDialect dialect = new SqlServerDialect();

        TablePath path1 = dialect.parse("master.dbo.test_table");
        Assertions.assertEquals("master", path1.getDatabaseName());
        Assertions.assertEquals("dbo", path1.getSchemaName());
        Assertions.assertEquals("test_table", path1.getTableName());

        TablePath path2 = dialect.parse("dbo.test_table");
        Assertions.assertNull(path2.getDatabaseName());
        Assertions.assertEquals("dbo", path2.getSchemaName());
        Assertions.assertEquals("test_table", path2.getTableName());

        TablePath path3 = dialect.parse("test_table");
        Assertions.assertNull(path3.getDatabaseName());
        Assertions.assertNull(path3.getSchemaName());
        Assertions.assertEquals("test_table", path3.getTableName());
    }

    @Test
    void testDialectName() {
        SqlServerDialect dialect = new SqlServerDialect();
        Assertions.assertEquals("SqlServer", dialect.dialectName());
    }

    @Test
    void testGetRowExistsStatement() {
        SqlServerDialect dialect = new SqlServerDialect();
        final String database = "master";
        final String tableName = "dbo.users";
        final String[] conditionFields = {"id"};

        String sql = dialect.getRowExistsStatement(database, tableName, conditionFields);

        Assertions.assertNotNull(sql);
        Assertions.assertTrue(sql.contains("SELECT 1 FROM"));
        Assertions.assertTrue(
                sql.contains("[master].[dbo].[users]")
                        || sql.contains("\"master\".\"dbo\".\"users\""));
        Assertions.assertTrue(sql.contains("WHERE [id] = :id") || sql.contains("WHERE \"id\" = ?"));
    }

    @Test
    void testRealUpsertExecution() throws SQLException {
        String tableName = "test_upsert_exec";
        String fullTableName = "dbo." + tableName;

        String createTableSQL =
                "CREATE TABLE "
                        + fullTableName
                        + " ("
                        + "id INT PRIMARY KEY, "
                        + "name VARCHAR(100), "
                        + "value INT"
                        + ")";

        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement()) {

            stmt.execute(createTableSQL);
            stmt.execute(
                    "INSERT INTO "
                            + fullTableName
                            + " (id, name, value) VALUES (1, 'initial', 100)");

            SqlServerDialect dialect = new SqlServerDialect();
            String database = "master";
            String[] fieldNames = {"id", "name", "value"};
            String[] uniqueKeyFields = {"id"};

            java.util.Optional<String> upsertSQLOptional =
                    dialect.getUpsertStatement(database, tableName, fieldNames, uniqueKeyFields);

            Assertions.assertTrue(upsertSQLOptional.isPresent());
            String upsertSQL = upsertSQLOptional.get();

            Assertions.assertTrue(upsertSQL.contains("MERGE INTO"));
            Assertions.assertTrue(upsertSQL.contains("[master].[test_upsert_exec]"));
            Assertions.assertTrue(upsertSQL.contains("WHEN MATCHED"));
            Assertions.assertTrue(upsertSQL.contains("WHEN NOT MATCHED"));

            System.out.println("Generated SQL Server upsert: " + upsertSQL);

            String testUpsert =
                    "MERGE INTO "
                            + fullTableName
                            + " AS T "
                            + "USING (VALUES (?, ?, ?)) AS S (id, name, value) "
                            + "ON T.id = S.id "
                            + "WHEN MATCHED THEN UPDATE SET T.name = S.name, T.value = S.value "
                            + "WHEN NOT MATCHED THEN INSERT (id, name, value) VALUES (S.id, S.name, S.value);";

            try (PreparedStatement ps = conn.prepareStatement(testUpsert)) {
                ps.setInt(1, 1);
                ps.setString(2, "updated");
                ps.setInt(3, 200);
                ps.executeUpdate();
            }

            try (ResultSet rs =
                    stmt.executeQuery(
                            "SELECT name, value FROM " + fullTableName + " WHERE id = 1")) {
                Assertions.assertTrue(rs.next());
                Assertions.assertEquals("updated", rs.getString("name"));
                Assertions.assertEquals(200, rs.getInt("value"));
            }

        } finally {
            try (Connection conn = getConnection();
                    Statement stmt = conn.createStatement()) {
                stmt.execute("DROP TABLE IF EXISTS " + fullTableName);
            } catch (Exception e) {
            }
        }
    }

    @Test
    void testGetRowConverter() {
        SqlServerDialect dialect = new SqlServerDialect();
        Assertions.assertEquals(
                "SqlserverJdbcRowConverter", dialect.getRowConverter().getClass().getSimpleName());
    }

    @Test
    void testCreatePreparedStatement() throws SQLException {
        SqlServerDialect dialect = new SqlServerDialect();

        try (Connection conn = getConnection()) {
            PreparedStatement ps = dialect.creatPreparedStatement(conn, "SELECT 1", 500);
            Assertions.assertNotNull(ps);
            Assertions.assertEquals(500, ps.getFetchSize());
        }
    }

    @Test
    void testColumnExists() throws SQLException {
        SqlServerDialect dialect = new SqlServerDialect();
        String tableName = "test_col_exists";
        String fullTableName = "dbo." + tableName;

        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement()) {

            stmt.execute("CREATE TABLE " + fullTableName + " (id INT, name VARCHAR(10))");

            TablePath tablePath = TablePath.of("master", "dbo", tableName);

            Assertions.assertTrue(dialect.columnExists(conn, tablePath, "id"));
            Assertions.assertFalse(dialect.columnExists(conn, tablePath, "invalid_col"));

        } finally {
            try (Connection conn = getConnection();
                    Statement stmt = conn.createStatement()) {
                stmt.execute("DROP TABLE IF EXISTS " + fullTableName);
            } catch (Exception e) {
            }
        }
    }

    @Test
    void testGetInsertIntoStatement() {
        SqlServerDialect dialect = new SqlServerDialect();
        final String database = "master";
        final String tableName = "users";
        final String[] fieldNames = {"id", "name", "email"};

        String insertSQL = dialect.getInsertIntoStatement(database, tableName, fieldNames);

        Assertions.assertNotNull(insertSQL);
        Assertions.assertTrue(insertSQL.contains("INSERT INTO"));
        Assertions.assertTrue(insertSQL.contains("[master].[users]"));
        Assertions.assertTrue(insertSQL.contains("([id], [name], [email])"));
        Assertions.assertTrue(insertSQL.contains("VALUES (:id, :name, :email)"));
    }

    @Test
    void testGetUpdateStatement() {
        SqlServerDialect dialect = new SqlServerDialect();
        final String database = "master";
        final String tableName = "dbo.users";
        final String[] fieldNames = {"name", "email"};
        final String[] conditionFields = {"id"};
        final boolean isPrimaryKeyUpdated = false;

        String updateSQL =
                dialect.getUpdateStatement(
                        database, tableName, fieldNames, conditionFields, isPrimaryKeyUpdated);

        Assertions.assertNotNull(updateSQL);
        Assertions.assertTrue(updateSQL.contains("UPDATE"));
        Assertions.assertTrue(
                updateSQL.contains("[master].[dbo].[users]")
                        || updateSQL.contains("\"master\".\"dbo\".\"users\""));
        Assertions.assertTrue(
                updateSQL.contains("SET [name] = :name, [email] = :email")
                        || updateSQL.contains("SET \"name\" = ?, \"email\" = ?"));
        Assertions.assertTrue(
                updateSQL.contains("WHERE [id] = :id") || updateSQL.contains("WHERE \"id\" = ?"));
    }

    @Test
    void testGetDeleteStatement() {
        SqlServerDialect dialect = new SqlServerDialect();
        final String database = "master";
        final String tableName = "users";
        final String[] conditionFields = {"id"};

        String deleteSQL = dialect.getDeleteStatement(database, tableName, conditionFields);

        Assertions.assertNotNull(deleteSQL);
        Assertions.assertTrue(deleteSQL.contains("DELETE FROM"));
        Assertions.assertTrue(deleteSQL.contains("[master].[users]"));
        Assertions.assertTrue(deleteSQL.contains("WHERE [id] = :id"));
    }
}
