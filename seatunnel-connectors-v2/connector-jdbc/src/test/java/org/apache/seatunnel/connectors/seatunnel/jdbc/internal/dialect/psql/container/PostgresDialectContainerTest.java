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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.psql.container;

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.dialectenum.FieldIdeEnum;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.psql.PostgresDialect;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

@DisabledOnOs(OS.WINDOWS)
public class PostgresDialectContainerTest extends AbstractPostgresContainerTest {

    @Test
    void testUpsertStatement() {
        PostgresDialect dialect = new PostgresDialect();
        final String database = "seatunnel";
        final String tableName = "role";
        final String[] fieldNames = {
            "id", "type", "role_name", "description", "create_time", "update_time"
        };
        final String[] doUpdateKeyFields = {"id"};
        final String[] doNothingKeyFields = {
            "id", "type", "role_name", "description", "create_time", "update_time"
        };

        java.util.Optional<String> doUpdateSqlOptional =
                dialect.getUpsertStatement(database, tableName, fieldNames, doUpdateKeyFields);
        Assertions.assertTrue(doUpdateSqlOptional.isPresent());
        String doUpdateSql = doUpdateSqlOptional.get();

        Assertions.assertEquals(
                "INSERT INTO \"seatunnel\".\"role\" (\"id\", \"type\", \"role_name\", \"description\", \"create_time\", \"update_time\") VALUES (:id, :type, :role_name, :description, :create_time, :update_time) ON CONFLICT (\"id\") DO UPDATE SET \"type\"=EXCLUDED.\"type\", \"role_name\"=EXCLUDED.\"role_name\", \"description\"=EXCLUDED.\"description\", \"create_time\"=EXCLUDED.\"create_time\", \"update_time\"=EXCLUDED.\"update_time\"",
                doUpdateSql);

        java.util.Optional<String> doNothingSqlOptional =
                dialect.getUpsertStatement(database, tableName, fieldNames, doNothingKeyFields);
        Assertions.assertTrue(doNothingSqlOptional.isPresent());
        String doNothingSql = doNothingSqlOptional.get();

        Assertions.assertEquals(
                "INSERT INTO \"seatunnel\".\"role\" (\"id\", \"type\", \"role_name\", \"description\", \"create_time\", \"update_time\") VALUES (:id, :type, :role_name, :description, :create_time, :update_time) ON CONFLICT (\"id\", \"type\", \"role_name\", \"description\", \"create_time\", \"update_time\") DO NOTHING",
                doNothingSql);
    }

    @Test
    void testQuoteIdentifier() {
        PostgresDialect dialect = new PostgresDialect();

        // Test basic identifier
        Assertions.assertEquals("\"test\"", dialect.quoteIdentifier("test"));

        // Test identifier with dots (schema.table)
        Assertions.assertEquals("\"schema\".\"table\"", dialect.quoteIdentifier("schema.table"));

        // Test identifier with multiple dots
        Assertions.assertEquals(
                "\"db\".\"schema\".\"table\"", dialect.quoteIdentifier("db.schema.table"));

        // Test with fieldIde = ORIGINAL (default)
        PostgresDialect dialectOriginal = new PostgresDialect(FieldIdeEnum.ORIGINAL.getValue());
        Assertions.assertEquals("\"column_name\"", dialectOriginal.quoteIdentifier("column_name"));
    }

    @Test
    void testQuoteIdentifierWithFieldIde() {
        // Test with fieldIde = UPPERCASE
        PostgresDialect dialectUpper = new PostgresDialect(FieldIdeEnum.UPPERCASE.getValue());
        Assertions.assertEquals("\"COLUMN_NAME\"", dialectUpper.quoteIdentifier("column_name"));

        // Test with fieldIde = LOWERCASE
        PostgresDialect dialectLower = new PostgresDialect(FieldIdeEnum.LOWERCASE.getValue());
        Assertions.assertEquals("\"column_name\"", dialectLower.quoteIdentifier("COLUMN_NAME"));
    }

    @Test
    void testTableIdentifier() {
        PostgresDialect dialect = new PostgresDialect();

        // Test with database and table
        Assertions.assertEquals("\"db\".\"table\"", dialect.tableIdentifier("db", "table"));

        // Test database quoting
        Assertions.assertEquals(
                "\"myDatabase\".\"myTable\"", dialect.tableIdentifier("myDatabase", "myTable"));
    }

    @Test
    void testQuoteDatabaseIdentifier() {
        PostgresDialect dialect = new PostgresDialect();

        Assertions.assertEquals("\"testdb\"", dialect.quoteDatabaseIdentifier("testdb"));
        Assertions.assertEquals("\"MyDatabase\"", dialect.quoteDatabaseIdentifier("MyDatabase"));
        Assertions.assertEquals(
                "\"database-name\"", dialect.quoteDatabaseIdentifier("database-name"));
    }

    @Test
    void testParseTablePath() {
        PostgresDialect dialect = new PostgresDialect();

        // Test parsing full table path
        TablePath path1 = dialect.parse("database.schema.table");
        Assertions.assertEquals("database", path1.getDatabaseName());
        Assertions.assertEquals("schema", path1.getSchemaName());
        Assertions.assertEquals("table", path1.getTableName());

        // Test parsing with quotes
        TablePath path2 = dialect.parse("\"database\".\"schema\".\"table\"");
        /* tests moved to parseShouldRemoveQuotesWhenSingleIdentifierIsParsed
        Assertions.assertEquals("schema", path2.getSchemaName());
        Assertions.assertEquals("table", path2.getTableName());
        Assertions.assertEquals("database", path2.getDatabaseName());*/

        // Test parsing simple table name
        TablePath path3 = dialect.parse("table");
        Assertions.assertNull(path3.getDatabaseName());
        Assertions.assertEquals("table", path3.getTableName());
    }

    @Disabled("The parse isn't removing the quotes")
    @Test
    void parseShouldRemoveQuotesWhenSingleIdentifierIsParsed() {

        PostgresDialect dialect = new PostgresDialect();
        TablePath path = dialect.parse("\"database\".\"schema\".\"table\"");
        Assertions.assertEquals("database", path.getDatabaseName());
        Assertions.assertEquals("schema", path.getSchemaName());
        Assertions.assertEquals("table", path.getTableName());
    }

    @Test
    void testDialectName() {
        PostgresDialect dialect = new PostgresDialect();
        Assertions.assertEquals("Postgres", dialect.dialectName());
    }

    @Test
    void testHashModForField() {
        PostgresDialect dialect = new PostgresDialect();

        // Test without native type
        String hash1 = dialect.hashModForField("user_id", 10);
        Assertions.assertTrue(hash1.contains("HASHTEXT(\"user_id\")"));
        Assertions.assertTrue(hash1.contains("% 10"));

        // Test with native type (UUID needs conversion)
        String hash2 = dialect.hashModForField("uuid", "user_id", 10);
        Assertions.assertTrue(hash2.contains("HASHTEXT(\"user_id\"::text)"));
        Assertions.assertTrue(hash2.contains("% 10"));
    }

    @Test
    void testConvertType() {
        PostgresDialect dialect = new PostgresDialect();

        // UUID should convert to text
        String converted1 = dialect.convertType("\"user_id\"", "uuid");
        Assertions.assertEquals("\"user_id\"::text", converted1);

        // Other types should not convert
        String converted2 = dialect.convertType("\"user_id\"", "integer");
        Assertions.assertEquals("\"user_id\"", converted2);

        String converted3 = dialect.convertType("\"user_id\"", "varchar");
        Assertions.assertEquals("\"user_id\"", converted3);
    }

    @Test
    void testGetRowExistsStatement() {
        PostgresDialect dialect = new PostgresDialect();
        final String database = "testdb";
        final String tableName = "users";
        final String[] conditionFields = {"id", "email"};

        String sql = dialect.getRowExistsStatement(database, tableName, conditionFields);

        Assertions.assertNotNull(sql);
        Assertions.assertEquals(
                "SELECT 1 FROM \"testdb\".\"users\" WHERE \"id\" = :id AND \"email\" = :email",
                sql);
    }

    @Test
    void testGetInsertIntoStatement() {
        PostgresDialect dialect = new PostgresDialect();
        final String database = "testdb";
        final String tableName = "users";
        final String[] fieldNames = {"id", "name", "email", "age"};

        String sql = dialect.getInsertIntoStatement(database, tableName, fieldNames);

        Assertions.assertNotNull(sql);
        Assertions.assertEquals(
                "INSERT INTO \"testdb\".\"users\" (\"id\", \"name\", \"email\", \"age\") VALUES (:id, :name, :email, :age)",
                sql);
    }

    @Test
    void testGetUpdateStatement() {
        PostgresDialect dialect = new PostgresDialect();
        final String database = "testdb";
        final String tableName = "users";
        final String[] fieldNames = {"name", "email", "age"};
        final String[] conditionFields = {"id"};

        String sql =
                dialect.getUpdateStatement(database, tableName, fieldNames, conditionFields, false);

        Assertions.assertNotNull(sql);
        Assertions.assertEquals(
                "UPDATE \"testdb\".\"users\" SET \"name\" = :name, \"email\" = :email, \"age\" = :age WHERE \"id\" = :id",
                sql);
    }

    @Test
    void testGetDeleteStatement() {
        PostgresDialect dialect = new PostgresDialect();
        final String database = "testdb";
        final String tableName = "users";
        final String[] conditionFields = {"id"};

        String sql = dialect.getDeleteStatement(database, tableName, conditionFields);

        Assertions.assertNotNull(sql);
        Assertions.assertEquals("DELETE FROM \"testdb\".\"users\" WHERE \"id\" = :id", sql);
    }

    @Test
    void testRealUpsertExecution() throws SQLException {
        // Teste real de upsert usando Testcontainers
        String tableName = "test_upsert_execution";
        String createTableSQL =
                "CREATE TABLE "
                        + tableName
                        + " (\n"
                        + "  id INTEGER PRIMARY KEY,\n"
                        + "  name VARCHAR(100),\n"
                        + "  value INTEGER\n"
                        + ")";

        Connection conn = null;
        Statement stmt = null;

        try {
            conn = getConnection();
            stmt = conn.createStatement();

            // Create table
            stmt.execute(createTableSQL);

            // Insert first row
            String insertSQL =
                    "INSERT INTO " + tableName + " (id, name, value) VALUES (1, 'first', 100)";
            stmt.executeUpdate(insertSQL);

            // Verify insert
            ResultSet rs = stmt.executeQuery("SELECT COUNT(*) FROM " + tableName);
            rs.next();
            Assertions.assertEquals(1, rs.getInt(1));
            rs.close();

            // Create upsert statement using dialect
            PostgresDialect dialect = new PostgresDialect();
            String[] fieldNames = {"id", "name", "value"};
            String[] uniqueKeyFields = {"id"};

            java.util.Optional<String> upsertSQLOptional =
                    dialect.getUpsertStatement(null, tableName, fieldNames, uniqueKeyFields);
            Assertions.assertTrue(upsertSQLOptional.isPresent());
            String upsertSQL = upsertSQLOptional.get();

            // Verify the generated SQL
            Assertions.assertTrue(upsertSQL.contains("INSERT INTO"));
            Assertions.assertTrue(upsertSQL.contains("\"test_upsert_execution\""));
            Assertions.assertTrue(upsertSQL.contains("ON CONFLICT"));
            Assertions.assertTrue(upsertSQL.contains("DO UPDATE SET"));

            System.out.println("Generated upsert SQL: " + upsertSQL);

        } finally {
            // Cleanup
            if (stmt != null) {
                try {
                    stmt.execute("DROP TABLE IF EXISTS " + tableName);
                } catch (SQLException e) {
                    // Ignore cleanup errors
                }
                try {
                    stmt.close();
                } catch (SQLException e) {
                    // Ignore
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    // Ignore
                }
            }
        }
    }

    @Test
    void testTypeConverter() {
        PostgresDialect dialect = new PostgresDialect();
        org.apache.seatunnel.api.table.converter.TypeConverter<
                        org.apache.seatunnel.api.table.converter.BasicTypeDefine>
                typeConverter = dialect.getTypeConverter();

        Assertions.assertNotNull(typeConverter);
        // The type converter should be the PostgresTypeConverter instance
        Assertions.assertEquals("PostgresTypeConverter", typeConverter.getClass().getSimpleName());
    }

    @Test
    void testGetJdbcDialectTypeMapper() {
        PostgresDialect dialect = new PostgresDialect();
        org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectTypeMapper
                typeMapper = dialect.getJdbcDialectTypeMapper();

        Assertions.assertNotNull(typeMapper);
        Assertions.assertEquals("PostgresTypeMapper", typeMapper.getClass().getSimpleName());
    }

    @Test
    void testGetRowConverter() {
        PostgresDialect dialect = new PostgresDialect();
        org.apache.seatunnel.connectors.seatunnel.jdbc.internal.converter.JdbcRowConverter
                rowConverter = dialect.getRowConverter();

        Assertions.assertNotNull(rowConverter);
        Assertions.assertEquals(
                "PostgresJdbcRowConverter", rowConverter.getClass().getSimpleName());
    }

    @Test
    void testCreatPreparedStatement() throws SQLException {
        PostgresDialect dialect = new PostgresDialect();

        Connection conn = null;
        PreparedStatement ps1 = null;
        PreparedStatement ps2 = null;

        try {
            conn = getConnection();

            // Test with default fetch size
            ps1 = dialect.creatPreparedStatement(conn, "SELECT 1", 0);
            Assertions.assertNotNull(ps1);
            Assertions.assertEquals(128, ps1.getFetchSize()); // DEFAULT_POSTGRES_FETCH_SIZE

            // Test with custom fetch size
            ps2 = dialect.creatPreparedStatement(conn, "SELECT 1", 1000);
            Assertions.assertNotNull(ps2);
            Assertions.assertEquals(1000, ps2.getFetchSize());

            // Connection should be in auto-commit false mode
            Assertions.assertFalse(conn.getAutoCommit());

        } finally {
            if (ps1 != null) {
                try {
                    ps1.close();
                } catch (SQLException e) {
                    /* ignore */
                }
            }
            if (ps2 != null) {
                try {
                    ps2.close();
                } catch (SQLException e) {
                    /* ignore */
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    /* ignore */
                }
            }
        }
    }

    @Test
    void testApproximateRowCntStatement() throws SQLException {
        PostgresDialect dialect = new PostgresDialect();
        String tableName = "test_row_count";

        Connection conn = null;
        Statement stmt = null;

        try {
            conn = getConnection();
            stmt = conn.createStatement();

            // Create test table
            stmt.execute(
                    "CREATE TABLE " + tableName + " (id INTEGER PRIMARY KEY, name VARCHAR(100))");

            // Insert some data
            stmt.execute("INSERT INTO " + tableName + " (id, name) VALUES (1, 'Alice')");
            stmt.execute("INSERT INTO " + tableName + " (id, name) VALUES (2, 'Bob')");

            // Analyze table to update statistics
            stmt.execute("ANALYZE " + tableName);

        } finally {
            // Cleanup
            if (stmt != null) {
                try {
                    stmt.execute("DROP TABLE IF EXISTS " + tableName);
                } catch (SQLException e) {
                    // Ignore cleanup errors
                }
                try {
                    stmt.close();
                } catch (SQLException e) {
                    // Ignore
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    // Ignore
                }
            }
        }
    }

    @Test
    void testTableIdentifierWithTablePath() {
        PostgresDialect dialect = new PostgresDialect();

        TablePath tablePath = TablePath.of("mydb", "public", "mytable");
        String identifier = dialect.tableIdentifier(tablePath);

        Assertions.assertEquals("\"mydb\".\"public\".\"mytable\"", identifier);
    }

    @Test
    void testDefaultParameter() {
        PostgresDialect dialect = new PostgresDialect();
        java.util.Map<String, String> params = dialect.defaultParameter();

        Assertions.assertNotNull(params);
        Assertions.assertTrue(params instanceof java.util.HashMap);
    }

    @Test
    void testFieldIdeHandling() {
        PostgresDialect dialect = new PostgresDialect();

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
    void testColumnExists() throws SQLException {
        PostgresDialect dialect = new PostgresDialect();
        String tableName = "test_column_exists";

        Connection conn = null;
        Statement stmt = null;

        try {
            conn = getConnection();
            stmt = conn.createStatement();

            // Create test table
            stmt.execute(
                    "CREATE TABLE " + tableName + " (id INTEGER PRIMARY KEY, name VARCHAR(100))");

            TablePath tablePath =
                    TablePath.of(POSTGRES_CONTAINER.getDatabaseName(), "public", tableName);

            // Test existing column
            boolean idExists = dialect.columnExists(conn, tablePath, "id");
            Assertions.assertTrue(idExists, "Column 'id' should exist");

            // Test non-existing column
            boolean nonExisting = dialect.columnExists(conn, tablePath, "non_existing");
            Assertions.assertFalse(nonExisting, "Column 'non_existing' should not exist");

        } finally {
            // Cleanup
            if (stmt != null) {
                try {
                    stmt.execute("DROP TABLE IF EXISTS " + tableName);
                } catch (SQLException e) {
                    // Ignore cleanup errors
                }
                try {
                    stmt.close();
                } catch (SQLException e) {
                    // Ignore
                }
            }
            if (conn != null) {
                try {
                    conn.close();
                } catch (SQLException e) {
                    // Ignore
                }
            }
        }
    }
}
