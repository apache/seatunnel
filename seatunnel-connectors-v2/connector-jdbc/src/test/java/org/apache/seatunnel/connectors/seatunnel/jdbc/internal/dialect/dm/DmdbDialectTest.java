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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.dm;

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.connectors.seatunnel.jdbc.exception.JdbcConnectorException;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.dialectenum.FieldIdeEnum;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Unit tests for {@link DmdbDialect}. Tests cover SQL generation (UPSERT, INSERT, UPDATE, DELETE,
 * EXISTS), identifier quoting, default value handling, and converter/mapper instantiation. No
 * running database required.
 */
public class DmdbDialectTest {
    @Test
    public void testIdentifierCaseSensitive() {
        DmdbDialectFactory factory = new DmdbDialectFactory();

        JdbcDialect dialect = factory.create();
        Assertions.assertEquals("\"test\"", dialect.quoteIdentifier("test"));
        Assertions.assertEquals("\"TEST\"", dialect.quoteIdentifier("TEST"));

        dialect = factory.create(null, FieldIdeEnum.ORIGINAL.getValue());
        Assertions.assertEquals("\"test\"", dialect.quoteIdentifier("test"));
        Assertions.assertEquals("\"TEST\"", dialect.quoteIdentifier("TEST"));

        dialect = factory.create(null, FieldIdeEnum.LOWERCASE.getValue());
        Assertions.assertEquals("\"test\"", dialect.quoteIdentifier("test"));
        Assertions.assertEquals("\"test\"", dialect.quoteIdentifier("TEST"));

        dialect = factory.create(null, FieldIdeEnum.UPPERCASE.getValue());
        Assertions.assertEquals("\"TEST\"", dialect.quoteIdentifier("test"));
        Assertions.assertEquals("\"TEST\"", dialect.quoteIdentifier("TEST"));
    }

    @Test
    void testValidateTableOptionsAcceptsSupportedKeys() {
        DmdbDialect dialect = new DmdbDialect(FieldIdeEnum.ORIGINAL.getValue());
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("tablespace", "MAIN");
        tableOptions.put("fillfactor", "80");
        Assertions.assertDoesNotThrow(() -> dialect.validateTableOptions(tableOptions));
    }

    @Test
    void testValidateTableOptionsFillfactorBoundary() {
        DmdbDialect dialect = new DmdbDialect(FieldIdeEnum.ORIGINAL.getValue());
        Assertions.assertDoesNotThrow(
                () -> dialect.validateTableOptions(Collections.singletonMap("fillfactor", "0")));
        Assertions.assertDoesNotThrow(
                () -> dialect.validateTableOptions(Collections.singletonMap("fillfactor", "100")));
    }

    @Test
    void testValidateTableOptionsRejectsUnsupportedKeys() {
        DmdbDialect dialect = new DmdbDialect(FieldIdeEnum.ORIGINAL.getValue());
        Map<String, String> tableOptions = new HashMap<>();
        tableOptions.put("pctfree", "10");
        JdbcConnectorException exception =
                Assertions.assertThrows(
                        JdbcConnectorException.class,
                        () -> dialect.validateTableOptions(tableOptions));
        Assertions.assertTrue(exception.getMessage().contains("Unsupported JDBC table_options"));
        Assertions.assertTrue(exception.getMessage().contains("Dameng"));
    }

    @Test
    void testValidateTableOptionsRejectBlankValues() {
        DmdbDialect dialect = new DmdbDialect(FieldIdeEnum.ORIGINAL.getValue());

        JdbcConnectorException blankTablespace =
                Assertions.assertThrows(
                        JdbcConnectorException.class,
                        () ->
                                dialect.validateTableOptions(
                                        Collections.singletonMap("tablespace", " ")));
        Assertions.assertTrue(blankTablespace.getMessage().contains("must not be blank"));

        JdbcConnectorException blankFillfactor =
                Assertions.assertThrows(
                        JdbcConnectorException.class,
                        () ->
                                dialect.validateTableOptions(
                                        Collections.singletonMap("fillfactor", " ")));
        Assertions.assertTrue(blankFillfactor.getMessage().contains("must not be blank"));
    }

    @Test
    void testValidateTableOptionsRejectInvalidFillfactor() {
        DmdbDialect dialect = new DmdbDialect(FieldIdeEnum.ORIGINAL.getValue());

        JdbcConnectorException nonNumeric =
                Assertions.assertThrows(
                        JdbcConnectorException.class,
                        () ->
                                dialect.validateTableOptions(
                                        Collections.singletonMap("fillfactor", "abc")));
        Assertions.assertTrue(nonNumeric.getMessage().contains("must be an integer between"));

        JdbcConnectorException outOfRange =
                Assertions.assertThrows(
                        JdbcConnectorException.class,
                        () ->
                                dialect.validateTableOptions(
                                        Collections.singletonMap("fillfactor", "101")));
        Assertions.assertTrue(outOfRange.getMessage().contains("must be an integer between"));
    }

    @Test
    void testValidateTableOptionsRejectIllegalTablespace() {
        DmdbDialect dialect = new DmdbDialect(FieldIdeEnum.ORIGINAL.getValue());

        JdbcConnectorException exception =
                Assertions.assertThrows(
                        JdbcConnectorException.class,
                        () ->
                                dialect.validateTableOptions(
                                        Collections.singletonMap("tablespace", "MAIN\"TS")));
        Assertions.assertTrue(exception.getMessage().contains("illegal characters"));
    }

    @Test
    void testNormalizeFillfactorAcceptsPlusSignAndLeadingZeros() {
        Assertions.assertEquals("80", DmdbDialect.normalizeFillfactorForDdl("+80"));
        Assertions.assertEquals("80", DmdbDialect.normalizeFillfactorForDdl("080"));
    }

    @Test
    void testNormalizeFillfactorRejectsOutOfRange() {
        JdbcConnectorException exception =
                Assertions.assertThrows(
                        JdbcConnectorException.class,
                        () -> DmdbDialect.normalizeFillfactorForDdl("101"));
        Assertions.assertTrue(exception.getMessage().contains("must be an integer between"));
    }

    @Test
    void testNormalizeTablespaceRejectsControlCharacters() {
        JdbcConnectorException tabCharacter =
                Assertions.assertThrows(
                        JdbcConnectorException.class,
                        () -> DmdbDialect.normalizeTablespaceForDdl("MAIN\tTS"));
        Assertions.assertTrue(tabCharacter.getMessage().contains("illegal characters"));
    }

    @Test
    public void testDialectName() {
        DmdbDialect dialect = new DmdbDialect(FieldIdeEnum.ORIGINAL.getValue());
        Assertions.assertEquals("Dameng", dialect.dialectName());
    }

    @Test
    public void testGetUpsertStatement() {
        DmdbDialect dialect = new DmdbDialect(FieldIdeEnum.ORIGINAL.getValue());
        String database = "testdb";
        String tableName = "users";
        String[] fieldNames = {"id", "name", "age"};
        String[] uniqueKeyFields = {"id"};

        Optional<String> upsertSQL =
                dialect.getUpsertStatement(database, tableName, fieldNames, uniqueKeyFields);
        Assertions.assertTrue(upsertSQL.isPresent());

        String sql = upsertSQL.get();
        Assertions.assertTrue(sql.contains("MERGE INTO"));
        Assertions.assertTrue(sql.contains("TARGET"));
        Assertions.assertTrue(sql.contains("SOURCE"));
        Assertions.assertTrue(sql.contains("WHEN MATCHED THEN"));
        Assertions.assertTrue(sql.contains("UPDATE SET"));
        Assertions.assertTrue(sql.contains("WHEN NOT MATCHED THEN"));
        Assertions.assertTrue(sql.contains("INSERT"));

        // Note: database name "testdb" is not quoted because DmdbDialect does not override
        // quoteDatabaseIdentifier(), which returns the identifier as-is by default.
        Assertions.assertEquals(
                " MERGE INTO testdb.\"users\" TARGET"
                        + " USING (SELECT :id \"id\", :name \"name\", :age \"age\") SOURCE"
                        + " ON (TARGET.\"id\"=SOURCE.\"id\") "
                        + " WHEN MATCHED THEN"
                        + " UPDATE SET TARGET.\"name\"=SOURCE.\"name\", TARGET.\"age\"=SOURCE.\"age\""
                        + " WHEN NOT MATCHED THEN"
                        + " INSERT (\"id\", \"name\", \"age\") VALUES (SOURCE.\"id\", SOURCE.\"name\", SOURCE.\"age\")",
                sql);
    }

    @Test
    public void testGetInsertIntoStatement() {
        DmdbDialect dialect = new DmdbDialect(FieldIdeEnum.ORIGINAL.getValue());
        String database = "testdb";
        String tableName = "users";
        String[] fieldNames = {"id", "name", "email", "age"};

        String sql = dialect.getInsertIntoStatement(database, tableName, fieldNames);

        Assertions.assertNotNull(sql);
        Assertions.assertEquals(
                "INSERT INTO testdb.\"users\" (\"id\", \"name\", \"email\", \"age\")"
                        + " VALUES (:id, :name, :email, :age)",
                sql);
    }

    @Test
    public void testGetDeleteStatement() {
        DmdbDialect dialect = new DmdbDialect(FieldIdeEnum.ORIGINAL.getValue());
        String database = "testdb";
        String tableName = "users";
        String[] conditionFields = {"id"};

        String sql = dialect.getDeleteStatement(database, tableName, conditionFields);

        Assertions.assertNotNull(sql);
        Assertions.assertEquals("DELETE FROM testdb.\"users\" WHERE \"id\" = :id", sql);
    }

    @Test
    public void testGetRowExistsStatement() {
        DmdbDialect dialect = new DmdbDialect(FieldIdeEnum.ORIGINAL.getValue());
        String database = "testdb";
        String tableName = "users";
        String[] conditionFields = {"id", "email"};

        String sql = dialect.getRowExistsStatement(database, tableName, conditionFields);

        Assertions.assertNotNull(sql);
        Assertions.assertEquals(
                "SELECT 1 FROM testdb.\"users\" WHERE \"id\" = :id AND \"email\" = :email", sql);
    }

    @Test
    public void testGetUpdateStatement() {
        DmdbDialect dialect = new DmdbDialect(FieldIdeEnum.ORIGINAL.getValue());
        String database = "testdb";
        String tableName = "users";
        String[] fieldNames = {"name", "email", "age"};
        String[] conditionFields = {"id"};

        String sql =
                dialect.getUpdateStatement(database, tableName, fieldNames, conditionFields, false);

        Assertions.assertNotNull(sql);
        Assertions.assertEquals(
                "UPDATE testdb.\"users\" SET \"name\" = :name,"
                        + " \"email\" = :email, \"age\" = :age WHERE \"id\" = :id",
                sql);
    }

    @Test
    public void testTableIdentifierWithTablePath() {
        DmdbDialect dialect = new DmdbDialect(FieldIdeEnum.ORIGINAL.getValue());

        TablePath tablePath = TablePath.of("mydb", "myschema", "mytable");
        String identifier = dialect.tableIdentifier(tablePath);

        Assertions.assertEquals("\"myschema\".\"mytable\"", identifier);
    }

    @Test
    public void testTableIdentifierWithDatabaseAndTable() {
        DmdbDialect dialect = new DmdbDialect(FieldIdeEnum.ORIGINAL.getValue());

        // database is null
        Assertions.assertEquals("\"users\"", dialect.tableIdentifier(null, "users"));

        // tableName contains dot (schema.table)
        Assertions.assertEquals(
                "\"myschema\".\"users\"", dialect.tableIdentifier("testdb", "myschema.users"));

        // normal case: database not quoted (quoteDatabaseIdentifier not overridden)
        Assertions.assertEquals("testdb.\"users\"", dialect.tableIdentifier("testdb", "users"));
    }

    @Test
    public void testExtractTableName() {
        DmdbDialect dialect = new DmdbDialect(FieldIdeEnum.ORIGINAL.getValue());

        TablePath tablePath = TablePath.of("mydb", "myschema", "mytable");
        String tableName = dialect.extractTableName(tablePath);

        Assertions.assertEquals("myschema.mytable", tableName);
    }

    @Test
    public void testParse() {
        DmdbDialect dialect = new DmdbDialect(FieldIdeEnum.ORIGINAL.getValue());

        TablePath path = dialect.parse("mydb.myschema.mytable");
        Assertions.assertEquals("mydb", path.getDatabaseName());
        Assertions.assertEquals("myschema", path.getSchemaName());
        Assertions.assertEquals("mytable", path.getTableName());
    }

    @Test
    public void testNeedsQuotesWithDefaultValue() {
        DmdbDialect dialect = new DmdbDialect(FieldIdeEnum.ORIGINAL.getValue());

        // String types need quotes
        BasicTypeDefine<Object> varcharType =
                BasicTypeDefine.builder().name("col").dataType("VARCHAR").build();
        Assertions.assertTrue(dialect.needsQuotesWithDefaultValue(varcharType));

        BasicTypeDefine<Object> charType =
                BasicTypeDefine.builder().name("col").dataType("CHAR").build();
        Assertions.assertTrue(dialect.needsQuotesWithDefaultValue(charType));

        BasicTypeDefine<Object> clobType =
                BasicTypeDefine.builder().name("col").dataType("CLOB").build();
        Assertions.assertTrue(dialect.needsQuotesWithDefaultValue(clobType));

        BasicTypeDefine<Object> textType =
                BasicTypeDefine.builder().name("col").dataType("TEXT").build();
        Assertions.assertTrue(dialect.needsQuotesWithDefaultValue(textType));

        BasicTypeDefine<Object> characterType =
                BasicTypeDefine.builder().name("col").dataType("CHARACTER").build();
        Assertions.assertTrue(dialect.needsQuotesWithDefaultValue(characterType));

        BasicTypeDefine<Object> varchar2Type =
                BasicTypeDefine.builder().name("col").dataType("VARCHAR2").build();
        Assertions.assertTrue(dialect.needsQuotesWithDefaultValue(varchar2Type));

        BasicTypeDefine<Object> nvarcharType =
                BasicTypeDefine.builder().name("col").dataType("NVARCHAR").build();
        Assertions.assertTrue(dialect.needsQuotesWithDefaultValue(nvarcharType));

        BasicTypeDefine<Object> longvarcharType =
                BasicTypeDefine.builder().name("col").dataType("LONGVARCHAR").build();
        Assertions.assertTrue(dialect.needsQuotesWithDefaultValue(longvarcharType));

        BasicTypeDefine<Object> longType =
                BasicTypeDefine.builder().name("col").dataType("LONG").build();
        Assertions.assertTrue(dialect.needsQuotesWithDefaultValue(longType));

        // Numeric types do not need quotes
        BasicTypeDefine<Object> intType =
                BasicTypeDefine.builder().name("col").dataType("INTEGER").build();
        Assertions.assertFalse(dialect.needsQuotesWithDefaultValue(intType));

        BasicTypeDefine<Object> decimalType =
                BasicTypeDefine.builder().name("col").dataType("DECIMAL").build();
        Assertions.assertFalse(dialect.needsQuotesWithDefaultValue(decimalType));
    }

    @Test
    public void testGetRowConverter() {
        DmdbDialect dialect = new DmdbDialect(FieldIdeEnum.ORIGINAL.getValue());
        Assertions.assertNotNull(dialect.getRowConverter());
        Assertions.assertEquals(
                "DmdbJdbcRowConverter", dialect.getRowConverter().getClass().getSimpleName());
    }

    @Test
    public void testGetTypeConverter() {
        DmdbDialect dialect = new DmdbDialect(FieldIdeEnum.ORIGINAL.getValue());
        Assertions.assertNotNull(dialect.getTypeConverter());
        Assertions.assertEquals(
                "DmdbTypeConverter", dialect.getTypeConverter().getClass().getSimpleName());
    }

    @Test
    public void testGetJdbcDialectTypeMapper() {
        DmdbDialect dialect = new DmdbDialect(FieldIdeEnum.ORIGINAL.getValue());
        Assertions.assertNotNull(dialect.getJdbcDialectTypeMapper());
        Assertions.assertEquals(
                "DmdbTypeMapper", dialect.getJdbcDialectTypeMapper().getClass().getSimpleName());
    }

    // ==================== CATALOG RELATED TESTS ====================

    @Test
    public void testParseCatalog() {
        DmdbDialect dialect = new DmdbDialect(FieldIdeEnum.ORIGINAL.getValue());

        // schemaFirst=true: "schema.table" -> schema=schema, table=table
        TablePath path = dialect.parse("SYSDBA.users");
        Assertions.assertNull(path.getDatabaseName());
        Assertions.assertEquals("SYSDBA", path.getSchemaName());
        Assertions.assertEquals("users", path.getTableName());

        // Three-part: "db.schema.table"
        TablePath fullPath = dialect.parse("DAMENG.SYSDBA.users");
        Assertions.assertEquals("DAMENG", fullPath.getDatabaseName());
        Assertions.assertEquals("SYSDBA", fullPath.getSchemaName());
        Assertions.assertEquals("users", fullPath.getTableName());

        // Single part: just table name
        TablePath simplePath = dialect.parse("users");
        Assertions.assertNull(simplePath.getDatabaseName());
        Assertions.assertNull(simplePath.getSchemaName());
        Assertions.assertEquals("users", simplePath.getTableName());
    }

    @Test
    public void testExtractCatalogTableName() {
        DmdbDialect dialect = new DmdbDialect(FieldIdeEnum.ORIGINAL.getValue());

        // With schema: returns "schema.table"
        TablePath pathWithSchema = TablePath.of(null, "SYSDBA", "users");
        Assertions.assertEquals("SYSDBA.users", dialect.extractTableName(pathWithSchema));

        // Without schema: returns just table name
        TablePath pathNoSchema = TablePath.of(null, null, "users");
        Assertions.assertEquals("users", dialect.extractTableName(pathNoSchema));
    }

    @Test
    public void testCatalogTableIdentifierWithTablePath() {
        DmdbDialect dialect = new DmdbDialect(FieldIdeEnum.ORIGINAL.getValue());

        // With schema: returns quoted "schema"."table"
        TablePath pathWithSchema = TablePath.of(null, "SYSDBA", "users");
        Assertions.assertEquals("\"SYSDBA\".\"users\"", dialect.tableIdentifier(pathWithSchema));

        // Without schema: returns just quoted table
        TablePath pathNoSchema = TablePath.of(null, null, "users");
        Assertions.assertEquals("\"users\"", dialect.tableIdentifier(pathNoSchema));
    }

    @Test
    public void testTableIdentifierWithDatabaseAndTableName() {
        DmdbDialect dialect = new DmdbDialect(FieldIdeEnum.ORIGINAL.getValue());

        // Normal case: database + table -> database."table"
        Assertions.assertEquals("testdb.\"users\"", dialect.tableIdentifier("testdb", "users"));

        // Null database: returns quoted table only
        Assertions.assertEquals("\"users\"", dialect.tableIdentifier(null, "users"));

        // Table name contains dot (schema.table): quotes each part, ignores database
        Assertions.assertEquals(
                "\"SYSDBA\".\"users\"", dialect.tableIdentifier("testdb", "SYSDBA.users"));
    }
}
