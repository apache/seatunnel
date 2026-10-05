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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.yashandb;

import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Optional;

public class YashanDbDialectTest {

    private static final YashanDbDialect DIALECT =
            new YashanDbDialect(
                    org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.dialectenum
                            .FieldIdeEnum.ORIGINAL
                            .getValue());

    @Test
    public void testDialectName() {
        Assertions.assertEquals(DatabaseIdentifier.YASHANDB, DIALECT.dialectName());
    }

    @Test
    public void testQuoteIdentifier() {
        Assertions.assertEquals("\"table_name\"", DIALECT.quoteIdentifier("table_name"));
        Assertions.assertEquals("\"COLUMN\"", DIALECT.quoteIdentifier("COLUMN"));
    }

    @Test
    public void testQuoteIdentifierWithDot() {
        String result = DIALECT.quoteIdentifier("SCHEMA.TABLE");
        Assertions.assertTrue(result.contains("\"SCHEMA\""));
        Assertions.assertTrue(result.contains("\"TABLE\""));
        Assertions.assertTrue(result.contains("."));
    }

    @Test
    public void testQuoteIdentifierWithUppercase() {
        YashanDbDialect upperDialect =
                new YashanDbDialect(
                        org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.dialectenum
                                .FieldIdeEnum.UPPERCASE
                                .getValue());
        Assertions.assertEquals("\"MY_COLUMN\"", upperDialect.quoteIdentifier("my_column"));
    }

    @Test
    public void testQuoteIdentifierWithLowercase() {
        YashanDbDialect lowerDialect =
                new YashanDbDialect(
                        org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.dialectenum
                                .FieldIdeEnum.LOWERCASE
                                .getValue());
        Assertions.assertEquals("\"my_column\"", lowerDialect.quoteIdentifier("MY_COLUMN"));
    }

    @Test
    public void testTableIdentifier() {
        String result = DIALECT.tableIdentifier("mydb", "mytable");
        Assertions.assertEquals("\"mytable\"", result);
    }

    @Test
    public void testTableIdentifierWithTablePath() {
        // TablePath.of(database, schema, table) - schema must be set explicitly
        TablePath tablePath = TablePath.of(null, "SCHEMA", "TABLE");
        String result = DIALECT.tableIdentifier(tablePath);
        // quoteIdentifier splits "SCHEMA.TABLE" on "." and quotes each part
        Assertions.assertEquals("\"SCHEMA\".\"TABLE\"", result);

        // TablePath.of(database, table) - schema is null, getSchemaAndTableName returns tableName
        // only
        TablePath tablePath2 = TablePath.of("mydb", "mytable");
        String result2 = DIALECT.tableIdentifier(tablePath2);
        Assertions.assertEquals("\"mytable\"", result2);
    }

    @Test
    public void testParse() {
        TablePath tablePath = DIALECT.parse("SCHEMA.TABLE");
        Assertions.assertEquals("SCHEMA", tablePath.getSchemaName());
        Assertions.assertEquals("TABLE", tablePath.getTableName());
    }

    @Test
    public void testParseTableOnly() {
        TablePath tablePath = DIALECT.parse("TABLE");
        Assertions.assertEquals("TABLE", tablePath.getTableName());
    }

    @Test
    public void testHashModForField() {
        String result = DIALECT.hashModForField("col1", 10);
        Assertions.assertTrue(result.contains("MOD"));
        Assertions.assertTrue(result.contains("ORA_HASH"));
        Assertions.assertTrue(result.contains("\"col1\""));
        Assertions.assertTrue(result.contains("10"));
    }

    @Test
    public void testDualTable() {
        Assertions.assertEquals(" FROM dual ", DIALECT.dualTable());
    }

    @Test
    public void testGetRowConverter() {
        Assertions.assertNotNull(DIALECT.getRowConverter());
        Assertions.assertTrue(DIALECT.getRowConverter() instanceof YashanDbJdbcRowConverter);
    }

    @Test
    public void testGetTypeConverter() {
        Assertions.assertNotNull(DIALECT.getTypeConverter());
        Assertions.assertTrue(DIALECT.getTypeConverter() instanceof YashanDbTypeConverter);
    }

    @Test
    public void testGetJdbcDialectTypeMapper() {
        Assertions.assertNotNull(DIALECT.getJdbcDialectTypeMapper());
        Assertions.assertTrue(DIALECT.getJdbcDialectTypeMapper() instanceof YashanDbTypeMapper);
    }

    @Test
    public void testGetUpsertStatement() {
        String[] fieldNames = {"id", "name", "age"};
        String[] uniqueKeyFields = {"id"};

        Optional<String> upsertStatement =
                DIALECT.getUpsertStatement("test_db", "test_table", fieldNames, uniqueKeyFields);

        Assertions.assertTrue(upsertStatement.isPresent());
        String sql = upsertStatement.get();
        Assertions.assertTrue(sql.contains("MERGE INTO"));
        Assertions.assertTrue(sql.contains("TARGET"));
        Assertions.assertTrue(sql.contains("SOURCE"));
        Assertions.assertTrue(sql.contains("WHEN MATCHED THEN"));
        Assertions.assertTrue(sql.contains("WHEN NOT MATCHED THEN"));
        Assertions.assertTrue(sql.contains("\"test_table\""));
        Assertions.assertTrue(sql.contains("\"id\""));
        Assertions.assertTrue(sql.contains("\"name\""));
        Assertions.assertTrue(sql.contains("\"age\""));
        Assertions.assertTrue(sql.contains("UPDATE SET"));
        Assertions.assertTrue(sql.contains("INSERT"));
    }

    @Test
    public void testGetUpsertStatementMultipleKeys() {
        String[] fieldNames = {"id", "code", "name", "value"};
        String[] uniqueKeyFields = {"id", "code"};

        Optional<String> upsertStatement =
                DIALECT.getUpsertStatement("test_db", "test_table", fieldNames, uniqueKeyFields);

        Assertions.assertTrue(upsertStatement.isPresent());
        String sql = upsertStatement.get();
        // ON condition should have both keys
        Assertions.assertTrue(sql.contains("TARGET.\"id\"=SOURCE.\"id\""));
        Assertions.assertTrue(sql.contains("TARGET.\"code\"=SOURCE.\"code\""));
        // UPDATE SET should only contain non-key fields
        Assertions.assertTrue(sql.contains("TARGET.\"name\"=SOURCE.\"name\""));
        Assertions.assertTrue(sql.contains("TARGET.\"value\"=SOURCE.\"value\""));
    }

    @Test
    public void testInsertIntoStatement() {
        String[] fieldNames = {"id", "name", "age"};
        String insertStatement =
                DIALECT.getInsertIntoStatement("test_db", "test_table", fieldNames);

        Assertions.assertNotNull(insertStatement);
        Assertions.assertTrue(insertStatement.contains("INSERT INTO"));
        Assertions.assertTrue(insertStatement.contains("\"test_table\""));
        Assertions.assertTrue(insertStatement.contains("\"id\""));
        Assertions.assertTrue(insertStatement.contains("\"name\""));
        Assertions.assertTrue(insertStatement.contains("\"age\""));
        Assertions.assertTrue(insertStatement.contains(":id"));
        Assertions.assertTrue(insertStatement.contains(":name"));
        Assertions.assertTrue(insertStatement.contains(":age"));
    }

    @Test
    public void testUpdateStatement() {
        String[] fieldNames = {"name", "age"};
        String[] conditionFields = {"id"};
        String updateStatement =
                DIALECT.getUpdateStatement(
                        "test_db", "test_table", fieldNames, conditionFields, false);

        Assertions.assertNotNull(updateStatement);
        Assertions.assertTrue(updateStatement.contains("UPDATE"));
        Assertions.assertTrue(updateStatement.contains("SET"));
        Assertions.assertTrue(updateStatement.contains("WHERE"));
        Assertions.assertTrue(updateStatement.contains("\"name\""));
        Assertions.assertTrue(updateStatement.contains("\"age\""));
        Assertions.assertTrue(updateStatement.contains("\"id\""));
    }

    @Test
    public void testDeleteStatement() {
        String[] conditionFields = {"id"};
        String deleteStatement =
                DIALECT.getDeleteStatement("test_db", "test_table", conditionFields);

        Assertions.assertNotNull(deleteStatement);
        Assertions.assertTrue(deleteStatement.contains("DELETE FROM"));
        Assertions.assertTrue(deleteStatement.contains("\"test_table\""));
        Assertions.assertTrue(deleteStatement.contains("\"id\""));
    }

    @Test
    public void testAllKeyTableOmitsEmptyUpdateSet() {
        String[] allFields = {"id", "name", "age"};
        Optional<String> upsertStatement =
                DIALECT.getUpsertStatement("test_db", "test_table", allFields, allFields);
        Assertions.assertTrue(upsertStatement.isPresent(), "upsert statement should be present");
        String sql = upsertStatement.get().toUpperCase();
        Assertions.assertFalse(
                sql.contains("WHEN MATCHED THEN UPDATE SET"),
                "all-key table must NOT emit an empty 'WHEN MATCHED THEN UPDATE SET' (got: "
                        + sql
                        + ")");
        Assertions.assertTrue(
                sql.contains("WHEN NOT MATCHED"), "all-key table must still insert unmatched rows");
        Assertions.assertTrue(
                sql.contains("INSERT"), "all-key table statement must contain an INSERT branch");
    }

    @Test
    public void testPartialKeyTableStillUpdates() {
        String[] allFields = {"id", "name", "age"};
        String[] uniqueKeyFields = {"id"};
        Optional<String> upsertStatement =
                DIALECT.getUpsertStatement("test_db", "test_table", allFields, uniqueKeyFields);
        Assertions.assertTrue(upsertStatement.isPresent(), "upsert statement should be present");
        String sql = upsertStatement.get().toUpperCase();
        Assertions.assertTrue(
                sql.contains("WHEN MATCHED THEN UPDATE SET"),
                "partial-key table must still emit 'WHEN MATCHED THEN UPDATE SET' (got: "
                        + sql
                        + ")");
    }
}
