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

package org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.ingres;

import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialect;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.JdbcDialectLoader;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.Types;
import java.util.stream.Stream;

class IngresDialectTest {

    private final JdbcDialect dialect =
            JdbcDialectLoader.load("jdbc:ingres://localhost:II7/test", null, "");

    @Test
    void testQuoteIdentifier() {
        Assertions.assertEquals("\"Order\"", dialect.quoteIdentifier("Order"));
        Assertions.assertEquals("\"a\"\"b\"", dialect.quoteIdentifier("a\"b"));
        Assertions.assertEquals("\"a.b\"", dialect.quoteIdentifier("a.b"));
        Assertions.assertEquals("\"with space\"", dialect.quoteIdentifier("with space"));
    }

    @Test
    void testConfiguredFieldCase() {
        JdbcDialect uppercase =
                JdbcDialectLoader.load("jdbc:ingres://localhost:II7/test", "", null, "uppercase");
        JdbcDialect lowercase =
                JdbcDialectLoader.load("jdbc:ingres://localhost:II7/test", "", null, "lowercase");
        Assertions.assertEquals("\"MIXED\"", uppercase.quoteIdentifier("Mixed"));
        Assertions.assertEquals("\"mixed\"", lowercase.quoteIdentifier("Mixed"));
        Assertions.assertEquals("\"Owner\"", uppercase.quoteDatabaseIdentifier("Owner"));
        Assertions.assertEquals(
                "\"Owner\".\"Orders\"", uppercase.tableIdentifier(uppercase.parse("Owner.Orders")));
    }

    @Test
    void testParseOwnerQualifiedTable() {
        TablePath table = dialect.parse("Owner.Orders");
        Assertions.assertNull(table.getDatabaseName());
        Assertions.assertEquals("Owner", table.getSchemaName());
        Assertions.assertEquals("Orders", table.getTableName());
        Assertions.assertEquals("\"Owner\".\"Orders\"", dialect.tableIdentifier(table));
    }

    @Test
    void testUnqualifiedTable() {
        TablePath table = dialect.parse("Orders");
        Assertions.assertNull(table.getSchemaName());
        Assertions.assertEquals("\"Orders\"", dialect.tableIdentifier(table));
        Assertions.assertEquals("\"Orders\"", dialect.tableIdentifier("database", "Orders"));
    }

    @Test
    void testTableIdentifierEscapesEachComponent() {
        Assertions.assertEquals(
                "\"own\"\"er\".\"a.b\"",
                dialect.tableIdentifier(TablePath.of("database", "own\"er", "a.b")));
        Assertions.assertEquals(
                "\"Owner\".\"Orders\"", dialect.tableIdentifier("database", "Owner.Orders"));
    }

    @Test
    void testSqlStatementsUseQuotedOwnerAndColumns() {
        String[] fields = {"id", "Order"};
        String[] keys = {"id"};
        Assertions.assertEquals(
                "INSERT INTO \"Owner\".\"Orders\" (\"id\", \"Order\") VALUES (:id, :Order)",
                dialect.getInsertIntoStatement("database", "Owner.Orders", fields));
        Assertions.assertEquals(
                "UPDATE \"Owner\".\"Orders\" SET \"Order\" = :Order WHERE \"id\" = :id",
                dialect.getUpdateStatement("database", "Owner.Orders", fields, keys, false));
        Assertions.assertEquals(
                "DELETE FROM \"Owner\".\"Orders\" WHERE \"id\" = :id",
                dialect.getDeleteStatement("database", "Owner.Orders", keys));
        Assertions.assertEquals(
                "SELECT 1 FROM \"Owner\".\"Orders\" WHERE \"id\" = :id",
                dialect.getRowExistsStatement("database", "Owner.Orders", keys));
    }

    @Test
    void testUsesExistingNonNativeUpsertFallback() {
        Assertions.assertFalse(
                dialect.getUpsertStatement(
                                "database",
                                "Owner.Orders",
                                new String[] {"id"},
                                new String[] {"id"})
                        .isPresent());
    }

    @Test
    void testDoesNotUseUnverifiedHashSplitting() {
        Assertions.assertFalse(dialect.supportHashSplitter());
    }

    @ParameterizedTest
    @MethodSource("standardTypes")
    void testStandardJdbcTypeMapping(int jdbcType, SeaTunnelDataType<?> expected) {
        Column column =
                dialect.getJdbcDialectTypeMapper()
                        .mappingColumn(
                                BasicTypeDefine.builder()
                                        .name("value")
                                        .sqlType(jdbcType)
                                        .precision(10L)
                                        .scale(2)
                                        .length(64L)
                                        .nullable(true)
                                        .build());
        Assertions.assertEquals(expected, column.getDataType());
        Assertions.assertEquals("value", column.getName());
        Assertions.assertTrue(column.isNullable());
    }

    private static Stream<Arguments> standardTypes() {
        return Stream.of(
                Arguments.of(Types.BOOLEAN, BasicType.BOOLEAN_TYPE),
                Arguments.of(Types.TINYINT, BasicType.BYTE_TYPE),
                Arguments.of(Types.SMALLINT, BasicType.SHORT_TYPE),
                Arguments.of(Types.INTEGER, BasicType.INT_TYPE),
                Arguments.of(Types.BIGINT, BasicType.LONG_TYPE),
                Arguments.of(Types.REAL, BasicType.FLOAT_TYPE),
                Arguments.of(Types.DOUBLE, BasicType.DOUBLE_TYPE),
                Arguments.of(Types.DECIMAL, new DecimalType(10, 2)),
                Arguments.of(Types.VARCHAR, BasicType.STRING_TYPE),
                Arguments.of(Types.VARBINARY, PrimitiveByteArrayType.INSTANCE),
                Arguments.of(Types.DATE, LocalTimeType.LOCAL_DATE_TYPE),
                Arguments.of(Types.TIME, LocalTimeType.LOCAL_TIME_TYPE),
                Arguments.of(Types.TIMESTAMP, LocalTimeType.LOCAL_DATE_TIME_TYPE));
    }
}
