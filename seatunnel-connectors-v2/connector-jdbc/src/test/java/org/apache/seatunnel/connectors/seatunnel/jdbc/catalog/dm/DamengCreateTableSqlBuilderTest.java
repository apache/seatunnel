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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.dm;

import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DamengCreateTableSqlBuilderTest {

    @Test
    public void TestCreateTableSqlBuilder() {
        TablePath tablePath = TablePath.of("test_database", "test_schema", "test_table");
        TableSchema tableSchema =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.of(
                                        "id", BasicType.LONG_TYPE, 22L, false, null, "id"))
                        .column(
                                PhysicalColumn.of(
                                        "name", BasicType.STRING_TYPE, 128L, false, null, "name"))
                        .column(
                                PhysicalColumn.of(
                                        "age", BasicType.INT_TYPE, (Long) null, true, null, "age"))
                        .column(
                                PhysicalColumn.of(
                                        "createTime",
                                        LocalTimeType.LOCAL_DATE_TIME_TYPE,
                                        3L,
                                        true,
                                        null,
                                        "createTime"))
                        .column(
                                PhysicalColumn.of(
                                        "lastUpdateTime",
                                        LocalTimeType.LOCAL_DATE_TIME_TYPE,
                                        3L,
                                        true,
                                        null,
                                        "lastUpdateTime"))
                        .primaryKey(PrimaryKey.of("id", Lists.newArrayList("id")))
                        .constraintKey(
                                Arrays.asList(
                                        ConstraintKey.of(
                                                ConstraintKey.ConstraintType.UNIQUE_KEY,
                                                "name",
                                                Lists.newArrayList(
                                                        ConstraintKey.ConstraintKeyColumn.of(
                                                                "name", null))),
                                        ConstraintKey.of(
                                                ConstraintKey.ConstraintType.INDEX_KEY,
                                                "age",
                                                Lists.newArrayList(
                                                        ConstraintKey.ConstraintKeyColumn.of(
                                                                "age", null)))))
                        .build();

        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("test_catalog", tablePath),
                        tableSchema,
                        new HashMap<>(),
                        new ArrayList<>(),
                        "User table");

        String createTableSql =
                new DamengCreateTableSqlBuilder(catalogTable, true).build(tablePath);
        String expect =
                "CREATE TABLE \"test_schema\".\"test_table\" (\n"
                        + "\"id\" BIGINT NOT NULL,\n"
                        + "\"name\" VARCHAR2(128) NOT NULL,\n"
                        + "\"age\" INT,\n"
                        + "\"createTime\" TIMESTAMP,\n"
                        + "\"lastUpdateTime\" TIMESTAMP,\n"
                        + "CONSTRAINT id_63d5 PRIMARY KEY (\"id\"),\n"
                        + "\tCONSTRAINT name_49b6 UNIQUE (\"name\")\n"
                        + ");\n"
                        + "COMMENT ON COLUMN \"test_schema\".\"test_table\".\"id\" IS 'id';\n"
                        + "COMMENT ON COLUMN \"test_schema\".\"test_table\".\"name\" IS 'name';\n"
                        + "COMMENT ON COLUMN \"test_schema\".\"test_table\".\"age\" IS 'age';\n"
                        + "COMMENT ON COLUMN \"test_schema\".\"test_table\".\"createTime\" IS 'createTime';\n"
                        + "COMMENT ON COLUMN \"test_schema\".\"test_table\".\"lastUpdateTime\" IS 'lastUpdateTime';";

        String regex1 = "id_\\w+";
        String regex2 = "name_\\w+";
        String replacedStr1 = createTableSql.replaceAll(regex1, "id_").replaceAll(regex2, "name_");
        String replacedStr2 = expect.replaceAll(regex1, "id_").replaceAll(regex2, "name_");
        Assertions.assertEquals(replacedStr2, replacedStr1);

        // skip index
        String createTableSqlSkipIndex =
                new DamengCreateTableSqlBuilder(catalogTable, false).build(tablePath);
        // create table sql is change; The old unit tests are no longer applicable
        String expectSkipIndex =
                "CREATE TABLE \"test_schema\".\"test_table\" (\n"
                        + "\"id\" BIGINT NOT NULL,\n"
                        + "\"name\" VARCHAR2(128) NOT NULL,\n"
                        + "\"age\" INT,\n"
                        + "\"createTime\" TIMESTAMP,\n"
                        + "\"lastUpdateTime\" TIMESTAMP\n"
                        + ");\n"
                        + "COMMENT ON COLUMN \"test_schema\".\"test_table\".\"id\" IS 'id';\n"
                        + "COMMENT ON COLUMN \"test_schema\".\"test_table\".\"name\" IS 'name';\n"
                        + "COMMENT ON COLUMN \"test_schema\".\"test_table\".\"age\" IS 'age';\n"
                        + "COMMENT ON COLUMN \"test_schema\".\"test_table\".\"createTime\" IS 'createTime';\n"
                        + "COMMENT ON COLUMN \"test_schema\".\"test_table\".\"lastUpdateTime\" IS 'lastUpdateTime';";
        Assertions.assertEquals(expectSkipIndex, createTableSqlSkipIndex);
    }

    @Test
    public void testColumnSinkType() {
        DamengCreateTableSqlBuilder sqlBuilder = mock(DamengCreateTableSqlBuilder.class);

        Column column = mock(Column.class);
        when(column.getSinkType()).thenReturn("VARCHAR(10)");
        when(column.getDataType()).thenReturn((SeaTunnelDataType) BasicType.INT_TYPE);
        when(column.getName()).thenReturn("col1");
        when(sqlBuilder.buildColumnSql(column)).thenCallRealMethod();

        String result = sqlBuilder.buildColumnSql(column);

        Assertions.assertEquals("\"col1\" VARCHAR(10) NOT NULL", result);
    }

    /**
     * Tests same-catalog scenario (Dameng to Dameng) where sourceType is preserved directly,
     * similar to PostgresCreateTableSqlBuilderTest testing otherDB=true/false.
     */
    @Test
    public void testBuildWithSameCatalogSourceType() {
        TablePath tablePath = TablePath.of("DAMENG", "SYSDBA", "employee");
        TableSchema tableSchema =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.of(
                                        "id",
                                        BasicType.LONG_TYPE,
                                        22L,
                                        false,
                                        null,
                                        "primary key",
                                        "BIGINT",
                                        Collections.emptyMap()))
                        .column(
                                PhysicalColumn.of(
                                        "name",
                                        BasicType.STRING_TYPE,
                                        100L,
                                        false,
                                        null,
                                        "employee name",
                                        "VARCHAR2(100)",
                                        Collections.emptyMap()))
                        .column(
                                PhysicalColumn.of(
                                        "salary",
                                        BasicType.DOUBLE_TYPE,
                                        10L,
                                        true,
                                        null,
                                        "",
                                        "DECIMAL(10,2)",
                                        Collections.emptyMap()))
                        .primaryKey(PrimaryKey.of("pk_id", Lists.newArrayList("id")))
                        .build();

        // Same catalog: source is Dameng, so sourceType should be used directly
        CatalogTable sameCatalogTable =
                CatalogTable.of(
                        TableIdentifier.of(DatabaseIdentifier.DAMENG, tablePath),
                        tableSchema,
                        new HashMap<>(),
                        new ArrayList<>(),
                        "");

        String sql = new DamengCreateTableSqlBuilder(sameCatalogTable, true).build(tablePath);
        // sourceType (BIGINT, VARCHAR2(100), DECIMAL(10,2)) should be preserved
        Assertions.assertTrue(
                sql.contains("BIGINT"), "Same catalog should preserve BIGINT sourceType");
        Assertions.assertTrue(
                sql.contains("VARCHAR2(100)"), "Same catalog should preserve VARCHAR2 sourceType");
        Assertions.assertTrue(
                sql.contains("DECIMAL(10,2)"), "Same catalog should preserve DECIMAL sourceType");
    }

    /**
     * Tests cross-catalog scenario (MySQL to Dameng) where types are converted via
     * DmdbTypeConverter instead of using sourceType.
     */
    @Test
    public void testBuildWithCrossCatalogConversion() {
        TablePath tablePath = TablePath.of("DAMENG", "SYSDBA", "imported_table");
        TableSchema tableSchema =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.of(
                                        "id", BasicType.LONG_TYPE, 22L, false, null, "id"))
                        .column(
                                PhysicalColumn.of(
                                        "name", BasicType.STRING_TYPE, 128L, false, null, "name"))
                        .build();

        // Cross catalog: source is MySQL, types should be converted by DmdbTypeConverter
        CatalogTable crossCatalogTable =
                CatalogTable.of(
                        TableIdentifier.of(DatabaseIdentifier.MYSQL, tablePath),
                        tableSchema,
                        new HashMap<>(),
                        new ArrayList<>(),
                        "");

        String sql = new DamengCreateTableSqlBuilder(crossCatalogTable, false).build(tablePath);
        Assertions.assertTrue(sql.startsWith("CREATE TABLE \"SYSDBA\".\"imported_table\""));
        // Cross-catalog columns should be converted by DmdbTypeConverter
        Assertions.assertTrue(sql.contains("BIGINT"), "Cross catalog LONG should map to BIGINT");
        Assertions.assertTrue(
                sql.contains("VARCHAR2(128)"), "Cross catalog STRING should map to VARCHAR2");
    }
}
