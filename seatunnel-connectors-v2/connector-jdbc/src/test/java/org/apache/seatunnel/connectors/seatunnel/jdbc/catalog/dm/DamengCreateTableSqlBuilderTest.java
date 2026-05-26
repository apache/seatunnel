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

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DamengCreateTableSqlBuilderTest {

    @Test
    public void TestCreateTableSqlBuilder() {
        TablePath tablePath = TablePath.of("test_database", "test_schema", "test_table");
        TableSchema tableSchema =
                TableSchema.builder()
                        .column(PhysicalColumn.of("id", BasicType.LONG_TYPE, 22, false, null, "id"))
                        .column(
                                PhysicalColumn.of(
                                        "name", BasicType.STRING_TYPE, 128, false, null, "name"))
                        .column(
                                PhysicalColumn.of(
                                        "age", BasicType.INT_TYPE, (Long) null, true, null, "age"))
                        .column(
                                PhysicalColumn.of(
                                        "createTime",
                                        LocalTimeType.LOCAL_DATE_TIME_TYPE,
                                        3,
                                        true,
                                        null,
                                        "createTime"))
                        .column(
                                PhysicalColumn.of(
                                        "lastUpdateTime",
                                        LocalTimeType.LOCAL_DATE_TIME_TYPE,
                                        3,
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

        java.util.List<String> createTableSqls =
                new DamengCreateTableSqlBuilder(catalogTable, true).build(tablePath);

        Assertions.assertEquals(6, createTableSqls.size());
        Assertions.assertTrue(createTableSqls.get(0).startsWith("CREATE TABLE"));

        String regex1 = "id_\\w+";
        String regex2 = "name_\\w+";
        String actualCreate =
                createTableSqls.get(0).replaceAll(regex1, "id_").replaceAll(regex2, "name_");
        String expectCreate =
                "CREATE TABLE \"test_schema\".\"test_table\" (\n"
                        + "\"id\" BIGINT NOT NULL,\n"
                        + "\"name\" VARCHAR2(128) NOT NULL,\n"
                        + "\"age\" INT,\n"
                        + "\"createTime\" TIMESTAMP,\n"
                        + "\"lastUpdateTime\" TIMESTAMP,\n"
                        + "CONSTRAINT id_ PRIMARY KEY (\"id\"),\n"
                        + "\tCONSTRAINT name_ UNIQUE (\"name\")\n"
                        + ")";
        Assertions.assertEquals(expectCreate, actualCreate);

        Assertions.assertEquals(
                "COMMENT ON COLUMN \"test_schema\".\"test_table\".\"id\" IS 'id'",
                createTableSqls.get(1));
        Assertions.assertEquals(
                "COMMENT ON COLUMN \"test_schema\".\"test_table\".\"name\" IS 'name'",
                createTableSqls.get(2));
        Assertions.assertEquals(
                "COMMENT ON COLUMN \"test_schema\".\"test_table\".\"age\" IS 'age'",
                createTableSqls.get(3));
        Assertions.assertEquals(
                "COMMENT ON COLUMN \"test_schema\".\"test_table\".\"createTime\" IS 'createTime'",
                createTableSqls.get(4));
        Assertions.assertEquals(
                "COMMENT ON COLUMN \"test_schema\".\"test_table\".\"lastUpdateTime\" IS 'lastUpdateTime'",
                createTableSqls.get(5));

        // skip index
        java.util.List<String> createTableSqlsSkipIndex =
                new DamengCreateTableSqlBuilder(catalogTable, false).build(tablePath);

        Assertions.assertEquals(6, createTableSqlsSkipIndex.size());
        String expectSkipIndex =
                "CREATE TABLE \"test_schema\".\"test_table\" (\n"
                        + "\"id\" BIGINT NOT NULL,\n"
                        + "\"name\" VARCHAR2(128) NOT NULL,\n"
                        + "\"age\" INT,\n"
                        + "\"createTime\" TIMESTAMP,\n"
                        + "\"lastUpdateTime\" TIMESTAMP\n"
                        + ")";
        Assertions.assertEquals(expectSkipIndex, createTableSqlsSkipIndex.get(0));
        Assertions.assertEquals(
                "COMMENT ON COLUMN \"test_schema\".\"test_table\".\"id\" IS 'id'",
                createTableSqlsSkipIndex.get(1));
        Assertions.assertEquals(
                "COMMENT ON COLUMN \"test_schema\".\"test_table\".\"lastUpdateTime\" IS 'lastUpdateTime'",
                createTableSqlsSkipIndex.get(5));
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
}
