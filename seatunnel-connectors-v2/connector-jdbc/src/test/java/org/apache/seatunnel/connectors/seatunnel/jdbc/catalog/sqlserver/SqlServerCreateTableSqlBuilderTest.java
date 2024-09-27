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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.sqlserver;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Lists;

import java.io.PrintStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

public class SqlServerCreateTableSqlBuilderTest {

    private static final PrintStream CONSOLE = System.out;

    @Test
    public void testBuild() {
        String dataBaseName = "test_database";
        String tableName = "test_table";
        TablePath tablePath = TablePath.of(dataBaseName, tableName);
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
                                        "blob_v",
                                        PrimitiveByteArrayType.INSTANCE,
                                        Long.MAX_VALUE,
                                        true,
                                        null,
                                        "blob_v"))
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
                                                ConstraintKey.ConstraintType.INDEX_KEY,
                                                "name",
                                                Lists.newArrayList(
                                                        ConstraintKey.ConstraintKeyColumn.of(
                                                                "name", null))),
                                        ConstraintKey.of(
                                                ConstraintKey.ConstraintType.INDEX_KEY,
                                                "blob_v",
                                                Lists.newArrayList(
                                                        ConstraintKey.ConstraintKeyColumn.of(
                                                                "blob_v", null)))))
                        .build();
        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("test_catalog", dataBaseName, tableName),
                        tableSchema,
                        new HashMap<>(),
                        new ArrayList<>(),
                        "User table");

        SqlServerCreateTableSqlBuilder sqlServerCreateTableSqlBuilder =
                SqlServerCreateTableSqlBuilder.builder(tablePath, catalogTable, true);
        String createTableSql = sqlServerCreateTableSqlBuilder.build(tablePath, catalogTable);
        // create table sql is change; The old unit tests are no longer applicable
        String expect =
                "IF OBJECT_ID('[test_database].[test_table]', 'U') IS NULL \n"
                        + "BEGIN \n"
                        + "CREATE TABLE [test_database].[test_table] ( \n"
                        + "\t[id] BIGINT NOT NULL, \n"
                        + "\t[name] NVARCHAR(128) NOT NULL, \n"
                        + "\t[age] INT NULL, \n"
                        + "\t[blob_v] VARBINARY(MAX) NULL, \n"
                        + "\t[createTime] DATETIME2 NULL, \n"
                        + "\t[lastUpdateTime] DATETIME2 NULL, \n"
                        + "\tPRIMARY KEY ([id])\n"
                        + ");\n"
                        + "EXEC test_database.sys.sp_addextendedproperty 'MS_Description', N'User table', 'schema', N'null', 'table', N'test_table';\n"
                        + "EXEC test_database.sys.sp_addextendedproperty 'MS_Description', N'blob_v', 'schema', N'null', 'table', N'test_table', 'column', N'blob_v';\n"
                        + "EXEC test_database.sys.sp_addextendedproperty 'MS_Description', N'createTime', 'schema', N'null', 'table', N'test_table', 'column', N'createTime';\n"
                        + "EXEC test_database.sys.sp_addextendedproperty 'MS_Description', N'name', 'schema', N'null', 'table', N'test_table', 'column', N'name';\n"
                        + "EXEC test_database.sys.sp_addextendedproperty 'MS_Description', N'id', 'schema', N'null', 'table', N'test_table', 'column', N'id';\n"
                        + "EXEC test_database.sys.sp_addextendedproperty 'MS_Description', N'age', 'schema', N'null', 'table', N'test_table', 'column', N'age';\n"
                        + "EXEC test_database.sys.sp_addextendedproperty 'MS_Description', N'lastUpdateTime', 'schema', N'null', 'table', N'test_table', 'column', N'lastUpdateTime';\n"
                        + "\n"
                        + "END";

        CONSOLE.println(expect);
        Assertions.assertEquals(expect, createTableSql);

        // skip index
        SqlServerCreateTableSqlBuilder sqlServerCreateTableSqlBuilderSkipIndex =
                SqlServerCreateTableSqlBuilder.builder(tablePath, catalogTable, false);
        String createTableSqlSkipIndex =
                sqlServerCreateTableSqlBuilderSkipIndex.build(tablePath, catalogTable);
        String expectSkipIndex =
                "IF OBJECT_ID('[test_database].[test_table]', 'U') IS NULL \n"
                        + "BEGIN \n"
                        + "CREATE TABLE [test_database].[test_table] ( \n"
                        + "\t[id] BIGINT NOT NULL, \n"
                        + "\t[name] NVARCHAR(128) NOT NULL, \n"
                        + "\t[age] INT NULL, \n"
                        + "\t[blob_v] VARBINARY(MAX) NULL, \n"
                        + "\t[createTime] DATETIME2 NULL, \n"
                        + "\t[lastUpdateTime] DATETIME2 NULL\n"
                        + ");\n"
                        + "EXEC test_database.sys.sp_addextendedproperty 'MS_Description', N'User table', 'schema', N'null', 'table', N'test_table';\n"
                        + "EXEC test_database.sys.sp_addextendedproperty 'MS_Description', N'blob_v', 'schema', N'null', 'table', N'test_table', 'column', N'blob_v';\n"
                        + "EXEC test_database.sys.sp_addextendedproperty 'MS_Description', N'createTime', 'schema', N'null', 'table', N'test_table', 'column', N'createTime';\n"
                        + "EXEC test_database.sys.sp_addextendedproperty 'MS_Description', N'name', 'schema', N'null', 'table', N'test_table', 'column', N'name';\n"
                        + "EXEC test_database.sys.sp_addextendedproperty 'MS_Description', N'id', 'schema', N'null', 'table', N'test_table', 'column', N'id';\n"
                        + "EXEC test_database.sys.sp_addextendedproperty 'MS_Description', N'age', 'schema', N'null', 'table', N'test_table', 'column', N'age';\n"
                        + "EXEC test_database.sys.sp_addextendedproperty 'MS_Description', N'lastUpdateTime', 'schema', N'null', 'table', N'test_table', 'column', N'lastUpdateTime';\n"
                        + "\n"
                        + "END";
        CONSOLE.println(expectSkipIndex);
        Assertions.assertEquals(expectSkipIndex, createTableSqlSkipIndex);
    }
}
