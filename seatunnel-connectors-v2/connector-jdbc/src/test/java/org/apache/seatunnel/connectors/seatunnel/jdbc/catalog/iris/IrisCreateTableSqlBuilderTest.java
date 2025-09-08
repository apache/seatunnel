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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.iris;

import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.LocalTimeType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

public class IrisCreateTableSqlBuilderTest {

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

        String createTableSql = new IrisCreateTableSqlBuilder(catalogTable, true).build(tablePath);
        // create table sql is change; The old unit tests are no longer applicable
        String expect =
                "CREATE TABLE \"test_schema\".\"test_table\" (\n"
                        + " %Description 'User table',\n"
                        + "\"id\" BIGINT NOT NULL %Description 'id',\n"
                        + "\"name\" VARCHAR(128) NOT NULL %Description 'name',\n"
                        + "\"age\" INTEGER %Description 'age',\n"
                        + "\"createTime\" TIMESTAMP2 %Description 'createTime',\n"
                        + "\"lastUpdateTime\" TIMESTAMP2 %Description 'lastUpdateTime',\n"
                        + " PRIMARY KEY (\"id\"),\n"
                        + "UNIQUE (\"name\")\n"
                        + ");\n"
                        + "CREATE INDEX test_table_age ON \"test_schema\".\"test_table\"(\"age\");";
        Assertions.assertEquals(expect, createTableSql);

        // skip index
        String createTableSqlSkipIndex =
                new IrisCreateTableSqlBuilder(catalogTable, false).build(tablePath);
        // create table sql is change; The old unit tests are no longer applicable
        String expectSkipIndex =
                "CREATE TABLE \"test_schema\".\"test_table\" (\n"
                        + " %Description 'User table',\n"
                        + "\"id\" BIGINT NOT NULL %Description 'id',\n"
                        + "\"name\" VARCHAR(128) NOT NULL %Description 'name',\n"
                        + "\"age\" INTEGER %Description 'age',\n"
                        + "\"createTime\" TIMESTAMP2 %Description 'createTime',\n"
                        + "\"lastUpdateTime\" TIMESTAMP2 %Description 'lastUpdateTime'\n"
                        + ");\n";
        Assertions.assertEquals(expectSkipIndex, createTableSqlSkipIndex);
    }
}
