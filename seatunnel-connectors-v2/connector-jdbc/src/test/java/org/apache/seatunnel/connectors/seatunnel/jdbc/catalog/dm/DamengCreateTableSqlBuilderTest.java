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

import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

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
}
