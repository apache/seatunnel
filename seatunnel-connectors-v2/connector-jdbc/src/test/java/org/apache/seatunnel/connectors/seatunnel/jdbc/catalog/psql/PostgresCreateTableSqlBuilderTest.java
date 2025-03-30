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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.psql;

import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.connectors.seatunnel.jdbc.internal.dialect.DatabaseIdentifier;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;

class PostgresCreateTableSqlBuilderTest {

    @Test
    void build() {
        Arrays.asList(true, false)
                .forEach(
                        otherDB -> {
                            CatalogTable catalogTable = catalogTable(otherDB);
                            PostgresCreateTableSqlBuilder postgresCreateTableSqlBuilder =
                                    new PostgresCreateTableSqlBuilder(catalogTable, true);
                            String createTableSql =
                                    postgresCreateTableSqlBuilder.build(
                                            catalogTable.getTableId().toTablePath());
                            String pattern =
                                    "CREATE TABLE \"test\" \\(\n"
                                            + "\"id\" int4 NOT NULL,\n"
                                            + "\"name\" text NOT NULL,\n"
                                            + "\"age\" int4 NOT NULL,\n"
                                            + "\tCONSTRAINT \"([a-zA-Z0-9]+)\" PRIMARY KEY \\(\"id\",\"name\"\\),\n"
                                            + "\tCONSTRAINT \"([a-zA-Z0-9]+)\" UNIQUE \\(\"name\"\\)\n"
                                            + "\\);";
                            Assertions.assertTrue(
                                    Pattern.compile(pattern).matcher(createTableSql).find());

                            Assertions.assertEquals(
                                    Lists.newArrayList("CREATE INDEX ON \"test\"(\"age\");"),
                                    postgresCreateTableSqlBuilder.getCreateIndexSqls());

                            // skip index
                            PostgresCreateTableSqlBuilder postgresCreateTableSqlBuilderSkipIndex =
                                    new PostgresCreateTableSqlBuilder(catalogTable, false);
                            String createTableSqlSkipIndex =
                                    postgresCreateTableSqlBuilderSkipIndex.build(
                                            catalogTable.getTableId().toTablePath());
                            Assertions.assertEquals(
                                    "CREATE TABLE \"test\" (\n"
                                            + "\"id\" int4 NOT NULL,\n"
                                            + "\"name\" text NOT NULL,\n"
                                            + "\"age\" int4 NOT NULL\n"
                                            + ");",
                                    createTableSqlSkipIndex);
                            Assertions.assertEquals(
                                    Lists.newArrayList(),
                                    postgresCreateTableSqlBuilderSkipIndex.getCreateIndexSqls());
                        });
    }

    private CatalogTable catalogTable(boolean otherDB) {
        TableIdentifier tableIdentifier =
                TableIdentifier.of(
                        otherDB ? DatabaseIdentifier.MYSQL : DatabaseIdentifier.POSTGRESQL,
                        "public",
                        "test");
        List<Column> columns;
        if (otherDB) {
            columns =
                    Lists.newArrayList(
                            PhysicalColumn.of("id", BasicType.INT_TYPE, 0, false, null, ""),
                            PhysicalColumn.of("name", BasicType.STRING_TYPE, 0, false, null, ""),
                            PhysicalColumn.of("age", BasicType.INT_TYPE, 0, false, null, ""));
        } else {
            columns =
                    Lists.newArrayList(
                            PhysicalColumn.of(
                                    "id",
                                    BasicType.INT_TYPE,
                                    0,
                                    false,
                                    null,
                                    "",
                                    "int4",
                                    false,
                                    false,
                                    null,
                                    Collections.emptyMap(),
                                    null),
                            PhysicalColumn.of(
                                    "name",
                                    BasicType.STRING_TYPE,
                                    0,
                                    false,
                                    null,
                                    "",
                                    "text",
                                    false,
                                    false,
                                    null,
                                    Collections.emptyMap(),
                                    null),
                            PhysicalColumn.of(
                                    "age",
                                    BasicType.INT_TYPE,
                                    0,
                                    false,
                                    null,
                                    "",
                                    "int4",
                                    false,
                                    false,
                                    null,
                                    Collections.emptyMap(),
                                    null));
        }
        TableSchema tableSchema =
                TableSchema.builder()
                        .columns(columns)
                        .primaryKey(PrimaryKey.of("pk_id_name", Lists.newArrayList("id", "name")))
                        .constraintKey(
                                Lists.newArrayList(
                                        ConstraintKey.of(
                                                ConstraintKey.ConstraintType.UNIQUE_KEY,
                                                "unique_name",
                                                Lists.newArrayList(
                                                        ConstraintKey.ConstraintKeyColumn.of(
                                                                "name",
                                                                ConstraintKey.ColumnSortType.ASC))),
                                        ConstraintKey.of(
                                                ConstraintKey.ConstraintType.INDEX_KEY,
                                                "index_age",
                                                Lists.newArrayList(
                                                        ConstraintKey.ConstraintKeyColumn.of(
                                                                "age",
                                                                ConstraintKey.ColumnSortType
                                                                        .ASC)))))
                        .build();

        return CatalogTable.of(
                tableIdentifier,
                tableSchema,
                Collections.emptyMap(),
                Collections.emptyList(),
                "test table");
    }
}
