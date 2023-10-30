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

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.google.common.collect.Lists;

import java.util.Collections;

class PostgresCreateTableSqlBuilderTest {

    @Test
    void build() {
        CatalogTable catalogTable = catalogTable();
        PostgresCreateTableSqlBuilder postgresCreateTableSqlBuilder =
                new PostgresCreateTableSqlBuilder(catalogTable);
        String createTableSql =
                postgresCreateTableSqlBuilder.build(catalogTable.getTableId().toTablePath());
        Assertions.assertEquals(
                "CREATE TABLE \"test\" (\n"
                        + "\"id\" int4 NOT NULL PRIMARY KEY,\n"
                        + "\"name\" text NOT NULL,\n"
                        + "\"age\" int4 NOT NULL,\n"
                        + "\tCONSTRAINT unique_name UNIQUE (\"name\")\n"
                        + ");",
                createTableSql);
        Assertions.assertEquals(
                Lists.newArrayList("CREATE INDEX test_index_age ON \"test\"(\"age\");"),
                postgresCreateTableSqlBuilder.getCreateIndexSqls());
    }

    private CatalogTable catalogTable() {
        TableIdentifier tableIdentifier = TableIdentifier.of("postgres", "public", "test");
        TableSchema tableSchema =
                TableSchema.builder()
                        .columns(
                                Lists.newArrayList(
                                        PhysicalColumn.of(
                                                "id", BasicType.INT_TYPE, 0, false, null, ""),
                                        PhysicalColumn.of(
                                                "name", BasicType.STRING_TYPE, 0, false, null, ""),
                                        PhysicalColumn.of(
                                                "age", BasicType.INT_TYPE, 0, false, null, "")))
                        .primaryKey(PrimaryKey.of("pk_id", Lists.newArrayList("id")))
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
