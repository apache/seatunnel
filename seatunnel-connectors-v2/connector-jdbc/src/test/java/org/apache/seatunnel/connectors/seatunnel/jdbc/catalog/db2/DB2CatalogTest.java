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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.db2;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

public class DB2CatalogTest {

    @Test
    void testCreateTableSqlMarksPrimaryKeyColumnsNotNull() {
        TablePath tablePath = TablePath.of("E2E", "SINK");
        TableSchema tableSchema =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.of(
                                        "C_INT", BasicType.INT_TYPE, (Long) null, true, null, null))
                        .column(
                                PhysicalColumn.of(
                                        "C_INTEGER",
                                        BasicType.INT_TYPE,
                                        (Long) null,
                                        true,
                                        null,
                                        null))
                        .primaryKey(PrimaryKey.of("PK_SINK", Arrays.asList("C_INT")))
                        .build();
        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("db2", "E2E", "SINK"),
                        tableSchema,
                        new HashMap<>(),
                        new ArrayList<>(),
                        null);
        DB2Catalog catalog =
                new DB2Catalog(
                        "db2",
                        "db2inst1",
                        "123456",
                        JdbcUrlUtil.getUrlInfo("jdbc:db2://127.0.0.1:50000/E2E"),
                        "E2E",
                        null);

        String createTableSql = catalog.getCreateTableSql(tablePath, catalogTable, true);

        Assertions.assertEquals(
                "CREATE TABLE \"E2E\".\"SINK\" (\"C_INT\" INT NOT NULL, "
                        + "\"C_INTEGER\" INT, PRIMARY KEY (\"C_INT\"))",
                createTableSql);
    }
}
