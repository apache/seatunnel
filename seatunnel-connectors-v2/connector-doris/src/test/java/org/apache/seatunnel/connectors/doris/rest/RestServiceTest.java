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

package org.apache.seatunnel.connectors.doris.rest;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.connectors.doris.source.DorisSourceTable;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class RestServiceTest {

    @Test
    void quotesReadFieldsThatAreDorisKeywords() {
        // Column names taken from the doris.read.field value reported in issue 13369. Doris FE
        // rejected the unquoted "From" column with a parse error, so every projected column must
        // be wrapped in backticks.
        DorisSourceTable table =
                sourceTable(Arrays.asList("Date", "Version", "From", "desc", "to", "system"), null);

        Assertions.assertEquals(
                "select `Date`,`Version`,`From`,`desc`,`to`,`system` from `test`.`test`",
                RestService.buildQuerySql(table, "test", "test"));
    }

    @Test
    void escapesBackticksInsideReadFields() {
        // A backtick inside a quoted Doris identifier is written as two backticks, the same rule
        // DorisCatalogUtil already applies to partition names.
        DorisSourceTable table = sourceTable(Collections.singletonList("a`b"), null);

        Assertions.assertEquals(
                "select `a``b` from `db`.`tbl`", RestService.buildQuerySql(table, "db", "tbl"));
    }

    @Test
    void keepsStarProjectionAndFilterQueryWhenSchemaIsEmpty() {
        // An empty schema keeps the original "select *" projection, and the user filter query is
        // appended unchanged.
        DorisSourceTable table = sourceTable(Collections.emptyList(), "`from` > 1");

        Assertions.assertEquals(
                "select * from `db`.`tbl` where `from` > 1",
                RestService.buildQuerySql(table, "db", "tbl"));
    }

    private static DorisSourceTable sourceTable(List<String> columnNames, String filterQuery) {
        List<Column> columns =
                columnNames.stream()
                        .map(
                                name ->
                                        PhysicalColumn.of(
                                                name,
                                                BasicType.STRING_TYPE,
                                                (Long) null,
                                                true,
                                                null,
                                                null))
                        .collect(Collectors.toList());
        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("doris", "db", "tbl"),
                        TableSchema.builder().columns(columns).build(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        "");
        return DorisSourceTable.builder()
                .tablePath(TablePath.of("db", "tbl"))
                .catalogTable(catalogTable)
                .filterQuery(filterQuery)
                .build();
    }
}
