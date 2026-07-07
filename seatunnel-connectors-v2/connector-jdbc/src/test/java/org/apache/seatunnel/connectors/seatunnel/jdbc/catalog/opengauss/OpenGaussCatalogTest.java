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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.opengauss;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.stream.Collectors;

@Disabled("Please Test it in your local environment")
@Slf4j
class OpenGaussCatalogTest {

    static OpenGaussCatalog catalog;

    @BeforeAll
    static void before() {
        /** env: opengauss 6.0.3 LTS on docker */
        catalog =
                new OpenGaussCatalog(
                        "gaussdb",
                        "gaussdb",
                        "openGauss&123",
                        JdbcUrlUtil.getUrlInfo("jdbc:opengauss://192.168.1.10:8000/postgres"),
                        "gaussdb",
                        "org.opengauss.Driver");

        catalog.open();
    }

    @Test
    void testGetTableSchemaAfterColsDropped() {
        String database = "seatunnel";
        String schemaName = "public";
        String tableName = "opengauss_cols_drop_test";
        TablePath tablePath = new TablePath(database, schemaName, tableName);
        catalog.createDatabase(tablePath, true);
        catalog.dropTable(tablePath, true);
        // create table
        String ddlSql =
                "create table if not exists "
                        + tableName
                        + "("
                        + "c1 int,"
                        + "c2 varchar(50),"
                        + "c3 int,"
                        + "c4 text"
                        + ")";
        catalog.executeSql(tablePath, ddlSql);
        CatalogTable table = catalog.getTable(tablePath);
        List<Column> cols = table.getTableSchema().getColumns();
        String colsString = cols.stream().map(Column::getName).collect(Collectors.joining(","));
        Assertions.assertEquals("c1,c2,c3,c4", colsString);
        log.info("raw cols: {}", colsString);
        // drop columns
        String dropColsSql = "alter table " + tableName + " drop column c3," + " drop column c4";
        catalog.executeSql(tablePath, dropColsSql);
        CatalogTable resultCatalogTable = catalog.getTable(tablePath);
        List<Column> resultCols = resultCatalogTable.getTableSchema().getColumns();
        String resultColsString =
                resultCols.stream().map(Column::getName).collect(Collectors.joining(","));
        Assertions.assertEquals("c1,c2", resultColsString);
        log.info("result cols: {}", resultColsString);
        Assertions.assertNotEquals(colsString, resultColsString);
    }
}
