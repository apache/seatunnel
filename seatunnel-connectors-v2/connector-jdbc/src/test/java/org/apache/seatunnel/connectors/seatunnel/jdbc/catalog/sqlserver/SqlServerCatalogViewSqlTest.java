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

import org.apache.seatunnel.api.table.catalog.TablePath;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class SqlServerCatalogViewSqlTest {

    private final SqlServerCatalog catalog =
            new SqlServerCatalog(
                    "sqlserver",
                    "sa",
                    "secret",
                    SqlServerURLParser.parse("jdbc:sqlserver://127.0.0.1:1433;database=shop"),
                    "dbo",
                    null);

    @Test
    void existenceCheckCoversTablesAndViews() {
        Assertions.assertEquals(
                "SELECT TABLE_SCHEMA, TABLE_NAME FROM [shop].INFORMATION_SCHEMA.TABLES"
                        + " WHERE TABLE_TYPE IN ('BASE TABLE', 'VIEW')"
                        + "  and  TABLE_SCHEMA = 'dbo' and TABLE_NAME = 'v_order'",
                catalog.getTableWithConditionSql(TablePath.of("shop", "dbo", "v_order")));
        Assertions.assertTrue(
                catalog.getListTableSql("shop").endsWith("WHERE TABLE_TYPE = 'BASE TABLE'"),
                "table listing stays tables only");
    }

    @Test
    void columnsOfOneObjectComeFromTablesAndViews() {
        String sql = catalog.getSelectColumnsSql(TablePath.of("shop", "dbo", "v_order"));
        Assertions.assertTrue(sql.contains("FROM sys.objects tbl\n"), sql);
        Assertions.assertTrue(
                sql.contains(
                        "WHERE tbl.type IN ('U', 'V') AND schema_name(tbl.schema_id) = 'dbo'"
                                + " AND tbl.name = 'v_order'\n"),
                sql);
    }

    @Test
    void theCdcTemplateStaysOnTables() {
        Assertions.assertTrue(
                SqlServerCatalog.SELECT_COLUMNS_SQL_TEMPLATE.contains("FROM sys.tables tbl\n"));
        Assertions.assertFalse(SqlServerCatalog.SELECT_COLUMNS_SQL_TEMPLATE.contains("tbl.type"));
    }
}
