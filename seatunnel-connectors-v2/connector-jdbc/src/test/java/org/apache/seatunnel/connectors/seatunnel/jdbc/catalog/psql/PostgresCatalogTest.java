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
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.mysql.MySqlCatalog;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import lombok.extern.slf4j.Slf4j;

@Disabled("Please Test it in your local environment")
@Slf4j
class PostgresCatalogTest {

    static PostgresCatalog catalog;

    @BeforeAll
    static void before() {
        catalog =
                new PostgresCatalog(
                        "postgres",
                        "pg",
                        "pg#2024",
                        JdbcUrlUtil.getUrlInfo("jdbc:postgresql://127.0.0.1:5432/postgres"),
                        null);

        catalog.open();
    }

    @Test
    void testCatalog() {
        MySqlCatalog mySqlCatalog =
                new MySqlCatalog(
                        "mysql",
                        "root",
                        "root@123",
                        JdbcUrlUtil.getUrlInfo("jdbc:mysql://127.0.0.1:33062/mingdongtest"));

        mySqlCatalog.open();

        CatalogTable table1 =
                mySqlCatalog.getTable(TablePath.of("mingdongtest", "all_types_table_02"));

        CatalogTable table =
                catalog.getTable(TablePath.of("st_test", "public", "all_types_table_02"));
        log.info("find table: " + table);

        catalog.createTable(
                new TablePath("liulitest", "public", "all_types_table_02"), table, false);
    }

    @Test
    void exists() {
        Assertions.assertFalse(catalog.databaseExists("postgres"));
        Assertions.assertFalse(
                catalog.tableExists(TablePath.of("postgres", "pg_catalog", "pg_aggregate")));
        Assertions.assertTrue(catalog.databaseExists("zdykdb"));
        Assertions.assertTrue(
                catalog.tableExists(TablePath.of("zdykdb", "pg_catalog", "pg_class")));
    }
}
