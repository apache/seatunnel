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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.mysql;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.psql.PostgresCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.sqlserver.SqlServerCatalog;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.sqlserver.SqlServerURLParser;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
@Disabled("Please Test it in your local environment")
class MySqlCatalogTest {

    static JdbcUrlUtil.UrlInfo sqlParse =
            SqlServerURLParser.parse("jdbc:sqlserver://127.0.0.1:1434;database=TestDB");
    static JdbcUrlUtil.UrlInfo MysqlUrlInfo =
            JdbcUrlUtil.getUrlInfo("jdbc:mysql://127.0.0.1:33061/liuliTest?useSSL=false");
    static JdbcUrlUtil.UrlInfo pg =
            JdbcUrlUtil.getUrlInfo("jdbc:postgresql://127.0.0.1:5432/liulitest");
    static TablePath tablePathSQL;
    static TablePath tablePathMySql;
    static TablePath tablePathPG;
    static TablePath tablePathOracle;
    private static String databaseName = "liuliTest";
    private static String schemaName = "dbo";
    private static String tableName = "AllDataTest";

    static SqlServerCatalog sqlServerCatalog;
    static MySqlCatalog mySqlCatalog;
    static PostgresCatalog postgresCatalog;

    static CatalogTable postgresCatalogTable;
    static CatalogTable mySqlCatalogTable;
    static CatalogTable sqlServerCatalogTable;

    @Test
    void listDatabases() {}

    @Test
    void listTables() {}

    @Test
    void getColumnsDefaultValue() {}

    @BeforeAll
    static void before() {
        tablePathSQL = TablePath.of(databaseName, "sqlserver_to_mysql");
        tablePathMySql = TablePath.of(databaseName, "mysql_to_mysql");
        tablePathPG = TablePath.of(databaseName, "pg_to_mysql");
        tablePathOracle = TablePath.of(databaseName, "oracle_to_mysql");
        sqlServerCatalog = new SqlServerCatalog("sqlserver", "sa", "root@123", sqlParse, null);
        mySqlCatalog = new MySqlCatalog("mysql", "root", "root@123", MysqlUrlInfo);
        postgresCatalog = new PostgresCatalog("postgres", "postgres", "postgres", pg, null);
        mySqlCatalog.open();
        sqlServerCatalog.open();
        postgresCatalog.open();
    }

    @Test
    @Order(1)
    void getTable() {
        postgresCatalogTable =
                postgresCatalog.getTable(
                        TablePath.of("liulitest", "public", "pg_types_table_no_array"));
        mySqlCatalogTable = mySqlCatalog.getTable(TablePath.of("liuliTest", "AllTypeCol"));
        sqlServerCatalogTable =
                sqlServerCatalog.getTable(TablePath.of("TestDB", "dbo", "AllDataTest"));
    }

    @Test
    @Order(2)
    void createTableInternal() {
        mySqlCatalog.createTable(tablePathMySql, mySqlCatalogTable, true);
        mySqlCatalog.createTable(tablePathPG, postgresCatalogTable, true);
        mySqlCatalog.createTable(tablePathSQL, sqlServerCatalogTable, true);
    }

    @Disabled
    // Manually dropping tables
    @Test
    void dropTableInternal() {
        mySqlCatalog.dropTable(tablePathSQL, true);
        mySqlCatalog.dropTable(tablePathMySql, true);
        mySqlCatalog.dropTable(tablePathPG, true);
    }

    @Test
    void createDatabaseInternal() {}

    @Test
    void dropDatabaseInternal() {}

    @AfterAll
    static void after() {
        sqlServerCatalog.close();
        mySqlCatalog.close();
        postgresCatalog.close();
    }
}
