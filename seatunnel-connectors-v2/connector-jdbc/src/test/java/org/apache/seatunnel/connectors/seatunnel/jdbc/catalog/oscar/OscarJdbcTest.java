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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.oscar;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseNotExistException;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

@Disabled("Please Test it in your local environment")
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
public class OscarJdbcTest {

    private static final JdbcUrlUtil.UrlInfo OSCAR_URL_INFO =
            JdbcUrlUtil.getUrlInfo("jdbc:oscar://127.0.0.1:2003/test");

    private static final String DATABASE_NAME = "TEST";
    private static final String SCHEMA_NAME = "OSCAR_USER01";
    private static final String TABLE_NAME = "STUDENT_INFO";

    private static final TablePath TABLE_PATH_OSCAR =
            TablePath.of(DATABASE_NAME, SCHEMA_NAME, TABLE_NAME);

    private static OscarCatalog OSCAR_CATALOG;

    private static CatalogTable OSCAR_CATALOGTABLE;

    @BeforeAll
    static void before() {
        OSCAR_CATALOG =
                new OscarCatalog(
                        "OSCAR_CATALOG",
                        "sysdba",
                        "Szoscar@55",
                        OSCAR_URL_INFO,
                        null,
                        "com.oscar.Driver");
        OSCAR_CATALOG.open();
    }

    @Test
    @Order(1)
    void exists() {
        /**
         * before execute sql drop schema if exists OSCAR_USER01 cascade; create schema
         * OSCAR_USER01; drop table if exists OSCAR_USER01.STUDENT_INFO; create table
         * OSCAR_USER01.STUDENT_INFO(id int,name varchar(50),age int);
         */
        Assertions.assertTrue(OSCAR_CATALOG.databaseExists(DATABASE_NAME));
        Assertions.assertTrue(OSCAR_CATALOG.tableExists(TABLE_PATH_OSCAR));
    }

    @Test
    @Order(2)
    void createTableInternal() {
        Assertions.assertDoesNotThrow(
                () -> OSCAR_CATALOGTABLE = OSCAR_CATALOG.getTable(TABLE_PATH_OSCAR));
        Assertions.assertDoesNotThrow(
                () ->
                        OSCAR_CATALOG.createTable(
                                TablePath.of(DATABASE_NAME, SCHEMA_NAME, TABLE_NAME + "_test"),
                                OSCAR_CATALOGTABLE,
                                false,
                                true));
    }

    @Test
    @Order(3)
    void dropTableInternal() {
        Assertions.assertDoesNotThrow(
                () ->
                        OSCAR_CATALOG.dropTable(
                                TablePath.of(DATABASE_NAME, SCHEMA_NAME, TABLE_NAME + "_test"),
                                false));
    }

    @Test
    @Order(4)
    void createDatabaseInternal() {
        Assertions.assertDoesNotThrow(() -> OSCAR_CATALOG.createDatabase(TABLE_PATH_OSCAR, true));
        Assertions.assertThrows(
                DatabaseAlreadyExistException.class,
                () -> OSCAR_CATALOG.createDatabase(TABLE_PATH_OSCAR, false));
        RuntimeException catalogException =
                Assertions.assertThrows(
                        RuntimeException.class,
                        () ->
                                OSCAR_CATALOG.createDatabase(
                                        TablePath.of("test_db.test.test1"), true));
        Assertions.assertInstanceOf(UnsupportedOperationException.class, catalogException);
        RuntimeException runtimeException =
                Assertions.assertThrows(
                        RuntimeException.class,
                        () ->
                                OSCAR_CATALOG.createDatabase(
                                        TablePath.of("test_db.test.test1"), false));
        Assertions.assertInstanceOf(UnsupportedOperationException.class, runtimeException);
    }

    @Test
    @Order(5)
    void dropDatabaseInternal() {
        Assertions.assertDoesNotThrow(
                () -> OSCAR_CATALOG.dropDatabase(TablePath.of("test_db.test.test1"), true));
        Assertions.assertThrows(
                DatabaseNotExistException.class,
                () -> OSCAR_CATALOG.dropDatabase(TablePath.of("test_db.test.test1"), false));
        RuntimeException runtimeException =
                Assertions.assertThrows(
                        RuntimeException.class,
                        () -> OSCAR_CATALOG.dropDatabase(TABLE_PATH_OSCAR, true));
        Assertions.assertInstanceOf(UnsupportedOperationException.class, runtimeException);
        RuntimeException catalogException =
                Assertions.assertThrows(
                        RuntimeException.class,
                        () -> OSCAR_CATALOG.dropDatabase(TABLE_PATH_OSCAR, false));
        Assertions.assertInstanceOf(UnsupportedOperationException.class, catalogException);
    }

    @Test
    @Order(6)
    void truncateTableInternal() {
        Assertions.assertDoesNotThrow(() -> OSCAR_CATALOG.truncateTable(TABLE_PATH_OSCAR, false));
        Assertions.assertDoesNotThrow(() -> OSCAR_CATALOG.truncateTable(TABLE_PATH_OSCAR, true));
    }

    @Test
    @Order(7)
    void listTablesInternal() {
        Assertions.assertDoesNotThrow(() -> OSCAR_CATALOG.listTables(DATABASE_NAME));
    }

    @Test
    @Order(8)
    void existsData() {
        Assertions.assertFalse(OSCAR_CATALOG.isExistsData(TABLE_PATH_OSCAR));
        Assertions.assertTrue(OSCAR_CATALOG.isExistsData(TablePath.of("TEST.SYSDBA.T_DOUBLE")));
    }
}
