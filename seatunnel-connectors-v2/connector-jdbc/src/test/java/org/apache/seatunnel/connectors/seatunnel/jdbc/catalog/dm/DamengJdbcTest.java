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
public class DamengJdbcTest {

    private static final JdbcUrlUtil.UrlInfo DM_URL_INFO =
            JdbcUrlUtil.getUrlInfo("jdbc:dm://172.16.17.156:30236");

    private static final String DATABASE_NAME = "DAMENG";
    private static final String SCHEMA_NAME = "DM_USER01";
    private static final String TABLE_NAME = "STUDENT_INFO";

    private static final TablePath TABLE_PATH_DM =
            TablePath.of(DATABASE_NAME, SCHEMA_NAME, TABLE_NAME);

    private static DamengCatalog DAMENG_CATALOG;

    private static CatalogTable DM_CATALOGTABLE;

    @BeforeAll
    static void before() {
        DAMENG_CATALOG =
                new DamengCatalog("DAMENG_CATALOG", "DM_USER01", "Te$Dt_1234", DM_URL_INFO, null);
        DAMENG_CATALOG.open();
    }

    @Test
    @Order(1)
    void exists() {
        Assertions.assertTrue(DAMENG_CATALOG.databaseExists(DATABASE_NAME));
        Assertions.assertTrue(DAMENG_CATALOG.tableExists(TABLE_PATH_DM));
    }

    @Test
    @Order(2)
    void createTableInternal() {
        Assertions.assertDoesNotThrow(
                () -> DM_CATALOGTABLE = DAMENG_CATALOG.getTable(TABLE_PATH_DM));
        Assertions.assertDoesNotThrow(
                () ->
                        DAMENG_CATALOG.createTable(
                                TablePath.of(DATABASE_NAME, SCHEMA_NAME, TABLE_NAME + "_test"),
                                DM_CATALOGTABLE,
                                false,
                                true));
    }

    @Test
    @Order(3)
    void dropTableInternal() {
        Assertions.assertDoesNotThrow(
                () ->
                        DAMENG_CATALOG.dropTable(
                                TablePath.of(DATABASE_NAME, SCHEMA_NAME, TABLE_NAME + "_test"),
                                false));
    }

    @Test
    @Order(4)
    void createDatabaseInternal() {
        Assertions.assertDoesNotThrow(() -> DAMENG_CATALOG.createDatabase(TABLE_PATH_DM, true));
        Assertions.assertThrows(
                DatabaseAlreadyExistException.class,
                () -> DAMENG_CATALOG.createDatabase(TABLE_PATH_DM, false));
        RuntimeException catalogException =
                Assertions.assertThrows(
                        RuntimeException.class,
                        () ->
                                DAMENG_CATALOG.createDatabase(
                                        TablePath.of("test_db.test.test1"), true));
        Assertions.assertInstanceOf(
                UnsupportedOperationException.class, catalogException.getCause());
        RuntimeException runtimeException =
                Assertions.assertThrows(
                        RuntimeException.class,
                        () ->
                                DAMENG_CATALOG.createDatabase(
                                        TablePath.of("test_db.test.test1"), false));
        Assertions.assertInstanceOf(
                UnsupportedOperationException.class, runtimeException.getCause());
    }

    @Test
    @Order(5)
    void dropDatabaseInternal() {
        Assertions.assertDoesNotThrow(
                () -> DAMENG_CATALOG.dropDatabase(TablePath.of("test_db.test.test1"), true));
        Assertions.assertThrows(
                DatabaseNotExistException.class,
                () -> DAMENG_CATALOG.dropDatabase(TablePath.of("test_db.test.test1"), false));
        RuntimeException runtimeException =
                Assertions.assertThrows(
                        RuntimeException.class,
                        () -> DAMENG_CATALOG.dropDatabase(TABLE_PATH_DM, true));
        Assertions.assertInstanceOf(
                UnsupportedOperationException.class, runtimeException.getCause());
        RuntimeException catalogException =
                Assertions.assertThrows(
                        RuntimeException.class,
                        () -> DAMENG_CATALOG.dropDatabase(TABLE_PATH_DM, false));
        Assertions.assertInstanceOf(
                UnsupportedOperationException.class, catalogException.getCause());
    }

    @Test
    @Order(6)
    void truncateTableInternal() {
        Assertions.assertDoesNotThrow(() -> DAMENG_CATALOG.truncateTable(TABLE_PATH_DM, false));
        Assertions.assertDoesNotThrow(() -> DAMENG_CATALOG.truncateTable(TABLE_PATH_DM, true));
    }

    @Test
    @Order(7)
    void listTablesInternal() {
        Assertions.assertDoesNotThrow(() -> DAMENG_CATALOG.listTables(DATABASE_NAME));
    }

    @Test
    @Order(8)
    void existsData() {
        Assertions.assertFalse(DAMENG_CATALOG.isExistsData(TABLE_PATH_DM));
        Assertions.assertTrue(DAMENG_CATALOG.isExistsData(TablePath.of("DAMENG.HIS.DEPARTMENTS")));
    }
}
