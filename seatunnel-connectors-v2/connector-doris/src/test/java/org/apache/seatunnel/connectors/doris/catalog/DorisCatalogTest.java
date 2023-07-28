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

package org.apache.seatunnel.connectors.doris.catalog;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import java.util.List;

class DorisCatalogTest {

    private static DorisCatalog dorisCatalog = null;

    @BeforeAll
    static void beforeAll() {

        String catalogName = "doris";
        String frontEndHosts = "127.0.0.1:8030";
        Integer queryPort = 9030;
        String username = "root";
        String password = "";

        if (dorisCatalog == null) {
            dorisCatalog =
                    new DorisCatalog(catalogName, frontEndHosts, queryPort, username, password);
            dorisCatalog.open();
        }
    }

    @AfterAll
    static void afterAll() {

        if (dorisCatalog != null) {
            dorisCatalog.close();
        }
    }

    @Test
    void databaseExists() {

        boolean res1 = dorisCatalog.databaseExists("test");
        boolean res2 = dorisCatalog.databaseExists("test1");

        Assertions.assertTrue(res1);
        Assertions.assertFalse(res2);
    }

    @Test
    void listDatabases() {

        List<String> databases = dorisCatalog.listDatabases();
        Assertions.assertEquals(databases.size(), 3);
    }

    @Test
    void listTables() {

        List<String> tables = dorisCatalog.listTables("test");
        Assertions.assertEquals(tables.size(), 15);
    }

    @Test
    void tableExists() {

        boolean res = dorisCatalog.tableExists(TablePath.of("test", "t1"));
        Assertions.assertTrue(res);
    }

    @Test
    void getTable() {

        CatalogTable table = dorisCatalog.getTable(TablePath.of("test", "t1"));
        Assertions.assertEquals(table.getTableId(), TableIdentifier.of("doris", "test", "t1"));
        Assertions.assertEquals(table.getTableSchema().getColumns().size(), 3);
    }

    @Test
    void createTable() {

        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> dorisCatalog.createTable(TablePath.of("test", "test"), null, false));
    }

    @Test
    void dropTable() {

        Assertions.assertThrows(
                UnsupportedOperationException.class,
                () -> dorisCatalog.dropTable(TablePath.of("test", "test"), false));
    }

    @Test
    void createDatabase() {

        Assertions.assertDoesNotThrow(
                () -> dorisCatalog.createDatabase(TablePath.of("test1", null), false));
        Assertions.assertDoesNotThrow(
                () -> dorisCatalog.createDatabase(TablePath.of("test1", null), true));
        Assertions.assertThrows(
                CatalogException.class,
                () -> dorisCatalog.createDatabase(TablePath.of("test1", null), false));
    }

    @Test
    void dropDatabase() {

        Assertions.assertDoesNotThrow(
                () -> dorisCatalog.dropDatabase(TablePath.of("test1", null), false));
        Assertions.assertDoesNotThrow(
                () -> dorisCatalog.dropDatabase(TablePath.of("test1", null), true));
        Assertions.assertThrows(
                CatalogException.class,
                () -> dorisCatalog.dropDatabase(TablePath.of("test1", null), false));
    }
}
