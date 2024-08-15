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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.oracle;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TablePath;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.List;

@Disabled("Please Test it in your local environment")
class OracleCatalogTest {

    static OracleCatalog catalog;

    @BeforeAll
    static void before() {
        catalog =
                new OracleCatalog(
                        "oracle",
                        "test",
                        "oracle",
                        OracleURLParser.parse("jdbc:oracle:thin:@127.0.0.1:1521:xe"),
                        null);

        catalog.open();
    }

    @Test
    void testCatalog() {

        List<String> strings = catalog.listDatabases();

        CatalogTable table = catalog.getTable(TablePath.of("XE", "TEST", "PG_TYPES_TABLE_CP1"));

        catalog.createTable(new TablePath("XE", "TEST", "TEST003"), table, false);
    }

    @Test
    void exist() {
        Assertions.assertTrue(catalog.databaseExists("ORCLCDB"));
        Assertions.assertTrue(catalog.tableExists(TablePath.of("ORCLCDB", "C##GGUSER", "myTable")));
        Assertions.assertFalse(catalog.databaseExists("ORCL"));
        Assertions.assertTrue(
                catalog.tableExists(
                        TablePath.of("ORCLCDB", "CDC_PDB", "ads_index_public_health_data")));
        Assertions.assertTrue(
                catalog.tableExists(TablePath.of("ORCLCDB", "CDC_PDB", "ADS_INDEX_DISEASE_DATA")));
    }
}
