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

import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

import java.util.List;

@Disabled("Please Test it in your local environment")
class PostgresCatalogTest {

    @Test
    void testCatalog() {
        JdbcUrlUtil.UrlInfo urlInfo =
                JdbcUrlUtil.getUrlInfo("jdbc:postgresql://127.0.0.1:5432/st_test");
        PostgresCatalog catalog = new PostgresCatalog("postgres", "postgres", "postgres", urlInfo);

        catalog.open();

        List<String> databases = catalog.listDatabases();
        System.out.println("find databases: " + databases);

        if (!catalog.databaseExists("default")) {
            catalog.createDatabase(TablePath.of("default", null), true);
        }

        databases = catalog.listDatabases();
        System.out.println("find databases: " + databases);

        List<String> st_test = catalog.listTables("st_test");
        System.out.println("find tables: " + st_test);

        CatalogTable table = catalog.getTable(TablePath.of("st_test", "public", "user_info_2"));
        System.out.println("find table: " + table);
    }
}
