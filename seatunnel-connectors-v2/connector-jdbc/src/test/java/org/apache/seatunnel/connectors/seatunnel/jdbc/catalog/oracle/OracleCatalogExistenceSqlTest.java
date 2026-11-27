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

import org.apache.seatunnel.api.table.catalog.TablePath;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

class OracleCatalogExistenceSqlTest {

    @Test
    void existenceCheckCoversTablesAndViews() {
        OracleCatalog catalog =
                new OracleCatalog(
                        "oracle",
                        "SCOTT",
                        "TIGER",
                        OracleURLParser.parse("jdbc:oracle:thin:@//localhost:1521/FREEPDB1"),
                        "SCOTT",
                        "oracle.jdbc.OracleDriver");
        String sql = catalog.getTableWithConditionSql(TablePath.of("FREEPDB1", "SCOTT", "V_ORDER"));
        Assertions.assertTrue(
                sql.startsWith(catalog.getListTableSql("FREEPDB1")),
                "keeps the table filters of the table listing: " + sql);
        Assertions.assertTrue(
                sql.endsWith(
                        "  and  OWNER = 'SCOTT' and table_name = 'V_ORDER'"
                                + " UNION ALL SELECT OWNER, VIEW_NAME FROM ALL_VIEWS"
                                + " WHERE OWNER = 'SCOTT' and VIEW_NAME = 'V_ORDER'"),
                sql);
        Assertions.assertFalse(
                catalog.getListTableSql("FREEPDB1").contains("ALL_VIEWS"),
                "table listing stays tables only");
    }
}
