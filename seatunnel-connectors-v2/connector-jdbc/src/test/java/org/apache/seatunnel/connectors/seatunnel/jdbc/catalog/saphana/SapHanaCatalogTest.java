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

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.saphana;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.sql.SQLException;
import java.util.Optional;

public class SapHanaCatalogTest {

    @Test
    public void testResolveQueryTableReturnsNoOriginPath() throws SQLException {
        // HANA maps TablePath namespaces differently from the generic origin path
        // (TablePath.databaseName is the HANA schema in the catalog, but the generic path fills
        // it from ResultSetMetaData catalog name), so resolving a physical origin could merge a
        // same-named table from the wrong schema. The override must therefore return an empty
        // origin so query_table_metadata_merge skips the merge entirely; removing the guard
        // reintroduces that wrong-schema risk.
        SapHanaCatalog catalog =
                new SapHanaCatalog(
                        "saphana",
                        "user",
                        "password",
                        JdbcUrlUtil.getUrlInfo("jdbc:sap://localhost:30015"),
                        null,
                        "com.sap.db.jdbc.Driver");
        SapHanaCatalog spy = Mockito.spy(catalog);
        CatalogTable tableOfQuery = Mockito.mock(CatalogTable.class);
        Mockito.doReturn(tableOfQuery).when(spy).getTable(Mockito.anyString());

        Assertions.assertSame(
                tableOfQuery, spy.resolveQueryTable("SELECT * FROM \"TEST\".\"T\"").getTable());
        Assertions.assertEquals(
                Optional.empty(),
                spy.resolveQueryTable("SELECT * FROM \"TEST\".\"T\"").getSinglePhysicalTablePath());
    }
}
