/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.redshift;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;

public class RedshiftCatalogTest {

    private static final CatalogTable CATALOG_TABLE =
            CatalogTable.of(
                    TableIdentifier.of("catalog", "database", "table"),
                    TableSchema.builder()
                            .columns(
                                    Arrays.asList(
                                            PhysicalColumn.of(
                                                    "test",
                                                    BasicType.STRING_TYPE,
                                                    (Long) null,
                                                    true,
                                                    null,
                                                    ""),
                                            PhysicalColumn.of(
                                                    "test2",
                                                    BasicType.STRING_TYPE,
                                                    (Long) null,
                                                    true,
                                                    null,
                                                    ""),
                                            PhysicalColumn.of(
                                                    "test3",
                                                    BasicType.STRING_TYPE,
                                                    (Long) null,
                                                    true,
                                                    null,
                                                    "")))
                            .primaryKey(
                                    new PrimaryKey(
                                            "test_primary_keys", Arrays.asList("test", "test2")))
                            .build(),
                    Collections.emptyMap(),
                    Collections.emptyList(),
                    "comment");

    @Test
    void testCreateTableSqlWithPrimaryKeys() {
        RedshiftCatalogFactory factory = new RedshiftCatalogFactory();
        RedshiftCatalog catalog =
                (RedshiftCatalog)
                        factory.createCatalog(
                                "test",
                                ReadonlyConfig.fromMap(
                                        new HashMap<String, Object>() {
                                            {
                                                put(
                                                        "base-url",
                                                        "jdbc:redshift://localhost:5432/test");
                                                put("username", "test");
                                                put("password", "test");
                                            }
                                        }));
        String sql = catalog.getCreateTableSql(TablePath.of("test.test.test"), CATALOG_TABLE, true);
        Assertions.assertEquals(
                "CREATE TABLE \"test\".\"test\" (\n"
                        + "\"test\" CHARACTER VARYING(65535),\n"
                        + "\"test2\" CHARACTER VARYING(65535),\n"
                        + "\"test3\" CHARACTER VARYING(65535),\n"
                        + "PRIMARY KEY (\"test\",\"test2\")\n"
                        + ");",
                sql);
    }
}
