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

package org.apache.seatunnel.connectors.seatunnel.paimon.catalog;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.Path;
import org.apache.paimon.table.FileStoreTable;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class PaimonWithCommentTest {

    private PaimonCatalog paimonCatalog;
    private TableSchema.Builder schemaBuilder;
    private final String CATALOG_NAME = "paimon_catalog";
    private final String DATABASE_NAME = "default";
    private final String TABLE_NAME = "test_with_comment";
    private final String warehousePath = "/tmp/paimon";
    private Catalog catalog;

    @BeforeEach
    public void before() {
        Map<String, Object> properties = new HashMap<>();
        properties.put("warehouse", warehousePath);
        properties.put("plugin_name", "Paimon");
        properties.put("database", DATABASE_NAME);
        properties.put("table", TABLE_NAME);
        Map<String, String> writeProps = new HashMap<>();
        writeProps.put("bucket", "1");
        properties.put("paimon.table.write-props", writeProps);
        ReadonlyConfig config = ReadonlyConfig.fromMap(properties);
        CatalogContext catalogContext = CatalogContext.create(new Path(warehousePath));
        catalog = CatalogFactory.createCatalog(catalogContext);
        paimonCatalog = new PaimonCatalog(CATALOG_NAME, config);
        paimonCatalog.open();
        paimonCatalog.createDatabase(TablePath.of(DATABASE_NAME, TABLE_NAME), true);
        this.schemaBuilder =
                TableSchema.builder()
                        .column(
                                PhysicalColumn.of(
                                        "c_string",
                                        BasicType.STRING_TYPE,
                                        (Long) null,
                                        true,
                                        null,
                                        "c_string"))
                        .column(
                                PhysicalColumn.of(
                                        "c_int",
                                        BasicType.INT_TYPE,
                                        (Long) null,
                                        false,
                                        null,
                                        "c_int"))
                        .column(
                                PhysicalColumn.of(
                                        "c_bigint",
                                        BasicType.LONG_TYPE,
                                        (Long) null,
                                        false,
                                        null,
                                        "c_bigint"));
    }

    @Test
    public void testCreateTableWithCommentAndNullable() throws Catalog.TableNotExistException {
        TableSchema tableSchema =
                schemaBuilder
                        .primaryKey(PrimaryKey.of("pk", Collections.singletonList("c_int")))
                        .build();
        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of(CATALOG_NAME, DATABASE_NAME, TABLE_NAME),
                        tableSchema,
                        new HashMap<>(),
                        new ArrayList<>(),
                        "test table");
        paimonCatalog.createTable(
                TablePath.of(DATABASE_NAME, null, TABLE_NAME), catalogTable, true);

        FileStoreTable table =
                (FileStoreTable) catalog.getTable(Identifier.create(DATABASE_NAME, TABLE_NAME));
        Assertions.assertEquals("test table", table.comment().get());
        table.schema()
                .fields()
                .forEach(
                        field -> {
                            Assertions.assertEquals(field.name(), field.description());
                            if (field.name().equals("c_string")) {
                                Assertions.assertTrue(field.type().isNullable());
                            } else {
                                Assertions.assertFalse(field.type().isNullable());
                            }
                        });
    }

    @AfterEach
    public void after() {
        paimonCatalog.dropDatabase(TablePath.of(DATABASE_NAME, TABLE_NAME), false);
        paimonCatalog.close();
    }
}
