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

package org.apache.seatunnel.e2e.connector.doris;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.connectors.doris.catalog.DorisCatalog;
import org.apache.seatunnel.connectors.doris.catalog.DorisCatalogFactory;
import org.apache.seatunnel.connectors.doris.config.DorisOptions;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Slf4j
public class DorisCatalogIT extends AbstractDorisIT {

    private static final String DATABASE = "test";
    private static final String SINK_TABLE = "doris_catalog_e2e";
    private DorisCatalogFactory factory;
    private DorisCatalog catalog;

    @BeforeAll
    public void init() {
        initCatalogFactory();
        initCatalog();
    }

    private void initCatalogFactory() {
        if (factory == null) {
            factory = new DorisCatalogFactory();
        }
    }

    private void initCatalog() {
        String catalogName = "doris";
        String frontEndNodes = container.getHost() + ":" + HTTP_PORT;

        factory = new DorisCatalogFactory();

        Map<String, Object> map = new HashMap<>();
        map.put(DorisOptions.FENODES.key(), frontEndNodes);
        map.put(DorisOptions.QUERY_PORT.key(), QUERY_PORT);
        map.put(DorisOptions.USERNAME.key(), USERNAME);
        map.put(DorisOptions.PASSWORD.key(), PASSWORD);
        map.put(DorisOptions.DEFAULT_DATABASE.key(), PASSWORD);

        catalog = (DorisCatalog) factory.createCatalog(catalogName, ReadonlyConfig.fromMap(map));

        catalog.open();
    }

    @Test
    void factoryIdentifier() {
        Assertions.assertEquals(factory.factoryIdentifier(), "Doris");
    }

    @Test
    void optionRule() {
        Assertions.assertNotNull(factory.optionRule());
    }

    @Test
    public void testCatalog() {

        if (catalog == null) {
            return;
        }

        TablePath tablePath = TablePath.of(DATABASE, SINK_TABLE);

        TableSchema.Builder builder = TableSchema.builder();
        builder.column(PhysicalColumn.of("k1", BasicType.INT_TYPE, 10, false, 0, "k1"));
        builder.column(PhysicalColumn.of("k2", BasicType.STRING_TYPE, 64, false, "", "k2"));
        builder.column(PhysicalColumn.of("v1", BasicType.DOUBLE_TYPE, 10, true, null, "v1"));
        builder.column(PhysicalColumn.of("v2", new DecimalType(10, 2), 0, false, 0.1, "v2"));
        builder.primaryKey(PrimaryKey.of("pk", Arrays.asList("k1", "k2")));
        CatalogTable catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("doris", DATABASE, SINK_TABLE),
                        builder.build(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        "test");

        boolean dbCreated = false;

        List<String> databases = catalog.listDatabases();
        Assertions.assertEquals(databases.size(), 1);

        if (!catalog.databaseExists(tablePath.getDatabaseName())) {
            catalog.createDatabase(tablePath, false);
            dbCreated = true;
        }

        Assertions.assertFalse(catalog.tableExists(tablePath));
        catalog.createTable(tablePath, catalogTable, false);
        Assertions.assertTrue(catalog.tableExists(tablePath));

        List<String> tables = catalog.listTables(tablePath.getDatabaseName());
        Assertions.assertEquals(tables.size(), 1);

        catalog.dropTable(tablePath, false);
        Assertions.assertFalse(catalog.tableExists(tablePath));

        if (dbCreated) {
            catalog.dropDatabase(tablePath, false);
            Assertions.assertFalse(catalog.databaseExists(tablePath.getDatabaseName()));
        }
    }

    @AfterAll
    public void close() {
        if (catalog != null) {
            catalog.close();
        }
    }
}
