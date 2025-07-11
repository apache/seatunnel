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
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;

import org.apache.paimon.catalog.Catalog;
import org.apache.paimon.catalog.CatalogContext;
import org.apache.paimon.catalog.CatalogFactory;
import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.fs.Path;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.types.DataTypes;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

@Slf4j
public class PaimonCatalogPrimaryTest {

    private PaimonCatalog paimonCatalog;
    private Catalog catalog;
    private final String DATABASE_NAME = "default";
    private final String CATALOG_NAME = "paimon_catalog";
    private final String TABLE_NAME = "test_table";
    private final String WAREHOUSE_PATH = "/tmp/paimon";
    private final Identifier identifier = Identifier.create(DATABASE_NAME, TABLE_NAME);

    @BeforeEach
    public void before()
            throws Catalog.DatabaseAlreadyExistException, Catalog.TableAlreadyExistException,
                    Catalog.DatabaseNotExistException {
        CatalogContext catalogContext = CatalogContext.create(new Path(WAREHOUSE_PATH));
        catalog = CatalogFactory.createCatalog(catalogContext);
        catalog.createDatabase(DATABASE_NAME, true);

        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.column("id", DataTypes.SMALLINT());
        schemaBuilder.column("name", DataTypes.STRING());
        schemaBuilder.column("age", DataTypes.TINYINT());
        schemaBuilder.primaryKey("id", "name");
        catalog.createTable(identifier, schemaBuilder.build(), true);

        Map<String, Object> properties = new HashMap<>();
        properties.put("warehouse", "/tmp/paimon");
        properties.put("plugin_name", "Paimon");
        properties.put("database", DATABASE_NAME);
        properties.put("table", TABLE_NAME);
        ReadonlyConfig config = ReadonlyConfig.fromMap(properties);
        paimonCatalog = new PaimonCatalog(CATALOG_NAME, config);
        paimonCatalog.open();
    }

    @Test
    public void primaryKey() {
        CatalogTable catalogTable = paimonCatalog.getTable(TablePath.of(DATABASE_NAME, TABLE_NAME));
        TableSchema tableSchema = catalogTable.getTableSchema();
        Assertions.assertEquals(
                tableSchema.getPrimaryKey().getColumnNames(), Arrays.asList("id", "name"));
    }

    @AfterEach
    public void after() throws Exception {
        catalog.dropTable(identifier, true);
        catalog.dropDatabase(DATABASE_NAME, true, true);
        catalog.close();
    }
}
