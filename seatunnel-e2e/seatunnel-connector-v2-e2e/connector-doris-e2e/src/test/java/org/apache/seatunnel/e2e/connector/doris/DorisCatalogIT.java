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
import org.apache.seatunnel.api.sink.SaveModeHandler;
import org.apache.seatunnel.api.sink.SupportSaveMode;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.connectors.doris.catalog.DorisCatalog;
import org.apache.seatunnel.connectors.doris.catalog.DorisCatalogFactory;
import org.apache.seatunnel.connectors.doris.config.DorisOptions;
import org.apache.seatunnel.connectors.doris.config.DorisSinkOptions;
import org.apache.seatunnel.connectors.doris.config.DorisSourceOptions;
import org.apache.seatunnel.connectors.doris.sink.DorisSinkFactory;
import org.apache.seatunnel.connectors.doris.source.DorisSourceFactory;

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
import java.util.stream.Collectors;

@Slf4j
public class DorisCatalogIT extends AbstractDorisIT {

    private static final String DATABASE = "test";
    private static final String SINK_TABLE = "doris_catalog_e2e";
    private static final TablePath tablePath = TablePath.of(DATABASE, SINK_TABLE);
    private static final CatalogTable catalogTable;

    static {
        TableSchema.Builder builder = TableSchema.builder();
        builder.column(PhysicalColumn.of("k1", BasicType.INT_TYPE, 10, false, 0, "k1"));
        builder.column(PhysicalColumn.of("k2", BasicType.STRING_TYPE, 64, false, "", "k2"));
        builder.column(PhysicalColumn.of("v1", BasicType.DOUBLE_TYPE, 10, true, null, "v1-'v1'"));
        builder.column(PhysicalColumn.of("v2", new DecimalType(10, 2), 0, false, 0.1, "v2"));
        builder.primaryKey(PrimaryKey.of("pk", Arrays.asList("k1", "k2")));
        catalogTable =
                CatalogTable.of(
                        TableIdentifier.of("doris", tablePath),
                        builder.build(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        "test - \\ 'test'");
    }

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

        catalog = (DorisCatalog) factory.createCatalog(catalogName, ReadonlyConfig.fromMap(map));

        catalog.open();
        catalog.createDatabase(tablePath, false);
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

        boolean dbCreated = false;

        List<String> databases = catalog.listDatabases();
        Assertions.assertFalse(databases.isEmpty());

        if (!catalog.databaseExists(tablePath.getDatabaseName())) {
            catalog.createDatabase(tablePath, false);
            dbCreated = true;
        }

        Assertions.assertFalse(catalog.tableExists(tablePath));
        catalog.createTable(tablePath, catalogTable, false);
        Assertions.assertTrue(catalog.tableExists(tablePath));

        List<String> tables = catalog.listTables(tablePath.getDatabaseName());
        Assertions.assertFalse(tables.isEmpty());

        catalog.dropTable(tablePath, false);
        Assertions.assertFalse(catalog.tableExists(tablePath));

        if (dbCreated) {
            catalog.dropDatabase(tablePath, false);
            Assertions.assertFalse(catalog.databaseExists(tablePath.getDatabaseName()));
        }
    }

    @Test
    void testSaveMode() {
        CatalogTable upstreamTable =
                CatalogTable.of(
                        TableIdentifier.of("doris", TablePath.of("test.test")), catalogTable);
        ReadonlyConfig config =
                ReadonlyConfig.fromMap(
                        new HashMap<String, Object>() {
                            {
                                put(
                                        DorisOptions.FENODES.key(),
                                        container.getHost() + ":" + HTTP_PORT);
                                put(DorisOptions.USERNAME.key(), USERNAME);
                                put(DorisOptions.PASSWORD.key(), PASSWORD);
                            }
                        });
        assertCreateTable(upstreamTable, config, "test.test");

        ReadonlyConfig config2 =
                ReadonlyConfig.fromMap(
                        new HashMap<String, Object>() {
                            {
                                put(
                                        DorisOptions.FENODES.key(),
                                        container.getHost() + ":" + HTTP_PORT);
                                put(DorisOptions.DATABASE.key(), "test2");
                                put(DorisOptions.TABLE.key(), "test2");
                                put(DorisOptions.USERNAME.key(), USERNAME);
                                put(DorisOptions.PASSWORD.key(), PASSWORD);
                            }
                        });
        assertCreateTable(upstreamTable, config2, "test2.test2");

        ReadonlyConfig config3 =
                ReadonlyConfig.fromMap(
                        new HashMap<String, Object>() {
                            {
                                put(
                                        DorisOptions.FENODES.key(),
                                        container.getHost() + ":" + HTTP_PORT);
                                put(DorisOptions.TABLE_IDENTIFIER.key(), "test3.test3");
                                put(DorisOptions.USERNAME.key(), USERNAME);
                                put(DorisOptions.PASSWORD.key(), PASSWORD);
                            }
                        });
        assertCreateTable(upstreamTable, config3, "test3.test3");

        ReadonlyConfig config4 =
                ReadonlyConfig.fromMap(
                        new HashMap<String, Object>() {
                            {
                                put(
                                        DorisOptions.FENODES.key(),
                                        container.getHost() + ":" + HTTP_PORT);
                                put(DorisOptions.DATABASE.key(), "test5");
                                put(DorisOptions.TABLE.key(), "${table_name}");
                                put(DorisOptions.USERNAME.key(), USERNAME);
                                put(DorisOptions.PASSWORD.key(), PASSWORD);
                            }
                        });
        assertCreateTable(upstreamTable, config4, "test5.test");

        ReadonlyConfig config5 =
                ReadonlyConfig.fromMap(
                        new HashMap<String, Object>() {
                            {
                                put(
                                        DorisOptions.FENODES.key(),
                                        container.getHost() + ":" + HTTP_PORT);
                                put(DorisOptions.DATABASE.key(), "test4");
                                put(DorisOptions.TABLE.key(), "test4");
                                put(DorisOptions.USERNAME.key(), USERNAME);
                                put(DorisOptions.PASSWORD.key(), PASSWORD);
                                put(DorisSinkOptions.NEEDS_UNSUPPORTED_TYPE_CASTING.key(), true);
                            }
                        });

        upstreamTable =
                CatalogTable.of(
                        upstreamTable.getTableId(),
                        TableSchema.builder()
                                .columns(upstreamTable.getTableSchema().getColumns())
                                .column(
                                        PhysicalColumn.of(
                                                "v3",
                                                new DecimalType(66, 22),
                                                66,
                                                false,
                                                null,
                                                "v3"))
                                .primaryKey(upstreamTable.getTableSchema().getPrimaryKey())
                                .constraintKey(upstreamTable.getTableSchema().getConstraintKeys())
                                .build(),
                        upstreamTable.getOptions(),
                        upstreamTable.getPartitionKeys(),
                        upstreamTable.getComment());
        CatalogTable newTable = assertCreateTable(upstreamTable, config5, "test4.test4");
        Assertions.assertEquals(
                BasicType.DOUBLE_TYPE, newTable.getTableSchema().getColumns().get(4).getDataType());
    }

    private CatalogTable assertCreateTable(
            CatalogTable upstreamTable, ReadonlyConfig config, String fullName) {
        DorisSinkFactory dorisSinkFactory = new DorisSinkFactory();
        TableSinkFactoryContext context =
                TableSinkFactoryContext.replacePlaceholderAndCreate(
                        upstreamTable,
                        config,
                        Thread.currentThread().getContextClassLoader(),
                        Collections.emptyList());
        SupportSaveMode sink = (SupportSaveMode) dorisSinkFactory.createSink(context).createSink();
        SaveModeHandler handler = sink.getSaveModeHandler().get();
        handler.open();
        handler.handleSaveMode();
        CatalogTable createdTable = catalog.getTable(TablePath.of(fullName));
        Assertions.assertEquals(
                upstreamTable.getTableSchema().getColumns().size(),
                createdTable.getTableSchema().getColumns().size());
        Assertions.assertIterableEquals(
                upstreamTable.getTableSchema().getColumns().stream()
                        .map(Column::getName)
                        .collect(Collectors.toList()),
                createdTable.getTableSchema().getColumns().stream()
                        .map(Column::getName)
                        .collect(Collectors.toList()));
        Assertions.assertEquals(
                "k1", createdTable.getTableSchema().getColumns().get(0).getComment());
        ;
        return createdTable;
    }

    @Test
    public void testDorisSourceSelectFieldsNotLossKeysInformation() {
        catalog.createTable(tablePath, catalogTable, true);
        DorisSourceFactory dorisSourceFactory = new DorisSourceFactory();
        SeaTunnelSource dorisSource =
                dorisSourceFactory
                        .createSource(
                                new TableSourceFactoryContext(
                                        ReadonlyConfig.fromMap(
                                                new HashMap<String, Object>() {
                                                    {
                                                        put(DorisOptions.DATABASE.key(), DATABASE);
                                                        put(DorisOptions.TABLE.key(), SINK_TABLE);
                                                        put(DorisOptions.USERNAME.key(), USERNAME);
                                                        put(DorisOptions.PASSWORD.key(), PASSWORD);
                                                        put(
                                                                DorisSourceOptions.DORIS_READ_FIELD
                                                                        .key(),
                                                                "k1,k2");
                                                        put(
                                                                DorisOptions.FENODES.key(),
                                                                container.getHost()
                                                                        + ":"
                                                                        + HTTP_PORT);
                                                        put(
                                                                DorisOptions.QUERY_PORT.key(),
                                                                QUERY_PORT);
                                                    }
                                                }),
                                        Thread.currentThread().getContextClassLoader()))
                        .createSource();
        CatalogTable table = (CatalogTable) dorisSource.getProducedCatalogTables().get(0);
        Assertions.assertIterableEquals(
                Arrays.asList("k1", "k2"), table.getTableSchema().getPrimaryKey().getColumnNames());
        catalog.dropTable(tablePath, false);
        Assertions.assertFalse(catalog.tableExists(tablePath));
    }

    @AfterAll
    public void close() {
        if (catalog != null) {
            catalog.close();
        }
    }
}
