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

package org.apache.seatunnel.connectors.seatunnel.jdbc.utils;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.ConstraintKey;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PrimaryKey;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseNotExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableNotExistException;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;

public class JdbcCatalogUtilsTest {
    private static final CatalogTable DEFAULT_TABLE =
            CatalogTable.of(
                    TableIdentifier.of("mysql-1", "database-x", null, "table-x"),
                    TableSchema.builder()
                            .column(
                                    PhysicalColumn.of(
                                            "f1",
                                            BasicType.LONG_TYPE,
                                            null,
                                            false,
                                            null,
                                            null,
                                            "int unsigned",
                                            false,
                                            false,
                                            null,
                                            null,
                                            null))
                            .column(
                                    PhysicalColumn.of(
                                            "f2",
                                            BasicType.STRING_TYPE,
                                            10,
                                            false,
                                            null,
                                            null,
                                            "varchar(10)",
                                            false,
                                            false,
                                            null,
                                            null,
                                            null))
                            .column(
                                    PhysicalColumn.of(
                                            "f3",
                                            BasicType.STRING_TYPE,
                                            20,
                                            false,
                                            null,
                                            null,
                                            "varchar(20)",
                                            false,
                                            false,
                                            null,
                                            null,
                                            null))
                            .primaryKey(PrimaryKey.of("pk1", Arrays.asList("f1")))
                            .constraintKey(
                                    ConstraintKey.of(
                                            ConstraintKey.ConstraintType.UNIQUE_KEY,
                                            "uk1",
                                            Arrays.asList(
                                                    ConstraintKey.ConstraintKeyColumn.of(
                                                            "f2", ConstraintKey.ColumnSortType.ASC),
                                                    ConstraintKey.ConstraintKeyColumn.of(
                                                            "f3",
                                                            ConstraintKey.ColumnSortType.ASC))))
                            .build(),
                    Collections.emptyMap(),
                    Collections.singletonList("f2"),
                    null);

    @Test
    public void testColumnEqualsMerge() {
        CatalogTable tableOfQuery =
                CatalogTable.of(
                        TableIdentifier.of("default", null, null, "default"),
                        TableSchema.builder()
                                .column(
                                        PhysicalColumn.of(
                                                "f2",
                                                BasicType.STRING_TYPE,
                                                10,
                                                true,
                                                null,
                                                null,
                                                null,
                                                false,
                                                false,
                                                null,
                                                null,
                                                null))
                                .column(
                                        PhysicalColumn.of(
                                                "f3",
                                                BasicType.STRING_TYPE,
                                                20,
                                                false,
                                                null,
                                                null,
                                                null,
                                                false,
                                                false,
                                                null,
                                                null,
                                                null))
                                .column(
                                        PhysicalColumn.of(
                                                "f1",
                                                BasicType.LONG_TYPE,
                                                null,
                                                true,
                                                null,
                                                null,
                                                null,
                                                false,
                                                false,
                                                null,
                                                null,
                                                null))
                                .build(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        null);

        CatalogTable mergeTable = JdbcCatalogUtils.mergeCatalogTable(DEFAULT_TABLE, tableOfQuery);
        Assertions.assertEquals(DEFAULT_TABLE.getTableId(), mergeTable.getTableId());
        Assertions.assertEquals(DEFAULT_TABLE.getOptions(), mergeTable.getOptions());
        Assertions.assertEquals(DEFAULT_TABLE.getComment(), mergeTable.getComment());
        Assertions.assertEquals(DEFAULT_TABLE.getCatalogName(), mergeTable.getCatalogName());
        Assertions.assertNotEquals(DEFAULT_TABLE.getTableSchema(), mergeTable.getTableSchema());
        Assertions.assertEquals(
                DEFAULT_TABLE.getTableSchema().getPrimaryKey(),
                mergeTable.getTableSchema().getPrimaryKey());
        Assertions.assertEquals(
                DEFAULT_TABLE.getTableSchema().getConstraintKeys(),
                mergeTable.getTableSchema().getConstraintKeys());

        Map<String, Column> columnMap =
                DEFAULT_TABLE.getTableSchema().getColumns().stream()
                        .collect(Collectors.toMap(e -> e.getName(), e -> e));
        List<Column> sortByQueryColumns =
                tableOfQuery.getTableSchema().getColumns().stream()
                        .map(e -> columnMap.get(e.getName()))
                        .collect(Collectors.toList());
        Assertions.assertEquals(sortByQueryColumns, mergeTable.getTableSchema().getColumns());
    }

    @Test
    public void testColumnIncludeMerge() {
        CatalogTable tableOfQuery =
                CatalogTable.of(
                        TableIdentifier.of("default", null, null, "default"),
                        TableSchema.builder()
                                .column(
                                        PhysicalColumn.of(
                                                "f1",
                                                BasicType.LONG_TYPE,
                                                null,
                                                true,
                                                null,
                                                null,
                                                null,
                                                false,
                                                false,
                                                null,
                                                null,
                                                null))
                                .column(
                                        PhysicalColumn.of(
                                                "f3",
                                                BasicType.STRING_TYPE,
                                                20,
                                                false,
                                                null,
                                                null,
                                                null,
                                                false,
                                                false,
                                                null,
                                                null,
                                                null))
                                .build(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        null);

        CatalogTable mergeTable = JdbcCatalogUtils.mergeCatalogTable(DEFAULT_TABLE, tableOfQuery);

        Assertions.assertEquals(DEFAULT_TABLE.getTableId(), mergeTable.getTableId());
        Assertions.assertEquals(
                DEFAULT_TABLE.getTableSchema().getPrimaryKey(),
                mergeTable.getTableSchema().getPrimaryKey());
        Assertions.assertEquals(
                DEFAULT_TABLE.getTableSchema().getColumns().stream()
                        .filter(c -> Arrays.asList("f1", "f3").contains(c.getName()))
                        .collect(Collectors.toList()),
                mergeTable.getTableSchema().getColumns());
        Assertions.assertTrue(mergeTable.getPartitionKeys().isEmpty());
        Assertions.assertTrue(mergeTable.getTableSchema().getConstraintKeys().isEmpty());
    }

    @Test
    public void testColumnNotIncludeMerge() {
        CatalogTable tableOfQuery =
                CatalogTable.of(
                        TableIdentifier.of("default", null, null, "default"),
                        TableSchema.builder()
                                .column(
                                        PhysicalColumn.of(
                                                "f1",
                                                BasicType.LONG_TYPE,
                                                null,
                                                true,
                                                null,
                                                null,
                                                null,
                                                false,
                                                false,
                                                null,
                                                null,
                                                null))
                                .column(
                                        PhysicalColumn.of(
                                                "f2",
                                                BasicType.STRING_TYPE,
                                                10,
                                                true,
                                                null,
                                                null,
                                                null,
                                                false,
                                                false,
                                                null,
                                                null,
                                                null))
                                .column(
                                        PhysicalColumn.of(
                                                "f3",
                                                BasicType.STRING_TYPE,
                                                20,
                                                false,
                                                null,
                                                null,
                                                null,
                                                false,
                                                false,
                                                null,
                                                null,
                                                null))
                                .column(
                                        PhysicalColumn.of(
                                                "f4",
                                                BasicType.STRING_TYPE,
                                                20,
                                                false,
                                                null,
                                                null,
                                                null,
                                                false,
                                                false,
                                                null,
                                                null,
                                                null))
                                .build(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        null);

        CatalogTable mergeTable = JdbcCatalogUtils.mergeCatalogTable(DEFAULT_TABLE, tableOfQuery);

        Assertions.assertEquals(
                DEFAULT_TABLE.getTableId().toTablePath(), mergeTable.getTableId().toTablePath());
        Assertions.assertEquals(DEFAULT_TABLE.getPartitionKeys(), mergeTable.getPartitionKeys());
        Assertions.assertEquals(
                DEFAULT_TABLE.getTableSchema().getPrimaryKey(),
                mergeTable.getTableSchema().getPrimaryKey());
        Assertions.assertEquals(
                DEFAULT_TABLE.getTableSchema().getConstraintKeys(),
                mergeTable.getTableSchema().getConstraintKeys());

        Assertions.assertEquals(
                tableOfQuery.getTableId().getCatalogName(),
                mergeTable.getTableId().getCatalogName());
        Assertions.assertEquals(
                tableOfQuery.getTableSchema().getColumns(),
                mergeTable.getTableSchema().getColumns());
    }

    @Test
    public void testDecimalColumnMerge() {
        CatalogTable tableOfQuery =
                CatalogTable.of(
                        TableIdentifier.of("default", null, null, "default"),
                        TableSchema.builder()
                                .column(
                                        PhysicalColumn.of(
                                                "f1",
                                                new DecimalType(10, 1),
                                                null,
                                                true,
                                                null,
                                                null,
                                                null,
                                                false,
                                                false,
                                                null,
                                                null,
                                                null))
                                .build(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        null);

        CatalogTable tableOfPath =
                CatalogTable.of(
                        TableIdentifier.of("default", null, null, "default"),
                        TableSchema.builder()
                                .column(
                                        PhysicalColumn.of(
                                                "f1",
                                                new DecimalType(10, 2),
                                                null,
                                                true,
                                                null,
                                                null,
                                                null,
                                                false,
                                                false,
                                                null,
                                                null,
                                                null))
                                .build(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        null);

        CatalogTable mergeTable = JdbcCatalogUtils.mergeCatalogTable(tableOfPath, tableOfQuery);
        // When column type is decimal, the precision and scale should not affect the merge result
        Assertions.assertEquals(
                tableOfPath.getTableSchema().getColumns().get(0),
                mergeTable.getTableSchema().getColumns().get(0));
    }

    @Test
    public void testCatalogGetTablesWithMysqlPattern() throws Exception {
        TestCatalog testCatalog = spy(new TestCatalog());

        TableSchema tableSchema =
                TableSchema.builder()
                        .column(PhysicalColumn.of("id", BasicType.INT_TYPE, 0, true, null, null))
                        .build();

        List<String> allDatabases = new ArrayList<>(Arrays.asList("test", "prod", "dev"));

        Map<String, List<String>> databaseTables = new HashMap<>();
        databaseTables.put(
                "test", Arrays.asList("table1", "table2", "table3", "table123", "tableabc"));
        databaseTables.put("prod", Arrays.asList("prod_table1", "prod_table2", "prod_table3"));
        databaseTables.put("dev", Arrays.asList("dev_table1", "dev_table2"));

        Map<TablePath, CatalogTable> tableMap = new HashMap<>();
        for (String database : allDatabases) {
            for (String tableName : databaseTables.get(database)) {
                TablePath tablePath = TablePath.of(database, null, tableName);
                CatalogTable table =
                        CatalogTable.of(
                                TableIdentifier.of(database, null, null, tableName),
                                tableSchema,
                                Collections.emptyMap(),
                                Collections.emptyList(),
                                "Test " + tableName);
                tableMap.put(tablePath, table);
            }
        }

        doAnswer(invocation -> new ArrayList<>(allDatabases)).when(testCatalog).listDatabases();

        for (String database : allDatabases) {
            doReturn(true).when(testCatalog).databaseExists(eq(database));
        }

        for (String database : allDatabases) {
            doReturn(new ArrayList<>(databaseTables.get(database)))
                    .when(testCatalog)
                    .listTables(eq(database));
        }

        for (String database : allDatabases) {
            List<TablePath> paths =
                    databaseTables.get(database).stream()
                            .map(tableName -> TablePath.of(database, null, tableName))
                            .collect(Collectors.toList());
            doReturn(paths).when(testCatalog).listTablePaths(eq(database));
        }

        doReturn(true).when(testCatalog).tableExists(any(TablePath.class));

        doAnswer(
                        invocation -> {
                            TablePath path = invocation.getArgument(0);
                            CatalogTable table = tableMap.get(path);
                            if (table == null) {
                                throw new TableNotExistException("test", path);
                            }
                            return table;
                        })
                .when(testCatalog)
                .getTable(any(TablePath.class));

        testMysqlRegexPattern(
                testCatalog,
                "test",
                "test.table\\d+",
                Arrays.asList("table1", "table2", "table3", "table123"));

        testMysqlRegexPattern(
                testCatalog,
                ".*",
                ".*table1",
                Arrays.asList("table1", "prod_table1", "dev_table1"));

        testMysqlRegexPattern(
                testCatalog,
                "prod",
                "prod.prod_table[1-2]",
                Arrays.asList("prod_table1", "prod_table2"));

        testMysqlRegexPattern(testCatalog, ".*", "nonexistent.*", Collections.emptyList());
    }

    private void testMysqlRegexPattern(
            Catalog catalog,
            String databasePattern,
            String tablePattern,
            List<String> expectedTablePaths) {

        Map<String, Object> configMap = new HashMap<>();
        configMap.put(ConnectorCommonOptions.DATABASE_PATTERN.key(), databasePattern);
        configMap.put(ConnectorCommonOptions.TABLE_PATTERN.key(), tablePattern);
        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        List<CatalogTable> tables = catalog.getTables(config);

        List<String> actualTablePaths =
                tables.stream()
                        .map(t -> t.getTableId().toTablePath().toString())
                        .collect(Collectors.toList());

        Set<String> actualTablePathSet = new HashSet<>(actualTablePaths);
        Set<String> expectedTablePathSet = new HashSet<>(expectedTablePaths);

        Assertions.assertEquals(
                expectedTablePathSet.size(),
                actualTablePathSet.size(),
                "Expected "
                        + expectedTablePathSet.size()
                        + " tables for pattern: "
                        + databasePattern
                        + "."
                        + tablePattern);

        if (!expectedTablePaths.isEmpty()) {
            for (String expectedTablePath : expectedTablePaths) {
                Assertions.assertTrue(
                        actualTablePathSet.contains(expectedTablePath),
                        "Expected table path "
                                + expectedTablePath
                                + " not found for pattern: "
                                + databasePattern
                                + "."
                                + tablePattern);
            }
        } else {
            Assertions.assertTrue(
                    actualTablePathSet.isEmpty(),
                    "Expected empty result for pattern: " + databasePattern + "." + tablePattern);
        }
    }

    @Test
    public void testCatalogGetTablesWithPostgresPattern() throws Exception {
        String catalogName = "postgres_catalog";
        TestCatalog postgresCatalog = spy(new TestCatalog());

        doReturn(catalogName).when(postgresCatalog).name();

        TableSchema tableSchema =
                TableSchema.builder()
                        .column(PhysicalColumn.of("id", BasicType.INT_TYPE, 0, true, null, null))
                        .build();

        List<String> allDatabases = new ArrayList<>(Arrays.asList("postgres", "test_db", "dev_db"));

        Map<String, List<String>> databaseSchemas = new HashMap<>();
        databaseSchemas.put("postgres", Arrays.asList("public", "schema1", "schema2"));
        databaseSchemas.put("test_db", Arrays.asList("public", "test_schema"));
        databaseSchemas.put("dev_db", Arrays.asList("public", "dev_schema"));

        Map<String, Map<String, List<String>>> schemasTables = new HashMap<>();

        Map<String, List<String>> postgresSchemas = new HashMap<>();
        postgresSchemas.put("public", Arrays.asList("users", "orders", "products", "customers"));
        postgresSchemas.put("schema1", Arrays.asList("table1", "table2", "table3"));
        postgresSchemas.put("schema2", Arrays.asList("log_2021", "log_2022", "log_2023"));
        schemasTables.put("postgres", postgresSchemas);

        Map<String, List<String>> testDbSchemas = new HashMap<>();
        testDbSchemas.put("public", Arrays.asList("test_table1", "test_table2"));
        testDbSchemas.put("test_schema", Arrays.asList("data_table1", "data_table2"));
        schemasTables.put("test_db", testDbSchemas);

        Map<String, List<String>> devDbSchemas = new HashMap<>();
        devDbSchemas.put("public", Arrays.asList("dev_table1", "dev_table2"));
        devDbSchemas.put("dev_schema", Arrays.asList("temp_table1", "temp_table2"));
        schemasTables.put("dev_db", devDbSchemas);

        Map<TablePath, CatalogTable> tableMap = new HashMap<>();
        for (String database : allDatabases) {
            for (String schema : databaseSchemas.get(database)) {
                for (String tableName : schemasTables.get(database).get(schema)) {
                    TablePath tablePath = TablePath.of(database, schema, tableName);
                    CatalogTable table =
                            CatalogTable.of(
                                    TableIdentifier.of(catalogName, database, schema, tableName),
                                    tableSchema,
                                    Collections.emptyMap(),
                                    Collections.emptyList(),
                                    "Test " + tableName);
                    tableMap.put(tablePath, table);
                }
            }
        }

        doAnswer(invocation -> new ArrayList<>(allDatabases)).when(postgresCatalog).listDatabases();

        for (String database : allDatabases) {
            doReturn(true).when(postgresCatalog).databaseExists(eq(database));
        }

        for (String database : allDatabases) {
            for (String schema : databaseSchemas.get(database)) {
                List<String> tables = schemasTables.get(database).get(schema);
                doReturn(new ArrayList<>(tables))
                        .when(postgresCatalog)
                        .listTables(eq(database + "." + schema));
            }
        }

        for (String database : allDatabases) {
            List<TablePath> paths = new ArrayList<>();
            for (String schema : databaseSchemas.get(database)) {
                for (String tableName : schemasTables.get(database).get(schema)) {
                    paths.add(TablePath.of(database, schema, tableName));
                }
            }
            doReturn(paths).when(postgresCatalog).listTablePaths(eq(database));
        }

        doReturn(true).when(postgresCatalog).tableExists(any(TablePath.class));

        doAnswer(
                        invocation -> {
                            TablePath path = invocation.getArgument(0);
                            CatalogTable table = tableMap.get(path);
                            if (table == null) {
                                throw new TableNotExistException("test", path);
                            }
                            return table;
                        })
                .when(postgresCatalog)
                .getTable(any(TablePath.class));

        testPostgresRegexPattern(
                postgresCatalog,
                "postgres",
                "postgres\\.public\\..*",
                Arrays.asList(
                        "postgres.public.users",
                        "postgres.public.orders",
                        "postgres.public.products",
                        "postgres.public.customers"));

        testPostgresRegexPattern(
                postgresCatalog,
                ".*",
                ".*\\.public\\..*table.*",
                Arrays.asList(
                        "test_db.public.test_table1",
                        "test_db.public.test_table2",
                        "dev_db.public.dev_table1",
                        "dev_db.public.dev_table2"));

        testPostgresRegexPattern(
                postgresCatalog,
                ".*",
                ".*\\..*\\.log_\\d{4}",
                Arrays.asList(
                        "postgres.schema2.log_2021",
                        "postgres.schema2.log_2022",
                        "postgres.schema2.log_2023"));

        testPostgresRegexPattern(
                postgresCatalog,
                "test_db",
                "test_db\\..*\\..*",
                Arrays.asList(
                        "test_db.public.test_table1",
                        "test_db.public.test_table2",
                        "test_db.test_schema.data_table1",
                        "test_db.test_schema.data_table2"));

        testPostgresRegexPattern(
                postgresCatalog, ".*", ".*\\..*\\.nonexistent.*", Collections.emptyList());
    }

    private void testPostgresRegexPattern(
            Catalog catalog,
            String databasePattern,
            String tablePattern,
            List<String> expectedTablePaths) {

        Map<String, Object> configMap = new HashMap<>();
        configMap.put(ConnectorCommonOptions.DATABASE_PATTERN.key(), databasePattern);
        configMap.put(ConnectorCommonOptions.TABLE_PATTERN.key(), tablePattern);
        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);

        List<CatalogTable> tables = catalog.getTables(config);

        List<String> actualTablePaths =
                tables.stream()
                        .map(
                                t -> {
                                    TableIdentifier id = t.getTableId();
                                    return id.getDatabaseName()
                                            + "."
                                            + id.getSchemaName()
                                            + "."
                                            + id.getTableName();
                                })
                        .collect(Collectors.toList());

        Set<String> actualTablePathSet = new HashSet<>(actualTablePaths);
        Set<String> expectedTablePathSet = new HashSet<>(expectedTablePaths);

        Assertions.assertEquals(
                expectedTablePathSet.size(),
                actualTablePathSet.size(),
                "Expected "
                        + expectedTablePathSet.size()
                        + " tables for pattern: "
                        + databasePattern
                        + "."
                        + tablePattern);

        if (!expectedTablePaths.isEmpty()) {
            for (String expectedTablePath : expectedTablePaths) {
                Assertions.assertTrue(
                        actualTablePathSet.contains(expectedTablePath),
                        "Expected table path "
                                + expectedTablePath
                                + " not found for pattern: "
                                + databasePattern
                                + "."
                                + tablePattern);
            }
        } else {
            Assertions.assertTrue(
                    actualTablePathSet.isEmpty(),
                    "Expected empty result for pattern: " + databasePattern + "." + tablePattern);
        }
    }

    private static class TestCatalog implements Catalog {

        @Override
        public void open() throws CatalogException {}

        @Override
        public void close() throws CatalogException {}

        @Override
        public String name() {
            return "TestCatalog";
        }

        @Override
        public String getDefaultDatabase() throws CatalogException {
            return "test";
        }

        @Override
        public boolean databaseExists(String databaseName) throws CatalogException {
            return false;
        }

        @Override
        public List<String> listDatabases() throws CatalogException {
            return Collections.emptyList();
        }

        @Override
        public List<String> listTables(String databaseName)
                throws CatalogException, DatabaseNotExistException {
            return Collections.emptyList();
        }

        @Override
        public boolean tableExists(TablePath tablePath) throws CatalogException {
            return false;
        }

        @Override
        public CatalogTable getTable(TablePath tablePath)
                throws CatalogException, TableNotExistException {
            throw new TableNotExistException("test", tablePath);
        }

        @Override
        public void createTable(TablePath tablePath, CatalogTable table, boolean ignoreIfExists)
                throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {}

        @Override
        public void dropTable(TablePath tablePath, boolean ignoreIfNotExists)
                throws TableNotExistException, CatalogException {}

        @Override
        public void createDatabase(TablePath tablePath, boolean ignoreIfExists)
                throws DatabaseAlreadyExistException, CatalogException {}

        @Override
        public void dropDatabase(TablePath tablePath, boolean ignoreIfNotExists)
                throws DatabaseNotExistException, CatalogException {}
    }
}
