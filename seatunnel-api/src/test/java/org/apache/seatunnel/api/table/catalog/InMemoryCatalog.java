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

package org.apache.seatunnel.api.table.catalog;

import org.apache.seatunnel.shade.com.google.common.collect.Lists;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseNotExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableNotExistException;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.common.exception.CommonError;

import org.apache.commons.lang3.tuple.Pair;

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class InMemoryCatalog implements Catalog {
    private final ReadonlyConfig options;
    private final String name;
    // database -> tables
    private final Map<String, List<CatalogTable>> catalogTables;
    private static final String DEFAULT_DATABASE = "default";
    private static final String UNSUPPORTED_DATABASE = "unsupported";
    @Getter private boolean isRunTruncateTable = false;

    InMemoryCatalog(String catalogName, ReadonlyConfig options) {
        this.name = catalogName;
        this.options = options;
        this.catalogTables = new HashMap<>();
        addDefaultTable();
    }

    // Add some default table for testing
    private void addDefaultTable() {
        this.catalogTables.put(DEFAULT_DATABASE, new ArrayList<>());
        this.catalogTables.put(UNSUPPORTED_DATABASE, new ArrayList<>());
        List<CatalogTable> tables = new ArrayList<>();
        this.catalogTables.put("st", tables);
        TableSchema tableSchema =
                TableSchema.builder()
                        .column(PhysicalColumn.of("id", BasicType.LONG_TYPE, 22, false, null, "id"))
                        .column(
                                PhysicalColumn.of(
                                        "name", BasicType.STRING_TYPE, 128, false, null, "name"))
                        .column(
                                PhysicalColumn.of(
                                        "age", BasicType.INT_TYPE, (Long) null, true, null, "age"))
                        .column(
                                PhysicalColumn.of(
                                        "createTime",
                                        LocalTimeType.LOCAL_DATE_TIME_TYPE,
                                        3,
                                        true,
                                        null,
                                        "createTime"))
                        .column(
                                PhysicalColumn.of(
                                        "lastUpdateTime",
                                        LocalTimeType.LOCAL_DATE_TIME_TYPE,
                                        3,
                                        true,
                                        null,
                                        "lastUpdateTime"))
                        .primaryKey(PrimaryKey.of("id", Lists.newArrayList("id")))
                        .constraintKey(
                                ConstraintKey.of(
                                        ConstraintKey.ConstraintType.INDEX_KEY,
                                        "name",
                                        Lists.newArrayList(
                                                ConstraintKey.ConstraintKeyColumn.of(
                                                        "name", null))))
                        .build();
        CatalogTable catalogTable1 =
                CatalogTable.of(
                        TableIdentifier.of(name, TablePath.of("st", "public", "table1")),
                        TableSchema.builder().build(),
                        new HashMap<>(),
                        new ArrayList<>(),
                        "In Memory Table");
        CatalogTable catalogTable2 =
                CatalogTable.of(
                        TableIdentifier.of(name, TablePath.of("st", "public", "table2")),
                        TableSchema.builder().build(),
                        new HashMap<>(),
                        new ArrayList<>(),
                        "In Memory Table",
                        name);
        tables.add(catalogTable1);
        tables.add(catalogTable2);

        CatalogTable unsupportedTable1 =
                CatalogTable.of(
                        TableIdentifier.of(
                                name, TablePath.of(UNSUPPORTED_DATABASE, "public", "table1")),
                        tableSchema,
                        new HashMap<>(),
                        new ArrayList<>(),
                        "In Memory Table");
        CatalogTable unsupportedTable2 =
                CatalogTable.of(
                        TableIdentifier.of(
                                name, TablePath.of(UNSUPPORTED_DATABASE, "public", "table2")),
                        tableSchema,
                        new HashMap<>(),
                        new ArrayList<>(),
                        "In Memory Table",
                        name);
        this.catalogTables.get(UNSUPPORTED_DATABASE).add(unsupportedTable1);
        this.catalogTables.get(UNSUPPORTED_DATABASE).add(unsupportedTable2);
    }

    @Override
    public void open() throws CatalogException {
        String username = options.get(InMemoryCatalogOptionRule.username);
        String password = options.get(InMemoryCatalogOptionRule.password);
        String host = options.get(InMemoryCatalogOptionRule.host);
        int port = options.get(InMemoryCatalogOptionRule.port);
        log.trace(
                String.format(
                        "InMemoryCatalog %s opening with %s/%s in %s:%s",
                        name, username, password, host, port));
    }

    @Override
    public void close() throws CatalogException {
        log.trace(String.format("InMemoryCatalog %s closing", name));
    }

    @Override
    public String name() {
        return "InMemory";
    }

    @Override
    public String getDefaultDatabase() throws CatalogException {
        return DEFAULT_DATABASE;
    }

    @Override
    public void truncateTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        isRunTruncateTable = true;
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        return catalogTables.containsKey(databaseName);
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        return new ArrayList<>(catalogTables.keySet());
    }

    @Override
    public List<String> listTables(String databaseName)
            throws CatalogException, DatabaseNotExistException {
        return catalogTables.get(databaseName).stream()
                .map(
                        table ->
                                table.getTableId().getSchemaName()
                                        + "."
                                        + table.getTableId().getTableName())
                .collect(Collectors.toList());
    }

    @Override
    public boolean tableExists(TablePath tablePath) throws CatalogException {
        if (catalogTables.containsKey(tablePath.getDatabaseName())) {
            List<CatalogTable> tables = catalogTables.get(tablePath.getDatabaseName());
            return tables.stream().anyMatch(t -> t.getTableId().toTablePath().equals(tablePath));
        }
        return false;
    }

    @Override
    public CatalogTable getTable(TablePath tablePath)
            throws CatalogException, TableNotExistException {
        if (catalogTables.containsKey(tablePath.getDatabaseName())) {
            if (tablePath.getDatabaseName().equals(UNSUPPORTED_DATABASE)) {
                List<Pair<String, String>> unsupportedFields =
                        Arrays.asList(
                                Pair.of("field1", "interval"), Pair.of("field2", "interval2"));
                buildColumnsWithErrorCheck(
                        tablePath,
                        new TableSchema.Builder(),
                        unsupportedFields.iterator(),
                        field -> {
                            throw CommonError.convertToSeaTunnelTypeError(
                                    name(), field.getValue(), field.getKey());
                        });
            }
            List<CatalogTable> tables = catalogTables.get(tablePath.getDatabaseName());
            return tables.stream()
                    .filter(t -> t.getTableId().toTablePath().equals(tablePath))
                    .findFirst()
                    .orElseThrow(() -> new TableNotExistException(name, tablePath));
        } else {
            throw new TableNotExistException(name, tablePath);
        }
    }

    @Override
    public void createTable(TablePath tablePath, CatalogTable table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        if (catalogTables.containsKey(tablePath.getDatabaseName())) {
            List<CatalogTable> tables = catalogTables.get(tablePath.getDatabaseName());
            if (tables.stream().anyMatch(t -> t.getTableId().toTablePath().equals(tablePath))) {
                if (ignoreIfExists) {
                    log.debug("Table {} already exists, ignore", tablePath.getFullName());
                } else {
                    throw new TableAlreadyExistException(name, tablePath);
                }
            } else {
                tables.add(table);
            }
        } else {
            throw new DatabaseNotExistException(name, tablePath.getDatabaseName());
        }
    }

    @Override
    public void dropTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        if (catalogTables.containsKey(tablePath.getDatabaseName())) {
            List<CatalogTable> tables = catalogTables.get(tablePath.getDatabaseName());
            if (tables.stream().anyMatch(t -> t.getTableId().toTablePath().equals(tablePath))) {
                tables.removeIf(t -> t.getTableId().toTablePath().equals(tablePath));
            } else {
                if (ignoreIfNotExists) {
                    log.debug("Table {} not exists, ignore", tablePath.getFullName());
                } else {
                    throw new TableNotExistException(name, tablePath);
                }
            }
        } else {
            throw new DatabaseNotExistException(name, tablePath.getDatabaseName());
        }
    }

    @Override
    public void createDatabase(TablePath tablePath, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException, CatalogException {
        if (catalogTables.containsKey(tablePath.getDatabaseName())) {
            if (ignoreIfExists) {
                log.debug("Database {} already exists, ignore", tablePath.getDatabaseName());
            } else {
                throw new DatabaseAlreadyExistException(name, tablePath.getDatabaseName());
            }
        } else {
            catalogTables.put(tablePath.getDatabaseName(), new ArrayList<>());
        }
    }

    @Override
    public void dropDatabase(TablePath tablePath, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, CatalogException {
        if (catalogTables.containsKey(tablePath.getDatabaseName())) {
            catalogTables.remove(tablePath.getDatabaseName());
        } else {
            if (ignoreIfNotExists) {
                log.debug("Database {} not exists, ignore", tablePath.getDatabaseName());
            } else {
                throw new DatabaseNotExistException(name, tablePath.getDatabaseName());
            }
        }
    }
}
