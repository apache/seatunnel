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

package org.apache.seatunnel.connectors.seatunnel.iceberg.catalog;

import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseNotExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableNotExistException;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SqlType;
import org.apache.seatunnel.connectors.seatunnel.iceberg.IcebergCatalogFactory;

import org.apache.iceberg.PartitionField;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.types.Type;
import org.apache.iceberg.types.Types;

import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
public class IcebergCatalog implements Catalog {

    IcebergCatalogFactory icebergCatalogFactory;
    String catalogName;
    IcebergDataTypeConvertor icebergDataTypeConvertor = new IcebergDataTypeConvertor();
    org.apache.iceberg.catalog.Catalog catalog;

    public IcebergCatalog(IcebergCatalogFactory icebergCatalogFactory, String catalogName) {
        this.icebergCatalogFactory = icebergCatalogFactory;
        this.catalogName = catalogName;
    }

    @Override
    public void open() throws CatalogException {
        log.info("Opening IcebergCatalog...");
        this.catalog = icebergCatalogFactory.create();
        log.info("IcebergCatalog opened successfully.");
    }

    @Override
    public void close() throws CatalogException {
        log.info("Closing IcebergCatalog...");
        if (catalog != null && catalog instanceof Closeable) {
            try {
                ((Closeable) catalog).close();
                log.info("IcebergCatalog closed successfully.");
            } catch (IOException e) {
                log.error("Error while closing IcebergCatalog.", e);
                throw new CatalogException(e);
            }
        }
    }

    @Override
    public String getDefaultDatabase() throws CatalogException {
        log.info("Fetching default database...");
        return "default";
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        if (catalog instanceof SupportsNamespaces) {
            boolean exists =
                    ((SupportsNamespaces) catalog).namespaceExists(Namespace.of(databaseName));
            log.info("Database {} existence status: {}", databaseName, exists);
            return exists;
        } else {
            throw new UnsupportedOperationException(
                    "catalog not implements SupportsNamespaces so can't check database exists");
        }
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        if (catalog instanceof SupportsNamespaces) {
            List<String> databases =
                    ((SupportsNamespaces) catalog)
                            .listNamespaces().stream()
                                    .map(Namespace::toString)
                                    .collect(Collectors.toList());
            log.info("Fetched {} databases.", databases.size());
            return databases;
        } else {
            throw new UnsupportedOperationException(
                    "catalog not implements SupportsNamespaces so can't list databases");
        }
    }

    @Override
    public List<String> listTables(String databaseName)
            throws CatalogException, DatabaseNotExistException {
        List<String> tables =
                catalog.listTables(Namespace.of(databaseName)).stream()
                        .map(tableIdentifier -> toTablePath(tableIdentifier).getTableName())
                        .collect(Collectors.toList());
        log.info("Fetched {} tables.", tables.size());
        return tables;
    }

    @Override
    public boolean tableExists(TablePath tablePath) throws CatalogException {
        boolean exists = catalog.tableExists(toIcebergTableIdentifier(tablePath));
        log.info("Table {} existence status: {}", tablePath, exists);
        return exists;
    }

    @Override
    public CatalogTable getTable(TablePath tablePath)
            throws CatalogException, TableNotExistException {
        TableIdentifier icebergTableIdentifier = toIcebergTableIdentifier(tablePath);
        try {
            CatalogTable catalogTable =
                    toCatalogTable(catalog.loadTable(icebergTableIdentifier), tablePath);
            log.info("Fetched table details for: {}", tablePath);
            return catalogTable;
        } catch (NoSuchTableException e) {
            throw new TableNotExistException("table not exist", tablePath, e);
        }
    }

    @Override
    public void createTable(TablePath tablePath, CatalogTable table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        log.info("Creating table at path: {}", tablePath);
        TableSchema tableSchema = table.getTableSchema();
        Schema icebergSchema = toIcebergSchema(tableSchema);
        PartitionSpec.Builder psBuilder = PartitionSpec.builderFor(icebergSchema);
        for (String p : table.getPartitionKeys()) {
            psBuilder.identity(p);
        }

        Map<String, String> options = new HashMap<>(table.getOptions());
        options.put("format-version", "2");
        log.info(
                "tablePath: {}, tableSchema: {}, partitionKeys: {}, options: {}",
                tablePath,
                tableSchema,
                table.getPartitionKeys(),
                options);
        catalog.createTable(
                toIcebergTableIdentifier(tablePath), icebergSchema, psBuilder.build(), options);
    }

    @Override
    public void dropTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        if (ignoreIfNotExists) {
            if (!tableExists(tablePath)) {
                log.info(
                        "Attempted to drop table at path: {}. The table does not exist, but proceeding as 'ignoreIfNotExists' is set to true.",
                        tablePath);
                return;
            }
        }
        catalog.dropTable(toIcebergTableIdentifier(tablePath), true);
        log.info("Dropped table at path: {}", tablePath);
    }

    @Override
    public void createDatabase(TablePath tablePath, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void dropDatabase(TablePath tablePath, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, CatalogException {
        throw new UnsupportedOperationException();
    }

    public boolean isExistsData(TablePath tablePath) {
        if (!tableExists(tablePath)) {
            throw new TableNotExistException("table not exist", tablePath);
        }
        TableIdentifier icebergTableIdentifier = toIcebergTableIdentifier(tablePath);
        Snapshot snapshot = catalog.loadTable(icebergTableIdentifier).currentSnapshot();
        if (snapshot != null) {
            String total = snapshot.summary().getOrDefault("total-records", null);
            return total != null && !total.equals("0");
        }
        return false;
    }

    public void truncateTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        if (!tableExists(tablePath)) {
            throw new TableNotExistException("table not exist", tablePath);
        }
        TableIdentifier icebergTableIdentifier = toIcebergTableIdentifier(tablePath);
        catalog.loadTable(icebergTableIdentifier)
                .newDelete()
                .deleteFromRowFilter(org.apache.iceberg.expressions.Expressions.alwaysTrue())
                .commit();
        log.info("Truncated table at path: {}", tablePath);
    }

    public void executeSql(String sql) {
        throw new UnsupportedOperationException();
    }

    public CatalogTable toCatalogTable(Table icebergTable, TablePath tablePath) {
        List<Types.NestedField> columns = icebergTable.schema().columns();
        TableSchema.Builder builder = TableSchema.builder();
        columns.stream()
                .forEach(
                        nestedField -> {
                            String name = nestedField.name();
                            SeaTunnelDataType<?> seaTunnelType =
                                    icebergDataTypeConvertor.toSeaTunnelType(
                                            nestedField.type().typeId());
                            PhysicalColumn physicalColumn =
                                    PhysicalColumn.of(
                                            name,
                                            seaTunnelType,
                                            null,
                                            true,
                                            null,
                                            nestedField.doc());
                            builder.column(physicalColumn);
                        });

        List<String> partitionKeys =
                icebergTable.spec().fields().stream()
                        .map(PartitionField::name)
                        .collect(Collectors.toList());

        return CatalogTable.of(
                org.apache.seatunnel.api.table.catalog.TableIdentifier.of(
                        catalogName, tablePath.getDatabaseName(), tablePath.getTableName()),
                builder.build(),
                icebergTable.properties(),
                partitionKeys,
                null,
                catalogName);
    }

    public org.apache.iceberg.Schema toIcebergSchema(TableSchema tableSchema) {
        int index = 1;
        List<Column> columns = tableSchema.getColumns();
        List<Types.NestedField> nestedFields = new ArrayList<>(columns.size());
        for (int i = 0; i < columns.size(); i++) {
            Column column = columns.get(i);
            SeaTunnelDataType<?> dataType = column.getDataType();
            String name = column.getName();
            Map<String, Object> options =
                    column.getOptions() != null
                            ? new HashMap<>(column.getOptions())
                            : new HashMap<>();
            if (dataType.getSqlType().equals(SqlType.DECIMAL)) {
                DecimalType decimalType = (DecimalType) dataType;
                options.put(IcebergDataTypeConvertor.PRECISION, decimalType.getPrecision());
                options.put(IcebergDataTypeConvertor.SCALE, decimalType.getScale());
            }
            Type type = icebergDataTypeConvertor.toConnectorTypeType(dataType, options);
            nestedFields.add(Types.NestedField.of(i + 1, true, name, type, column.getComment()));
        }
        return new org.apache.iceberg.Schema(nestedFields);
    }

    public static org.apache.iceberg.catalog.TableIdentifier toIcebergTableIdentifier(
            TablePath tablePath) {
        return org.apache.iceberg.catalog.TableIdentifier.of(
                tablePath.getDatabaseName(), tablePath.getTableName());
    }

    public static TablePath toTablePath(
            org.apache.iceberg.catalog.TableIdentifier tableIdentifier) {
        return TablePath.of(tableIdentifier.namespace().toString(), tableIdentifier.name());
    }
}
