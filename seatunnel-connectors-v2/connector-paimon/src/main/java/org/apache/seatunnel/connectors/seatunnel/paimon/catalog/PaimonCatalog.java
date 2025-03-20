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

import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseNotExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableNotExistException;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.common.constants.PluginType;
import org.apache.seatunnel.connectors.seatunnel.paimon.config.PaimonConfig;
import org.apache.seatunnel.connectors.seatunnel.paimon.config.PaimonSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.paimon.exception.PaimonConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.paimon.exception.PaimonConnectorException;
import org.apache.seatunnel.connectors.seatunnel.paimon.sink.PaimonSink;
import org.apache.seatunnel.connectors.seatunnel.paimon.utils.SchemaUtil;

import org.apache.paimon.catalog.Identifier;
import org.apache.paimon.schema.Schema;
import org.apache.paimon.schema.SchemaChange;
import org.apache.paimon.table.FileStoreTable;
import org.apache.paimon.table.Table;
import org.apache.paimon.types.DataField;
import org.apache.paimon.types.DataType;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.seatunnel.api.table.factory.FactoryUtil.discoverFactory;

@Slf4j
public class PaimonCatalog implements Catalog, PaimonTable {
    private static final String DEFAULT_DATABASE = "default";

    private final String catalogName;
    private final ReadonlyConfig readonlyConfig;
    private final PaimonCatalogLoader paimonCatalogLoader;
    private org.apache.paimon.catalog.Catalog catalog;

    public PaimonCatalog(String catalogName, ReadonlyConfig readonlyConfig) {
        this.readonlyConfig = readonlyConfig;
        this.catalogName = catalogName;
        this.paimonCatalogLoader = new PaimonCatalogLoader(new PaimonConfig(readonlyConfig));
    }

    @Override
    public void open() throws CatalogException {
        this.catalog = paimonCatalogLoader.loadCatalog();
    }

    @Override
    public void close() throws CatalogException {
        if (catalog != null && catalog instanceof Closeable) {
            try {
                ((Closeable) catalog).close();
            } catch (IOException e) {
                log.error("Error while closing PaimonCatalog.", e);
                throw new CatalogException(e);
            }
        }
    }

    @Override
    public String name() {
        return this.catalogName;
    }

    @Override
    public String getDefaultDatabase() throws CatalogException {
        return DEFAULT_DATABASE;
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        return catalog.databaseExists(databaseName);
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        return catalog.listDatabases();
    }

    @Override
    public List<String> listTables(String databaseName)
            throws CatalogException, DatabaseNotExistException {
        try {
            return catalog.listTables(databaseName);
        } catch (org.apache.paimon.catalog.Catalog.DatabaseNotExistException e) {
            throw new DatabaseNotExistException(this.catalogName, databaseName);
        }
    }

    @Override
    public boolean tableExists(TablePath tablePath) throws CatalogException {
        return catalog.tableExists(toIdentifier(tablePath));
    }

    @Override
    public CatalogTable getTable(TablePath tablePath)
            throws CatalogException, TableNotExistException {
        try {
            FileStoreTable paimonFileStoreTableTable = (FileStoreTable) getPaimonTable(tablePath);
            return toCatalogTable(paimonFileStoreTableTable, tablePath);
        } catch (Exception e) {
            throw new TableNotExistException(this.catalogName, tablePath);
        }
    }

    public CatalogTable getTableWithProjection(TablePath tablePath, int[] projectionIndex)
            throws CatalogException, TableNotExistException {
        try {
            FileStoreTable paimonFileStoreTableTable = (FileStoreTable) getPaimonTable(tablePath);
            return toCatalogTable(paimonFileStoreTableTable, tablePath, projectionIndex);
        } catch (Exception e) {
            throw new TableNotExistException(this.catalogName, tablePath);
        }
    }

    @Override
    public Table getPaimonTable(TablePath tablePath)
            throws CatalogException, TableNotExistException {
        try {
            return catalog.getTable(toIdentifier(tablePath));
        } catch (org.apache.paimon.catalog.Catalog.TableNotExistException e) {
            throw new TableNotExistException(this.catalogName, tablePath);
        }
    }

    @Override
    public void createTable(TablePath tablePath, CatalogTable table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        try {
            Schema paimonSchema =
                    SchemaUtil.toPaimonSchema(
                            table.getTableSchema(), new PaimonSinkConfig(readonlyConfig));
            catalog.createTable(toIdentifier(tablePath), paimonSchema, ignoreIfExists);
        } catch (org.apache.paimon.catalog.Catalog.TableAlreadyExistException e) {
            throw new TableAlreadyExistException(this.catalogName, tablePath);
        } catch (org.apache.paimon.catalog.Catalog.DatabaseNotExistException e) {
            throw new DatabaseNotExistException(this.catalogName, tablePath.getDatabaseName());
        } catch (Exception e) {
            resolveException(e);
        }
    }

    @Override
    public void dropTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        try {
            catalog.dropTable(toIdentifier(tablePath), ignoreIfNotExists);
        } catch (org.apache.paimon.catalog.Catalog.TableNotExistException e) {
            throw new TableNotExistException(this.catalogName, tablePath);
        }
    }

    @Override
    public void createDatabase(TablePath tablePath, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException, CatalogException {
        try {
            catalog.createDatabase(tablePath.getDatabaseName(), ignoreIfExists);
        } catch (org.apache.paimon.catalog.Catalog.DatabaseAlreadyExistException e) {
            throw new DatabaseAlreadyExistException(this.catalogName, tablePath.getDatabaseName());
        }
    }

    @Override
    public void truncateTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        try {
            Identifier identifier = toIdentifier(tablePath);
            FileStoreTable table = (FileStoreTable) catalog.getTable(identifier);
            Schema schema = buildPaimonSchema(table.schema());
            dropTable(tablePath, ignoreIfNotExists);
            catalog.createTable(identifier, schema, ignoreIfNotExists);
        } catch (org.apache.paimon.catalog.Catalog.TableNotExistException e) {
            throw new TableNotExistException(this.catalogName, tablePath);
        } catch (org.apache.paimon.catalog.Catalog.TableAlreadyExistException e) {
            throw new DatabaseAlreadyExistException(this.catalogName, tablePath.getDatabaseName());
        } catch (org.apache.paimon.catalog.Catalog.DatabaseNotExistException e) {
            throw new DatabaseNotExistException(this.catalogName, tablePath.getDatabaseName());
        }
    }

    private Schema buildPaimonSchema(@NonNull org.apache.paimon.schema.TableSchema schema) {
        Schema.Builder builder = Schema.newBuilder();
        schema.fields()
                .forEach(field -> builder.column(field.name(), field.type(), field.description()));
        builder.options(schema.options());
        builder.primaryKey(schema.primaryKeys());
        builder.partitionKeys(schema.partitionKeys());
        builder.comment(schema.comment());
        return builder.build();
    }

    @Override
    public void dropDatabase(TablePath tablePath, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, CatalogException {
        try {
            catalog.dropDatabase(tablePath.getDatabaseName(), ignoreIfNotExists, true);
        } catch (Exception e) {
            throw new DatabaseNotExistException(this.catalogName, tablePath.getDatabaseName());
        }
    }

    private CatalogTable toCatalogTable(
            FileStoreTable paimonFileStoreTableTable, TablePath tablePath) {
        return toCatalogTable(paimonFileStoreTableTable, tablePath, null);
    }

    private CatalogTable toCatalogTable(
            FileStoreTable paimonFileStoreTableTable, TablePath tablePath, int[] projectionIndex) {
        org.apache.paimon.schema.TableSchema schema = paimonFileStoreTableTable.schema();
        List<DataField> dataFields = schema.fields();
        if (!Objects.isNull(projectionIndex)) {
            Map<Integer, DataField> indexMap =
                    IntStream.range(0, dataFields.size())
                            .boxed()
                            .collect(Collectors.toMap(i -> i, dataFields::get));

            dataFields =
                    java.util.Arrays.stream(projectionIndex)
                            .distinct()
                            .filter(indexMap::containsKey)
                            .mapToObj(indexMap::get)
                            .collect(Collectors.toList());
        }
        TableSchema.Builder builder = TableSchema.builder();
        dataFields.forEach(
                dataField -> {
                    BasicTypeDefine.BasicTypeDefineBuilder<DataType> typeDefineBuilder =
                            BasicTypeDefine.<DataType>builder()
                                    .name(dataField.name())
                                    .comment(dataField.description())
                                    .nativeType(dataField.type())
                                    .nullable(dataField.type().isNullable());
                    Column column = SchemaUtil.toSeaTunnelType(typeDefineBuilder.build());
                    builder.column(column);
                });

        List<String> partitionKeys = schema.partitionKeys();

        return CatalogTable.of(
                org.apache.seatunnel.api.table.catalog.TableIdentifier.of(
                        catalogName, tablePath.getDatabaseName(), tablePath.getTableName()),
                builder.build(),
                paimonFileStoreTableTable.options(),
                partitionKeys,
                null,
                catalogName);
    }

    private Identifier toIdentifier(TablePath tablePath) {
        return Identifier.create(tablePath.getDatabaseName(), tablePath.getTableName());
    }

    private void resolveException(Exception e) {
        Throwable cause = e.getCause();
        if (cause instanceof UnsupportedOperationException) {
            String message = cause.getMessage();
            if (message.contains("The type ")
                    && message.contains(" in primary key field ")
                    && message.contains(" is unsupported")) {
                throw new PaimonConnectorException(
                        PaimonConnectorErrorCode.UNSUPPORTED_PRIMARY_DATATYPE, message);
            }
        } else if (cause instanceof RuntimeException) {
            String message = cause.getMessage();
            if (message.contains("Cannot define 'bucket-key' in unaware or dynamic bucket mode.")) {
                throw new PaimonConnectorException(
                        PaimonConnectorErrorCode.WRITE_PROPS_BUCKET_KEY_ERROR, message);
            }
        }
        throw new CatalogException("An unexpected error occurred", e);
    }

    // --------------------------------------------------------------------------------------------
    // SPI load paimon catalog
    // --------------------------------------------------------------------------------------------

    public static PaimonCatalog loadPaimonCatalog(ReadonlyConfig readonlyConfig) {
        org.apache.seatunnel.api.table.factory.CatalogFactory catalogFactory =
                discoverFactory(
                        Thread.currentThread().getContextClassLoader(),
                        org.apache.seatunnel.api.table.factory.CatalogFactory.class,
                        PaimonSink.PLUGIN_NAME);
        if (catalogFactory == null) {
            throw new PaimonConnectorException(
                    SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                    String.format(
                            "PluginName: %s, PluginType: %s, Message: %s",
                            PaimonSink.PLUGIN_NAME,
                            PluginType.SINK,
                            "Cannot find paimon catalog factory"));
        }
        return (PaimonCatalog)
                catalogFactory.createCatalog(catalogFactory.factoryIdentifier(), readonlyConfig);
    }

    // --------------------------------------------------------------------------------------------
    // alterTable
    // --------------------------------------------------------------------------------------------

    public void alterTable(
            Identifier identifier, SchemaChange schemaChange, boolean ignoreIfNotExists) {
        try {
            catalog.alterTable(identifier, schemaChange, true);
        } catch (org.apache.paimon.catalog.Catalog.TableNotExistException e) {
            throw new CatalogException("TableNotExistException: {}", e);
        } catch (org.apache.paimon.catalog.Catalog.ColumnAlreadyExistException e) {
            throw new CatalogException("ColumnAlreadyExistException: {}", e);
        } catch (org.apache.paimon.catalog.Catalog.ColumnNotExistException e) {
            throw new CatalogException("ColumnNotExistException: {}", e);
        }
    }
}
