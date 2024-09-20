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

package org.apache.seatunnel.connectors.seatunnel.typesense.catalog;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigUtil;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.InfoPreviewResult;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.PreviewResult;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseNotExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableNotExistException;
import org.apache.seatunnel.api.table.converter.BasicTypeDefine;
import org.apache.seatunnel.connectors.seatunnel.typesense.client.TypesenseClient;
import org.apache.seatunnel.connectors.seatunnel.typesense.client.TypesenseType;

import lombok.extern.slf4j.Slf4j;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;

@Slf4j
public class TypesenseCatalog implements Catalog {

    private final String catalogName;
    private final String defaultDatabase;

    private final ReadonlyConfig config;
    private TypesenseClient typesenseClient;

    public TypesenseCatalog(String catalogName, String defaultDatabase, ReadonlyConfig config) {
        this.catalogName = checkNotNull(catalogName, "catalogName cannot be null");
        this.defaultDatabase = defaultDatabase;
        this.config = checkNotNull(config, "Typesense Config cannot be null");
    }

    @Override
    public void open() throws CatalogException {
        typesenseClient = TypesenseClient.createInstance(config);
    }

    @Override
    public void close() throws CatalogException {
        // Nothing
    }

    @Override
    public String name() {
        return catalogName;
    }

    @Override
    public String getDefaultDatabase() throws CatalogException {
        return defaultDatabase;
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        return typesenseClient.collectionExists(databaseName);
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        return typesenseClient.collectionList();
    }

    @Override
    public List<String> listTables(String databaseName)
            throws CatalogException, DatabaseNotExistException {
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(catalogName, databaseName);
        }
        return Arrays.asList(databaseName);
    }

    @Override
    public boolean tableExists(TablePath tablePath) throws CatalogException {
        checkNotNull(tablePath);
        return databaseExists(tablePath.getTableName());
    }

    @Override
    public CatalogTable getTable(TablePath tablePath)
            throws CatalogException, TableNotExistException {
        checkNotNull(tablePath, "tablePath cannot be null");
        TableSchema.Builder builder = TableSchema.builder();
        Map<String, BasicTypeDefine<TypesenseType>> fieldTypeMapping =
                typesenseClient.getFieldTypeMapping(tablePath.getTableName());
        buildColumnsWithErrorCheck(
                tablePath,
                builder,
                fieldTypeMapping.entrySet().iterator(),
                nameAndType -> {
                    return PhysicalColumn.of(
                            nameAndType.getKey(),
                            TypesenseTypeConverter.INSTANCE
                                    .convert(nameAndType.getValue())
                                    .getDataType(),
                            (Long) null,
                            true,
                            null,
                            null);
                });

        return CatalogTable.of(
                TableIdentifier.of(
                        catalogName, tablePath.getDatabaseName(), tablePath.getTableName()),
                builder.build(),
                buildTableOptions(tablePath),
                Collections.emptyList(),
                "");
    }

    private Map<String, String> buildTableOptions(TablePath tablePath) {
        Map<String, String> options = new HashMap<>();
        options.put("connector", "typesense");
        options.put("config", ConfigUtil.convertToJsonString(tablePath));
        return options;
    }

    @Override
    public void createTable(TablePath tablePath, CatalogTable table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        checkNotNull(tablePath, "tablePath cannot be null");
        if (!tableExists(tablePath)) {
            if (!ignoreIfExists) {
                throw new TableAlreadyExistException(catalogName, tablePath);
            }
            return;
        }
        typesenseClient.createCollection(tablePath.getTableName());
    }

    @Override
    public void dropTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        checkNotNull(tablePath);
        if (!tableExists(tablePath)) {
            if (!ignoreIfNotExists) {
                throw new TableNotExistException(catalogName, tablePath);
            }
            return;
        }
        try {
            typesenseClient.dropCollection(tablePath.getTableName());
        } catch (Exception ex) {
            throw new CatalogException(
                    String.format(
                            "Failed to drop table %s in catalog %s",
                            tablePath.getTableName(), catalogName),
                    ex);
        }
    }

    @Override
    public void createDatabase(TablePath tablePath, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException, CatalogException {
        createTable(tablePath, null, ignoreIfExists);
    }

    @Override
    public void dropDatabase(TablePath tablePath, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, CatalogException {
        dropTable(tablePath, ignoreIfNotExists);
    }

    @Override
    public void truncateTable(TablePath tablePath, boolean ignoreIfNotExists) {
        typesenseClient.truncateCollectionData(tablePath.getTableName());
    }

    @Override
    public boolean isExistsData(TablePath tablePath) {
        return typesenseClient.collectionDocNum(tablePath.getTableName()) > 0;
    }

    @Override
    public PreviewResult previewAction(
            ActionType actionType, TablePath tablePath, Optional<CatalogTable> catalogTable) {
        if (actionType == ActionType.CREATE_TABLE) {
            return new InfoPreviewResult("create collection " + tablePath.getTableName());
        } else if (actionType == ActionType.DROP_TABLE) {
            return new InfoPreviewResult("delete collection " + tablePath.getTableName());
        } else if (actionType == ActionType.TRUNCATE_TABLE) {
            return new InfoPreviewResult(
                    "delete and create collection " + tablePath.getTableName());
        } else if (actionType == ActionType.CREATE_DATABASE) {
            return new InfoPreviewResult("create collection " + tablePath.getTableName());
        } else if (actionType == ActionType.DROP_DATABASE) {
            return new InfoPreviewResult("delete collection " + tablePath.getTableName());
        } else {
            throw new UnsupportedOperationException("Unsupported action type: " + actionType);
        }
    }
}
