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

package org.apache.seatunnel.connectors.seatunnel.hbase.catalog;

import org.apache.seatunnel.api.configuration.util.ConfigUtil;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.InfoPreviewResult;
import org.apache.seatunnel.api.table.catalog.PreviewResult;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseNotExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableNotExistException;
import org.apache.seatunnel.connectors.seatunnel.hbase.client.HbaseClient;
import org.apache.seatunnel.connectors.seatunnel.hbase.config.HbaseParameters;

import lombok.extern.slf4j.Slf4j;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkNotNull;

/** Hbase catalog implementation. */
@Slf4j
public class HbaseCatalog implements Catalog {

    private final String catalogName;
    private final String defaultDatabase;
    private final HbaseParameters hbaseParameters;

    private HbaseClient hbaseClient;

    public HbaseCatalog(
            String catalogName, String defaultDatabase, HbaseParameters hbaseParameters) {
        this.catalogName = checkNotNull(catalogName, "catalogName cannot be null");
        this.defaultDatabase = defaultDatabase;
        this.hbaseParameters = checkNotNull(hbaseParameters, "Hbase Config cannot be null");
    }

    @Override
    public void open() throws CatalogException {
        try {
            hbaseClient = HbaseClient.createInstance(hbaseParameters);
        } catch (Exception e) {
            throw new CatalogException(String.format("Failed to open catalog %s", catalogName), e);
        }
    }

    @Override
    public void close() throws CatalogException {
        hbaseClient.close();
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
        return hbaseClient.databaseExists(databaseName);
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        return hbaseClient.listDatabases();
    }

    @Override
    public List<String> listTables(String databaseName)
            throws CatalogException, DatabaseNotExistException {
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(catalogName, databaseName);
        }
        return hbaseClient.listTables(databaseName);
    }

    @Override
    public boolean tableExists(TablePath tablePath) throws CatalogException {
        checkNotNull(tablePath);
        return hbaseClient.tableExists(tablePath.getTableName());
    }

    @Override
    public CatalogTable getTable(TablePath tablePath)
            throws CatalogException, TableNotExistException {
        throw new UnsupportedOperationException("Not implement");
    }

    @Override
    public void createTable(TablePath tablePath, CatalogTable table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        checkNotNull(tablePath, "tablePath cannot be null");
        if (tableExists(tablePath)) {
            if (!ignoreIfExists) {
                throw new TableAlreadyExistException(catalogName, tablePath);
            }
            return;
        }
        hbaseClient.createTable(
                tablePath.getDatabaseName(),
                tablePath.getTableName(),
                hbaseParameters.getFamilyNames().values().stream()
                        .filter(value -> !"all_columns".equals(value))
                        .collect(Collectors.toList()),
                ignoreIfExists);
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
        hbaseClient.dropTable(tablePath.getDatabaseName(), tablePath.getTableName());
    }

    @Override
    public void createDatabase(TablePath tablePath, boolean ignoreIfExists)
            throws DatabaseAlreadyExistException, CatalogException {
        if (databaseExists(tablePath.getDatabaseName())) {
            if (!ignoreIfExists) {
                throw new DatabaseAlreadyExistException(catalogName, tablePath.getDatabaseName());
            }
            return;
        }
        hbaseClient.createNamespace(tablePath.getDatabaseName());
    }

    @Override
    public void dropDatabase(TablePath tablePath, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, CatalogException {
        if (!databaseExists(tablePath.getDatabaseName())) {
            if (!ignoreIfNotExists) {
                throw new DatabaseNotExistException(catalogName, tablePath.getDatabaseName());
            }
            return;
        }
        hbaseClient.deleteNamespace(tablePath.getDatabaseName());
    }

    @Override
    public void truncateTable(TablePath tablePath, boolean ignoreIfNotExists) {
        if (!tableExists(tablePath)) {
            if (!ignoreIfNotExists) {
                throw new TableNotExistException(catalogName, tablePath);
            }
            return;
        }
        hbaseClient.truncateTable(tablePath.getDatabaseName(), tablePath.getTableName());
    }

    @Override
    public boolean isExistsData(TablePath tablePath) {
        return hbaseClient.isExistsData(tablePath.getDatabaseName(), tablePath.getTableName());
    }

    private Map<String, String> buildTableOptions(TablePath tablePath) {
        Map<String, String> options = new HashMap<>();
        options.put("connector", "hbase");
        options.put("config", ConfigUtil.convertToJsonString(tablePath));
        return options;
    }

    @Override
    public PreviewResult previewAction(
            ActionType actionType, TablePath tablePath, Optional<CatalogTable> catalogTable) {
        if (actionType == ActionType.CREATE_TABLE) {
            return new InfoPreviewResult("create index " + tablePath.getTableName());
        } else if (actionType == ActionType.DROP_TABLE) {
            return new InfoPreviewResult("delete index " + tablePath.getTableName());
        } else if (actionType == ActionType.TRUNCATE_TABLE) {
            return new InfoPreviewResult("delete and create index " + tablePath.getTableName());
        } else if (actionType == ActionType.CREATE_DATABASE) {
            return new InfoPreviewResult("create index " + tablePath.getTableName());
        } else if (actionType == ActionType.DROP_DATABASE) {
            return new InfoPreviewResult("delete index " + tablePath.getTableName());
        } else {
            throw new UnsupportedOperationException("Unsupported action type: " + actionType);
        }
    }
}
