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

package org.apache.seatunnel.connectors.seatunnel.easysearch.catalog;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.configuration.util.ConfigUtil;
import org.apache.seatunnel.api.table.catalog.Catalog;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.catalog.exception.CatalogException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.DatabaseNotExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableAlreadyExistException;
import org.apache.seatunnel.api.table.catalog.exception.TableNotExistException;
import org.apache.seatunnel.connectors.seatunnel.easysearch.client.EasysearchClient;
import org.apache.seatunnel.connectors.seatunnel.easysearch.dto.EasysearchClusterInfo;
import org.apache.seatunnel.connectors.seatunnel.easysearch.dto.source.IndexDocsCount;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Easysearch catalog implementation.
 *
 * <p>In Easysearch, we use the index as the database and table.
 */
public class EasysearchCatalog implements Catalog {

    private static final Logger LOGGER = LoggerFactory.getLogger(EasysearchCatalog.class);

    private final String catalogName;
    private final String defaultDatabase;
    private final Config pluginConfig;

    private EasysearchClient ezsClient;

    // todo: do we need default database?
    public EasysearchCatalog(String catalogName, String defaultDatabase, Config easySearchConfig) {
        this.catalogName = checkNotNull(catalogName, "catalogName cannot be null");
        this.defaultDatabase = defaultDatabase;
        this.pluginConfig = checkNotNull(easySearchConfig, "easySearchConfig cannot be null");
    }

    @Override
    public void open() throws CatalogException {
        try {
            ezsClient = EasysearchClient.createInstance(pluginConfig);
            EasysearchClusterInfo easysearchClusterInfo = ezsClient.getClusterInfo();
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(
                        "Success open ezs catalog: {}, cluster info: {}",
                        catalogName,
                        easysearchClusterInfo);
            }
        } catch (Exception e) {
            throw new CatalogException(String.format("Failed to open catalog %s", catalogName), e);
        }
    }

    @Override
    public void close() throws CatalogException {
        ezsClient.close();
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
        // check if the index exist
        try {
            List<IndexDocsCount> indexDocsCount = ezsClient.getIndexDocsCount(databaseName);
            return true;
        } catch (Exception e) {
            throw new CatalogException(
                    String.format(
                            "Failed to check if catalog %s database %s exists",
                            catalogName, databaseName),
                    e);
        }
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        return ezsClient.listIndex();
    }

    @Override
    public List<String> listTables(String databaseName)
            throws CatalogException, DatabaseNotExistException {
        if (!databaseExists(databaseName)) {
            throw new DatabaseNotExistException(catalogName, databaseName);
        }
        return Lists.newArrayList(databaseName);
    }

    @Override
    public boolean tableExists(TablePath tablePath) throws CatalogException {
        checkNotNull(tablePath);
        // todo: Check if the database name is the same with table name
        return databaseExists(tablePath.getTableName());
    }

    @Override
    public CatalogTable getTable(TablePath tablePath)
            throws CatalogException, TableNotExistException {
        // Get the index mapping?
        checkNotNull(tablePath, "tablePath cannot be null");
        EasysearchDataTypeConvertor easySearchDataTypeConvertor = new EasysearchDataTypeConvertor();
        TableSchema.Builder builder = TableSchema.builder();
        Map<String, String> fieldTypeMapping =
                ezsClient.getFieldTypeMapping(tablePath.getTableName(), Collections.emptyList());
        fieldTypeMapping.forEach(
                (fieldName, fieldType) -> {
                    // todo: we need to add a new type TEXT or add length in STRING type
                    PhysicalColumn physicalColumn =
                            PhysicalColumn.of(
                                    fieldName,
                                    easySearchDataTypeConvertor.toSeaTunnelType(
                                            fieldName, fieldType),
                                    (Long) null,
                                    true,
                                    null,
                                    null);
                    builder.column(physicalColumn);
                });

        return CatalogTable.of(
                TableIdentifier.of(
                        catalogName, tablePath.getDatabaseName(), tablePath.getTableName()),
                builder.build(),
                buildTableOptions(tablePath),
                Collections.emptyList(),
                "");
    }

    @Override
    public void createTable(TablePath tablePath, CatalogTable table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        // Create the index
        checkNotNull(tablePath, "tablePath cannot be null");
        if (tableExists(tablePath)) {
            if (!ignoreIfExists) {
                throw new TableAlreadyExistException(catalogName, tablePath, null);
            }
            return;
        }
        ezsClient.createIndex(tablePath.getTableName());
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
            ezsClient.dropIndex(tablePath.getTableName());
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
        try {
            createTable(tablePath, null, ignoreIfExists);
        } catch (TableAlreadyExistException ex) {
            throw new DatabaseAlreadyExistException(catalogName, tablePath.getDatabaseName());
        }
    }

    @Override
    public void dropDatabase(TablePath tablePath, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, CatalogException {
        try {
            dropTable(tablePath, ignoreIfNotExists);
        } catch (TableNotExistException ex) {
            throw new DatabaseNotExistException(catalogName, tablePath.getDatabaseName());
        }
    }

    private Map<String, String> buildTableOptions(TablePath tablePath) {
        Map<String, String> options = new HashMap<>();
        options.put("connector", "easysearch");
        // todo: Right now, we don't use the config in the plugin config, do we need to add
        // bootstrap servers here?
        options.put("config", ConfigUtil.convertToJsonString(tablePath));
        return options;
    }
}
