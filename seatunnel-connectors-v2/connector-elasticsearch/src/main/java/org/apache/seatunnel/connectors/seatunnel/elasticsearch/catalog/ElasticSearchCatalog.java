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

package org.apache.seatunnel.connectors.seatunnel.elasticsearch.catalog;

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
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsRestClient;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsType;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.dto.ElasticsearchClusterInfo;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.dto.source.IndexDocsCount;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import lombok.extern.slf4j.Slf4j;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Elasticsearch catalog implementation.
 *
 * <p>In ElasticSearch, we use the index as the database and table.
 */
@Slf4j
public class ElasticSearchCatalog implements Catalog {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticSearchCatalog.class);

    private final String catalogName;
    private final String defaultDatabase;
    private final ReadonlyConfig config;

    private EsRestClient esRestClient;

    // todo: do we need default database?
    public ElasticSearchCatalog(String catalogName, String defaultDatabase, ReadonlyConfig config) {
        this.catalogName = checkNotNull(catalogName, "catalogName cannot be null");
        this.defaultDatabase = defaultDatabase;
        this.config = checkNotNull(config, "elasticSearchConfig cannot be null");
    }

    @Override
    public void open() throws CatalogException {
        try {
            esRestClient = EsRestClient.createInstance(config);
            ElasticsearchClusterInfo elasticsearchClusterInfo = esRestClient.getClusterInfo();
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(
                        "Success open es catalog: {}, cluster info: {}",
                        catalogName,
                        elasticsearchClusterInfo);
            }
        } catch (Exception e) {
            throw new CatalogException(String.format("Failed to open catalog %s", catalogName), e);
        }
    }

    @Override
    public void close() throws CatalogException {
        esRestClient.close();
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
            return esRestClient.checkIndexExist(databaseName);
        } catch (Exception e) {
            log.error(
                    String.format(
                            "Failed to check if catalog %s database %s exists",
                            catalogName, databaseName),
                    e);
            return false;
        }
    }

    @Override
    public List<String> listDatabases() throws CatalogException {
        return esRestClient.listIndex();
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
        TableSchema.Builder builder = TableSchema.builder();
        Map<String, BasicTypeDefine<EsType>> fieldTypeMapping =
                esRestClient.getFieldTypeMapping(tablePath.getTableName(), Collections.emptyList());
        buildColumnsWithErrorCheck(
                tablePath,
                builder,
                fieldTypeMapping.entrySet().iterator(),
                nameAndType -> {
                    // todo: we need to add a new type TEXT or add length in STRING type
                    return PhysicalColumn.of(
                            nameAndType.getKey(),
                            ElasticSearchTypeConverter.INSTANCE
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

    @Override
    public void createTable(TablePath tablePath, CatalogTable table, boolean ignoreIfExists)
            throws TableAlreadyExistException, DatabaseNotExistException, CatalogException {
        // Create the index
        checkNotNull(tablePath, "tablePath cannot be null");
        if (tableExists(tablePath)) {
            if (!ignoreIfExists) {
                throw new TableAlreadyExistException(catalogName, tablePath);
            }
            return;
        }
        esRestClient.createIndex(tablePath.getTableName());
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
            esRestClient.dropIndex(tablePath.getTableName());
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

    @Override
    public void truncateTable(TablePath tablePath, boolean ignoreIfNotExists) {
        esRestClient.clearIndexData(tablePath.getTableName());
    }

    @Override
    public boolean isExistsData(TablePath tablePath) {
        final List<IndexDocsCount> indexDocsCount =
                esRestClient.getIndexDocsCount(tablePath.getTableName());
        return indexDocsCount.get(0).getDocsCount() > 0;
    }

    private Map<String, String> buildTableOptions(TablePath tablePath) {
        Map<String, String> options = new HashMap<>();
        options.put("connector", "elasticsearch");
        // todo: Right now, we don't use the config in the plugin config, do we need to add
        // bootstrapt servers here?
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
