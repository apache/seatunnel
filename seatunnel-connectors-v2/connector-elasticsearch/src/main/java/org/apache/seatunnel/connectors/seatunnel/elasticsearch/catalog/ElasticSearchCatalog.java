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
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.client.EsRestClient;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.dto.ElasticsearchClusterInfo;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.dto.source.IndexDocsCount;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Elasticsearch catalog implementation.
 *
 * <p>In ElasticSearch, we use the index as the database and table.
 */
public class ElasticSearchCatalog implements Catalog {

    private static final Logger LOGGER = LoggerFactory.getLogger(ElasticSearchCatalog.class);

    private final String catalogName;
    private final String defaultDatabase;
    private final Config pluginConfig;

    private EsRestClient esRestClient;

    // todo: do we need default database?
    public ElasticSearchCatalog(
            String catalogName, String defaultDatabase, Config elasticSearchConfig) {
        this.catalogName = checkNotNull(catalogName, "catalogName cannot be null");
        this.defaultDatabase = defaultDatabase;
        this.pluginConfig = checkNotNull(elasticSearchConfig, "elasticSearchConfig cannot be null");
    }

    @Override
    public void open() throws CatalogException {
        try {
            esRestClient = EsRestClient.createInstance(pluginConfig);
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
    public String getDefaultDatabase() throws CatalogException {
        return defaultDatabase;
    }

    @Override
    public boolean databaseExists(String databaseName) throws CatalogException {
        // check if the index exist
        try {
            List<IndexDocsCount> indexDocsCount = esRestClient.getIndexDocsCount(databaseName);
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
        ElasticSearchDataTypeConvertor elasticSearchDataTypeConvertor =
                new ElasticSearchDataTypeConvertor();
        TableSchema.Builder builder = TableSchema.builder();
        Map<String, String> fieldTypeMapping =
                esRestClient.getFieldTypeMapping(tablePath.getTableName(), Collections.emptyList());
        fieldTypeMapping.forEach(
                (fieldName, fieldType) -> {
                    // todo: we need to add a new type TEXT or add length in STRING type
                    PhysicalColumn physicalColumn =
                            PhysicalColumn.of(
                                    fieldName,
                                    elasticSearchDataTypeConvertor.toSeaTunnelType(
                                            fieldName, fieldType),
                                    null,
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
            if (ignoreIfExists) {
                return;
            } else {
                throw new TableAlreadyExistException(catalogName, tablePath, null);
            }
        }
        esRestClient.createIndex(tablePath.getTableName());
    }

    @Override
    public void dropTable(TablePath tablePath, boolean ignoreIfNotExists)
            throws TableNotExistException, CatalogException {
        checkNotNull(tablePath);
        if (!tableExists(tablePath) && !ignoreIfNotExists) {
            throw new TableNotExistException(catalogName, tablePath);
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
        createTable(tablePath, null, ignoreIfExists);
    }

    @Override
    public void dropDatabase(TablePath tablePath, boolean ignoreIfNotExists)
            throws DatabaseNotExistException, CatalogException {
        dropTable(tablePath, ignoreIfNotExists);
    }

    private Map<String, String> buildTableOptions(TablePath tablePath) {
        Map<String, String> options = new HashMap<>();
        options.put("connector", "elasticsearch");
        // todo: Right now, we don't use the config in the plugin config, do we need to add
        // bootstrapt servers here?
        options.put("config", ConfigUtil.convertToJsonString(tablePath));
        return options;
    }
}
