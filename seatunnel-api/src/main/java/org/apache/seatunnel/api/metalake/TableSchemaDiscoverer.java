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

package org.apache.seatunnel.api.metalake;

import org.apache.seatunnel.shade.com.google.common.annotations.VisibleForTesting;
import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.metadata.MetadataConfig;
import org.apache.seatunnel.api.metadata.MetadataProviderManager;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.options.table.CatalogOptions;
import org.apache.seatunnel.api.options.table.ColumnOptions;
import org.apache.seatunnel.api.options.table.FieldOptions;
import org.apache.seatunnel.api.options.table.TableIdentifierOptions;
import org.apache.seatunnel.api.options.table.TableSchemaOptions;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@Slf4j
public class TableSchemaDiscoverer implements AutoCloseable {

    private final MetadataConfig metaDataConfig;
    private final ReadonlyConfig sourceOptions;
    private final String catalogName;

    public TableSchemaDiscoverer(TableSourceFactoryContext context, String catalogName) {
        this.metaDataConfig = context.getMetadataConfig();
        this.sourceOptions = context.getOptions();
        this.catalogName = catalogName;
    }

    @VisibleForTesting
    protected TableSchemaDiscoverer(
            MetadataConfig metaDataConfig, ReadonlyConfig sourceOptions, String catalogName) {
        this.sourceOptions = sourceOptions;
        this.catalogName = catalogName;
        this.metaDataConfig = metaDataConfig;
    }

    public List<CatalogTable> discoverTableSchemas() {
        // schema
        if (sourceOptions.getOptional(ConnectorCommonOptions.SCHEMA).isPresent()) {
            return Collections.singletonList(discoverTableSchema(sourceOptions));
        }
        // table_config
        if (sourceOptions.getOptional(TableSchemaOptions.TABLE_CONFIGS).isPresent()) {
            return sourceOptions.get(TableSchemaOptions.TABLE_CONFIGS).stream()
                    .map(ReadonlyConfig::fromMap)
                    .map(this::discoverTableSchema)
                    .collect(Collectors.toList());
        }
        // table_list
        if (sourceOptions.getOptional(CatalogOptions.TABLE_LIST).isPresent()) {
            return sourceOptions.get(CatalogOptions.TABLE_LIST).stream()
                    .map(ReadonlyConfig::fromMap)
                    .map(this::discoverTableSchema)
                    .collect(Collectors.toList());
        }
        return Collections.singletonList(CatalogTableUtil.buildSimpleTextTable());
    }

    private CatalogTable discoverTableSchema(ReadonlyConfig sourceOptions) {
        final Map<String, Object> schemaMap = sourceOptions.get(ConnectorCommonOptions.SCHEMA);
        ReadonlyConfig schemaConfig = ReadonlyConfig.fromMap(schemaMap);
        // fields or columns
        if (schemaConfig.getOptional(ColumnOptions.COLUMNS).isPresent()
                || sourceOptions.getOptional(FieldOptions.FIELDS).isPresent()) {
            return discoverTableSchemaFromConfig(sourceOptions);
        }
        // metadata_table_id
        if (schemaConfig.getOptional(ColumnOptions.METADATA_TABLE_ID).isPresent()) {
            return discoverTableSchemaFromMetaLake(
                    schemaConfig.get(ColumnOptions.METADATA_TABLE_ID),
                    schemaConfig.get(TableIdentifierOptions.TABLE));
        }
        return buildSimpleTextTable(schemaConfig);
    }

    private CatalogTable discoverTableSchemaFromConfig(ReadonlyConfig readonlyConfig) {
        return CatalogTableUtil.buildWithConfig(catalogName, readonlyConfig);
    }

    private CatalogTable discoverTableSchemaFromMetaLake(
            String metaDataTableId, String schemaConfigTable) {
        Optional<TableSchema> tableSchema =
                MetadataProviderManager.resolveTableSchema(metaDataTableId, metaDataConfig);
        if (!tableSchema.isPresent()) {
            return CatalogTableUtil.buildSimpleTextTable();
        }
        TableIdentifier tableIdentifier;
        if (StringUtils.isEmpty(schemaConfigTable)) {
            tableIdentifier = TableIdentifier.of(catalogName, TablePath.of(metaDataTableId));
        } else {
            tableIdentifier = TableIdentifier.of(catalogName, TablePath.of(schemaConfigTable));
        }
        return CatalogTable.of(
                tableIdentifier,
                tableSchema.get(),
                new HashMap<>(),
                new ArrayList<>(),
                null,
                catalogName);
    }

    private CatalogTable buildSimpleTextTable(ReadonlyConfig schemaConfig) {
        CatalogTable catalogTable = CatalogTableUtil.buildSimpleTextTable();
        if (schemaConfig.getOptional(TableIdentifierOptions.TABLE).isPresent()) {
            String table = schemaConfig.get(TableIdentifierOptions.TABLE);
            return CatalogTable.of(
                    TableIdentifier.of(catalogName, TablePath.of(table)), catalogTable);
        }
        return catalogTable;
    }

    @VisibleForTesting
    protected boolean enableMetaLakeClient(ReadonlyConfig sourceOptions) {
        // schema
        if (sourceOptions.getOptional(ConnectorCommonOptions.SCHEMA).isPresent()) {
            final Map<String, Object> schemaMap = sourceOptions.get(ConnectorCommonOptions.SCHEMA);
            ReadonlyConfig schemaConfig = ReadonlyConfig.fromMap(schemaMap);
            if (schemaConfig.getOptional(ColumnOptions.METADATA_TABLE_ID).isPresent()) {
                return true;
            }
        }
        // table_config
        if (sourceOptions.getOptional(TableSchemaOptions.TABLE_CONFIGS).isPresent()) {
            return sourceOptions.get(TableSchemaOptions.TABLE_CONFIGS).stream()
                    .map(ReadonlyConfig::fromMap)
                    .anyMatch(this.getEnableMetaLakeClientPredicate());
        }
        // table_list
        if (sourceOptions.getOptional(CatalogOptions.TABLE_LIST).isPresent()) {
            return sourceOptions.get(CatalogOptions.TABLE_LIST).stream()
                    .map(ReadonlyConfig::fromMap)
                    .anyMatch(this.getEnableMetaLakeClientPredicate());
        }
        return false;
    }

    private Predicate<ReadonlyConfig> getEnableMetaLakeClientPredicate() {
        return config -> {
            if (config.getOptional(ConnectorCommonOptions.SCHEMA).isPresent()) {
                final Map<String, Object> schemaMap = config.get(ConnectorCommonOptions.SCHEMA);
                ReadonlyConfig schemaConfig = ReadonlyConfig.fromMap(schemaMap);
                return schemaConfig.getOptional(ColumnOptions.METADATA_TABLE_ID).isPresent();
            }
            return false;
        };
    }

    /** Close the metalake client and release resources. */
    @Override
    public void close() {}
}
