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

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.google.common.annotations.VisibleForTesting;
import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.options.EnvCommonOptions;
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
import org.apache.seatunnel.common.constants.MetaLakeType;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static org.apache.seatunnel.api.table.schema.exception.SchemaEvolutionErrorCode.GET_META_LAKE_TABLE_SCHEMA_FAILED;

@Slf4j
public class TableSchemaDiscoverer implements AutoCloseable {

    private final ReadonlyConfig envOptions;
    private final ReadonlyConfig sourceOptions;
    private final String catalogName;
    private MetalakeClient metalakeClient;
    private final MetaLakeTableSchemaConvertor metaLakeTableSchemaConvertor;

    public TableSchemaDiscoverer(TableSourceFactoryContext context, String catalogName) {
        this.envOptions = context.getEnvOptions();
        this.sourceOptions = context.getOptions();
        this.catalogName = catalogName;
        if (enableMetaLakeClient(context.getOptions())) {
            this.metalakeClient = MetaLakeFactory.createClient(getMetaLakeType());
        }
        this.metaLakeTableSchemaConvertor = MetaLakeFactory.createTypeMapper(getMetaLakeType());
    }

    @VisibleForTesting
    protected TableSchemaDiscoverer(
            ReadonlyConfig envOptions,
            ReadonlyConfig sourceOptions,
            String catalogName,
            MetalakeClient metalakeClient,
            MetaLakeTableSchemaConvertor convertor) {
        this.envOptions = envOptions;
        this.sourceOptions = sourceOptions;
        this.catalogName = catalogName;
        this.metalakeClient = metalakeClient;
        this.metaLakeTableSchemaConvertor = convertor;
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
        // schema_url
        if (schemaConfig.getOptional(ColumnOptions.SCHEMA_URL).isPresent()) {
            return discoverTableSchemaFromMetaLake(
                    schemaConfig.get(ColumnOptions.SCHEMA_URL),
                    schemaConfig.get(TableIdentifierOptions.TABLE));
        }
        return buildSimpleTextTable(schemaConfig);
    }

    private CatalogTable discoverTableSchemaFromConfig(ReadonlyConfig readonlyConfig) {
        return CatalogTableUtil.buildWithConfig(catalogName, readonlyConfig);
    }

    private CatalogTable discoverTableSchemaFromMetaLake(String schemaUrl, String configTablePath) {
        try {
            JsonNode schemaNode = metalakeClient.getTableSchema(schemaUrl);
            final TablePath tableSchemaPath;
            if (StringUtils.isNotEmpty(configTablePath)) {
                tableSchemaPath = TablePath.of(configTablePath);
            } else {
                tableSchemaPath = metalakeClient.getTableSchemaPath(schemaUrl);
            }
            final TableSchema tableSchema = metaLakeTableSchemaConvertor.convertor(schemaNode);
            return metaLakeTableSchemaConvertor.buildCatalogTable(
                    catalogName, tableSchemaPath, tableSchema);
        } catch (IOException e) {
            String errorMsg =
                    String.format(
                            "Failed to get table schema from MetaLake. "
                                    + "Schema URL: %s, "
                                    + "Configured table path: %s, "
                                    + "Catalog name: %s, "
                                    + "Error: %s",
                            schemaUrl,
                            configTablePath != null ? configTablePath : "not configured",
                            catalogName,
                            e.getMessage());
            throw new SeaTunnelRuntimeException(
                    GET_META_LAKE_TABLE_SCHEMA_FAILED, new IOException(errorMsg, e));
        }
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
    protected MetaLakeType getMetaLakeType() {
        // first source
        if (sourceOptions.getOptional(TableSchemaOptions.METALAKE_TYPE).isPresent()) {
            return sourceOptions.get(TableSchemaOptions.METALAKE_TYPE);
        }
        // second env
        if (envOptions != null) {
            if (envOptions.getOptional(EnvCommonOptions.METALAKE_TYPE).isPresent()) {
                return envOptions.get(EnvCommonOptions.METALAKE_TYPE);
            }
        }
        // third system
        if (StringUtils.isNotEmpty(
                System.getenv(EnvCommonOptions.METALAKE_TYPE.key().toUpperCase()))) {
            try {
                return MetaLakeType.valueOf(
                        System.getenv(EnvCommonOptions.METALAKE_TYPE.key().toUpperCase()));
            } catch (Exception e) {
                log.warn(
                        "The environment variable configuration is incorrect and automatically downgraded to GRAVITINO.",
                        e);
                return MetaLakeType.GRAVITINO;
            }
        }
        // default
        return MetaLakeType.GRAVITINO;
    }

    @VisibleForTesting
    protected boolean enableMetaLakeClient(ReadonlyConfig sourceOptions) {
        // schema
        if (sourceOptions.getOptional(ConnectorCommonOptions.SCHEMA).isPresent()) {
            final Map<String, Object> schemaMap = sourceOptions.get(ConnectorCommonOptions.SCHEMA);
            ReadonlyConfig schemaConfig = ReadonlyConfig.fromMap(schemaMap);
            if (schemaConfig.getOptional(ColumnOptions.SCHEMA_URL).isPresent()) {
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
                return schemaConfig.getOptional(ColumnOptions.SCHEMA_URL).isPresent();
            }
            return false;
        };
    }

    /** Close the metalake client and release resources. */
    @Override
    public void close() {
        if (metalakeClient != null) {
            metalakeClient.close();
        }
    }
}
