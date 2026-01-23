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
import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.options.EnvCommonOptions;
import org.apache.seatunnel.api.options.table.CatalogOptions;
import org.apache.seatunnel.api.options.table.ColumnOptions;
import org.apache.seatunnel.api.options.table.FieldOptions;
import org.apache.seatunnel.api.options.table.TableSchemaOptions;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.common.constants.MetaLakeType;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.seatunnel.api.table.schema.exception.SchemaEvolutionErrorCode.GET_META_LAKE_TABLE_SCHEMA_FAILED;
import static org.apache.seatunnel.api.table.schema.exception.SchemaEvolutionErrorCode.INVALID_SCHEMA_STRUCTURE;

@Slf4j
public class MetaLakeSchemaDiscoverer {

    private final ReadonlyConfig envOptions;
    private final ReadonlyConfig sourceOptions;
    private final String catalogName;
    private final MetalakeClient metalakeClient;
    private final MetaLakeTypeMapper metaLakeTypeMapper;

    public MetaLakeSchemaDiscoverer(TableSourceFactoryContext context, String catalogName) {
        this.envOptions = context.getEnvOptions();
        this.sourceOptions = context.getOptions();
        this.catalogName = catalogName;
        this.metalakeClient = MetaLakeFactory.createClient(getMetaLakeType());
        this.metaLakeTypeMapper = MetaLakeFactory.createTypeMapper(getMetaLakeType());
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
        return new ArrayList<>();
    }

    private CatalogTable discoverTableSchema(ReadonlyConfig schemaConfig) {
        // fields or columns
        if (schemaConfig.getOptional(ColumnOptions.COLUMNS).isPresent()
                || schemaConfig.getOptional(FieldOptions.FIELDS).isPresent()) {
            return discoverTableSchemaFromConfig(schemaConfig);
        }
        // schema_url
        if (schemaConfig.getOptional(ColumnOptions.SCHEMA_URL).isPresent()) {
            return discoverTableSchemaFromMetaLake(schemaConfig.get(ColumnOptions.SCHEMA_URL));
        }
        throw new SeaTunnelRuntimeException(
                INVALID_SCHEMA_STRUCTURE,
                "Schema config need option [schema], please correct your config first");
    }

    private CatalogTable discoverTableSchemaFromConfig(ReadonlyConfig readonlyConfig) {
        return CatalogTableUtil.buildWithConfig(catalogName, readonlyConfig);
    }

    private CatalogTable discoverTableSchemaFromMetaLake(String schemaUrl) {
        try {
            JsonNode schemaNode = metalakeClient.getTableSchema(schemaUrl);
            return metaLakeTypeMapper.convertor(schemaNode);
        } catch (IOException e) {
            throw new SeaTunnelRuntimeException(GET_META_LAKE_TABLE_SCHEMA_FAILED, e);
        }
    }

    private MetaLakeType getMetaLakeType() {
        // first source
        if (sourceOptions.getOptional(TableSchemaOptions.METALAKE_TYPE).isPresent()) {
            return sourceOptions.get(TableSchemaOptions.METALAKE_TYPE);
        }
        // second env
        if (envOptions.getOptional(EnvCommonOptions.METALAKE_TYPE).isPresent()) {
            return envOptions.get(EnvCommonOptions.METALAKE_TYPE);
        }
        // third system
        if (StringUtils.isNotEmpty(
                System.getenv(EnvCommonOptions.METALAKE_TYPE.key().toUpperCase()))) {
            return MetaLakeType.valueOf(
                    System.getenv(EnvCommonOptions.METALAKE_TYPE.key().toUpperCase()));
        }
        // default
        return MetaLakeType.GRAVITINO;
    }
}
