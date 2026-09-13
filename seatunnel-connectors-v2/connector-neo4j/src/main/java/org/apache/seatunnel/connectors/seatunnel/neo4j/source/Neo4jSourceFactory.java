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

package org.apache.seatunnel.connectors.seatunnel.neo4j.source;

import org.apache.seatunnel.shade.com.typesafe.config.ConfigValueFactory;

import org.apache.seatunnel.api.common.SeaTunnelAPIErrorCode;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConditionExtension;
import org.apache.seatunnel.api.configuration.util.Conditions;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.connectors.seatunnel.neo4j.config.Neo4jAuthenticationConditions;
import org.apache.seatunnel.connectors.seatunnel.neo4j.config.Neo4jSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.neo4j.config.Neo4jSourceQueryInfo;
import org.apache.seatunnel.connectors.seatunnel.neo4j.exception.Neo4jConnectorException;

import com.google.auto.service.AutoService;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@AutoService(Factory.class)
public class Neo4jSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return Neo4jSourceOptions.PLUGIN_NAME;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        Neo4jSourceOptions.KEY_NEO4J_URI,
                        Conditions.extension(
                                Neo4jSourceOptions.KEY_NEO4J_URI,
                                Neo4jAuthenticationConditions.AUTHENTICATION_METHOD))
                .required(Neo4jSourceOptions.KEY_DATABASE)
                .exclusive(Neo4jSourceOptions.KEY_QUERY, ConnectorCommonOptions.TABLE_CONFIGS)
                .optional(
                        Neo4jSourceOptions.KEY_QUERY,
                        Conditions.notBlank(Neo4jSourceOptions.KEY_QUERY),
                        Conditions.extension(
                                Neo4jSourceOptions.KEY_QUERY, new SingleTableConfigValidator()))
                .optional(
                        ConnectorCommonOptions.TABLE_CONFIGS,
                        Conditions.notEmpty(ConnectorCommonOptions.TABLE_CONFIGS),
                        Conditions.extension(
                                ConnectorCommonOptions.TABLE_CONFIGS, new TableConfigsValidator()))
                .optional(
                        Neo4jSourceOptions.KEY_USERNAME,
                        Conditions.extension(
                                Neo4jSourceOptions.KEY_USERNAME,
                                Neo4jAuthenticationConditions.USERNAME_REQUIRES_PASSWORD))
                .optional(
                        Neo4jSourceOptions.KEY_PASSWORD,
                        Neo4jSourceOptions.KEY_BEARER_TOKEN,
                        Neo4jSourceOptions.KEY_KERBEROS_TICKET,
                        Neo4jSourceOptions.KEY_MAX_CONNECTION_TIMEOUT,
                        Neo4jSourceOptions.KEY_MAX_TRANSACTION_RETRY_TIME,
                        ConnectorCommonOptions.SCHEMA)
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return Neo4jSource.class;
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        return () -> (SeaTunnelSource<T, SplitT, StateT>) createNeo4jSource(context.getOptions());
    }

    private Neo4jSource createNeo4jSource(ReadonlyConfig config) {
        if (!config.getOptional(ConnectorCommonOptions.TABLE_CONFIGS).isPresent()) {
            return new Neo4jSource(
                    CatalogTableUtil.buildWithConfig(config),
                    new Neo4jSourceQueryInfo(config.toConfig()));
        }

        List<Map<String, Object>> entries = config.get(ConnectorCommonOptions.TABLE_CONFIGS);
        if (entries.isEmpty()) {
            throw configError("'tables_configs' must not be empty");
        }

        List<CatalogTable> catalogTables = new ArrayList<>(entries.size());
        List<Neo4jSourceTableConfig> tableConfigs = new ArrayList<>(entries.size());
        Set<String> tableIds = new HashSet<>();

        for (int i = 0; i < entries.size(); i++) {
            ReadonlyConfig tableConfig = ReadonlyConfig.fromMap(entries.get(i));
            String query = tableConfig.getOptional(Neo4jSourceOptions.KEY_QUERY).orElse(null);
            if (query == null || query.trim().isEmpty()) {
                throw configError(
                        String.format(
                                "tables_configs[%d]: 'query' must be configured and non-blank", i));
            }

            CatalogTable catalogTable;
            try {
                catalogTable = CatalogTableUtil.buildWithConfig(tableConfig);
            } catch (RuntimeException e) {
                throw new Neo4jConnectorException(
                        SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED,
                        String.format("tables_configs[%d]: invalid 'schema' configuration", i),
                        e);
            }

            String tableId = catalogTable.getTableId().toTablePath().toString();
            if (!tableIds.add(tableId)) {
                throw configError(
                        String.format(
                                "Duplicate schema.table '%s' found in tables_configs", tableId));
            }

            catalogTables.add(catalogTable);
            tableConfigs.add(
                    new Neo4jSourceTableConfig(query, catalogTable.getSeaTunnelRowType(), tableId));
        }

        Neo4jSourceQueryInfo connectionInfo =
                new Neo4jSourceQueryInfo(
                        config.toConfig()
                                .withValue(
                                        Neo4jSourceOptions.KEY_QUERY.key(),
                                        ConfigValueFactory.fromAnyRef(
                                                tableConfigs.get(0).getQuery())));
        return new Neo4jSource(catalogTables, connectionInfo, tableConfigs);
    }

    private static Neo4jConnectorException configError(String message) {
        return new Neo4jConnectorException(SeaTunnelAPIErrorCode.CONFIG_VALIDATION_FAILED, message);
    }

    static class SingleTableConfigValidator implements ConditionExtension<String> {

        @Override
        public String description() {
            return "'schema' must be configured when using a root-level 'query'";
        }

        @Override
        public boolean evaluate(ReadonlyConfig config, String query)
                throws OptionValidationException {
            Map<String, Object> schema =
                    config.getOptional(ConnectorCommonOptions.SCHEMA).orElse(null);
            if (schema == null || schema.isEmpty()) {
                throw new OptionValidationException(
                        "'schema' must be configured when using a root-level 'query'");
            }
            return true;
        }
    }

    static class TableConfigsValidator implements ConditionExtension<List<Map<String, Object>>> {

        @Override
        public String description() {
            return "each 'tables_configs' entry must contain a non-blank 'query' and a schema with a unique 'table'";
        }

        @Override
        public boolean evaluate(ReadonlyConfig config, List<Map<String, Object>> entries)
                throws OptionValidationException {
            if (config.getOptional(ConnectorCommonOptions.SCHEMA).isPresent()) {
                throw new OptionValidationException(
                        "root-level 'schema' cannot be used with 'tables_configs'");
            }

            Set<String> tableIds = new HashSet<>();
            for (int i = 0; i < entries.size(); i++) {
                Map<String, Object> entry = entries.get(i);
                Object query = entry.get(Neo4jSourceOptions.KEY_QUERY.key());
                if (!(query instanceof String) || ((String) query).trim().isEmpty()) {
                    throw new OptionValidationException(
                            "tables_configs[%d]: 'query' must be configured and non-blank", i);
                }

                Object schemaValue = entry.get(ConnectorCommonOptions.SCHEMA.key());
                if (!(schemaValue instanceof Map) || ((Map<?, ?>) schemaValue).isEmpty()) {
                    throw new OptionValidationException(
                            "tables_configs[%d]: 'schema' must be configured and non-empty", i);
                }

                Object tableValue =
                        ((Map<?, ?>) schemaValue).get(ConnectorCommonOptions.TABLE.key());
                if (!(tableValue instanceof String) || ((String) tableValue).trim().isEmpty()) {
                    throw new OptionValidationException(
                            "tables_configs[%d]: 'schema.table' must be configured and non-blank",
                            i);
                }

                String tableId = ((String) tableValue).trim();
                if (!tableIds.add(tableId)) {
                    throw new OptionValidationException(
                            "tables_configs[%d]: duplicate 'schema.table' value '%s'", i, tableId);
                }
            }
            return true;
        }
    }
}
