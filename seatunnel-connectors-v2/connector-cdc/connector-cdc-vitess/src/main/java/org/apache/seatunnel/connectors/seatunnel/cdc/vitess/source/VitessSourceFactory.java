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

package org.apache.seatunnel.connectors.seatunnel.cdc.vitess.source;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.options.ConnectorCommonOptions;
import org.apache.seatunnel.api.options.table.ColumnOptions;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.ChangeStreamTableSourceFactory;
import org.apache.seatunnel.api.table.factory.ChangeStreamTableSourceState;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.connectors.cdc.base.option.SourceOptions;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.seatunnel.cdc.vitess.config.VitessSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.vitess.config.VitessSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.cdc.vitess.source.split.VitessSourceSplit;

import com.google.auto.service.AutoService;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/** Factory for the Vitess CDC source connector. */
@AutoService(Factory.class)
public class VitessSourceFactory implements ChangeStreamTableSourceFactory {

    @Override
    public String factoryIdentifier() {
        return VitessSource.IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(VitessSourceOptions.HOSTNAME, VitessSourceOptions.KEYSPACE)
                .exclusive(ConnectorCommonOptions.SCHEMA, ConnectorCommonOptions.TABLE_CONFIGS)
                .optional(
                        ConnectorCommonOptions.TABLE_NAMES,
                        ConnectorCommonOptions.TABLE_PATTERN,
                        ConnectorCommonOptions.METALAKE_TYPE,
                        VitessSourceOptions.PORT,
                        VitessSourceOptions.USERNAME,
                        VitessSourceOptions.PASSWORD,
                        VitessSourceOptions.SHARD,
                        VitessSourceOptions.STARTUP_MODE,
                        VitessSourceOptions.TABLET_TYPE,
                        VitessSourceOptions.STOP_ON_RESHARD,
                        VitessSourceOptions.KEEPALIVE_INTERVAL_MS,
                        VitessSourceOptions.GRPC_HEADERS,
                        VitessSourceOptions.GRPC_MAX_INBOUND_MESSAGE_SIZE,
                        VitessSourceOptions.SERVER_TIME_ZONE,
                        SourceOptions.FORMAT,
                        SourceOptions.DEBEZIUM_PROPERTIES)
                .conditional(
                        VitessSourceOptions.STARTUP_MODE,
                        StartupMode.SPECIFIC,
                        VitessSourceOptions.STARTUP_SPECIFIC_OFFSET_VGTID)
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return VitessSource.class;
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        return createVitessTableSource(context, Collections.emptyList());
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> restoreSource(
                    TableSourceFactoryContext context,
                    ChangeStreamTableSourceState<StateT, SplitT> state) {
        return createVitessTableSource(context, extractCheckpointTables(state));
    }

    @SuppressWarnings("unchecked")
    private <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createVitessTableSource(
                    TableSourceFactoryContext context, List<CatalogTable> checkpointTables) {
        return () -> {
            List<CatalogTable> catalogTables =
                    checkpointTables.isEmpty() ? resolveCatalogTables(context) : checkpointTables;
            VitessSourceConfig sourceConfig =
                    VitessSourceConfig.of(context.getOptions(), catalogTables);
            return (SeaTunnelSource<T, SplitT, StateT>) new VitessSource(sourceConfig);
        };
    }

    /**
     * Vitess does not provide a SeaTunnel catalog implementation yet, so the first delivery keeps
     * schema discovery deterministic by requiring explicit schema metadata from the user.
     */
    private List<CatalogTable> resolveCatalogTables(TableSourceFactoryContext context) {
        validateSchemaMetadataContract(context);
        validateSelectionOptions(context);
        return filterDeclaredTables(context, discoverTableSchemas(context));
    }

    private void validateSchemaMetadataContract(TableSourceFactoryContext context) {
        Optional<Map<String, Object>> singleSchema =
                context.getOptions().getOptional(ConnectorCommonOptions.SCHEMA);
        Optional<List<Map<String, Object>>> tableConfigs =
                context.getOptions().getOptional(ConnectorCommonOptions.TABLE_CONFIGS);
        if (singleSchema.isPresent()) {
            validateRootSchemaDefinition(singleSchema.get(), "schema");
            return;
        }
        if (tableConfigs.isPresent()) {
            List<Map<String, Object>> configs = tableConfigs.get();
            if (configs.isEmpty()) {
                throw new IllegalArgumentException("tables_configs can not be empty.");
            }
            for (int index = 0; index < configs.size(); index++) {
                validateSchemaDefinition(configs.get(index), "tables_configs[" + index + "]");
            }
            return;
        }
        throw new IllegalArgumentException(
                "Vitess CDC requires explicit schema metadata through either 'schema' or 'tables_configs'.");
    }

    @SuppressWarnings("unchecked")
    private void validateRootSchemaDefinition(Map<String, Object> schema, String optionName) {
        if (!schema.containsKey(ColumnOptions.COLUMNS.key())
                && !schema.containsKey(ColumnOptions.METADATA_TABLE_ID.key())) {
            throw new IllegalArgumentException(
                    String.format(
                            "Vitess CDC requires explicit columns or metadata_table_id in '%s' so table schemas stay deterministic.",
                            optionName));
        }
    }

    @SuppressWarnings("unchecked")
    private void validateSchemaDefinition(Map<String, Object> config, String optionName) {
        Object schemaValue = config.get(ConnectorCommonOptions.SCHEMA.key());
        if (!(schemaValue instanceof Map)) {
            throw new IllegalArgumentException(
                    String.format(
                            "Vitess CDC requires explicit schema metadata. '%s' must contain a schema block.",
                            optionName));
        }
        validateRootSchemaDefinition((Map<String, Object>) schemaValue, optionName + ".schema");
    }

    private void validateSelectionOptions(TableSourceFactoryContext context) {
        if (context.getOptions().getOptional(ConnectorCommonOptions.TABLE_NAMES).isPresent()
                && context.getOptions()
                        .getOptional(ConnectorCommonOptions.TABLE_PATTERN)
                        .isPresent()) {
            throw new IllegalArgumentException(
                    "Vitess CDC accepts either table-names or table-pattern, but not both.");
        }
    }

    private List<CatalogTable> filterDeclaredTables(
            TableSourceFactoryContext context, List<CatalogTable> declaredTables) {
        Optional<List<String>> explicitTableNames =
                context.getOptions().getOptional(ConnectorCommonOptions.TABLE_NAMES);
        if (explicitTableNames.isPresent()) {
            List<CatalogTable> selectedTables = new ArrayList<>(explicitTableNames.get().size());
            for (String tableName : explicitTableNames.get()) {
                CatalogTable matchedTable =
                        declaredTables.stream()
                                .filter(table -> table.getTablePath().toString().equals(tableName))
                                .findFirst()
                                .orElseThrow(
                                        () ->
                                                new IllegalArgumentException(
                                                        String.format(
                                                                "Vitess CDC did not find schema metadata for declared table '%s'. Add it to schema/tables_configs first.",
                                                                tableName)));
                selectedTables.add(matchedTable);
            }
            return selectedTables;
        }

        Optional<String> tablePattern =
                context.getOptions().getOptional(ConnectorCommonOptions.TABLE_PATTERN);
        if (tablePattern.isPresent()) {
            Pattern regex = Pattern.compile(tablePattern.get());
            List<CatalogTable> selectedTables =
                    declaredTables.stream()
                            .filter(
                                    table ->
                                            regex.matcher(table.getTablePath().toString())
                                                    .matches())
                            .collect(Collectors.toList());
            if (selectedTables.isEmpty()) {
                throw new IllegalArgumentException(
                        String.format(
                                "Vitess CDC table-pattern '%s' did not match any declared schema table.",
                                tablePattern.get()));
            }
            return selectedTables;
        }

        return declaredTables;
    }

    private <SplitT extends SourceSplit, StateT extends Serializable>
            List<CatalogTable> extractCheckpointTables(
                    ChangeStreamTableSourceState<StateT, SplitT> state) {
        if (state == null || state.getSplits() == null) {
            return Collections.emptyList();
        }
        for (List<SplitT> splitGroup : state.getSplits()) {
            if (splitGroup == null) {
                continue;
            }
            for (SplitT split : splitGroup) {
                if (!(split instanceof VitessSourceSplit)) {
                    continue;
                }
                List<CatalogTable> checkpointTables =
                        ((VitessSourceSplit) split).getCheckpointTables();
                if (checkpointTables != null && !checkpointTables.isEmpty()) {
                    return checkpointTables;
                }
            }
        }
        return Collections.emptyList();
    }
}
