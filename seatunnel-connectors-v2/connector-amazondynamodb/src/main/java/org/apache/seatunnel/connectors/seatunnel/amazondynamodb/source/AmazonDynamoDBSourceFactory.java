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

package org.apache.seatunnel.connectors.seatunnel.amazondynamodb.source;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.configuration.Option;
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
import org.apache.seatunnel.connectors.seatunnel.amazondynamodb.config.AmazonDynamoDBConfig;

import com.google.auto.service.AutoService;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.seatunnel.api.options.ConnectorCommonOptions.SCHEMA;
import static org.apache.seatunnel.api.options.ConnectorCommonOptions.TABLE_CONFIGS;
import static org.apache.seatunnel.connectors.seatunnel.amazondynamodb.config.AmazonDynamoDBSourceOptions.ACCESS_KEY_ID;
import static org.apache.seatunnel.connectors.seatunnel.amazondynamodb.config.AmazonDynamoDBSourceOptions.PARALLEL_SCAN_THREADS;
import static org.apache.seatunnel.connectors.seatunnel.amazondynamodb.config.AmazonDynamoDBSourceOptions.REGION;
import static org.apache.seatunnel.connectors.seatunnel.amazondynamodb.config.AmazonDynamoDBSourceOptions.SCAN_ITEM_LIMIT;
import static org.apache.seatunnel.connectors.seatunnel.amazondynamodb.config.AmazonDynamoDBSourceOptions.SECRET_ACCESS_KEY;
import static org.apache.seatunnel.connectors.seatunnel.amazondynamodb.config.AmazonDynamoDBSourceOptions.TABLE;
import static org.apache.seatunnel.connectors.seatunnel.amazondynamodb.config.AmazonDynamoDBSourceOptions.URL;

@AutoService(Factory.class)
public class AmazonDynamoDBSourceFactory implements TableSourceFactory {

    private static final List<Option<?>> CONNECTION_OPTIONS =
            Arrays.asList(URL, REGION, ACCESS_KEY_ID, SECRET_ACCESS_KEY);

    private static final Set<String> TABLE_OPTION_KEYS =
            new HashSet<>(
                    Arrays.asList(
                            TABLE.key(),
                            SCHEMA.key(),
                            SCAN_ITEM_LIMIT.key(),
                            PARALLEL_SCAN_THREADS.key()));

    @Override
    public String factoryIdentifier() {
        return "AmazonDynamoDB";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(URL, REGION, ACCESS_KEY_ID, SECRET_ACCESS_KEY)
                .exclusive(TABLE, TABLE_CONFIGS)
                .optional(TABLE, Conditions.extension(TABLE, new SingleTableValidator()))
                .optional(
                        TABLE_CONFIGS,
                        Conditions.notEmpty(TABLE_CONFIGS),
                        Conditions.extension(TABLE_CONFIGS, new TableConfigsValidator()))
                .optional(SCHEMA, SCAN_ITEM_LIMIT, PARALLEL_SCAN_THREADS)
                .build();
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        return () -> {
            ReadonlyConfig config = context.getOptions();
            if (config.getOptional(TABLE_CONFIGS).isPresent()) {
                return (SeaTunnelSource<T, SplitT, StateT>) createMultiTableSource(config);
            }
            return (SeaTunnelSource<T, SplitT, StateT>)
                    new AmazonDynamoDBSource(
                            new AmazonDynamoDBConfig(config),
                            CatalogTableUtil.buildWithConfig(config));
        };
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return AmazonDynamoDBSource.class;
    }

    /**
     * Builds one table per tables_configs entry. Connection options come from the source level;
     * scan_item_limit and parallel_scan_threads fall back to the source level when an entry does
     * not set them. The table identity is schema.table, or the DynamoDB table name when the schema
     * does not name the table.
     */
    static AmazonDynamoDBSource createMultiTableSource(ReadonlyConfig config) {
        if (config.getOptional(SCHEMA).isPresent()) {
            throw new OptionValidationException(
                    "root-level 'schema' cannot be used with 'tables_configs'");
        }
        List<Map<String, Object>> entries = config.get(TABLE_CONFIGS);
        if (entries.isEmpty()) {
            throw new OptionValidationException("'tables_configs' must not be empty");
        }
        Config sharedConfig = config.toConfig().withoutPath(TABLE_CONFIGS.key());
        List<CatalogTable> catalogTables = new ArrayList<>(entries.size());
        List<AmazonDynamoDBSourceTable> tables = new ArrayList<>(entries.size());
        Set<String> tableIds = new HashSet<>();
        for (int i = 0; i < entries.size(); i++) {
            try {
                Map<String, Object> entry = entries.get(i);
                for (Option<?> option : CONNECTION_OPTIONS) {
                    if (entry.containsKey(option.key())) {
                        throw new OptionValidationException(
                                "tables_configs[%d]: '%s' must be configured at source level",
                                i, option.key());
                    }
                }
                for (String key : entry.keySet()) {
                    if (!TABLE_OPTION_KEYS.contains(key)) {
                        throw new OptionValidationException(
                                "tables_configs[%d]: unsupported table option '%s'", i, key);
                    }
                }
                Object tableValue = entry.get(TABLE.key());
                // An unquoted all-digit table name is parsed as a number.
                String tableName =
                        tableValue instanceof String || tableValue instanceof Number
                                ? tableValue.toString()
                                : null;
                if (tableName == null || tableName.trim().isEmpty()) {
                    throw new OptionValidationException(
                            "tables_configs[%d]: 'table' must be configured and non-blank", i);
                }
                Object schemaValue = entry.get(SCHEMA.key());
                if (!(schemaValue instanceof Map) || ((Map<?, ?>) schemaValue).isEmpty()) {
                    throw new OptionValidationException(
                            "tables_configs[%d]: 'schema' must be configured and non-empty", i);
                }
                Map<String, Object> schema = new LinkedHashMap<>((Map<String, Object>) schemaValue);
                Object schemaTable = schema.get(ConnectorCommonOptions.TABLE.key());
                if (schemaTable == null
                        || (schemaTable instanceof String
                                && ((String) schemaTable).trim().isEmpty())) {
                    schema.put(ConnectorCommonOptions.TABLE.key(), tableName);
                }
                Map<String, Object> tableEntry = new LinkedHashMap<>(entry);
                tableEntry.put(TABLE.key(), tableName);
                tableEntry.put(SCHEMA.key(), schema);
                ReadonlyConfig tableConfig =
                        ReadonlyConfig.fromConfig(
                                ReadonlyConfig.fromMap(tableEntry)
                                        .toConfig()
                                        .withFallback(sharedConfig));

                CatalogTable catalogTable;
                try {
                    catalogTable = CatalogTableUtil.buildWithConfig(tableConfig);
                } catch (RuntimeException e) {
                    throw new OptionValidationException(
                            "tables_configs[%d]: invalid 'schema' (set 'schema.table' when the table name is not a valid table path): %s",
                            i, e.getMessage());
                }
                AmazonDynamoDBConfig tableReadConfig = new AmazonDynamoDBConfig(tableConfig);
                if (tableReadConfig.getScanItemLimit() <= 0
                        || tableReadConfig.getParallelScanThreads() <= 0) {
                    throw new OptionValidationException(
                            "tables_configs[%d]: '%s' and '%s' must be positive",
                            i, SCAN_ITEM_LIMIT.key(), PARALLEL_SCAN_THREADS.key());
                }
                String tableId = catalogTable.getTableId().toTablePath().toString();
                if (!tableIds.add(tableId)) {
                    throw new OptionValidationException(
                            "tables_configs[%d]: duplicate table identity '%s'", i, tableId);
                }
                catalogTables.add(catalogTable);
                tables.add(
                        new AmazonDynamoDBSourceTable(
                                tableId, tableReadConfig, catalogTable.getSeaTunnelRowType()));
            } catch (OptionValidationException e) {
                throw e;
            } catch (RuntimeException e) {
                throw new OptionValidationException(
                        "tables_configs[%d]: invalid table entry: %s", i, e.getMessage());
            }
        }
        return new AmazonDynamoDBSource(new AmazonDynamoDBConfig(config), catalogTables, tables);
    }

    static class SingleTableValidator implements ConditionExtension<String> {

        @Override
        public String description() {
            return "'schema' must be configured when using a root-level 'table'";
        }

        @Override
        public boolean evaluate(ReadonlyConfig config, String table)
                throws OptionValidationException {
            Map<String, Object> schema = config.getOptional(SCHEMA).orElse(null);
            if (schema == null || schema.isEmpty()) {
                throw new OptionValidationException(
                        "'schema' must be configured when using a root-level 'table'");
            }
            return true;
        }
    }

    static class TableConfigsValidator implements ConditionExtension<List<Map<String, Object>>> {

        @Override
        public String description() {
            return "each 'tables_configs' entry must contain a non-blank 'table' and a 'schema' with a unique table identity";
        }

        @Override
        public boolean evaluate(ReadonlyConfig config, List<Map<String, Object>> entries)
                throws OptionValidationException {
            if (entries == null || entries.isEmpty()) {
                // reported by Conditions.notEmpty
                return true;
            }
            createMultiTableSource(config);
            return true;
        }
    }
}
