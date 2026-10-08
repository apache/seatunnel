/*
 *  Licensed to the Apache Software Foundation (ASF) under one or more
 *     contributor license agreements.  See the NOTICE file distributed with
 *     this work for additional information regarding copyright ownership.
 *     The ASF licenses this file to You under the Apache License, Version 2.0
 *     (the "License"); you may not use this file except in compliance with
 *     the License.  You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *     Unless required by applicable law or agreed to in writing, software
 *     distributed under the License is distributed on an "AS IS" BASIS,
 *     WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *     See the License for the specific language governing permissions and
 *     limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.influxdb.source;

import org.apache.seatunnel.shade.com.typesafe.config.Config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConditionExtension;
import org.apache.seatunnel.api.configuration.util.Conditions;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
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
import org.apache.seatunnel.connectors.seatunnel.influxdb.config.InfluxDBSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.influxdb.config.SourceConfig;

import com.google.auto.service.AutoService;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@AutoService(Factory.class)
public class InfluxDBSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return "InfluxDB";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(InfluxDBSourceOptions.URL)
                .exclusive(InfluxDBSourceOptions.SQL, ConnectorCommonOptions.TABLE_CONFIGS)
                .optional(
                        InfluxDBSourceOptions.SQL,
                        Conditions.extension(InfluxDBSourceOptions.SQL, new SingleTableValidator()))
                .optional(
                        ConnectorCommonOptions.TABLE_CONFIGS,
                        Conditions.notEmpty(ConnectorCommonOptions.TABLE_CONFIGS),
                        Conditions.extension(
                                ConnectorCommonOptions.TABLE_CONFIGS, new TablesValidator()))
                .bundled(InfluxDBSourceOptions.USERNAME, InfluxDBSourceOptions.PASSWORD)
                .bundled(
                        InfluxDBSourceOptions.LOWER_BOUND,
                        InfluxDBSourceOptions.UPPER_BOUND,
                        InfluxDBSourceOptions.PARTITION_NUM,
                        InfluxDBSourceOptions.SPLIT_COLUMN)
                .optional(
                        InfluxDBSourceOptions.DATABASES,
                        ConnectorCommonOptions.SCHEMA,
                        InfluxDBSourceOptions.EPOCH,
                        InfluxDBSourceOptions.SQL_WHERE,
                        InfluxDBSourceOptions.CONNECT_TIMEOUT_MS,
                        InfluxDBSourceOptions.QUERY_TIMEOUT_SEC)
                .build();
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        return () -> {
            ReadonlyConfig config = context.getOptions();
            ConfigValidator.of(config).validate(optionRule());
            return (SeaTunnelSource<T, SplitT, StateT>)
                    (config.getOptional(ConnectorCommonOptions.TABLE_CONFIGS).isPresent()
                            ? new InfluxDBSource(buildTables(config))
                            : new InfluxDBSource(
                                    CatalogTableUtil.buildWithConfig(config),
                                    SourceConfig.loadConfig(config)));
        };
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return InfluxDBSource.class;
    }

    private static OptionRule tableRule() {
        return OptionRule.builder()
                .required(
                        InfluxDBSourceOptions.SQL,
                        InfluxDBSourceOptions.DATABASES,
                        ConnectorCommonOptions.SCHEMA)
                .bundled(
                        InfluxDBSourceOptions.LOWER_BOUND,
                        InfluxDBSourceOptions.UPPER_BOUND,
                        InfluxDBSourceOptions.PARTITION_NUM,
                        InfluxDBSourceOptions.SPLIT_COLUMN)
                .build();
    }

    private static List<InfluxDBSourceTable> buildTables(ReadonlyConfig config) {
        if (config.getOptional(ConnectorCommonOptions.SCHEMA).isPresent()) {
            throw new OptionValidationException(
                    "root-level 'schema' cannot be used with 'tables_configs'");
        }
        if (config.getOptional(InfluxDBSourceOptions.SQL_WHERE).isPresent()) {
            throw new OptionValidationException(
                    "With tables_configs, put each WHERE predicate inside that entry's sql, not in a separate where option");
        }
        for (String key :
                Arrays.asList("lower_bound", "upper_bound", "partition_num", "split_column")) {
            if (config.toConfig().hasPath(key)) {
                throw new OptionValidationException(
                        "Configure '%s' inside each tables_configs entry", key);
            }
        }
        Set<String> supported =
                new HashSet<>(
                        Arrays.asList(
                                "sql",
                                "database",
                                "schema",
                                "lower_bound",
                                "upper_bound",
                                "partition_num",
                                "split_column"));
        List<InfluxDBSourceTable> tables = new ArrayList<>();
        Set<String> ids = new HashSet<>();
        List<Map<String, Object>> entries = config.get(ConnectorCommonOptions.TABLE_CONFIGS);
        if (entries.isEmpty()) {
            throw new OptionValidationException("'tables_configs' must not be empty");
        }
        Config sharedConfig =
                config.toConfig().withoutPath(ConnectorCommonOptions.TABLE_CONFIGS.key());
        for (int i = 0; i < entries.size(); i++) {
            Map<String, Object> entry = entries.get(i);
            for (String key : entry.keySet()) {
                if (!supported.contains(key)) {
                    throw new OptionValidationException(
                            "tables_configs[%d]: unsupported table option '%s'; connection options belong at root level",
                            i, key);
                }
            }
            ReadonlyConfig tableConfig =
                    ReadonlyConfig.fromConfig(
                            ReadonlyConfig.fromMap(entry).toConfig().withFallback(sharedConfig));
            try {
                ConfigValidator.of(tableConfig).validate(tableRule());
                String sql = tableConfig.get(InfluxDBSourceOptions.SQL);
                String database = tableConfig.get(InfluxDBSourceOptions.DATABASES);
                if (sql.trim().isEmpty() || database.trim().isEmpty()) {
                    throw new OptionValidationException("'sql' and 'database' must be non-blank");
                }
                Object table =
                        tableConfig
                                .get(ConnectorCommonOptions.SCHEMA)
                                .get(ConnectorCommonOptions.TABLE.key());
                if (!(table instanceof String) || ((String) table).trim().isEmpty()) {
                    throw new OptionValidationException(
                            "'schema.table' must be configured and non-blank");
                }
                CatalogTable catalogTable = CatalogTableUtil.buildWithConfig(tableConfig);
                SourceConfig sourceConfig = SourceConfig.loadConfig(tableConfig);
                if (sourceConfig.getPartitionNum() < 0
                        || (sourceConfig.getPartitionNum() > 0
                                && (sourceConfig.getLowerBound() > sourceConfig.getUpperBound()
                                        || sourceConfig.getSplitKey().trim().isEmpty()))) {
                    throw new OptionValidationException(
                            "invalid range split configuration: partition_num must be non-negative, lower_bound <= upper_bound, and split_column non-blank");
                }
                String id = catalogTable.getTableId().toTablePath().toString();
                if (sourceConfig.getPartitionNum() > 0
                        && (!sql.matches(
                                        "(?is)\\s*select\\s+(?:\\*|\\w+(?:\\s*,\\s*\\w+)*)\\s+from\\s+\\w+(?:\\s+where\\s+.+)?\\s*")
                                || sql.matches("(?s).*[\\\"';()/].*")
                                || sql.contains("--")
                                || sql.matches(
                                        "(?is).*\\b(group|order|limit|offset|slimit|soffset|fill|tz)\\b.*"))) {
                    throw new OptionValidationException(
                            "range splitting in tables_configs supports simple SELECT fields FROM measurement with an optional WHERE predicate; use an unpartitioned query for quoted identifiers, string literals, functions, subqueries, or trailing clauses");
                }
                if (!ids.add(id)) {
                    throw new OptionValidationException("duplicate table identity '%s'", id);
                }
                tables.add(new InfluxDBSourceTable(catalogTable, sourceConfig));
            } catch (RuntimeException e) {
                throw new OptionValidationException("tables_configs[%d]: %s", i, e.getMessage());
            }
        }
        return tables;
    }

    static class SingleTableValidator implements ConditionExtension<String> {
        @Override
        public String description() {
            return "'database' and 'schema' are required with root-level 'sql'";
        }

        @Override
        public boolean evaluate(ReadonlyConfig config, String sql) {
            ConfigValidator.of(config).validate(tableRule());
            return true;
        }
    }

    static class TablesValidator implements ConditionExtension<List<Map<String, Object>>> {
        @Override
        public String description() {
            return "each table needs a query, database, and schema with a unique table identity";
        }

        @Override
        public boolean evaluate(ReadonlyConfig config, List<Map<String, Object>> entries) {
            buildTables(config);
            return true;
        }
    }
}
