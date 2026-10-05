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

package org.apache.seatunnel.connectors.seatunnel.iotdb.source;

import org.apache.seatunnel.api.configuration.Option;
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
import org.apache.seatunnel.connectors.seatunnel.iotdb.config.IoTDBSourceOptions;

import com.google.auto.service.AutoService;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@AutoService(Factory.class)
public class IoTDBSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return "IoTDB";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(
                        IoTDBSourceOptions.NODE_URLS,
                        IoTDBSourceOptions.USERNAME,
                        IoTDBSourceOptions.PASSWORD)
                .exclusive(IoTDBSourceOptions.SQL, ConnectorCommonOptions.TABLE_CONFIGS)
                .optional(
                        IoTDBSourceOptions.SQL,
                        Conditions.extension(IoTDBSourceOptions.SQL, new SingleTableValidator()))
                .optional(
                        ConnectorCommonOptions.TABLE_CONFIGS,
                        Conditions.notEmpty(ConnectorCommonOptions.TABLE_CONFIGS),
                        Conditions.extension(
                                ConnectorCommonOptions.TABLE_CONFIGS, new TableConfigsValidator()))
                .optional(
                        ConnectorCommonOptions.SCHEMA,
                        IoTDBSourceOptions.FETCH_SIZE,
                        IoTDBSourceOptions.THRIFT_DEFAULT_BUFFER_SIZE,
                        IoTDBSourceOptions.THRIFT_MAX_FRAME_SIZE,
                        IoTDBSourceOptions.ENABLE_CACHE_LEADER,
                        IoTDBSourceOptions.VERSION,
                        IoTDBSourceOptions.LOWER_BOUND,
                        IoTDBSourceOptions.UPPER_BOUND,
                        IoTDBSourceOptions.NUM_PARTITIONS)
                .build();
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        ConfigValidator.of(context.getOptions()).validate(optionRule());
        List<CatalogTable> catalogTables = new ArrayList<>();
        Set<String> tableIds = new HashSet<>();
        for (ReadonlyConfig config : tableConfigs(context.getOptions())) {
            CatalogTable table = CatalogTableUtil.buildWithConfig(config);
            if (!tableIds.add(table.getTablePath().toString())) {
                throw new OptionValidationException(
                        "Duplicate table identity in tables_configs: %s", table.getTablePath());
            }
            catalogTables.add(table);
        }
        return () ->
                (SeaTunnelSource<T, SplitT, StateT>)
                        new IoTDBSource(catalogTables, context.getOptions());
    }

    static List<ReadonlyConfig> tableConfigs(ReadonlyConfig config) {
        if (!config.getOptional(ConnectorCommonOptions.TABLE_CONFIGS).isPresent()) {
            return Collections.singletonList(config);
        }
        List<ReadonlyConfig> tables = new ArrayList<>();
        for (Map<String, Object> entry : config.get(ConnectorCommonOptions.TABLE_CONFIGS)) {
            tables.add(ReadonlyConfig.fromMap(entry));
        }
        return tables;
    }

    static class SingleTableValidator implements ConditionExtension<String> {
        @Override
        public String description() {
            return "root-level sql requires schema";
        }

        @Override
        public boolean evaluate(ReadonlyConfig config, String sql) {
            if (!config.getOptional(ConnectorCommonOptions.SCHEMA).isPresent()) {
                throw new OptionValidationException(
                        "'schema' must be configured with root-level 'sql'");
            }
            return true;
        }
    }

    static class TableConfigsValidator implements ConditionExtension<List<Map<String, Object>>> {
        @Override
        public String description() {
            return "each tables_configs entry requires sql and a schema with a unique table identity";
        }

        @Override
        public boolean evaluate(ReadonlyConfig config, List<Map<String, Object>> entries) {
            if (config.getOptional(ConnectorCommonOptions.SCHEMA).isPresent()
                    || config.getOptional(IoTDBSourceOptions.LOWER_BOUND).isPresent()
                    || config.getOptional(IoTDBSourceOptions.UPPER_BOUND).isPresent()
                    || config.getOptional(IoTDBSourceOptions.NUM_PARTITIONS).isPresent()) {
                throw new OptionValidationException(
                        "With tables_configs, configure schema and time partitions inside each entry");
            }
            Set<String> tableIds = new HashSet<>();
            for (int i = 0; i < entries.size(); i++) {
                ReadonlyConfig table = ReadonlyConfig.fromMap(entries.get(i));
                for (Option<?> connectionOption :
                        new Option<?>[] {
                            IoTDBSourceOptions.NODE_URLS,
                            IoTDBSourceOptions.USERNAME,
                            IoTDBSourceOptions.PASSWORD,
                            IoTDBSourceOptions.FETCH_SIZE,
                            IoTDBSourceOptions.THRIFT_DEFAULT_BUFFER_SIZE,
                            IoTDBSourceOptions.THRIFT_MAX_FRAME_SIZE,
                            IoTDBSourceOptions.ENABLE_CACHE_LEADER,
                            IoTDBSourceOptions.VERSION
                        }) {
                    if (table.getOptional(connectionOption).isPresent()) {
                        throw new OptionValidationException(
                                "tables_configs[%d]: '%s' must be configured at source level",
                                i, connectionOption.key());
                    }
                }
                String sql = table.getOptional(IoTDBSourceOptions.SQL).orElse("");
                Map<String, Object> schema =
                        table.getOptional(ConnectorCommonOptions.SCHEMA)
                                .orElse(Collections.emptyMap());
                Object name = schema.get(ConnectorCommonOptions.TABLE.key());
                if (sql.trim().isEmpty()
                        || !(name instanceof String)
                        || ((String) name).trim().isEmpty()) {
                    throw new OptionValidationException(
                            "tables_configs[%d] requires non-blank sql and schema.table", i);
                }
                CatalogTable catalog = CatalogTableUtil.buildWithConfig(table);
                if (!tableIds.add(catalog.getTablePath().toString())) {
                    throw new OptionValidationException(
                            "tables_configs[%d]: duplicate table identity '%s'",
                            i, catalog.getTablePath());
                }
                if (table.getOptional(IoTDBSourceOptions.NUM_PARTITIONS).isPresent()
                        || table.getOptional(IoTDBSourceOptions.LOWER_BOUND).isPresent()
                        || table.getOptional(IoTDBSourceOptions.UPPER_BOUND).isPresent()) {
                    if (!sql.matches("(?is)^\\s*select\\s+.+\\s+from\\s+.+")
                            || sql.matches("(?is).*\\bselect\\b.*\\bselect\\b.*")
                            || sql.split("(?i)\\bfrom\\b", 2)[0].contains("(")
                            || sql.contains("--")
                            || sql.contains("/*")
                            || sql.contains("*/")
                            || sql.matches("(?s).*[\"'`;].*")
                            || sql.matches(
                                    "(?is).*\\b(group\\s+by|order\\s+by|limit|offset|slimit|soffset|fill|into)\\b.*")) {
                        throw new OptionValidationException(
                                "tables_configs[%d]: time partitioning supports a simple SELECT with optional WHERE and ALIGN BY; use an unpartitioned query for quoted expressions or other SQL clauses",
                                i);
                    }
                    if (!table.getOptional(IoTDBSourceOptions.NUM_PARTITIONS).isPresent()
                            || !table.getOptional(IoTDBSourceOptions.LOWER_BOUND).isPresent()
                            || !table.getOptional(IoTDBSourceOptions.UPPER_BOUND).isPresent()
                            || table.get(IoTDBSourceOptions.NUM_PARTITIONS) <= 0
                            || table.get(IoTDBSourceOptions.LOWER_BOUND)
                                    >= table.get(IoTDBSourceOptions.UPPER_BOUND)
                            || table.get(IoTDBSourceOptions.UPPER_BOUND) == Long.MAX_VALUE
                            || table.get(IoTDBSourceOptions.UPPER_BOUND)
                                            - table.get(IoTDBSourceOptions.LOWER_BOUND)
                                    < 0
                            || table.get(IoTDBSourceOptions.UPPER_BOUND)
                                            - table.get(IoTDBSourceOptions.LOWER_BOUND)
                                    == Long.MAX_VALUE) {
                        throw new OptionValidationException(
                                "tables_configs[%d]: provide positive num_partitions and a valid lower_bound < upper_bound time range",
                                i);
                    }
                }
            }
            return true;
        }
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return IoTDBSource.class;
    }
}
