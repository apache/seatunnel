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

package org.apache.seatunnel.connectors.seatunnel.openmldb.source;

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
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.connectors.seatunnel.openmldb.config.OpenMldbParameters;
import org.apache.seatunnel.connectors.seatunnel.openmldb.config.OpenMldbSourceOptions;

import com.google.auto.service.AutoService;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.seatunnel.api.configuration.util.Conditions.notBlank;

@AutoService(Factory.class)
public class OpenMldbSourceFactory implements TableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return "OpenMldb";
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(OpenMldbSourceOptions.CLUSTER_MODE)
                .exclusive(OpenMldbSourceOptions.SQL, ConnectorCommonOptions.TABLE_CONFIGS)
                .optional(OpenMldbSourceOptions.SQL, notBlank(OpenMldbSourceOptions.SQL))
                .optional(
                        ConnectorCommonOptions.TABLE_CONFIGS,
                        Conditions.notEmpty(ConnectorCommonOptions.TABLE_CONFIGS),
                        Conditions.extension(
                                ConnectorCommonOptions.TABLE_CONFIGS, new TablesValidator()))
                .required(OpenMldbSourceOptions.DATABASE)
                .optional(OpenMldbSourceOptions.SESSION_TIMEOUT)
                .optional(OpenMldbSourceOptions.REQUEST_TIMEOUT)
                .conditional(
                        OpenMldbSourceOptions.CLUSTER_MODE,
                        false,
                        OpenMldbSourceOptions.HOST,
                        OpenMldbSourceOptions.PORT)
                .conditional(
                        OpenMldbSourceOptions.CLUSTER_MODE,
                        true,
                        OpenMldbSourceOptions.ZK_HOST,
                        OpenMldbSourceOptions.ZK_PATH)
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return OpenMldbSource.class;
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        ConfigValidator.of(context.getOptions()).validate(optionRule());
        if (context.getOptions().getOptional(ConnectorCommonOptions.TABLE_CONFIGS).isPresent()) {
            List<OpenMldbParameters> tables = buildTables(context.getOptions());
            List<CatalogTable> catalogTables = new ArrayList<>();
            for (Map<String, Object> entry :
                    context.getOptions().get(ConnectorCommonOptions.TABLE_CONFIGS)) {
                catalogTables.add(
                        CatalogTableUtil.buildWithConfig(
                                "OpenMldb", ReadonlyConfig.fromMap(entry)));
            }
            return () ->
                    (SeaTunnelSource<T, SplitT, StateT>) new OpenMldbSource(tables, catalogTables);
        }
        OpenMldbParameters openMldbParameters =
                OpenMldbParameters.buildWithConfig(context.getOptions().toConfig());
        return () -> (SeaTunnelSource<T, SplitT, StateT>) new OpenMldbSource(openMldbParameters);
    }

    private static List<OpenMldbParameters> buildTables(ReadonlyConfig config) {
        List<OpenMldbParameters> tables = new ArrayList<>();
        for (Map<String, Object> entry : config.get(ConnectorCommonOptions.TABLE_CONFIGS)) {
            tables.add(
                    OpenMldbParameters.buildWithConfig(
                            ReadonlyConfig.fromMap(entry)
                                    .toConfig()
                                    .withFallback(
                                            config.toConfig()
                                                    .withoutPath(
                                                            ConnectorCommonOptions.TABLE_CONFIGS
                                                                    .key()))));
        }
        return tables;
    }

    static class TablesValidator implements ConditionExtension<List<Map<String, Object>>> {
        @Override
        public String description() {
            return "each table requires non-blank sql and an explicit schema with a unique table identity";
        }

        @Override
        public boolean evaluate(ReadonlyConfig config, List<Map<String, Object>> entries) {
            if (config.getOptional(ConnectorCommonOptions.SCHEMA).isPresent()) {
                throw new OptionValidationException(
                        "With tables_configs, configure schema.table inside each entry");
            }
            Set<String> ids = new HashSet<>();
            Set<String> supported = new HashSet<>(Arrays.asList("sql", "database", "schema"));
            for (int i = 0; i < entries.size(); i++) {
                Map<String, Object> entry = entries.get(i);
                if (entry == null || !supported.containsAll(entry.keySet())) {
                    throw new OptionValidationException(
                            "tables_configs[%d]: only sql, database and schema are supported; connection options belong at source level",
                            i);
                }
                Object schema = entry.get("schema");
                if (!(schema instanceof Map)
                        || !(((Map<?, ?>) schema).get("fields") instanceof Map)
                        || ((Map<?, ?>) ((Map<?, ?>) schema).get("fields")).isEmpty()) {
                    throw new OptionValidationException(
                            "tables_configs[%d]: schema.table and non-empty schema.fields are required",
                            i);
                }
                Object table = ((Map<?, ?>) schema).get("table");
                Object database =
                        entry.containsKey("database")
                                ? entry.get("database")
                                : config.get(OpenMldbSourceOptions.DATABASE);
                if (!nonBlank(entry.get("sql")) || !nonBlank(table) || !nonBlank(database)) {
                    throw new OptionValidationException(
                            "tables_configs[%d]: sql, database and schema.table must be non-blank strings",
                            i);
                }
                CatalogTable catalogTable =
                        CatalogTableUtil.buildWithConfig("OpenMldb", ReadonlyConfig.fromMap(entry));
                for (SeaTunnelDataType<?> type :
                        catalogTable.getSeaTunnelRowType().getFieldTypes()) {
                    switch (type.getSqlType()) {
                        case BOOLEAN:
                        case SMALLINT:
                        case INT:
                        case BIGINT:
                        case FLOAT:
                        case DOUBLE:
                        case STRING:
                        case DATE:
                        case TIMESTAMP:
                            break;
                        default:
                            throw new OptionValidationException(
                                    "tables_configs[%d]: unsupported OpenMldb type '%s'", i, type);
                    }
                }
                String id = catalogTable.getTableId().toTablePath().toString();
                if (!ids.add(id)) {
                    throw new OptionValidationException(
                            "tables_configs[%d]: duplicate table identity '%s'", i, id);
                }
            }
            return true;
        }

        private boolean nonBlank(Object value) {
            return value instanceof String && !((String) value).trim().isEmpty();
        }
    }
}
