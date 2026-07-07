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

package org.apache.seatunnel.connectors.seatunnel.cdc.mongodb;

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
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.cdc.base.option.StopMode;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.config.MongodbIncrementalSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.cdc.mongodb.exception.MongodbConnectorException;

import com.google.auto.service.AutoService;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT;

@AutoService(Factory.class)
public class MongodbIncrementalSourceFactory implements TableSourceFactory {

    @Override
    public String factoryIdentifier() {
        return MongodbIncrementalSource.IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return MongodbIncrementalSourceOptions.getBaseRule()
                .required(MongodbIncrementalSourceOptions.HOSTS)
                .required(
                        MongodbIncrementalSourceOptions.DATABASE,
                        Conditions.notEmpty(MongodbIncrementalSourceOptions.DATABASE))
                .required(
                        MongodbIncrementalSourceOptions.COLLECTION,
                        Conditions.notEmpty(MongodbIncrementalSourceOptions.COLLECTION)
                                .and(
                                        Conditions.extension(
                                                MongodbIncrementalSourceOptions.COLLECTION,
                                                new CollectionCountConsistencyValidator())))
                .exclusive(
                        MongodbIncrementalSourceOptions.SCHEMA,
                        MongodbIncrementalSourceOptions.TABLE_CONFIGS)
                .optional(
                        MongodbIncrementalSourceOptions.SCHEMA,
                        Conditions.mapNotEmpty(MongodbIncrementalSourceOptions.SCHEMA))
                .optional(
                        MongodbIncrementalSourceOptions.TABLE_CONFIGS,
                        Conditions.notEmpty(MongodbIncrementalSourceOptions.TABLE_CONFIGS))
                .optional(
                        MongodbIncrementalSourceOptions.USERNAME,
                        MongodbIncrementalSourceOptions.PASSWORD,
                        MongodbIncrementalSourceOptions.CONNECTION_OPTIONS,
                        MongodbIncrementalSourceOptions.DEBEZIUM_PROPERTIES)
                .optional(
                        MongodbIncrementalSourceOptions.BATCH_SIZE,
                        Conditions.greaterOrEqual(MongodbIncrementalSourceOptions.BATCH_SIZE, 0))
                .optional(
                        MongodbIncrementalSourceOptions.POLL_AWAIT_TIME_MILLIS,
                        Conditions.greaterThan(
                                MongodbIncrementalSourceOptions.POLL_AWAIT_TIME_MILLIS, 0))
                .optional(
                        MongodbIncrementalSourceOptions.POLL_MAX_BATCH_SIZE,
                        Conditions.greaterThan(
                                MongodbIncrementalSourceOptions.POLL_MAX_BATCH_SIZE, 0))
                .optional(
                        MongodbIncrementalSourceOptions.HEARTBEAT_INTERVAL_MILLIS,
                        Conditions.greaterOrEqual(
                                MongodbIncrementalSourceOptions.HEARTBEAT_INTERVAL_MILLIS, 0))
                .optional(
                        MongodbIncrementalSourceOptions.INCREMENTAL_SNAPSHOT_CHUNK_SIZE_MB,
                        Conditions.greaterThan(
                                MongodbIncrementalSourceOptions.INCREMENTAL_SNAPSHOT_CHUNK_SIZE_MB,
                                0))
                .optional(
                        MongodbIncrementalSourceOptions.STARTUP_MODE,
                        Conditions.extension(
                                MongodbIncrementalSourceOptions.STARTUP_MODE,
                                new MongoStartModeValidator()))
                .optional(
                        MongodbIncrementalSourceOptions.STOP_MODE,
                        Conditions.extension(
                                MongodbIncrementalSourceOptions.STOP_MODE,
                                new MongoStopModeValidator()))
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return MongodbIncrementalSource.class;
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        return () -> {
            List<CatalogTable> catalogTables = buildWithConfig(context.getOptions());
            List<String> collections =
                    context.getOptions().get(MongodbIncrementalSourceOptions.COLLECTION);
            catalogTables = updateAndValidateCatalogTableId(catalogTables, collections);
            return (SeaTunnelSource<T, SplitT, StateT>)
                    new MongodbIncrementalSource<>(context.getOptions(), catalogTables);
        };
    }

    static class CollectionCountConsistencyValidator implements ConditionExtension<List<String>> {
        @Override
        public String description() {
            return "collection count must align with schema/table_configs definition";
        }

        @Override
        public boolean evaluate(ReadonlyConfig config, List<String> value) {
            if (value == null || value.isEmpty()) {
                return false;
            }
            List<Map<String, Object>> tableConfigs =
                    config.get(MongodbIncrementalSourceOptions.TABLE_CONFIGS);
            if (tableConfigs != null) {
                return value.size() == tableConfigs.size();
            }
            Map<String, Object> schema = config.get(MongodbIncrementalSourceOptions.SCHEMA);
            if (schema != null) {
                return value.size() == 1;
            }
            return true;
        }
    }

    static class MongoStartModeValidator implements ConditionExtension<StartupMode> {
        @Override
        public String description() {
            return "startup.mode rules: TIMESTAMP requires startup.timestamp >= 0; "
                    + "SPECIFIC requires startup.specific-offset.file non-blank and startup.specific-offset.pos >= 0";
        }

        @Override
        public boolean evaluate(ReadonlyConfig config, StartupMode value)
                throws OptionValidationException {
            switch (value) {
                case TIMESTAMP:
                    Long startupTimestamp =
                            config.get(MongodbIncrementalSourceOptions.STARTUP_TIMESTAMP);
                    if (startupTimestamp == null || startupTimestamp < 0) {
                        throw new OptionValidationException(
                                "When startup.mode is TIMESTAMP, startup.timestamp must be configured and >= 0, "
                                        + "but was: "
                                        + startupTimestamp);
                    }
                    break;
                case SPECIFIC:
                    String startupSpecificOffsetFile =
                            config.get(
                                    MongodbIncrementalSourceOptions.STARTUP_SPECIFIC_OFFSET_FILE);
                    Long startupSpecificOffsetPos =
                            config.get(MongodbIncrementalSourceOptions.STARTUP_SPECIFIC_OFFSET_POS);

                    if (startupSpecificOffsetFile == null
                            || startupSpecificOffsetFile.trim().isEmpty()) {
                        throw new OptionValidationException(
                                "When startup.mode is SPECIFIC, startup.specific-offset.file must be configured and not blank.");
                    }

                    if (startupSpecificOffsetPos == null || startupSpecificOffsetPos < 0) {
                        throw new OptionValidationException(
                                "When startup.mode is SPECIFIC, startup.specific-offset.pos must be configured and >= 0, "
                                        + "but was: "
                                        + startupSpecificOffsetPos);
                    }
                    break;
            }

            return true;
        }
    }

    static class MongoStopModeValidator implements ConditionExtension<StopMode> {
        @Override
        public String description() {
            return "stop.mode rules: TIMESTAMP requires stop.timestamp >= 0; "
                    + "SPECIFIC requires stop.specific-offset.file non-blank and stop.specific-offset.pos >= 0; ";
        }

        @Override
        public boolean evaluate(ReadonlyConfig config, StopMode value)
                throws OptionValidationException {
            switch (value) {
                case SPECIFIC:
                    String stopSpecificOffsetFile =
                            config.get(MongodbIncrementalSourceOptions.STOP_SPECIFIC_OFFSET_FILE);
                    Long stopSpecificOffsetPos =
                            config.get(MongodbIncrementalSourceOptions.STOP_SPECIFIC_OFFSET_POS);

                    if (stopSpecificOffsetFile == null || stopSpecificOffsetFile.trim().isEmpty()) {
                        throw new OptionValidationException(
                                "When stop.mode is SPECIFIC, stop.specific-offset.file must be configured and not blank.");
                    }

                    if (stopSpecificOffsetPos == null || stopSpecificOffsetPos < 0) {
                        throw new OptionValidationException(
                                "When stop.mode is SPECIFIC, stop.specific-offset.pos must be configured and >= 0, "
                                        + "but was: "
                                        + stopSpecificOffsetPos);
                    }
                    break;
                case TIMESTAMP:
                    Long stopTimestamp = config.get(MongodbIncrementalSourceOptions.STOP_TIMESTAMP);
                    if (stopTimestamp == null || stopTimestamp < 0) {
                        throw new OptionValidationException(
                                "When stop.mode is TIMESTAMP, stop.timestamp must be configured and >= 0, "
                                        + "but was: "
                                        + stopTimestamp);
                    }
                    break;
            }

            return true;
        }
    }

    private List<CatalogTable> updateAndValidateCatalogTableId(
            List<CatalogTable> catalogTables, List<String> collections) {
        for (int i = 0; i < catalogTables.size(); i++) {
            CatalogTable catalogTable = catalogTables.get(i);
            String collectionName = collections.get(i);
            String fullName = catalogTable.getTablePath().getFullName();
            if (fullName.equals(TablePath.DEFAULT.getFullName())) {
                if (catalogTables.size() == 1) {
                    TableIdentifier updatedIdentifier =
                            TableIdentifier.of(
                                    catalogTable.getCatalogName(), TablePath.of(collectionName));
                    return Collections.singletonList(
                            CatalogTable.of(updatedIdentifier, catalogTable));
                }
            } else if (!fullName.equals(collectionName)) {
                throw new MongodbConnectorException(
                        ILLEGAL_ARGUMENT,
                        String.format(
                                "Inconsistent naming found at index %d: "
                                        + "The collection name '%s' must match the schema table name '%s'.",
                                i, collectionName, fullName));
            }
        }
        return catalogTables;
    }

    private List<CatalogTable> buildWithConfig(ReadonlyConfig config) {
        String factoryId = config.get(ConnectorCommonOptions.PLUGIN_NAME).replace("-CDC", "");
        Map<String, Object> schemaMap = config.get(ConnectorCommonOptions.SCHEMA);
        if (schemaMap != null) {
            CatalogTable catalogTable = CatalogTableUtil.buildWithConfig(factoryId, config);
            return Collections.singletonList(catalogTable);
        }
        List<Map<String, Object>> schemaMaps = config.get(ConnectorCommonOptions.TABLE_CONFIGS);
        if (schemaMaps != null) {
            return schemaMaps.stream()
                    .map(
                            map ->
                                    CatalogTableUtil.buildWithConfig(
                                            factoryId, ReadonlyConfig.fromMap(map)))
                    .collect(Collectors.toList());
        }
        return Collections.emptyList();
    }
}
