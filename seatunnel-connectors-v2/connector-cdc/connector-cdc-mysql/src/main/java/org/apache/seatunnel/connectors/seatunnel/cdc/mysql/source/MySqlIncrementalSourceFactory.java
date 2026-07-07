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

package org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConditionExtension;
import org.apache.seatunnel.api.configuration.util.Conditions;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceTableConfig;
import org.apache.seatunnel.connectors.cdc.base.option.JdbcSourceOptions;
import org.apache.seatunnel.connectors.cdc.base.option.SourceOptions;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.cdc.base.option.StopMode;
import org.apache.seatunnel.connectors.cdc.base.source.BaseChangeStreamTableSourceFactory;
import org.apache.seatunnel.connectors.cdc.base.utils.CatalogTableUtils;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.config.MySqlIncrementalSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.config.MySqlSourceConfigFactory;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;

@AutoService(Factory.class)
@Slf4j
public class MySqlIncrementalSourceFactory extends BaseChangeStreamTableSourceFactory {
    @Override
    public String factoryIdentifier() {
        return MySqlIncrementalSource.IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return JdbcSourceOptions.getBaseRule()
                .required(
                        MySqlIncrementalSourceOptions.USERNAME,
                        MySqlIncrementalSourceOptions.PASSWORD,
                        MySqlIncrementalSourceOptions.URL)
                .exclusive(
                        MySqlIncrementalSourceOptions.TABLE_NAMES,
                        MySqlIncrementalSourceOptions.TABLE_PATTERN)
                .optional(
                        MySqlIncrementalSourceOptions.TABLE_NAMES,
                        Conditions.notEmpty(MySqlIncrementalSourceOptions.TABLE_NAMES)
                                .and(
                                        Conditions.extension(
                                                MySqlIncrementalSourceOptions.TABLE_NAMES,
                                                new MysqlTableNameValidator())))
                .optional(
                        MySqlIncrementalSourceOptions.DATABASE_NAMES,
                        MySqlIncrementalSourceOptions.SERVER_TIME_ZONE,
                        MySqlIncrementalSourceOptions
                                .CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND,
                        MySqlIncrementalSourceOptions
                                .CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND,
                        MySqlIncrementalSourceOptions.SPLIT_ALLOW_SAMPLING,
                        MySqlIncrementalSourceOptions.TABLE_NAMES_CONFIG,
                        MySqlIncrementalSourceOptions.SCHEMA_CHANGES_ENABLED,
                        MySqlIncrementalSourceOptions.INT_TYPE_NARROWING)
                .optional(
                        MySqlIncrementalSourceOptions.SCHEMA_CHANGES_INCLUDE,
                        Conditions.extension(
                                MySqlIncrementalSourceOptions.SCHEMA_CHANGES_INCLUDE,
                                SourceOptions.SchemaChangeNameValidator.INCLUDE))
                .optional(
                        MySqlIncrementalSourceOptions.SCHEMA_CHANGES_EXCLUDE,
                        Conditions.extension(
                                MySqlIncrementalSourceOptions.SCHEMA_CHANGES_EXCLUDE,
                                SourceOptions.SchemaChangeNameValidator.EXCLUDE))
                .optional(
                        MySqlIncrementalSourceOptions.CONNECT_TIMEOUT_MS,
                        Conditions.greaterOrEqual(
                                MySqlIncrementalSourceOptions.CONNECT_TIMEOUT_MS, 0L))
                .optional(
                        MySqlIncrementalSourceOptions.CONNECT_MAX_RETRIES,
                        Conditions.greaterOrEqual(
                                MySqlIncrementalSourceOptions.CONNECT_MAX_RETRIES, 0))
                .optional(
                        MySqlIncrementalSourceOptions.CONNECTION_POOL_SIZE,
                        Conditions.greaterThan(
                                MySqlIncrementalSourceOptions.CONNECTION_POOL_SIZE, 0))
                .optional(
                        MySqlIncrementalSourceOptions.SAMPLE_SHARDING_THRESHOLD,
                        Conditions.greaterOrEqual(
                                MySqlIncrementalSourceOptions.SAMPLE_SHARDING_THRESHOLD, 0))
                .optional(
                        MySqlIncrementalSourceOptions.INVERSE_SAMPLING_RATE,
                        Conditions.greaterThan(
                                MySqlIncrementalSourceOptions.INVERSE_SAMPLING_RATE, 0))
                .optional(
                        MySqlIncrementalSourceOptions.SERVER_ID,
                        Conditions.matches(
                                MySqlIncrementalSourceOptions.SERVER_ID, "^\\d+(-\\d+)?$"))
                .optional(
                        MySqlIncrementalSourceOptions.STARTUP_MODE,
                        Conditions.extension(
                                MySqlIncrementalSourceOptions.STARTUP_MODE,
                                new MySqlStartModeValidator()))
                .optional(
                        MySqlIncrementalSourceOptions.STOP_MODE,
                        Conditions.extension(
                                MySqlIncrementalSourceOptions.STOP_MODE,
                                new MySqlStopModeValidator()))
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return MySqlIncrementalSource.class;
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> restoreSource(
                    TableSourceFactoryContext context, List<CatalogTable> restoreTables) {
        return () -> {
            // Load the JDBC driver in to DriverManager
            try {
                Class.forName("com.mysql.cj.jdbc.Driver");
            } catch (Exception e) {
                log.warn("Failed to load JDBC driver com.mysql.cj.jdbc.Driver ", e);
            }
            ReadonlyConfig config = context.getOptions();
            List<CatalogTable> catalogTables =
                    CatalogTableUtil.getCatalogTables(config, context.getClassLoader());
            boolean enableSchemaChange =
                    context.getOptions()
                            .getOptional(SourceOptions.SCHEMA_CHANGES_ENABLED)
                            .orElse(
                                    // TODO remove this after all users used the new schema change
                                    // option
                                    context.getOptions()
                                            .getOptional(SourceOptions.DEBEZIUM_PROPERTIES)
                                            .map(
                                                    e ->
                                                            e.getOrDefault(
                                                                    MySqlSourceConfigFactory
                                                                            .SCHEMA_CHANGE_KEY,
                                                                    SourceOptions
                                                                            .SCHEMA_CHANGES_ENABLED
                                                                            .defaultValue()
                                                                            .toString()))
                                            .map(Boolean::parseBoolean)
                                            .orElse(
                                                    SourceOptions.SCHEMA_CHANGES_ENABLED
                                                            .defaultValue()));
            if (!restoreTables.isEmpty() && enableSchemaChange) {
                catalogTables = mergeTableStruct(catalogTables, restoreTables);
            }

            Optional<List<JdbcSourceTableConfig>> tableConfigs =
                    context.getOptions().getOptional(JdbcSourceOptions.TABLE_NAMES_CONFIG);
            if (tableConfigs.isPresent()) {
                catalogTables =
                        CatalogTableUtils.mergeCatalogTableConfig(
                                catalogTables,
                                tableConfigs.get(),
                                text -> TablePath.of(text, false));
            }
            return (SeaTunnelSource<T, SplitT, StateT>)
                    new MySqlIncrementalSource<>(config, catalogTables);
        };
    }

    /**
     * MySQL-specific table name validator that only accepts the two-segment {@code database.table}
     * format. MySQL does not have a separate schema namespace, so three-segment identifiers are
     * invalid.
     */
    static class MysqlTableNameValidator implements ConditionExtension<List<String>> {

        @Override
        public String description() {
            return "each table name must be in 'database.table' format (exactly two segments)";
        }

        @Override
        public boolean evaluate(ReadonlyConfig config, List<String> value) {
            if (value == null || value.isEmpty()) {
                return false;
            }
            return value.stream().allMatch(MysqlTableNameValidator::isTwoSegmentName);
        }

        private static boolean isTwoSegmentName(String name) {
            if (name == null || name.isEmpty()) {
                return false;
            }
            String[] segments = name.split("\\.", -1);
            if (segments.length != 2) {
                return false;
            }
            for (String seg : segments) {
                if (seg.trim().isEmpty()) {
                    return false;
                }
            }
            return true;
        }
    }

    static class MySqlStartModeValidator implements ConditionExtension<StartupMode> {
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
                            config.get(MySqlIncrementalSourceOptions.STARTUP_TIMESTAMP);
                    if (startupTimestamp == null || startupTimestamp < 0) {
                        throw new OptionValidationException(
                                "When startup.mode is TIMESTAMP, startup.timestamp must be configured and >= 0, "
                                        + "but was: "
                                        + startupTimestamp);
                    }
                    break;
                case SPECIFIC:
                    String startupSpecificOffsetFile =
                            config.get(MySqlIncrementalSourceOptions.STARTUP_SPECIFIC_OFFSET_FILE);
                    Long startupSpecificOffsetPos =
                            config.get(MySqlIncrementalSourceOptions.STARTUP_SPECIFIC_OFFSET_POS);

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

    static class MySqlStopModeValidator implements ConditionExtension<StopMode> {
        @Override
        public String description() {
            return "stop.mode=SPECIFIC requires stop.specific-offset.file != null && !blank and stop.specific-offset.pos >= 0";
        }

        @Override
        public boolean evaluate(ReadonlyConfig config, StopMode value)
                throws OptionValidationException {
            switch (value) {
                case SPECIFIC:
                    String stopSpecificOffsetFile =
                            config.get(MySqlIncrementalSourceOptions.STOP_SPECIFIC_OFFSET_FILE);
                    Long stopSpecificOffsetPos =
                            config.get(MySqlIncrementalSourceOptions.STOP_SPECIFIC_OFFSET_POS);

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
                    Long stopTimestamp = config.get(MySqlIncrementalSourceOptions.STOP_TIMESTAMP);
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
}
