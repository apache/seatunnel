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

package org.apache.seatunnel.connectors.seatunnel.cdc.sqlserver.source;

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
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceTableConfig;
import org.apache.seatunnel.connectors.cdc.base.option.SourceOptions;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.cdc.base.option.StopMode;
import org.apache.seatunnel.connectors.cdc.base.utils.CatalogTableUtils;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;

@AutoService(Factory.class)
@Slf4j
public class SqlServerIncrementalSourceFactory implements TableSourceFactory {

    @Override
    public String factoryIdentifier() {
        return SqlServerIncrementalSource.IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return SqlServerIncrementalSourceOptions.getBaseRule()
                .required(
                        SqlServerIncrementalSourceOptions.USERNAME,
                        SqlServerIncrementalSourceOptions.PASSWORD,
                        SqlServerIncrementalSourceOptions.URL)
                .exclusive(ConnectorCommonOptions.TABLE_NAMES, ConnectorCommonOptions.TABLE_PATTERN)
                .optional(
                        ConnectorCommonOptions.TABLE_NAMES,
                        Conditions.notEmpty(ConnectorCommonOptions.TABLE_NAMES)
                                .and(
                                        Conditions.extension(
                                                ConnectorCommonOptions.TABLE_NAMES,
                                                new SourceOptions.QualifiedTableNameValidator())))
                .required(
                        SqlServerIncrementalSourceOptions.DATABASE_NAMES,
                        Conditions.notEmpty(SqlServerIncrementalSourceOptions.DATABASE_NAMES))
                .optional(
                        SqlServerIncrementalSourceOptions.SERVER_TIME_ZONE,
                        SqlServerIncrementalSourceOptions
                                .CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND,
                        SqlServerIncrementalSourceOptions
                                .CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND,
                        SqlServerIncrementalSourceOptions.SPLIT_ALLOW_SAMPLING,
                        SqlServerIncrementalSourceOptions.TABLE_NAMES_CONFIG,
                        SqlServerIncrementalSourceOptions.SCHEMA_CHANGES_ENABLED)
                .optional(
                        SqlServerIncrementalSourceOptions.SCHEMA_CHANGES_INCLUDE,
                        Conditions.extension(
                                SqlServerIncrementalSourceOptions.SCHEMA_CHANGES_INCLUDE,
                                SourceOptions.SchemaChangeNameValidator.INCLUDE))
                .optional(
                        SqlServerIncrementalSourceOptions.SCHEMA_CHANGES_EXCLUDE,
                        Conditions.extension(
                                SqlServerIncrementalSourceOptions.SCHEMA_CHANGES_EXCLUDE,
                                SourceOptions.SchemaChangeNameValidator.EXCLUDE))
                .optional(
                        SqlServerIncrementalSourceOptions.CONNECT_TIMEOUT_MS,
                        Conditions.greaterOrEqual(
                                SqlServerIncrementalSourceOptions.CONNECT_TIMEOUT_MS, 0L))
                .optional(
                        SqlServerIncrementalSourceOptions.CONNECT_MAX_RETRIES,
                        Conditions.greaterOrEqual(
                                SqlServerIncrementalSourceOptions.CONNECT_MAX_RETRIES, 0))
                .optional(
                        SqlServerIncrementalSourceOptions.CONNECTION_POOL_SIZE,
                        Conditions.greaterThan(
                                SqlServerIncrementalSourceOptions.CONNECTION_POOL_SIZE, 0))
                .optional(
                        SqlServerIncrementalSourceOptions.SAMPLE_SHARDING_THRESHOLD,
                        Conditions.greaterOrEqual(
                                SqlServerIncrementalSourceOptions.SAMPLE_SHARDING_THRESHOLD, 0))
                .optional(
                        SqlServerIncrementalSourceOptions.INVERSE_SAMPLING_RATE,
                        Conditions.greaterThan(
                                SqlServerIncrementalSourceOptions.INVERSE_SAMPLING_RATE, 0))
                .optional(
                        SqlServerIncrementalSourceOptions.STARTUP_MODE,
                        Conditions.extension(
                                SqlServerIncrementalSourceOptions.STARTUP_MODE,
                                new SqlServerStartModeValidator()))
                .optional(
                        SqlServerIncrementalSourceOptions.STOP_MODE,
                        Conditions.extension(
                                SqlServerIncrementalSourceOptions.STOP_MODE,
                                new SqlServerStopModeValidator()))
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return SqlServerIncrementalSource.class;
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> createSource(TableSourceFactoryContext context) {
        return () -> {
            // Load the JDBC driver in to DriverManager
            try {
                Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            } catch (Exception e) {
                log.warn(
                        "Failed to load JDBC driver {}",
                        "com.microsoft.sqlserver.jdbc.SQLServerDriver",
                        e);
            }
            List<CatalogTable> catalogTables =
                    CatalogTableUtil.getCatalogTables(
                            context.getOptions(), context.getClassLoader());
            Optional<List<JdbcSourceTableConfig>> tableConfigs =
                    context.getOptions()
                            .getOptional(SqlServerIncrementalSourceOptions.TABLE_NAMES_CONFIG);
            if (tableConfigs.isPresent()) {
                catalogTables =
                        CatalogTableUtils.mergeCatalogTableConfig(
                                catalogTables,
                                tableConfigs.get(),
                                text -> TablePath.of(text, true));
            }
            return new SqlServerIncrementalSource(context.getOptions(), catalogTables);
        };
    }

    static class SqlServerStartModeValidator implements ConditionExtension<StartupMode> {
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
                            config.get(SqlServerIncrementalSourceOptions.STARTUP_TIMESTAMP);
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
                                    SqlServerIncrementalSourceOptions.STARTUP_SPECIFIC_OFFSET_FILE);
                    Long startupSpecificOffsetPos =
                            config.get(
                                    SqlServerIncrementalSourceOptions.STARTUP_SPECIFIC_OFFSET_POS);

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

    static class SqlServerStopModeValidator implements ConditionExtension<StopMode> {
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
                            config.get(SqlServerIncrementalSourceOptions.STOP_SPECIFIC_OFFSET_FILE);
                    Long stopSpecificOffsetPos =
                            config.get(SqlServerIncrementalSourceOptions.STOP_SPECIFIC_OFFSET_POS);

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
                    Long stopTimestamp =
                            config.get(SqlServerIncrementalSourceOptions.STOP_TIMESTAMP);
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
