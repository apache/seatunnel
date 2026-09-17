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
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.connector.TableSource;
import org.apache.seatunnel.api.table.factory.Factory;
import org.apache.seatunnel.api.table.factory.SupportSourceDryRunValidation;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceTableConfig;
import org.apache.seatunnel.connectors.cdc.base.option.JdbcSourceOptions;
import org.apache.seatunnel.connectors.cdc.base.option.SourceOptions;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.cdc.base.option.StopMode;
import org.apache.seatunnel.connectors.cdc.base.source.BaseChangeStreamTableSourceFactory;
import org.apache.seatunnel.connectors.cdc.base.utils.CatalogTableUtils;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.config.MySqlIncrementalSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.config.MySqlSourceConfigFactory;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.utils.MySqlConnectionUtils;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcCommonOptions;

import com.google.auto.service.AutoService;
import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnection;
import lombok.extern.slf4j.Slf4j;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;

@AutoService(Factory.class)
@Slf4j
public class MySqlIncrementalSourceFactory extends BaseChangeStreamTableSourceFactory
        implements SupportSourceDryRunValidation {
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
                        MySqlIncrementalSourceOptions.DATABASE_NAMES,
                        MySqlIncrementalSourceOptions.SERVER_ID,
                        MySqlIncrementalSourceOptions.SERVER_TIME_ZONE,
                        MySqlIncrementalSourceOptions.CONNECT_TIMEOUT_MS,
                        MySqlIncrementalSourceOptions.CONNECT_MAX_RETRIES,
                        MySqlIncrementalSourceOptions.CONNECTION_POOL_SIZE,
                        MySqlIncrementalSourceOptions
                                .CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_LOWER_BOUND,
                        MySqlIncrementalSourceOptions
                                .CHUNK_KEY_EVEN_DISTRIBUTION_FACTOR_UPPER_BOUND,
                        MySqlIncrementalSourceOptions.SAMPLE_SHARDING_THRESHOLD,
                        MySqlIncrementalSourceOptions.INVERSE_SAMPLING_RATE,
                        MySqlIncrementalSourceOptions.SPLIT_ALLOW_SAMPLING,
                        MySqlIncrementalSourceOptions.TABLE_NAMES_CONFIG,
                        MySqlIncrementalSourceOptions.SCHEMA_CHANGES_ENABLED,
                        MySqlIncrementalSourceOptions.SCHEMA_CHANGES_INCLUDE,
                        MySqlIncrementalSourceOptions.SCHEMA_CHANGES_EXCLUDE,
                        MySqlIncrementalSourceOptions.INT_TYPE_NARROWING,
                        SourceOptions.STARTUP_SPECIFIC_OFFSET_FILE,
                        SourceOptions.STARTUP_SPECIFIC_OFFSET_POS,
                        MySqlIncrementalSourceOptions.STARTUP_SPECIFIC_OFFSET_GTID_SET,
                        MySqlIncrementalSourceOptions.STARTUP_SPECIFIC_OFFSET_SKIP_EVENTS,
                        MySqlIncrementalSourceOptions.STARTUP_SPECIFIC_OFFSET_SKIP_ROWS)
                .optional(
                        MySqlIncrementalSourceOptions.STARTUP_MODE,
                        MySqlIncrementalSourceOptions.STOP_MODE)
                .conditional(
                        MySqlIncrementalSourceOptions.STARTUP_MODE,
                        StartupMode.INITIAL,
                        SourceOptions.EXACTLY_ONCE)
                .conditional(
                        MySqlIncrementalSourceOptions.STOP_MODE,
                        StopMode.SPECIFIC,
                        SourceOptions.STOP_SPECIFIC_OFFSET_FILE,
                        SourceOptions.STOP_SPECIFIC_OFFSET_POS)
                .conditional(
                        MySqlIncrementalSourceOptions.STARTUP_MODE,
                        StartupMode.TIMESTAMP,
                        SourceOptions.STARTUP_TIMESTAMP)
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return MySqlIncrementalSource.class;
    }

    @Override
    public List<CatalogTable> inferSchemaForDryRun(TableSourceFactoryContext context)
            throws Exception {
        Class.forName("com.mysql.cj.jdbc.Driver");
        // This internally opens a real JDBC connection to read table metadata,
        // which implicitly validates connectivity and basic SELECT privilege.
        return CatalogTableUtil.getCatalogTables(context.getOptions(), context.getClassLoader());
    }

    @Override
    public void validateConnectionForDryRun(
            TableSourceFactoryContext context, List<CatalogTable> catalogTables) throws Exception {
        validateMySqlPermissions(context.getOptions());
    }

    /**
     * Validates MySQL CDC required privileges (REPLICATION SLAVE and REPLICATION CLIENT). This
     * method is called both during dry-run and during normal task submission so that permission
     * issues are surfaced as early as possible.
     */
    private void validateMySqlPermissions(ReadonlyConfig config) {
        // Build a minimal Debezium Configuration from user config to create a MySqlConnection.
        JdbcUrlUtil.UrlInfo urlInfo = JdbcUrlUtil.getUrlInfo(config.get(JdbcCommonOptions.URL));
        String username = config.get(MySqlIncrementalSourceOptions.USERNAME);
        String password = config.get(MySqlIncrementalSourceOptions.PASSWORD);
        long connectTimeoutMs =
                config.getOptional(MySqlIncrementalSourceOptions.CONNECT_TIMEOUT_MS)
                        .orElse(JdbcSourceOptions.CONNECT_TIMEOUT_MS.defaultValue());

        Configuration dbzConfiguration =
                Configuration.create()
                        .with("database.hostname", urlInfo.getHost())
                        .with("database.port", urlInfo.getPort())
                        .with("database.user", username)
                        .with("database.password", password)
                        .with("connect.timeout.ms", String.valueOf(connectTimeoutMs))
                        .with(
                                "database.serverTimezone",
                                config.getOptional(MySqlIncrementalSourceOptions.SERVER_TIME_ZONE)
                                        .orElse(JdbcSourceOptions.SERVER_TIME_ZONE.defaultValue()))
                        .build();

        try (MySqlConnection connection =
                MySqlConnectionUtils.createMySqlConnection(dbzConfiguration)) {
            connection.connect();

            // Check REPLICATION SLAVE privilege (required for reading binlog events)
            if (!connection.userHasPrivileges("REPLICATION SLAVE")) {
                throw new SeaTunnelException(
                        "MySQL user '"
                                + username
                                + "' does not have the 'REPLICATION SLAVE' privilege "
                                + "required for CDC binlog reading. "
                                + "Please execute: GRANT REPLICATION SLAVE ON *.* TO '"
                                + username
                                + "'@'%';");
            }

            // Check REPLICATION CLIENT privilege (required for SHOW MASTER STATUS)
            if (!connection.userHasPrivileges("REPLICATION CLIENT")) {
                throw new SeaTunnelException(
                        "MySQL user '"
                                + username
                                + "' does not have the 'REPLICATION CLIENT' privilege "
                                + "required for CDC binlog reading. "
                                + "Please execute: GRANT REPLICATION CLIENT ON *.* TO '"
                                + username
                                + "'@'%';");
            }
        } catch (SeaTunnelException e) {
            throw e;
        } catch (Exception e) {
            throw new SeaTunnelException(
                    "Failed to validate MySQL CDC permissions for user '"
                            + username
                            + "': "
                            + e.getMessage(),
                    e);
        }
    }

    @Override
    public <T, SplitT extends SourceSplit, StateT extends Serializable>
            TableSource<T, SplitT, StateT> restoreSource(
                    TableSourceFactoryContext context, List<CatalogTable> restoreTables) {
        // Validate MySQL CDC required privileges before creating the source.
        // This runs at task submission time (both HTTP API and CLI) so that
        // permission issues surface immediately rather than during sync.
        validateMySqlPermissions(context.getOptions());

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
}
