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

package org.apache.seatunnel.connectors.seatunnel.cdc.postgres.source;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.cdc.base.config.SourceConfig;
import org.apache.seatunnel.connectors.cdc.base.config.StartupConfig;
import org.apache.seatunnel.connectors.cdc.base.dialect.DataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.cdc.base.option.StopMode;
import org.apache.seatunnel.connectors.cdc.base.source.offset.OffsetFactory;
import org.apache.seatunnel.connectors.seatunnel.cdc.pgbase.source.PgBaseIncrementalSource;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgres.config.PostgresIncrementalSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgres.config.PostgresSourceConfigFactory;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgres.source.offset.LsnOffsetFactory;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcCommonOptions;

import java.util.List;
import java.util.Optional;

/**
 * PostgreSQL incremental source backed by the shared PG-base source behavior.
 *
 * <p>Keeps PostgreSQL-specific options, dialect selection and offset handling in this connector.
 */
public class PostgresIncrementalSource<T> extends PgBaseIncrementalSource<T, JdbcSourceConfig> {

    /**
     * Preserves serialized job DAG compatibility with the Postgres source released in 2.3.13.
     *
     * <p>Cross-checked with {@code serialver} against the real {@code PostgresIncrementalSource}
     * class shipped in the {@code connector-cdc-postgres} 2.3.12 and 2.3.13 Maven Central
     * artifacts, where both versions compute this same default UID (2.3.11 computes a different
     * one, so it was already incompatible with 2.3.12 before this PR). This constant is not a
     * guess; do not regenerate it when the class hierarchy or implementation changes.
     */
    private static final long serialVersionUID = -9086519839702872016L;

    static final String IDENTIFIER = "Postgres-CDC";

    public PostgresIncrementalSource(ReadonlyConfig options, List<CatalogTable> catalogTables) {
        super(options, catalogTables);
    }

    @Override
    public String getPluginName() {
        return IDENTIFIER;
    }

    @Override
    public Option<StartupMode> getStartupModeOption() {
        return PostgresSourceOptions.STARTUP_MODE;
    }

    @Override
    public Option<StopMode> getStopModeOption() {
        return PostgresSourceOptions.STOP_MODE;
    }

    @Override
    protected StartupConfig getStartupConfig(ReadonlyConfig config) {
        StartupConfig startupConfig = super.getStartupConfig(config);
        validateStartupOptions(config, startupConfig);
        return startupConfig;
    }

    @Override
    public SourceConfig.Factory<JdbcSourceConfig> createSourceConfigFactory(ReadonlyConfig config) {
        PostgresSourceConfigFactory configFactory = new PostgresSourceConfigFactory();
        configFactory.fromReadonlyConfig(readonlyConfig);
        JdbcUrlUtil.UrlInfo urlInfo = JdbcUrlUtil.getUrlInfo(config.get(JdbcCommonOptions.URL));
        configFactory.originUrl(urlInfo.getOrigin());
        configFactory.hostname(urlInfo.getHost());
        configFactory.port(urlInfo.getPort());
        configFactory.startupOptions(startupConfig);
        configFactory.stopOptions(stopConfig);
        return configFactory;
    }

    @Override
    public DataSourceDialect<JdbcSourceConfig> createDataSourceDialect(ReadonlyConfig config) {
        return new PostgresDialect(
                (PostgresSourceConfigFactory) configFactory,
                catalogTables,
                config.get(PostgresIncrementalSourceOptions.REQUIRE_REPLICA_IDENTITY_FULL));
    }

    @Override
    public OffsetFactory createOffsetFactory(ReadonlyConfig config) {
        return new LsnOffsetFactory(
                (PostgresSourceConfigFactory) configFactory, (PostgresDialect) dataSourceDialect);
    }

    @Override
    public Optional<String> driverName() {
        return Optional.of("org.postgresql.Driver");
    }

    /**
     * Validates PostgreSQL committed-offset startup before source config creation.
     *
     * <p>Debezium can only resume from a committed LSN when the replication slot is explicit and
     * stable across job attempts.
     */
    private void validateStartupOptions(ReadonlyConfig options, StartupConfig startupConfig) {
        if (startupConfig.getStartupMode() != StartupMode.COMMITTED_OFFSET) {
            return;
        }
        Optional<String> slotName =
                options.getOptional(PostgresIncrementalSourceOptions.SLOT_NAME)
                        .map(String::trim)
                        .filter(name -> !name.isEmpty());
        if (!slotName.isPresent()) {
            throw new SeaTunnelException(
                    String.format(
                            "PostgreSQL-CDC startup.mode '%s' requires an explicit '%s' option.",
                            StartupMode.COMMITTED_OFFSET,
                            PostgresIncrementalSourceOptions.SLOT_NAME.key()));
        }
    }
}
