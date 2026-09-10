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

package org.apache.seatunnel.connectors.seatunnel.cdc.gaussdb;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.cdc.base.config.SourceConfig;
import org.apache.seatunnel.connectors.cdc.base.dialect.DataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.option.SourceOptions;
import org.apache.seatunnel.connectors.cdc.base.source.offset.OffsetFactory;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgres.config.PostgresIncrementalSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgres.config.PostgresSourceConfigFactory;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgres.source.PostgresIncrementalSource;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcCommonOptions;

import java.util.List;

/** Incremental CDC source for GaussDB logical replication, including mppdb decoding. */
public class GaussDBIncrementalSource<T> extends PostgresIncrementalSource<T> {

    /** Source factory identifier exposed in SeaTunnel job configuration. */
    static final String IDENTIFIER = "GaussDB-CDC";

    /**
     * Creates a GaussDB CDC source backed by the PostgreSQL CDC runtime.
     *
     * @param options source options parsed from the job configuration
     * @param catalogTables catalog tables resolved for the captured GaussDB tables
     */
    public GaussDBIncrementalSource(ReadonlyConfig options, List<CatalogTable> catalogTables) {
        super(options, catalogTables);
    }

    /** Rejects schema evolution only for the GaussDB-specific decoder. */
    @Override
    protected void validateSchemaEvolutionOptions(ReadonlyConfig options) {
        GaussDBMppdbConfig mppdbConfig = new GaussDBMppdbConfig(options);
        if (mppdbConfig.usesMppdbDecoding() && options.get(SourceOptions.SCHEMA_CHANGES_ENABLED)) {
            throw new SeaTunnelException(
                    "GaussDB-CDC schema evolution is not supported with mppdb_decoding because the plugin does not emit PostgreSQL RELATION messages.");
        }
        if (!mppdbConfig.usesMppdbDecoding()) {
            super.validateSchemaEvolutionOptions(options);
        }
    }

    /** Creates the GaussDB-aware source configuration for mppdb decoding. */
    @Override
    public SourceConfig.Factory<JdbcSourceConfig> createSourceConfigFactory(ReadonlyConfig config) {
        GaussDBMppdbConfig mppdbConfig = new GaussDBMppdbConfig(config);
        if (!mppdbConfig.usesMppdbDecoding()) {
            return super.createSourceConfigFactory(config);
        }
        PostgresSourceConfigFactory configFactory = new GaussDBSourceConfigFactory();
        configFactory.fromReadonlyConfig(readonlyConfig);
        JdbcUrlUtil.UrlInfo urlInfo = JdbcUrlUtil.getUrlInfo(config.get(JdbcCommonOptions.URL));
        configFactory.originUrl(urlInfo.getOrigin());
        configFactory.hostname(urlInfo.getHost());
        configFactory.port(urlInfo.getPort());
        configFactory.startupOptions(startupConfig);
        configFactory.stopOptions(stopConfig);
        return configFactory;
    }

    /** Uses the GaussDB version adapter and selects the mppdb WAL reader when configured. */
    @Override
    public DataSourceDialect<JdbcSourceConfig> createDataSourceDialect(ReadonlyConfig config) {
        GaussDBMppdbConfig mppdbConfig = new GaussDBMppdbConfig(config);
        return new GaussDBDialect(
                (PostgresSourceConfigFactory) configFactory,
                catalogTables,
                config.get(PostgresIncrementalSourceOptions.REQUIRE_REPLICA_IDENTITY_FULL),
                mppdbConfig);
    }

    /** Uses the PostgreSQL LSN state format and prepares mppdb slots before latest startup. */
    @Override
    public OffsetFactory createOffsetFactory(ReadonlyConfig config) {
        GaussDBDialect dialect = (GaussDBDialect) dataSourceDialect;
        if (!dialect.usesMppdbDecoding()) {
            return super.createOffsetFactory(config);
        }
        return new GaussDBLsnOffsetFactory((PostgresSourceConfigFactory) configFactory, dialect);
    }

    /**
     * Returns the user-facing connector name for metrics and diagnostics.
     *
     * @return GaussDB CDC connector identifier
     */
    @Override
    public String getPluginName() {
        return IDENTIFIER;
    }
}
