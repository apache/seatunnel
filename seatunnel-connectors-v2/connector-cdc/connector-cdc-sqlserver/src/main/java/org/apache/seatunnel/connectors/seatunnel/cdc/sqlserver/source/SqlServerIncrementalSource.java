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

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.cdc.base.config.SourceConfig;
import org.apache.seatunnel.connectors.cdc.base.dialect.DataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.option.JdbcSourceOptions;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.cdc.base.option.StopMode;
import org.apache.seatunnel.connectors.cdc.base.source.IncrementalSource;
import org.apache.seatunnel.connectors.cdc.base.source.offset.OffsetFactory;
import org.apache.seatunnel.connectors.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.seatunnel.connectors.cdc.debezium.DeserializeFormat;
import org.apache.seatunnel.connectors.cdc.debezium.row.DebeziumJsonDeserializeSchema;
import org.apache.seatunnel.connectors.cdc.debezium.row.SeaTunnelRowDebeziumDeserializeSchema;
import org.apache.seatunnel.connectors.seatunnel.cdc.sqlserver.config.SqlServerSourceConfigFactory;
import org.apache.seatunnel.connectors.seatunnel.cdc.sqlserver.source.offset.LsnOffsetFactory;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.JdbcCatalogOptions;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.sqlserver.SqlServerURLParser;

import java.time.ZoneId;
import java.util.List;

public class SqlServerIncrementalSource<T> extends IncrementalSource<T, JdbcSourceConfig>
        implements SupportParallelism {

    static final String IDENTIFIER = "SqlServer-CDC";

    public SqlServerIncrementalSource(ReadonlyConfig options, List<CatalogTable> catalogTables) {
        super(options, catalogTables);
    }

    @Override
    public String getPluginName() {
        return IDENTIFIER;
    }

    @Override
    public Option<StartupMode> getStartupModeOption() {
        return SqlServerSourceOptions.STARTUP_MODE;
    }

    @Override
    public Option<StopMode> getStopModeOption() {
        return SqlServerSourceOptions.STOP_MODE;
    }

    @Override
    public SourceConfig.Factory<JdbcSourceConfig> createSourceConfigFactory(ReadonlyConfig config) {
        SqlServerSourceConfigFactory configFactory = new SqlServerSourceConfigFactory();
        configFactory.fromReadonlyConfig(readonlyConfig);
        configFactory.startupOptions(startupConfig);
        configFactory.stopOptions(stopConfig);
        JdbcUrlUtil.UrlInfo urlInfo =
                SqlServerURLParser.parse(config.get(JdbcCatalogOptions.BASE_URL));
        configFactory.originUrl(urlInfo.getOrigin());
        configFactory.hostname(urlInfo.getHost());
        configFactory.port(urlInfo.getPort());
        return configFactory;
    }

    @SuppressWarnings("unchecked")
    @Override
    public DebeziumDeserializationSchema<T> createDebeziumDeserializationSchema(
            ReadonlyConfig config) {
        if (DeserializeFormat.COMPATIBLE_DEBEZIUM_JSON.equals(
                config.get(JdbcSourceOptions.FORMAT))) {
            return (DebeziumDeserializationSchema<T>)
                    new DebeziumJsonDeserializeSchema(
                            config.get(JdbcSourceOptions.DEBEZIUM_PROPERTIES));
        }

        String zoneId = config.get(JdbcSourceOptions.SERVER_TIME_ZONE);
        return (DebeziumDeserializationSchema<T>)
                SeaTunnelRowDebeziumDeserializeSchema.builder()
                        .setTables(catalogTables)
                        .setServerTimeZone(ZoneId.of(zoneId))
                        .build();
    }

    @Override
    public DataSourceDialect<JdbcSourceConfig> createDataSourceDialect(ReadonlyConfig config) {
        return new SqlServerDialect((SqlServerSourceConfigFactory) configFactory, catalogTables);
    }

    @Override
    public OffsetFactory createOffsetFactory(ReadonlyConfig config) {
        return new LsnOffsetFactory(
                (SqlServerSourceConfigFactory) configFactory, (SqlServerDialect) dataSourceDialect);
    }
}
