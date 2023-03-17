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

package org.apache.seatunnel.connectors.seatunnel.cdc.sqlserver.source.source;

import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.catalog.CatalogOptions;
import org.apache.seatunnel.api.table.factory.TableSourceFactory;
import org.apache.seatunnel.connectors.cdc.base.option.JdbcSourceOptions;
import org.apache.seatunnel.connectors.cdc.base.option.SourceOptions;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.cdc.base.option.StopMode;

public class SqlServerIncrementalSourceFactory implements TableSourceFactory {

    @Override
    public String factoryIdentifier() {
        return SqlServerIncrementalSource.IDENTIFIER;
    }

    @Override
    public OptionRule optionRule() {
        return JdbcSourceOptions.getBaseRule()
                .required(
                        JdbcSourceOptions.HOSTNAME,
                        JdbcSourceOptions.USERNAME,
                        JdbcSourceOptions.PASSWORD,
                        CatalogOptions.TABLE_NAMES)
                .optional(
                        JdbcSourceOptions.DATABASE_NAMES,
                        JdbcSourceOptions.PORT,
                        JdbcSourceOptions.SERVER_TIME_ZONE,
                        JdbcSourceOptions.CONNECT_TIMEOUT_MS,
                        JdbcSourceOptions.CONNECT_MAX_RETRIES,
                        JdbcSourceOptions.CONNECTION_POOL_SIZE)
                .optional(SqlServerSourceOptions.STARTUP_MODE, SqlServerSourceOptions.STOP_MODE)
                .conditional(
                        SqlServerSourceOptions.STARTUP_MODE,
                        StartupMode.SPECIFIC,
                        SourceOptions.STARTUP_SPECIFIC_OFFSET_POS)
                .conditional(
                        SqlServerSourceOptions.STOP_MODE,
                        StopMode.SPECIFIC,
                        SourceOptions.STOP_SPECIFIC_OFFSET_POS)
                .conditional(
                        SqlServerSourceOptions.STARTUP_MODE,
                        StartupMode.TIMESTAMP,
                        SourceOptions.STARTUP_TIMESTAMP)
                .conditional(
                        SqlServerSourceOptions.STOP_MODE,
                        StopMode.TIMESTAMP,
                        SourceOptions.STOP_TIMESTAMP)
                .build();
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return SqlServerIncrementalSource.class;
    }
}
