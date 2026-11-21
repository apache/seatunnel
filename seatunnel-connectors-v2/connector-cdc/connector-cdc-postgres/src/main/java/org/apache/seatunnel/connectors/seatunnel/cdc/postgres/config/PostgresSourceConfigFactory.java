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

package org.apache.seatunnel.connectors.seatunnel.cdc.postgres.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.pgbase.config.PgBaseSourceConfigFactory;

import io.debezium.connector.postgresql.PostgresConnector;

import java.util.List;
import java.util.Properties;

/**
 * PostgreSQL-specific source config factory built on top of the PG-base Debezium property assembly.
 */
public class PostgresSourceConfigFactory extends PgBaseSourceConfigFactory<PostgresSourceConfig> {

    private static final long serialVersionUID = 1L;

    private static final String DATABASE_SERVER_NAME = "postgres_cdc_source";

    private static final String DRIVER_CLASS_NAME = "org.postgresql.Driver";

    private String decodingPluginName =
            PostgresIncrementalSourceOptions.DECODING_PLUGIN_NAME.defaultValue();

    private String slotName = PostgresIncrementalSourceOptions.SLOT_NAME.defaultValue();

    private List<String> schemaList;

    @Override
    public PostgresSourceConfigFactory fromReadonlyConfig(ReadonlyConfig config) {
        super.fromReadonlyConfig(config);
        this.decodingPluginName = config.get(PostgresIncrementalSourceOptions.DECODING_PLUGIN_NAME);
        this.slotName = config.get(PostgresIncrementalSourceOptions.SLOT_NAME);
        this.schemaList = config.get(PostgresIncrementalSourceOptions.SCHEMA_NAME);
        return this;
    }

    @Override
    protected String connectorClassName() {
        return PostgresConnector.class.getCanonicalName();
    }

    @Override
    protected String databaseServerName() {
        return DATABASE_SERVER_NAME;
    }

    @Override
    protected String driverClassName() {
        return DRIVER_CLASS_NAME;
    }

    @Override
    protected void configureConnectorProperties(Properties props, int subtask) {
        props.setProperty("plugin.name", decodingPluginName);
        props.setProperty("slot.name", slotName);
        if (schemaList != null) {
            props.setProperty("schema.include.list", String.join(",", schemaList));
        }
    }

    @Override
    protected PostgresSourceConfig createSourceConfig(Properties props, String driverClassName) {
        return new PostgresSourceConfig(
                startupConfig,
                stopConfig,
                databaseList,
                tableList,
                splitSize,
                splitColumn,
                distributionFactorUpper,
                distributionFactorLower,
                sampleShardingThreshold,
                inverseSamplingRate,
                sampleShardingAllow,
                props,
                driverClassName,
                hostname,
                port,
                username,
                password,
                originUrl,
                fetchSize,
                serverTimeZone,
                connectTimeoutMillis,
                connectMaxRetries,
                connectionPoolSize,
                exactlyOnce);
    }
}
