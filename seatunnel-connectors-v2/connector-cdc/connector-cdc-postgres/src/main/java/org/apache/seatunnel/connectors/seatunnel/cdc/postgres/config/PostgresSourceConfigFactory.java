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

import static com.google.common.base.Preconditions.checkNotNull;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfigFactory;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgres.option.PostgresOptions;

import io.debezium.connector.postgresql.PostgresConnector;

import java.util.Properties;

public class PostgresSourceConfigFactory extends JdbcSourceConfigFactory {

    private static final String DATABASE_SERVER_NAME = "postgres_cdc_source";

    private static final String DRIVER_CLASS_NAME = "org.postgresql.Driver";

    private String decodingPluginName = PostgresOptions.DECODING_PLUGIN_NAME.defaultValue();

    private String slotName = PostgresOptions.SLOT_NAME.defaultValue();

    @Override
    public JdbcSourceConfigFactory fromReadonlyConfig(ReadonlyConfig config) {
        super.fromReadonlyConfig(config);
        this.decodingPluginName = config.get(PostgresOptions.DECODING_PLUGIN_NAME);
        this.slotName = config.get(PostgresOptions.SLOT_NAME);
        return this;
    }

    @Override
    public PostgresSourceConfig create(int subtask) {
        Properties props = new Properties();
        props.setProperty("connector.class", PostgresConnector.class.getCanonicalName());
        // hard code server name, because we don't need to distinguish it, docs:
        // Logical name that identifies and provides a namespace for the particular PostgreSQL
        // database server/cluster being monitored. The logical name should be unique across
        // all other connectors, since it is used as a prefix for all Kafka topic names coming
        // from this connector. Only alphanumeric characters and underscores should be used.
        props.setProperty("database.server.name", DATABASE_SERVER_NAME);
        props.setProperty("database.hostname", checkNotNull(hostname));
        props.setProperty("database.user", checkNotNull(username));
        props.setProperty("database.password", checkNotNull(password));
        props.setProperty("database.port", String.valueOf(port));
        props.setProperty("database.dbname", checkNotNull(databaseList.get(0)));
        props.setProperty("plugin.name", decodingPluginName);
        props.setProperty("slot.name", slotName);

        if (databaseList != null) {
            props.setProperty("schema.include.list", String.join(",", databaseList));
        }
        if (tableList != null) {
            props.setProperty("table.include.list", String.join(",", tableList));
        }

        if (dbzProperties != null) {
            props.putAll(dbzProperties);
        }

        return new PostgresSourceConfig(
                startupConfig,
                stopConfig,
                databaseList,
                tableList,
                splitSize,
                distributionFactorUpper,
                distributionFactorLower,
                props,
                DRIVER_CLASS_NAME,
                hostname,
                port,
                username,
                password,
                fetchSize,
                serverTimeZone,
                connectTimeoutMillis,
                connectMaxRetries,
                connectionPoolSize);
    }
}
