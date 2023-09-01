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
import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfigFactory;
import org.apache.seatunnel.connectors.cdc.base.option.JdbcSourceOptions;
import org.apache.seatunnel.connectors.cdc.debezium.EmbeddedDatabaseHistory;
import org.apache.seatunnel.connectors.seatunnel.cdc.postgres.option.PostgresOptions;

import io.debezium.connector.postgresql.PostgresConnector;

import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

public class PostgresSourceConfigFactory extends JdbcSourceConfigFactory {

    private static final String DATABASE_SERVER_NAME = "postgres_cdc_source";

    private static final String DRIVER_CLASS_NAME = "org.postgresql.Driver";

    private String decodingPluginName = PostgresOptions.DECODING_PLUGIN_NAME.defaultValue();

    private String slotName = PostgresOptions.SLOT_NAME.defaultValue();

    private String publicationName = PostgresOptions.PUBLICATION_NAME.defaultValue();

    protected boolean exactlyOnce = JdbcSourceOptions.EXACTLY_ONCE.defaultValue();

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
        props.setProperty("publication.name", publicationName);

        // database history
        props.setProperty("database.history", EmbeddedDatabaseHistory.class.getCanonicalName());
        props.setProperty("database.history.instance.name", UUID.randomUUID() + "_" + subtask);
        props.setProperty("database.history.skip.unparseable.ddl", String.valueOf(true));
        props.setProperty("database.history.refer.ddl", String.valueOf(true));

        if (tableList != null) {
            // PostgreSQL identifier is of the form schemaName.tableName
            String tableIncludeList =
                    tableList.stream()
                            .map(table -> table.substring(table.indexOf(".") + 1))
                            .collect(Collectors.joining(","));
            props.setProperty("table.include.list", tableIncludeList);
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
                sampleShardingThreshold,
                inverseSamplingRate,
                props,
                DRIVER_CLASS_NAME,
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
