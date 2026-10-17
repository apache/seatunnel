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

package org.apache.seatunnel.connectors.seatunnel.cdc.mariadb.config;

import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfigFactory;
import org.apache.seatunnel.connectors.cdc.debezium.EmbeddedDatabaseHistory;

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnectorConfig;

import java.util.Properties;
import java.util.UUID;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkNotNull;

/** A factory to initialize {@link MariaDbSourceConfig}. */
public class MariaDbSourceConfigFactory extends JdbcSourceConfigFactory {
    private static final long serialVersionUID = 1L;

    public static final String SCHEMA_CHANGE_KEY = "include.schema.changes";

    private ServerIdRange serverIdRange;

    public MariaDbSourceConfigFactory serverId(String serverId) {
        this.serverIdRange = ServerIdRange.from(serverId);
        return this;
    }

    /** Creates a new {@link MariaDbSourceConfig} for the given subtask {@code subtaskId}. */
    public MariaDbSourceConfig create(int subtaskId) {
        Properties props = new Properties();
        props.setProperty("database.server.name", "mariadb_binlog_source");
        props.setProperty("database.hostname", checkNotNull(hostname));
        props.setProperty("database.user", checkNotNull(username));
        props.setProperty("database.password", checkNotNull(password));
        props.setProperty("database.port", String.valueOf(port));
        props.setProperty("database.fetchSize", String.valueOf(fetchSize));
        props.setProperty("database.responseBuffering", "adaptive");
        props.setProperty("database.serverTimezone", serverTimeZone);
        props.setProperty(MySqlConnectorConfig.JDBC_DRIVER.name(), "org.mariadb.jdbc.Driver");
        props.setProperty("database.jdbc.driver", "org.mariadb.jdbc.Driver");

        // database history
        props.setProperty("database.history", EmbeddedDatabaseHistory.class.getCanonicalName());
        props.setProperty("database.history.instance.name", UUID.randomUUID() + "_" + subtaskId);
        props.setProperty("database.history.skip.unparseable.ddl", String.valueOf(true));
        props.setProperty("database.history.refer.ddl", String.valueOf(true));

        props.setProperty("connect.timeout.ms", String.valueOf(connectTimeoutMillis));

        // setting debezium capture mariadb ddl
        props.setProperty(SCHEMA_CHANGE_KEY, String.valueOf(schemaChangeEnabled));
        // disable the offset flush totally
        props.setProperty("offset.flush.interval.ms", String.valueOf(Long.MAX_VALUE));
        // disable tombstones
        props.setProperty("tombstones.on.delete", String.valueOf(false));
        // debezium use "long" mode to handle unsigned bigint by default,
        // but it'll cause lose of precise when the value is larger than 2^63,
        // so use "precise" mode to avoid it.
        props.put("bigint.unsigned.handling.mode", "precise");
        props.setProperty("int_type_narrowing", String.valueOf(true));

        if (serverIdRange != null) {
            props.setProperty("database.server.id.range", String.valueOf(serverIdRange));
            long serverId = serverIdRange.getServerId(subtaskId);
            props.setProperty("database.server.id", String.valueOf(serverId));
        }
        if (databaseList != null) {
            props.setProperty("database.include.list", String.join(",", databaseList));
        } else if (databasePattern != null) {
            props.setProperty("database.include.list", databasePattern);
        }
        if (tableList != null) {
            props.setProperty("table.include.list", String.join(",", tableList));
        } else if (tablePattern != null) {
            props.setProperty("table.include.list", tablePattern);
        }
        if (serverTimeZone != null) {
            props.setProperty("database.serverTimezone", serverTimeZone);
        }

        // override the user-defined debezium properties
        if (dbzProperties != null) {
            dbzProperties.forEach(props::put);
        }

        Configuration dbzConfiguration = Configuration.from(props);
        String driverClassName =
                dbzConfiguration.getString(
                        MySqlConnectorConfig.JDBC_DRIVER.name(), "org.mariadb.jdbc.Driver");
        MariaDbSourceConfig config =
                new MariaDbSourceConfig(
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
        // Propagate the enableConcurrentRead flag so the chunk splitter can skip split analysis.
        config.setEnableConcurrentRead(this.enableConcurrentRead);
        return config;
    }
}
