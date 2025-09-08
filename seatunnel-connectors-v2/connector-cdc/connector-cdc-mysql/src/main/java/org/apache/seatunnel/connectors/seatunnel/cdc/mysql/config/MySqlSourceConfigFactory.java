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

package org.apache.seatunnel.connectors.seatunnel.cdc.mysql.config;

import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfigFactory;
import org.apache.seatunnel.connectors.cdc.debezium.EmbeddedDatabaseHistory;

import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnectorConfig;

import java.util.Properties;
import java.util.UUID;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkNotNull;

/** A factory to initialize {@link MySqlSourceConfig}. */
public class MySqlSourceConfigFactory extends JdbcSourceConfigFactory {
    public static final String SCHEMA_CHANGE_KEY = "include.schema.changes";

    private ServerIdRange serverIdRange;

    /**
     * A numeric ID or a numeric ID range of this database client, The numeric ID syntax is like
     * '5400', the numeric ID range syntax is like '5400-5408', The numeric ID range syntax is
     * required when 'scan.incremental.snapshot.enabled' enabled. Every ID must be unique across all
     * currently-running database processes in the MySQL cluster. This connector joins the MySQL
     * cluster as another server (with this unique ID) so it can read the binlog. By default, a
     * random number is generated between 6500 and 2,148,492,146, though we recommend setting an
     * explicit value."
     */
    public MySqlSourceConfigFactory serverId(String serverId) {
        this.serverIdRange = ServerIdRange.from(serverId);
        return this;
    }

    /** Creates a new {@link MySqlSourceConfig} for the given subtask {@code subtaskId}. */
    public MySqlSourceConfig create(int subtaskId) {
        Properties props = new Properties();
        // hard code server name, because we don't need to distinguish it, docs:
        // Logical name that identifies and provides a namespace for the particular
        // MySQL database server/cluster being monitored. The logical name should be
        // unique across all other connectors, since it is used as a prefix for all
        // Kafka topic names emanating from this connector.
        // Only alphanumeric characters and underscores should be used.
        props.setProperty("database.server.name", "mysql_binlog_source");
        props.setProperty("database.hostname", checkNotNull(hostname));
        props.setProperty("database.user", checkNotNull(username));
        props.setProperty("database.password", checkNotNull(password));
        props.setProperty("database.port", String.valueOf(port));
        props.setProperty("database.fetchSize", String.valueOf(fetchSize));
        props.setProperty("database.responseBuffering", "adaptive");
        props.setProperty("database.serverTimezone", serverTimeZone);

        // database history
        props.setProperty("database.history", EmbeddedDatabaseHistory.class.getCanonicalName());
        props.setProperty("database.history.instance.name", UUID.randomUUID() + "_" + subtaskId);
        props.setProperty("database.history.skip.unparseable.ddl", String.valueOf(true));
        props.setProperty("database.history.refer.ddl", String.valueOf(true));

        props.setProperty("connect.timeout.ms", String.valueOf(connectTimeoutMillis));
        // the underlying debezium reader should always capture the schema changes and forward them.
        // Note: the includeSchemaChanges parameter is used to control emitting the schema record,
        // only DataStream API program need to emit the schema record, the Table API need not

        // setting debezium capture mysql ddl
        props.setProperty(SCHEMA_CHANGE_KEY, String.valueOf(schemaChangeEnabled));
        // disable the offset flush totally
        props.setProperty("offset.flush.interval.ms", String.valueOf(Long.MAX_VALUE));
        // disable tombstones
        props.setProperty("tombstones.on.delete", String.valueOf(false));
        // debezium use "long" mode to handle unsigned bigint by default,
        // but it'll cause lose of precise when the value is larger than 2^63,
        // so use "precise" mode to avoid it.
        props.put("bigint.unsigned.handling.mode", "precise");

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
        String driverClassName = dbzConfiguration.getString(MySqlConnectorConfig.JDBC_DRIVER);
        return new MySqlSourceConfig(
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
