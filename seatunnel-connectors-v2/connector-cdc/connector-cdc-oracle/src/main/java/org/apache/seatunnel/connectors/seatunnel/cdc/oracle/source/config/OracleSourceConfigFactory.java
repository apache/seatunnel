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

package org.apache.seatunnel.connectors.seatunnel.cdc.oracle.source.config;

import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfigFactory;
import org.apache.seatunnel.connectors.cdc.debezium.EmbeddedDatabaseHistory;

import io.debezium.connector.oracle.OracleConnector;

import java.util.List;
import java.util.Properties;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkNotNull;

/** Factory for creating {@link OracleSourceConfig}. */
public class OracleSourceConfigFactory extends JdbcSourceConfigFactory {

    private static final String DATABASE_SERVER_NAME = "oracle_logminer";

    private static final String DRIVER_CLASS_NAME = "oracle.jdbc.OracleDriver";

    private List<String> schemaList;
    private String chunkKeyColumn;
    /**
     * An optional list of regular expressions that match schema names to be monitored; any schema
     * name not included in the whitelist will be excluded from monitoring. By default all
     * non-system schemas will be monitored.
     */
    public JdbcSourceConfigFactory schemaList(List<String> schemaList) {
        this.schemaList = schemaList;
        return this;
    }

    public JdbcSourceConfigFactory chunkKeyColumn(String chunkKeyColumn) {
        this.chunkKeyColumn = chunkKeyColumn;
        return this;
    }

    @Override
    public OracleSourceConfig create(int subtask) {
        Properties props = new Properties();
        props.setProperty("connector.class", OracleConnector.class.getCanonicalName());

        // hard code server name, because we don't need to distinguish it, docs:
        // Logical name that identifies and provides a namespace for the Oracle database
        // server that you want Debezium to capture. The logical name should be unique across
        // all other connectors, since it is used as a prefix for all Kafka topic names
        // emanating from this connector. Only alphanumeric characters and underscores should be
        // used.
        props.setProperty("database.server.name", DATABASE_SERVER_NAME);
        props.setProperty("database.user", checkNotNull(username));
        props.setProperty("database.password", checkNotNull(password));
        props.setProperty("database.history.skip.unparseable.ddl", String.valueOf(true));
        props.setProperty("database.dbname", checkNotNull(databaseList.get(0)));

        props.setProperty("database.history", EmbeddedDatabaseHistory.class.getCanonicalName());
        props.setProperty("database.history.instance.name", UUID.randomUUID() + "_" + subtask);
        props.setProperty("database.history.skip.unparseable.ddl", String.valueOf(true));
        props.setProperty("database.history.refer.ddl", String.valueOf(true));

        // TODO Not yet supported
        props.setProperty("include.schema.changes", String.valueOf(false));

        props.setProperty("tombstones.on.delete", String.valueOf(false));

        if (originUrl != null) {
            props.setProperty("database.url", originUrl);
        } else {
            checkNotNull(hostname, "hostname is required when url is not configured");
            props.setProperty("database.hostname", hostname);
            checkNotNull(port, "port is required when url is not configured");
            props.setProperty("database.port", String.valueOf(port));
        }

        if (schemaList != null) {
            props.setProperty("schema.include.list", String.join(",", schemaList));
        }
        if (tableList != null) {
            props.setProperty("table.include.list", String.join(",", tableList));
        }

        if (dbzProperties != null) {
            dbzProperties.forEach(props::put);
        }

        return new OracleSourceConfig(
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
                exactlyOnce,
                chunkKeyColumn);
    }
}
