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

package org.apache.seatunnel.connectors.seatunnel.cdc.oracle.config;

import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfigFactory;
import org.apache.seatunnel.connectors.cdc.debezium.EmbeddedDatabaseHistory;

import io.debezium.connector.oracle.OracleConnector;

import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkNotNull;

/** A factory to initialize {@link OracleSourceConfig}. */
public class OracleSourceConfigFactory extends JdbcSourceConfigFactory {

    private static final long serialVersionUID = 1L;
    private static final String DATABASE_SERVER_NAME = "oracle_logminer";

    private static final String DRIVER_CLASS_NAME = "oracle.jdbc.driver.OracleDriver";

    private List<String> schemaList;

    private Boolean useSelectCount;

    private Boolean skipAnalyze;
    /**
     * An optional list of regular expressions that match schema names to be monitored; any schema
     * name not included in the whitelist will be excluded from monitoring. By default all
     * non-system schemas will be monitored.
     */
    public JdbcSourceConfigFactory schemaList(List<String> schemaList) {
        this.schemaList = schemaList;
        return this;
    }

    public JdbcSourceConfigFactory useSelectCount(Boolean useSelectCount) {
        this.useSelectCount = useSelectCount;
        return this;
    }

    public JdbcSourceConfigFactory skipAnalyze(Boolean skipAnalyze) {
        this.skipAnalyze = skipAnalyze;
        return this;
    }

    /** Creates a new {@link OracleSourceConfig} for the given subtask {@code subtaskId}. */
    public OracleSourceConfig create(int subtask) {

        try {
            Class.forName(DRIVER_CLASS_NAME);
        } catch (ClassNotFoundException e) {
            throw new RuntimeException(e);
        }

        Properties props = new Properties();
        props.setProperty("connector.class", OracleConnector.class.getCanonicalName());
        // Logical name that identifies and provides a namespace for the particular Oracle
        // database server being
        // monitored. The logical name should be unique across all other connectors, since it is
        // used as a prefix
        // for all Kafka topic names emanating from this connector. Only alphanumeric characters
        // and
        // underscores should be used.
        props.setProperty("database.server.name", DATABASE_SERVER_NAME);
        props.setProperty("database.url", checkNotNull(originUrl));
        props.setProperty("database.user", checkNotNull(username));
        props.setProperty("database.password", checkNotNull(password));
        props.setProperty("database.dbname", checkNotNull(databaseList.get(0)));

        // database history
        props.setProperty("database.history", EmbeddedDatabaseHistory.class.getCanonicalName());
        props.setProperty("database.history.instance.name", UUID.randomUUID() + "_" + subtask);
        props.setProperty("database.history.skip.unparseable.ddl", String.valueOf(true));
        props.setProperty("database.history.refer.ddl", String.valueOf(true));

        props.setProperty("connect.timeout.ms", String.valueOf(connectTimeoutMillis));
        // disable tombstones
        props.setProperty("tombstones.on.delete", String.valueOf(false));

        // Optimize logminer latency
        props.setProperty("log.mining.strategy", "online_catalog");

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
            // Oracle identifier is of the form schemaName.tableName
            props.setProperty(
                    "table.include.list",
                    tableList.stream()
                            .map(
                                    tableStr -> {
                                        String[] splits = tableStr.split("\\.");
                                        if (splits.length == 2) {
                                            return tableStr;
                                        }
                                        if (splits.length == 3) {
                                            return String.join(".", splits[1], splits[2]);
                                        }
                                        throw new IllegalArgumentException(
                                                "Invalid table name: " + tableStr);
                                    })
                            .collect(Collectors.joining(",")));
        }

        // override the user-defined debezium properties
        if (dbzProperties != null) {
            props.putAll(dbzProperties);
        }

        return new OracleSourceConfig(
                useSelectCount,
                skipAnalyze,
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

    private void validateConfig() throws IllegalArgumentException {
        if (databaseList.size() != 1) {
            throw new IllegalArgumentException(
                    "Oracle only supports single database, databaseList: " + databaseList);
        }
        for (String database : databaseList) {
            for (int i = 0; i < database.length(); i++) {
                if (Character.isLetter(database.charAt(i))
                        && !Character.isUpperCase(database.charAt(i))) {
                    throw new IllegalArgumentException(
                            "Oracle database name must be in all uppercase, database: " + database);
                }
            }
        }
        for (String table : tableList) {
            if (table.split("\\.").length != 3 && table.split("\\.").length != 2) {
                throw new IllegalArgumentException(
                        "Oracle table name format must be is: ${database}.${schema}.${table} or ${schema}.${table}, table: "
                                + table);
            }
            for (int i = 0; i < table.length(); i++) {
                if (Character.isLetter(table.charAt(i))
                        && !Character.isUpperCase(table.charAt(i))) {
                    throw new IllegalArgumentException(
                            "Oracle table name must be in all uppercase, table: " + table);
                }
            }
        }
    }
}
