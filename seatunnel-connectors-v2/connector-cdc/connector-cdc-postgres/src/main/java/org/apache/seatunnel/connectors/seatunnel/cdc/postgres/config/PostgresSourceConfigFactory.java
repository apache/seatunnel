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
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.cdc.debezium.EmbeddedDatabaseHistory;

import io.debezium.connector.postgresql.PostgresConnector;

import java.util.List;
import java.util.Properties;
import java.util.UUID;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkArgument;
import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkNotNull;

public class PostgresSourceConfigFactory extends JdbcSourceConfigFactory {

    private static final long serialVersionUID = 1L;

    private static final String DATABASE_SERVER_NAME = "postgres_cdc_source";

    private static final String DRIVER_CLASS_NAME = "org.postgresql.Driver";

    /**
     * PostgreSQL replication slot name charset, matching the server's own identifier rules. The
     * derived backfill slot name ({@link PostgresSourceConfig#getSlotNameForBackfillTask()})
     * appends a fixed-charset suffix to this value and truncates by Java {@code char} count to
     * PostgreSQL's 63-byte identifier limit; restricting the configured name to single-byte ASCII
     * here keeps that char-count truncation provably byte-accurate, and rejects invalid names at
     * job submission instead of a late, confusing failure once the backfill slot lifecycle starts.
     */
    private static final Pattern SLOT_NAME_PATTERN = Pattern.compile("[a-z0-9_]{1,63}");

    private String decodingPluginName =
            PostgresIncrementalSourceOptions.DECODING_PLUGIN_NAME.defaultValue();

    private String slotName = PostgresIncrementalSourceOptions.SLOT_NAME.defaultValue();

    private List<String> schemaList;

    @Override
    public JdbcSourceConfigFactory fromReadonlyConfig(ReadonlyConfig config) {
        super.fromReadonlyConfig(config);
        this.decodingPluginName = config.get(PostgresIncrementalSourceOptions.DECODING_PLUGIN_NAME);
        this.slotName = config.get(PostgresIncrementalSourceOptions.SLOT_NAME);
        checkArgument(
                SLOT_NAME_PATTERN.matcher(this.slotName).matches(),
                "PostgreSQL slot.name '%s' is invalid: it must match %s (PostgreSQL replication"
                        + " slot identifier rules), since it is also used to derive the per-reader"
                        + " snapshot backfill slot name.",
                this.slotName,
                SLOT_NAME_PATTERN.pattern());
        this.schemaList = config.get(PostgresIncrementalSourceOptions.SCHEMA_NAME);
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

        // database history
        props.setProperty("database.history", EmbeddedDatabaseHistory.class.getCanonicalName());
        props.setProperty("database.history.instance.name", UUID.randomUUID() + "_" + subtask);
        props.setProperty("database.history.skip.unparseable.ddl", String.valueOf(true));
        props.setProperty("database.history.refer.ddl", String.valueOf(true));

        props.setProperty("database.tcpKeepAlive", String.valueOf(true));
        if (schemaList != null) {
            props.setProperty("schema.include.list", String.join(",", schemaList));
        }

        if (tableList != null) {
            // pg identifier is of the form schemaName.tableName
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
                                                "Invalid table name: "
                                                        + tableStr
                                                        + " ,Postgres identifier is of the form schemaName.tableName");
                                    })
                            .collect(Collectors.joining(",")));
        }

        if (dbzProperties != null) {
            props.putAll(dbzProperties);
        }
        // Debezium PostgreSQL does not emit DDL records, but SeaTunnel uses this flag to enable
        // synthetic schema records produced from pgoutput RELATION messages. Apply it after the
        // Debezium pass-through properties so the SeaTunnel option remains authoritative.
        props.setProperty("include.schema.changes", String.valueOf(schemaChangeEnabled));
        if (startupConfig != null && startupConfig.getStartupMode() == StartupMode.SNAPSHOT_ONLY) {
            props.setProperty("snapshot.mode", "initial_only");
        } else if (startupConfig != null
                && startupConfig.getStartupMode() == StartupMode.COMMITTED_OFFSET) {
            props.setProperty("snapshot.mode", "never");
        }

        PostgresSourceConfig config =
                new PostgresSourceConfig(
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
        config.setSubtaskId(subtask);
        // Propagate the enableConcurrentRead flag so the chunk splitter can skip split analysis.
        config.setEnableConcurrentRead(this.enableConcurrentRead);
        return config;
    }
}
