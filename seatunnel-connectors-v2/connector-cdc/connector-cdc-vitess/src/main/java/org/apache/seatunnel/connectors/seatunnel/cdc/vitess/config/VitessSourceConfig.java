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

package org.apache.seatunnel.connectors.seatunnel.cdc.vitess.config;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.cdc.base.option.SourceOptions;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.seatunnel.connectors.cdc.debezium.DeserializeFormat;
import org.apache.seatunnel.connectors.cdc.debezium.row.DebeziumJsonDeserializeSchema;
import org.apache.seatunnel.connectors.cdc.debezium.row.SeaTunnelRowDebeziumDeserializeSchema;
import org.apache.seatunnel.connectors.seatunnel.cdc.vitess.source.split.VitessSourceSplit;
import org.apache.seatunnel.connectors.seatunnel.cdc.vitess.source.split.VitessTableSchemaState;

import io.debezium.connector.vitess.SourceInfo;
import io.debezium.connector.vitess.Vgtid;

import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * Immutable runtime configuration for the Vitess CDC source.
 *
 * <p>The connector keeps startup semantics here instead of leaking Debezium-specific options into
 * the source reader.
 */
public final class VitessSourceConfig {

    /** Captured tables must stay deterministic so downstream table identity remains stable. */
    private final List<CatalogTable> catalogTables;

    /** SeaTunnel-owned connector options. */
    private final ReadonlyConfig options;

    /** Startup mode is limited on purpose for the first delivery. */
    private final StartupMode startupMode;

    /** Stable VGTID used when startup.mode=specific. */
    private final String specificStartupVgtid;

    private VitessSourceConfig(
            ReadonlyConfig options,
            List<CatalogTable> catalogTables,
            StartupMode startupMode,
            String specificStartupVgtid) {
        this.options = options;
        this.catalogTables = Collections.unmodifiableList(new ArrayList<>(catalogTables));
        this.startupMode = startupMode;
        this.specificStartupVgtid = specificStartupVgtid;
    }

    /**
     * Builds the validated connector configuration.
     *
     * <p>Vitess does not expose database/schema names the same way as MySQL-based connectors, so
     * table paths must already be deterministic before the source starts.
     */
    public static VitessSourceConfig of(ReadonlyConfig options, List<CatalogTable> catalogTables) {
        if (catalogTables == null || catalogTables.isEmpty()) {
            throw new IllegalArgumentException(
                    "Vitess CDC requires resolved catalog tables for deterministic table identity.");
        }

        String keyspace = options.get(VitessSourceOptions.KEYSPACE);
        for (CatalogTable catalogTable : catalogTables) {
            String databaseName = catalogTable.getTablePath().getDatabaseName();
            if (databaseName == null) {
                throw new IllegalArgumentException(
                        String.format(
                                "Vitess CDC requires database-qualified table paths, but table '%s' does not define a database name.",
                                catalogTable.getTablePath()));
            }
            if (!keyspace.equals(databaseName)) {
                throw new IllegalArgumentException(
                        String.format(
                                "Vitess CDC captures one keyspace per source. Table '%s' does not belong to keyspace '%s'.",
                                catalogTable.getTablePath(), keyspace));
            }
            if (catalogTable.getTablePath().getSchemaName() != null) {
                throw new IllegalArgumentException(
                        String.format(
                                "Vitess CDC does not support schema-qualified table paths. Table '%s' contains an unexpected schema component.",
                                catalogTable.getTablePath()));
            }
        }

        StartupMode startupMode = options.get(VitessSourceOptions.STARTUP_MODE);
        String specificVgtid =
                options.getOptional(VitessSourceOptions.STARTUP_SPECIFIC_OFFSET_VGTID).orElse(null);
        if (startupMode == StartupMode.SPECIFIC) {
            if (specificVgtid == null) {
                throw new IllegalArgumentException(
                        "startup.specific-offset.vgtid is required when startup.mode=specific.");
            }
            // Parse eagerly so configuration failures surface before the source thread starts.
            Vgtid.of(specificVgtid);
        }

        return new VitessSourceConfig(options, catalogTables, startupMode, specificVgtid);
    }

    /** Returns the user-facing connector name used by plugin registration. */
    public String getPluginName() {
        return "Vitess-CDC";
    }

    /** Returns the resolved output tables. */
    public List<CatalogTable> getCatalogTables() {
        return catalogTables;
    }

    /** Creates the initial split so checkpoint state already contains the startup intent. */
    public VitessSourceSplit createInitialSplit() {
        return new VitessSourceSplit(
                VitessSourceSplit.SPLIT_ID,
                createStartupOffset(),
                VitessTableSchemaState.serializeCatalogTables(catalogTables),
                catalogTables);
    }

    /** Creates the Debezium deserializer selected by the SeaTunnel format option. */
    @SuppressWarnings("unchecked")
    public DebeziumDeserializationSchema<SeaTunnelRow> createDeserializer() {
        if (DeserializeFormat.COMPATIBLE_DEBEZIUM_JSON.equals(options.get(SourceOptions.FORMAT))) {
            return new DebeziumJsonDeserializeSchema(getDebeziumPropertiesAsMap());
        }
        return SeaTunnelRowDebeziumDeserializeSchema.builder()
                .setTables(catalogTables)
                .setServerTimeZone(ZoneId.of(options.get(VitessSourceOptions.SERVER_TIME_ZONE)))
                .build();
    }

    /**
     * Builds Debezium properties while keeping SeaTunnel-owned semantics authoritative.
     *
     * <p>Pass-through Debezium properties are applied first so connector-owned options can safely
     * overwrite conflicting low-level keys.
     */
    public Properties toDebeziumProperties() {
        Properties properties = new Properties();
        properties.putAll(getDebeziumPropertiesAsMap());

        String logicalName = buildLogicalName();
        properties.setProperty("connector.class", "io.debezium.connector.vitess.VitessConnector");
        properties.setProperty("name", logicalName);
        properties.setProperty("tasks.max", "1");
        properties.setProperty("database.server.name", logicalName);
        properties.setProperty("database.hostname", options.get(VitessSourceOptions.HOSTNAME));
        properties.setProperty(
                "database.port", String.valueOf(options.get(VitessSourceOptions.PORT)));
        properties.setProperty("vitess.keyspace", options.get(VitessSourceOptions.KEYSPACE));
        properties.setProperty(
                "vitess.tablet.type", options.get(VitessSourceOptions.TABLET_TYPE).name());
        properties.setProperty(
                "vitess.stop_on_reshard",
                String.valueOf(options.get(VitessSourceOptions.STOP_ON_RESHARD)));
        properties.setProperty(
                "vitess.keepalive.interval.ms",
                String.valueOf(options.get(VitessSourceOptions.KEEPALIVE_INTERVAL_MS)));
        properties.setProperty(
                "vitess.grpc.max_inbound_message_size",
                String.valueOf(options.get(VitessSourceOptions.GRPC_MAX_INBOUND_MESSAGE_SIZE)));
        properties.setProperty("plugin.name", "decoderbufs");
        properties.setProperty("include.schema.changes", "false");

        options.getOptional(VitessSourceOptions.USERNAME)
                .ifPresent(
                        username -> {
                            properties.setProperty("database.user", username);
                            properties.setProperty("vitess.database.user", username);
                        });
        options.getOptional(VitessSourceOptions.PASSWORD)
                .ifPresent(
                        password -> {
                            properties.setProperty("database.password", password);
                            properties.setProperty("vitess.database.password", password);
                        });
        options.getOptional(VitessSourceOptions.SHARD)
                .ifPresent(shard -> properties.setProperty("vitess.shard", shard));
        options.getOptional(VitessSourceOptions.GRPC_HEADERS)
                .ifPresent(headers -> properties.setProperty("vitess.grpc.headers", headers));

        properties.setProperty(
                "table.include.list",
                catalogTables.stream()
                        .map(catalogTable -> catalogTable.getTablePath().toString())
                        .collect(Collectors.joining(",")));

        // LATEST intentionally uses Vitess' symbolic current position for convenience startup.
        // SPECIFIC uses an explicit VGTID and is the reproducible startup path.
        if (startupMode == StartupMode.SPECIFIC) {
            properties.setProperty("vitess.gtid", specificStartupVgtid);
        } else {
            properties.setProperty("vitess.gtid", "current");
        }

        return properties;
    }

    /** Returns the initial offset stored in the split before the first CDC row is emitted. */
    public Map<String, Object> createStartupOffset() {
        if (startupMode != StartupMode.SPECIFIC) {
            return null;
        }
        Map<String, Object> offset = new HashMap<>();
        offset.put(SourceInfo.VGTID_KEY, specificStartupVgtid);
        return offset;
    }

    private Map<String, String> getDebeziumPropertiesAsMap() {
        return options.getOptional(SourceOptions.DEBEZIUM_PROPERTIES)
                .orElse(Collections.emptyMap());
    }

    private String buildLogicalName() {
        String keyspace = options.get(VitessSourceOptions.KEYSPACE);
        String sanitized = Pattern.compile("[^A-Za-z0-9_]").matcher(keyspace).replaceAll("_");
        return "seatunnel_vitess_" + sanitized;
    }

    @Override
    public String toString() {
        return "VitessSourceConfig{"
                + "catalogTables="
                + catalogTables.stream()
                        .map(catalogTable -> catalogTable.getTablePath().toString())
                        .collect(Collectors.toList())
                + ", startupMode="
                + startupMode
                + ", specificStartupVgtid='"
                + specificStartupVgtid
                + '\''
                + '}';
    }
}
