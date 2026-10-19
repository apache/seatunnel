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

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.pgbase.config.PgBaseSourceConfigFactory;

import io.debezium.connector.postgresql.PostgresConnector;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;

/**
 * PostgreSQL-specific source config factory built on top of the PG-base Debezium property assembly.
 */
public class PostgresSourceConfigFactory extends PgBaseSourceConfigFactory<PostgresSourceConfig> {

    private static final long serialVersionUID = 1L;

    private static final String DATABASE_SERVER_NAME = "postgres_cdc_source";

    private static final String DRIVER_CLASS_NAME = "org.postgresql.Driver";

    private static final Set<String> SSL_MODES =
            Collections.unmodifiableSet(
                    new HashSet<>(
                            Arrays.asList(
                                    "disable",
                                    "allow",
                                    "prefer",
                                    "require",
                                    "verify-ca",
                                    "verify-full")));

    private static final Set<String> PUBLICATION_AUTOCREATE_MODES =
            Collections.unmodifiableSet(
                    new HashSet<>(Arrays.asList("disabled", "filtered", "all_tables")));

    private String decodingPluginName =
            PostgresIncrementalSourceOptions.DECODING_PLUGIN_NAME.defaultValue();

    private String slotName = PostgresIncrementalSourceOptions.SLOT_NAME.defaultValue();

    private List<String> schemaList;

    private String sslmode;

    private String publicationName;

    private String publicationAutocreateMode;

    /**
     * Explicit top-level {@code heartbeat.interval.ms}. {@code null} means the user did not set it,
     * so a legacy {@code debezium.heartbeat.interval.ms} may still override the 30s default.
     */
    private Long heartbeatIntervalMs;

    @Override
    public PostgresSourceConfigFactory fromReadonlyConfig(ReadonlyConfig config) {
        super.fromReadonlyConfig(config);
        this.decodingPluginName = config.get(PostgresIncrementalSourceOptions.DECODING_PLUGIN_NAME);
        this.slotName = config.get(PostgresIncrementalSourceOptions.SLOT_NAME);
        this.schemaList = config.get(PostgresIncrementalSourceOptions.SCHEMA_NAME);
        this.sslmode =
                config.getOptional(PostgresIncrementalSourceOptions.SSL_MODE)
                        .map(PostgresSourceConfigFactory::validateSslMode)
                        .orElse(null);
        this.publicationName =
                config.getOptional(PostgresIncrementalSourceOptions.PUBLICATION_NAME)
                        .map(String::trim)
                        .filter(StringUtils::isNotEmpty)
                        .orElse(null);
        this.publicationAutocreateMode =
                config.getOptional(PostgresIncrementalSourceOptions.PUBLICATION_AUTOCREATE_MODE)
                        .map(PostgresSourceConfigFactory::validatePublicationAutocreateMode)
                        .orElse(null);
        this.heartbeatIntervalMs =
                config.getOptional(PostgresIncrementalSourceOptions.HEARTBEAT_INTERVAL_MS)
                        .map(PostgresSourceConfigFactory::validateHeartbeatIntervalMs)
                        .orElse(null);
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

    /**
     * Writes sslmode, publication, and heartbeat options after {@code debezium{}} is merged so the
     * SeaTunnel options stay authoritative.
     *
     * <p>sslmode resolution: SeaTunnel {@code sslmode} if set, else the JDBC URL {@code sslmode}
     * query parameter, else the property is left unset. Publication properties are written only
     * when the matching SeaTunnel option is set. {@code heartbeat.interval.ms} resolves as:
     * explicit top-level option, else {@code debezium.heartbeat.interval.ms}, else {@code 30000}.
     */
    @Override
    protected void applyAuthoritativeConnectorProperties(Properties props) {
        String resolvedSslMode = resolveSslMode();
        if (resolvedSslMode != null) {
            props.setProperty("database.sslmode", resolvedSslMode);
        }
        if (StringUtils.isNotEmpty(publicationName)) {
            props.setProperty("publication.name", publicationName);
        }
        if (StringUtils.isNotEmpty(publicationAutocreateMode)) {
            props.setProperty("publication.autocreate.mode", publicationAutocreateMode);
        }
        if (heartbeatIntervalMs != null) {
            props.setProperty("heartbeat.interval.ms", String.valueOf(heartbeatIntervalMs));
        } else {
            props.putIfAbsent(
                    "heartbeat.interval.ms",
                    String.valueOf(
                            PostgresIncrementalSourceOptions.HEARTBEAT_INTERVAL_MS.defaultValue()));
        }
    }

    private String resolveSslMode() {
        if (StringUtils.isNotEmpty(sslmode)) {
            return sslmode;
        }
        return sslModeFromJdbcUrl(originUrl);
    }

    /**
     * Reads {@code sslmode} from a JDBC URL query string such as {@code ?sslmode=disable}.
     *
     * @param jdbcUrl original JDBC URL, possibly including a query suffix
     * @return sslmode value, or {@code null} when the URL has no sslmode parameter
     */
    static String sslModeFromJdbcUrl(String jdbcUrl) {
        if (jdbcUrl == null) {
            return null;
        }
        int queryStart = jdbcUrl.indexOf('?');
        if (queryStart < 0 || queryStart + 1 >= jdbcUrl.length()) {
            return null;
        }
        String query = jdbcUrl.substring(queryStart + 1);
        int fragmentStart = query.indexOf('#');
        if (fragmentStart >= 0) {
            query = query.substring(0, fragmentStart);
        }
        for (String pair : query.split("&")) {
            int eq = pair.indexOf('=');
            if (eq <= 0) {
                continue;
            }
            if ("sslmode".equalsIgnoreCase(pair.substring(0, eq).trim())) {
                String value = pair.substring(eq + 1).trim();
                return value.isEmpty() ? null : value;
            }
        }
        return null;
    }

    private static String validateSslMode(String sslMode) {
        String normalized = sslMode == null ? null : sslMode.trim().toLowerCase(Locale.ROOT);
        if (StringUtils.isEmpty(normalized)) {
            return null;
        }
        if (!SSL_MODES.contains(normalized)) {
            throw new IllegalArgumentException(
                    "Invalid sslmode '"
                            + sslMode
                            + "'. Valid values: disable, allow, prefer, require, verify-ca, verify-full.");
        }
        return normalized;
    }

    private static Long validateHeartbeatIntervalMs(Long intervalMs) {
        if (intervalMs == null) {
            return null;
        }
        if (intervalMs < 0) {
            throw new IllegalArgumentException(
                    "heartbeat.interval.ms must be >= 0, but was " + intervalMs);
        }
        return intervalMs;
    }

    private static String validatePublicationAutocreateMode(String mode) {
        String normalized = mode == null ? null : mode.trim().toLowerCase(Locale.ROOT);
        if (StringUtils.isEmpty(normalized)) {
            return null;
        }
        if (!PUBLICATION_AUTOCREATE_MODES.contains(normalized)) {
            throw new IllegalArgumentException(
                    "Invalid publication.autocreate.mode '"
                            + mode
                            + "'. Valid values: disabled, filtered, all_tables.");
        }
        return normalized;
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
