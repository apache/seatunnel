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
     * Writes sslmode and publication options after {@code debezium{}} is merged so the SeaTunnel
     * options stay authoritative.
     *
     * <p>{@code database.sslmode} is written only when the SeaTunnel {@code sslmode} option is
     * explicitly set; otherwise it is left untouched so {@code debezium.database.sslmode} or the
     * Debezium default {@code disable} applies. The JDBC URL {@code sslmode} is intentionally not
     * inherited, keeping existing jobs on their previous SSL behavior. Publication properties are
     * written only when the matching SeaTunnel option is set.
     */
    @Override
    protected void applyAuthoritativeConnectorProperties(Properties props) {
        if (StringUtils.isNotEmpty(sslmode)) {
            props.setProperty("database.sslmode", sslmode);
        }
        if (StringUtils.isNotEmpty(publicationName)) {
            props.setProperty("publication.name", publicationName);
        }
        if (StringUtils.isNotEmpty(publicationAutocreateMode)) {
            props.setProperty("publication.autocreate.mode", publicationAutocreateMode);
        }
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
