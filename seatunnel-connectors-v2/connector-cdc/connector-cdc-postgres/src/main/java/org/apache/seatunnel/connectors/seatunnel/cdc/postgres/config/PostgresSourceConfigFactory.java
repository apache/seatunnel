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
import org.apache.seatunnel.connectors.seatunnel.cdc.pgbase.config.PgBaseSourceConfigFactory;

import io.debezium.connector.postgresql.PostgresConnector;

import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

import static org.apache.seatunnel.shade.com.google.common.base.Preconditions.checkArgument;

/**
 * PostgreSQL-specific source config factory built on top of the PG-base Debezium property assembly.
 */
public class PostgresSourceConfigFactory extends PgBaseSourceConfigFactory<PostgresSourceConfig> {

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
    public PostgresSourceConfig create(int subtask) {
        PostgresSourceConfig config = super.create(subtask);
        String effectiveSlotName = config.getDbzConfiguration().getString("slot.name");
        validateSlotName(effectiveSlotName);
        config.setSubtaskId(subtask);
        return config;
    }

    @Override
    public PostgresSourceConfigFactory fromReadonlyConfig(ReadonlyConfig config) {
        super.fromReadonlyConfig(config);
        this.decodingPluginName = config.get(PostgresIncrementalSourceOptions.DECODING_PLUGIN_NAME);
        this.slotName = config.get(PostgresIncrementalSourceOptions.SLOT_NAME);
        this.schemaList = config.get(PostgresIncrementalSourceOptions.SCHEMA_NAME);
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

    private static void validateSlotName(String slotName) {
        checkArgument(
                SLOT_NAME_PATTERN.matcher(slotName).matches(),
                "PostgreSQL slot.name '%s' is invalid: it must match %s (PostgreSQL replication"
                        + " slot identifier rules), since it is also used to derive the per-reader"
                        + " snapshot backfill slot name.",
                slotName,
                SLOT_NAME_PATTERN.pattern());
    }
}
