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

package org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source;

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.SupportParallelism;
import org.apache.seatunnel.api.source.SupportSchemaEvolution;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.schema.SchemaChangeType;
import org.apache.seatunnel.common.utils.JdbcUrlUtil;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.cdc.base.config.JdbcSourceConfig;
import org.apache.seatunnel.connectors.cdc.base.config.SourceConfig;
import org.apache.seatunnel.connectors.cdc.base.config.StartupConfig;
import org.apache.seatunnel.connectors.cdc.base.dialect.DataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.option.JdbcSourceOptions;
import org.apache.seatunnel.connectors.cdc.base.option.SourceOptions;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.cdc.base.option.StopMode;
import org.apache.seatunnel.connectors.cdc.base.schema.SchemaChangeEventFilter;
import org.apache.seatunnel.connectors.cdc.base.source.IncrementalSource;
import org.apache.seatunnel.connectors.cdc.base.source.offset.OffsetFactory;
import org.apache.seatunnel.connectors.cdc.debezium.ConnectTableChangeSerializer;
import org.apache.seatunnel.connectors.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.seatunnel.connectors.cdc.debezium.DeserializeFormat;
import org.apache.seatunnel.connectors.cdc.debezium.row.DebeziumJsonDeserializeSchema;
import org.apache.seatunnel.connectors.cdc.debezium.row.SeaTunnelRowDebeziumDeserializeSchema;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.config.MySqlIncrementalSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.config.MySqlSourceConfigFactory;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source.offset.BinlogOffset;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source.offset.BinlogOffsetFactory;
import org.apache.seatunnel.connectors.seatunnel.jdbc.config.JdbcCommonOptions;

import org.apache.kafka.connect.data.Struct;

import io.debezium.connector.mysql.GtidSet;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;

import java.time.ZoneId;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;
import java.util.stream.Collectors;

public class MySqlIncrementalSource<T> extends IncrementalSource<T, JdbcSourceConfig>
        implements SupportParallelism, SupportSchemaEvolution {
    static final String IDENTIFIER = "MySQL-CDC";

    public MySqlIncrementalSource(ReadonlyConfig options, List<CatalogTable> catalogTables) {
        super(options, catalogTables);
    }

    @Override
    protected StartupConfig getStartupConfig(ReadonlyConfig config) {
        return createStartupConfig(config);
    }

    /* Route MySQL specific startup through the map-based offset path for GTID and skip metadata. */
    static StartupConfig createStartupConfig(ReadonlyConfig config) {
        StartupMode startupMode = config.get(MySqlIncrementalSourceOptions.STARTUP_MODE);
        if (StartupMode.SPECIFIC.equals(startupMode) || StartupMode.MIXED.equals(startupMode)) {
            return new StartupConfig(startupMode, createSpecificStartupOffset(config));
        }

        validateNoSpecificStartupOffset(config, startupMode);
        return new StartupConfig(
                startupMode,
                config.get(SourceOptions.STARTUP_SPECIFIC_OFFSET_FILE),
                config.get(SourceOptions.STARTUP_SPECIFIC_OFFSET_POS),
                config.get(SourceOptions.STARTUP_TIMESTAMP));
    }

    /*
     * Debezium's MySqlOffsetContext.Loader requires file and pos for a specific startup offset.
     * GTID and skip fields are optional metadata on that same offset, not an alternative anchor.
     */
    private static Map<String, String> createSpecificStartupOffset(ReadonlyConfig config) {
        Optional<String> gtidSet = getValidatedGtidSet(config);
        boolean hasFile =
                config.getOptional(SourceOptions.STARTUP_SPECIFIC_OFFSET_FILE).isPresent();
        boolean hasPos = config.getOptional(SourceOptions.STARTUP_SPECIFIC_OFFSET_POS).isPresent();

        if (hasFile != hasPos) {
            throw new IllegalArgumentException(
                    String.format(
                            "'%s' and '%s' must be configured together when '%s' is 'specific'.",
                            SourceOptions.STARTUP_SPECIFIC_OFFSET_FILE.key(),
                            SourceOptions.STARTUP_SPECIFIC_OFFSET_POS.key(),
                            SourceOptions.STARTUP_MODE_KEY));
        }

        if (!hasFile) {
            throw new IllegalArgumentException(
                    String.format(
                            "'%s' requires '%s' with '%s' when the mode is 'specific'.",
                            SourceOptions.STARTUP_MODE_KEY,
                            SourceOptions.STARTUP_SPECIFIC_OFFSET_FILE.key(),
                            SourceOptions.STARTUP_SPECIFIC_OFFSET_POS.key()));
        }

        long skipEvents =
                getNonNegativeSkipValue(
                        config, MySqlIncrementalSourceOptions.STARTUP_SPECIFIC_OFFSET_SKIP_EVENTS);
        long skipRows =
                getNonNegativeSkipValue(
                        config, MySqlIncrementalSourceOptions.STARTUP_SPECIFIC_OFFSET_SKIP_ROWS);

        Map<String, String> offset = new LinkedHashMap<>();
        String file = config.get(SourceOptions.STARTUP_SPECIFIC_OFFSET_FILE);
        if (StringUtils.isBlank(file)) {
            throw new IllegalArgumentException(
                    String.format(
                            "'%s' must not be blank.",
                            SourceOptions.STARTUP_SPECIFIC_OFFSET_FILE.key()));
        }
        offset.put(BinlogOffset.BINLOG_FILENAME_OFFSET_KEY, file);
        offset.put(
                BinlogOffset.BINLOG_POSITION_OFFSET_KEY,
                String.valueOf(config.get(SourceOptions.STARTUP_SPECIFIC_OFFSET_POS)));
        if (gtidSet.isPresent()) {
            offset.put(BinlogOffset.GTID_SET_KEY, gtidSet.get());
        }
        offset.put(BinlogOffset.EVENTS_TO_SKIP_OFFSET_KEY, String.valueOf(skipEvents));
        offset.put(BinlogOffset.ROWS_TO_SKIP_OFFSET_KEY, String.valueOf(skipRows));
        return offset;
    }

    /* Validate the configured GTID set before adding it to Debezium's offset map. */
    private static Optional<String> getValidatedGtidSet(ReadonlyConfig config) {
        Optional<String> configuredGtidSet =
                config.getOptional(MySqlIncrementalSourceOptions.STARTUP_SPECIFIC_OFFSET_GTID_SET);
        if (!configuredGtidSet.isPresent()) {
            return Optional.empty();
        }

        String gtidSet = configuredGtidSet.get().trim();
        if (StringUtils.isBlank(gtidSet)) {
            throw new IllegalArgumentException(
                    String.format(
                            "'%s' must not be blank.",
                            MySqlIncrementalSourceOptions.STARTUP_SPECIFIC_OFFSET_GTID_SET.key()));
        }

        try {
            new GtidSet(gtidSet);
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    String.format(
                            "Invalid '%s' value '%s'.",
                            MySqlIncrementalSourceOptions.STARTUP_SPECIFIC_OFFSET_GTID_SET.key(),
                            gtidSet),
                    e);
        }
        return Optional.of(gtidSet);
    }

    private static long getNonNegativeSkipValue(ReadonlyConfig config, Option<Long> option) {
        long value = config.getOptional(option).orElse(0L);
        if (value < 0) {
            throw new IllegalArgumentException(
                    String.format("'%s' must be greater than or equal to 0.", option.key()));
        }
        return value;
    }

    private static void validateNoSpecificStartupOffset(
            ReadonlyConfig config, StartupMode startupMode) {
        if (hasSpecificStartupOffset(config)) {
            throw new IllegalArgumentException(
                    String.format(
                            "'startup.specific-offset.*' options can only be used when '%s' is 'specific' or 'mixed', but current mode is '%s'.",
                            SourceOptions.STARTUP_MODE_KEY, startupMode));
        }
    }

    private static boolean hasSpecificStartupOffset(ReadonlyConfig config) {
        return config.getOptional(SourceOptions.STARTUP_SPECIFIC_OFFSET_FILE).isPresent()
                || config.getOptional(SourceOptions.STARTUP_SPECIFIC_OFFSET_POS).isPresent()
                || config.getOptional(
                                MySqlIncrementalSourceOptions.STARTUP_SPECIFIC_OFFSET_GTID_SET)
                        .isPresent()
                || config.getOptional(
                                MySqlIncrementalSourceOptions.STARTUP_SPECIFIC_OFFSET_SKIP_EVENTS)
                        .isPresent()
                || config.getOptional(
                                MySqlIncrementalSourceOptions.STARTUP_SPECIFIC_OFFSET_SKIP_ROWS)
                        .isPresent();
    }

    @Override
    public Option<StartupMode> getStartupModeOption() {
        return MySqlIncrementalSourceOptions.STARTUP_MODE;
    }

    @Override
    protected Set<TableId> getMixedSnapshotTables(
            List<TableId> capturedTables, boolean isTableIdCaseSensitive) {
        return resolveMixedSnapshotTables(readonlyConfig, capturedTables, isTableIdCaseSensitive);
    }

    /** Resolves configured mixed-mode snapshot table names to the discovered table identifiers. */
    static Set<TableId> resolveMixedSnapshotTables(
            ReadonlyConfig config, List<TableId> capturedTables, boolean isTableIdCaseSensitive) {
        if (!config.getOptional(MySqlIncrementalSourceOptions.TABLE_NAMES).isPresent()
                || config.getOptional(MySqlIncrementalSourceOptions.TABLE_PATTERN).isPresent()) {
            throw new IllegalArgumentException(
                    "The mixed startup mode supports table-names only and does not support table-pattern.");
        }
        List<String> configuredTableNames =
                config.getOptional(MySqlIncrementalSourceOptions.STARTUP_SNAPSHOT_TABLE_NAMES)
                        .orElseThrow(
                                () ->
                                        new IllegalArgumentException(
                                                "The mixed startup mode requires startup.snapshot-table-names."));
        if (configuredTableNames.isEmpty()) {
            throw new IllegalArgumentException(
                    "The mixed startup mode requires at least one startup.snapshot-table-names entry.");
        }
        Set<TableId> snapshotTables = new HashSet<>();
        for (String configuredTableName : configuredTableNames) {
            if (StringUtils.isBlank(configuredTableName)) {
                throw new IllegalArgumentException(
                        "The mixed startup snapshot table name must not be blank.");
            }
            TableId configuredTableId = TableId.parse(configuredTableName);
            Optional<TableId> capturedTable =
                    capturedTables.stream()
                            .filter(
                                    tableId ->
                                            isTableIdCaseSensitive
                                                    ? tableId.equals(configuredTableId)
                                                    : tableId.compareToIgnoreCase(configuredTableId)
                                                            == 0)
                            .findFirst();
            if (!capturedTable.isPresent()) {
                throw new IllegalArgumentException(
                        String.format(
                                "The mixed startup snapshot table '%s' is not captured by this source.",
                                configuredTableName));
            }
            if (!snapshotTables.add(capturedTable.get())) {
                throw new IllegalArgumentException(
                        String.format(
                                "The mixed startup snapshot table '%s' is configured more than once.",
                                configuredTableName));
            }
        }
        return snapshotTables;
    }

    @Override
    public Option<StopMode> getStopModeOption() {
        return MySqlIncrementalSourceOptions.STOP_MODE;
    }

    @Override
    public String getPluginName() {
        return IDENTIFIER;
    }

    @Override
    public SourceConfig.Factory<JdbcSourceConfig> createSourceConfigFactory(ReadonlyConfig config) {
        MySqlSourceConfigFactory configFactory = new MySqlSourceConfigFactory();
        configFactory.serverId(config.get(JdbcSourceOptions.SERVER_ID));
        configFactory.fromReadonlyConfig(readonlyConfig);
        // Carry int_type_narrowing through the debezium properties map rather than a factory field.
        // Adding a field/method to the Serializable MySqlSourceConfigFactory drifts its
        // serialVersionUID and breaks rolling upgrades (jobs submitted on the prior version fail to
        // deserialize). This runs after fromReadonlyConfig (which resets dbzProperties from the
        // user
        // debezium block), re-merging those user props then appending int_type_narrowing.
        Properties dbzProperties = new Properties();
        config.getOptional(JdbcSourceOptions.DEBEZIUM_PROPERTIES).ifPresent(dbzProperties::putAll);
        dbzProperties.setProperty(
                "int_type_narrowing",
                String.valueOf(config.get(JdbcCommonOptions.INT_TYPE_NARROWING)));
        configFactory.debeziumProperties(dbzProperties);
        JdbcUrlUtil.UrlInfo urlInfo = JdbcUrlUtil.getUrlInfo(config.get(JdbcCommonOptions.URL));
        configFactory.originUrl(urlInfo.getOrigin());
        configFactory.hostname(urlInfo.getHost());
        configFactory.port(urlInfo.getPort());
        configFactory.startupOptions(startupConfig);
        configFactory.stopOptions(stopConfig);
        return configFactory;
    }

    @SuppressWarnings("unchecked")
    @Override
    public DebeziumDeserializationSchema<T> createDebeziumDeserializationSchema(
            ReadonlyConfig config) {
        Map<TableId, Struct> tableIdTableChangeMap = tableChanges();

        if (DeserializeFormat.COMPATIBLE_DEBEZIUM_JSON.equals(
                config.get(JdbcSourceOptions.FORMAT))) {
            return (DebeziumDeserializationSchema<T>)
                    new DebeziumJsonDeserializeSchema(
                            config.get(JdbcSourceOptions.DEBEZIUM_PROPERTIES),
                            tableIdTableChangeMap);
        }

        String zoneId = config.get(JdbcSourceOptions.SERVER_TIME_ZONE);
        return (DebeziumDeserializationSchema<T>)
                SeaTunnelRowDebeziumDeserializeSchema.builder()
                        .setTables(catalogTables)
                        .setServerTimeZone(ZoneId.of(zoneId))
                        .setTableIdTableChangeMap(tableIdTableChangeMap)
                        .setSchemaChangeResolver(
                                new MySqlSchemaChangeResolver(createSourceConfigFactory(config)))
                        .setSchemaChangeEventFilter(SchemaChangeEventFilter.fromConfig(config))
                        .build();
    }

    @Override
    public DataSourceDialect<JdbcSourceConfig> createDataSourceDialect(ReadonlyConfig config) {
        return new MySqlDialect((MySqlSourceConfigFactory) configFactory, catalogTables);
    }

    @Override
    public OffsetFactory createOffsetFactory(ReadonlyConfig config) {
        return new BinlogOffsetFactory(
                (MySqlSourceConfigFactory) configFactory, (MySqlDialect) dataSourceDialect);
    }

    private Map<TableId, Struct> tableChanges() {
        JdbcSourceConfig jdbcSourceConfig = configFactory.create(0);
        MySqlDialect mySqlDialect =
                new MySqlDialect((MySqlSourceConfigFactory) configFactory, catalogTables);
        List<TableId> discoverTables = mySqlDialect.discoverDataCollections(jdbcSourceConfig);
        ConnectTableChangeSerializer connectTableChangeSerializer =
                new ConnectTableChangeSerializer();
        try (JdbcConnection jdbcConnection = mySqlDialect.openJdbcConnection(jdbcSourceConfig)) {
            return discoverTables.stream()
                    .collect(
                            Collectors.toMap(
                                    Function.identity(),
                                    (tableId) -> {
                                        TableChanges tableChanges = new TableChanges();
                                        tableChanges.create(
                                                mySqlDialect
                                                        .queryTableSchema(jdbcConnection, tableId)
                                                        .getTable());
                                        return connectTableChangeSerializer
                                                .serialize(tableChanges)
                                                .get(0);
                                    }));
        } catch (Exception e) {
            throw new SeaTunnelException(e);
        }
    }

    @Override
    public List<SchemaChangeType> supports() {
        return Arrays.asList(
                SchemaChangeType.ADD_COLUMN,
                SchemaChangeType.DROP_COLUMN,
                SchemaChangeType.RENAME_COLUMN,
                SchemaChangeType.UPDATE_COLUMN,
                SchemaChangeType.ALTER_TABLE_COMMENT,
                SchemaChangeType.ALTER_COLUMN_COMMENT);
    }

    @Override
    public Optional<String> driverName() {
        return Optional.of("com.mysql.cj.jdbc.Driver");
    }
}
