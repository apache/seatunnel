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
import org.apache.seatunnel.connectors.cdc.base.source.IncrementalSource;
import org.apache.seatunnel.connectors.cdc.base.source.offset.OffsetFactory;
import org.apache.seatunnel.connectors.cdc.debezium.ConnectTableChangeSerializer;
import org.apache.seatunnel.connectors.cdc.debezium.DebeziumDeserializationSchema;
import org.apache.seatunnel.connectors.cdc.debezium.DeserializeFormat;
import org.apache.seatunnel.connectors.cdc.debezium.row.DebeziumJsonDeserializeSchema;
import org.apache.seatunnel.connectors.cdc.debezium.row.SeaTunnelRowDebeziumDeserializeSchema;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.config.MySqlSourceConfigFactory;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source.offset.BinlogOffsetFactory;
import org.apache.seatunnel.connectors.seatunnel.jdbc.catalog.JdbcCatalogOptions;

import org.apache.kafka.connect.data.Struct;

import io.debezium.jdbc.JdbcConnection;
import io.debezium.relational.TableId;
import io.debezium.relational.history.TableChanges;
import lombok.extern.slf4j.Slf4j;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

@Slf4j
public class MySqlIncrementalSource<T> extends IncrementalSource<T, JdbcSourceConfig>
        implements SupportParallelism, SupportSchemaEvolution {
    static final String IDENTIFIER = "MySQL-CDC";

    public MySqlIncrementalSource(ReadonlyConfig options, List<CatalogTable> catalogTables) {
        super(options, catalogTables);
    }

    @Override
    public Option<StartupMode> getStartupModeOption() {
        return MySqlSourceOptions.STARTUP_MODE;
    }

    @Override
    public Option<StopMode> getStopModeOption() {
        return MySqlSourceOptions.STOP_MODE;
    }

    @Override
    public String getPluginName() {
        return IDENTIFIER;
    }

    @Override
    protected StartupConfig getStartupConfig(ReadonlyConfig config) {
        StartupMode startupMode = config.get(getStartupModeOption());

        if (startupMode == StartupMode.TIMESTAMP) {
            long timestamp;
            String sourceDescription;

            // Prioritize using SourceOptions.STARTUP_TIMESTAMP
            if (config.getOptional(SourceOptions.STARTUP_TIMESTAMP).isPresent()) {
                timestamp = config.get(SourceOptions.STARTUP_TIMESTAMP);
                sourceDescription = "startup.timestamp";
                log.info(
                        "Using timestamp startup mode with value: {} ({})",
                        timestamp,
                        formatTimestamp(timestamp));
            }
            // If STARTUP_TIMESTAMP is not specified, try to parse start-time
            else if (config.getOptional(MySqlSourceOptions.START_TIME).isPresent()) {
                String startTimeStr = config.get(MySqlSourceOptions.START_TIME);
                sourceDescription = "start-time";
                try {
                    SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    timestamp = formatter.parse(startTimeStr).getTime();
                    log.info(
                            "Parsed start-time '{}' to timestamp: {} ({})",
                            startTimeStr,
                            timestamp,
                            formatTimestamp(timestamp));
                } catch (ParseException e) {
                    log.error("Failed to parse start-time: {}", startTimeStr, e);
                    throw new IllegalArgumentException(
                            String.format(
                                    "Invalid start-time format: %s. Expected format: yyyy-MM-dd HH:mm:ss",
                                    startTimeStr),
                            e);
                }
            } else {
                throw new IllegalArgumentException(
                        "Either startup.timestamp or start-time must be specified when startup mode is TIMESTAMP");
            }

            // Check and log if the timestamp is in the future
            checkAndLogFutureTimestamp(timestamp, sourceDescription);

            return new StartupConfig(startupMode, null, null, timestamp);
        }

        return super.getStartupConfig(config);
    }

    /** Check if the specified timestamp is in the future and log accordingly. */
    private void checkAndLogFutureTimestamp(long timestamp, String sourceParameter) {
        long currentTime = System.currentTimeMillis();
        if (timestamp > currentTime) {
            long secondsInFuture = (timestamp - currentTime) / 1000;
            if (secondsInFuture < 60) {
                log.info(
                        "Specified {} is {} seconds in the future. Data changes will be captured when this time is reached.",
                        sourceParameter,
                        secondsInFuture);
            } else if (secondsInFuture < 3600) {
                log.info(
                        "Specified {} is {} minutes in the future. Data changes will only be captured when this time is reached.",
                        sourceParameter,
                        secondsInFuture / 60);
            } else if (secondsInFuture < 86400) {
                log.info(
                        "Specified {} is {} hours in the future. Data changes will only be captured when this time is reached.",
                        sourceParameter,
                        secondsInFuture / 3600);
            } else {
                log.info(
                        "Specified {} is {} days in the future (current time: {}). Data changes will only be captured when this time is reached.",
                        sourceParameter,
                        secondsInFuture / 86400,
                        formatTimestamp(currentTime));
            }
        }
    }

    /** Format the timestamp to a readable string. */
    private String formatTimestamp(long timestamp) {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(timestamp));
    }

    @Override
    public SourceConfig.Factory<JdbcSourceConfig> createSourceConfigFactory(ReadonlyConfig config) {
        MySqlSourceConfigFactory configFactory = new MySqlSourceConfigFactory();
        configFactory.serverId(config.get(JdbcSourceOptions.SERVER_ID));
        configFactory.fromReadonlyConfig(readonlyConfig);
        JdbcUrlUtil.UrlInfo urlInfo =
                JdbcUrlUtil.getUrlInfo(config.get(JdbcCatalogOptions.BASE_URL));
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
                SchemaChangeType.UPDATE_COLUMN);
    }
}
