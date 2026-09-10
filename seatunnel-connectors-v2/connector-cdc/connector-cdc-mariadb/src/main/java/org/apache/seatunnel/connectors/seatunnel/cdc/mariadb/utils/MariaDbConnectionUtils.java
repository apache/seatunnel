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

package org.apache.seatunnel.connectors.seatunnel.cdc.mariadb.utils;

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.seatunnel.cdc.mariadb.config.CustomMariaDbConnectionConfiguration;
import org.apache.seatunnel.connectors.seatunnel.cdc.mariadb.offset.MariaDbBinlogOffset;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.EventData;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.RotateEventData;
import io.debezium.config.Configuration;
import io.debezium.connector.mysql.MySqlConnection;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.MySqlDatabaseSchema;
import io.debezium.connector.mysql.MySqlSystemVariables;
import io.debezium.connector.mysql.MySqlTopicSelector;
import io.debezium.connector.mysql.MySqlValueConverters;
import io.debezium.jdbc.JdbcConnection;
import io.debezium.jdbc.JdbcValueConverters;
import io.debezium.jdbc.TemporalPrecisionMode;
import io.debezium.relational.TableId;
import io.debezium.schema.TopicSelector;
import io.debezium.util.SchemaNameAdjuster;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/** MariaDB connection Utilities. */
public class MariaDbConnectionUtils {

    public static final String SHOW_MASTER_STATUS = "SHOW MASTER STATUS";
    public static final String SHOW_BINARY_LOGS = "SHOW BINARY LOGS";

    /** Creates a new {@link MySqlConnection} configured for MariaDB, but does not open it. */
    public static MySqlConnection createMariaDbConnection(Configuration dbzConfiguration) {
        return new MySqlConnection(new CustomMariaDbConnectionConfiguration(dbzConfiguration));
    }

    /** Alias for {@link #createMariaDbConnection(Configuration)}. */
    public static MySqlConnection createMySqlConnection(Configuration dbzConfiguration) {
        return createMariaDbConnection(dbzConfiguration);
    }

    /** Creates a new {@link BinaryLogClient} for consuming MariaDB binlog. */
    public static BinaryLogClient createBinaryClient(Configuration dbzConfiguration) {
        final MySqlConnectorConfig connectorConfig = new MySqlConnectorConfig(dbzConfiguration);
        return new BinaryLogClient(
                connectorConfig.hostname(),
                connectorConfig.port(),
                connectorConfig.username(),
                connectorConfig.password());
    }

    /** Creates a new {@link MySqlDatabaseSchema} to monitor the latest MariaDB database schemas. */
    public static MySqlDatabaseSchema createMariaDbDatabaseSchema(
            MySqlConnectorConfig dbzMySqlConfig, boolean isTableIdCaseSensitive) {
        TopicSelector<TableId> topicSelector = MySqlTopicSelector.defaultSelector(dbzMySqlConfig);
        SchemaNameAdjuster schemaNameAdjuster = SchemaNameAdjuster.create();
        MySqlValueConverters valueConverters = getValueConverters(dbzMySqlConfig);
        return new MySqlDatabaseSchema(
                dbzMySqlConfig,
                valueConverters,
                topicSelector,
                schemaNameAdjuster,
                isTableIdCaseSensitive);
    }

    /** Fetch earliest binlog offsets in MariaDB Server. */
    public static MariaDbBinlogOffset earliestBinlogOffset(JdbcConnection jdbc) {
        JdbcConnection.ResultSetMapper<MariaDbBinlogOffset> getCurrentBinlogOffset =
                rs -> {
                    final String binlogFilename = rs.getString(1);
                    final long binlogPosition = 4L;
                    return new MariaDbBinlogOffset(
                            binlogFilename, binlogPosition, 0L, 0, 0, null, null);
                };
        return getBinlogOffset(jdbc, SHOW_BINARY_LOGS, getCurrentBinlogOffset);
    }

    /** Fetch current binlog offsets in MariaDB Server. */
    public static MariaDbBinlogOffset currentBinlogOffset(JdbcConnection jdbc) {
        JdbcConnection.ResultSetMapper<MariaDbBinlogOffset> getCurrentBinlogOffset =
                rs -> {
                    final String binlogFilename = rs.getString(1);
                    final long binlogPosition = rs.getLong(2);
                    String gtidSet = rs.getMetaData().getColumnCount() > 4 ? rs.getString(5) : null;
                    if (StringUtils.isBlank(gtidSet)) {
                        try {
                            gtidSet =
                                    jdbc.queryAndMap(
                                            "SELECT @@GLOBAL.gtid_binlog_pos",
                                            r -> r.next() ? r.getString(1) : null);
                        } catch (Exception ignored) {
                            // ignore if not supported
                        }
                    }
                    return new MariaDbBinlogOffset(
                            binlogFilename, binlogPosition, 0L, 0, 0, gtidSet, null);
                };
        return getBinlogOffset(jdbc, SHOW_MASTER_STATUS, getCurrentBinlogOffset);
    }

    private static MariaDbBinlogOffset getBinlogOffset(
            JdbcConnection jdbc,
            String showMasterStmt,
            JdbcConnection.ResultSetMapper<MariaDbBinlogOffset> function) {
        try {
            return jdbc.queryAndMap(
                    showMasterStmt,
                    rs -> {
                        if (rs.next()) {
                            return function.apply(rs);
                        } else {
                            throw new SeaTunnelException(
                                    "Cannot read the binlog filename and position via '"
                                            + showMasterStmt
                                            + "'. Make sure your server is correctly configured");
                        }
                    });
        } catch (SQLException e) {
            throw new SeaTunnelException(
                    "Cannot read the binlog filename and position via '"
                            + showMasterStmt
                            + "'. Make sure your server is correctly configured",
                    e);
        }
    }

    private static MySqlValueConverters getValueConverters(MySqlConnectorConfig dbzMySqlConfig) {
        TemporalPrecisionMode timePrecisionMode = dbzMySqlConfig.getTemporalPrecisionMode();
        JdbcValueConverters.DecimalMode decimalMode = dbzMySqlConfig.getDecimalMode();
        String bigIntUnsignedHandlingModeStr =
                dbzMySqlConfig
                        .getConfig()
                        .getString(MySqlConnectorConfig.BIGINT_UNSIGNED_HANDLING_MODE);
        MySqlConnectorConfig.BigIntUnsignedHandlingMode bigIntUnsignedHandlingMode =
                MySqlConnectorConfig.BigIntUnsignedHandlingMode.parse(
                        bigIntUnsignedHandlingModeStr);
        JdbcValueConverters.BigIntUnsignedMode bigIntUnsignedMode =
                bigIntUnsignedHandlingMode.asBigIntUnsignedMode();

        boolean timeAdjusterEnabled =
                dbzMySqlConfig.getConfig().getBoolean(MySqlConnectorConfig.ENABLE_TIME_ADJUSTER);
        return new MySqlValueConverters(
                decimalMode,
                timePrecisionMode,
                bigIntUnsignedMode,
                dbzMySqlConfig.binaryHandlingMode(),
                timeAdjusterEnabled ? MySqlValueConverters::adjustTemporal : x -> x,
                MySqlValueConverters::defaultParsingErrorHandler);
    }

    public static boolean isTableIdCaseSensitive(JdbcConnection connection) {
        return !"0"
                .equals(
                        readMySqlSystemVariables(connection)
                                .get(MySqlSystemVariables.LOWER_CASE_TABLE_NAMES));
    }

    public static Map<String, String> readMySqlSystemVariables(JdbcConnection connection) {
        return querySystemVariables(connection, "SHOW VARIABLES");
    }

    private static Map<String, String> querySystemVariables(
            JdbcConnection connection, String statement) {
        final Map<String, String> variables = new HashMap<>();
        try {
            connection.query(
                    statement,
                    rs -> {
                        while (rs.next()) {
                            String varName = rs.getString(1);
                            String value = rs.getString(2);
                            if (varName != null && value != null) {
                                variables.put(varName, value);
                            }
                        }
                    });
        } catch (SQLException e) {
            throw new SeaTunnelException("Error reading MariaDB variables: " + e.getMessage(), e);
        }

        return variables;
    }

    public static MariaDbBinlogOffset findBinlogOffsetBytimestamp(
            JdbcConnection jdbc, BinaryLogClient client, long timestamp) {
        List<String> binlogFiles = new ArrayList<>();
        JdbcConnection.ResultSetConsumer rsc =
                rs -> {
                    while (rs.next()) {
                        String fileName = rs.getString(1);
                        long fileSize = rs.getLong(2);
                        if (fileSize > 0) {
                            binlogFiles.add(fileName);
                        }
                    }
                };
        try {
            jdbc.query(SHOW_BINARY_LOGS, rsc);
            if (binlogFiles.isEmpty()) {
                return MariaDbBinlogOffset.INITIAL_OFFSET;
            }
            String binlogName = searchBinlogName(client, timestamp, binlogFiles);
            return new MariaDbBinlogOffset(binlogName, 0);
        } catch (Exception e) {
            throw new SeaTunnelException(e);
        }
    }

    private static String searchBinlogName(
            BinaryLogClient client, long targetMs, List<String> binlogFiles)
            throws IOException, InterruptedException {
        int startIdx = 0;
        int endIdx = binlogFiles.size() - 1;

        while (startIdx <= endIdx) {
            int mid = startIdx + (endIdx - startIdx) / 2;
            long midTs = getBinlogTimestamp(client, binlogFiles.get(mid));
            if (midTs < targetMs) {
                startIdx = mid + 1;
            } else if (targetMs < midTs) {
                endIdx = mid - 1;
            } else {
                return binlogFiles.get(mid);
            }
        }

        return endIdx < 0 ? binlogFiles.get(0) : binlogFiles.get(endIdx);
    }

    public static long getBinlogTimestamp(BinaryLogClient client, String binlogFile)
            throws IOException {
        AtomicLong binlogTimestamps = new AtomicLong();
        BinaryLogClient.EventListener eventListener =
                event -> {
                    EventData data = event.getData();
                    if (data instanceof RotateEventData) {
                        return;
                    }

                    EventHeaderV4 header = event.getHeader();
                    long timestamp = header.getTimestamp();
                    if (timestamp > 0 && binlogTimestamps.get() == 0) {
                        binlogTimestamps.set(timestamp);
                        try {
                            client.disconnect();
                        } catch (IOException e) {
                            throw new RuntimeException(e);
                        }
                    }
                };

        try {
            client.registerEventListener(eventListener);
            client.setBinlogFilename(binlogFile);
            client.setBinlogPosition(0);
            client.connect();
        } finally {
            client.unregisterEventListener(eventListener);
        }
        return binlogTimestamps.get();
    }
}
