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

package org.apache.seatunnel.connectors.seatunnel.databend.sink;

import org.apache.seatunnel.api.sink.SinkAggregatedCommitter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.connectors.seatunnel.databend.config.DatabendSinkConfig;
import org.apache.seatunnel.connectors.seatunnel.databend.exception.DatabendConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.databend.exception.DatabendConnectorException;
import org.apache.seatunnel.connectors.seatunnel.databend.util.DatabendUtil;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Aggregated committer for Databend sink that handles CDC (Change Data Capture) operations. In CDC
 * mode, this committer performs merge operations to apply changes to the target table. Merge
 * operations are only performed when the accumulated record count reaches the configured batch
 * size, which helps optimize performance by reducing the frequency of merge operations.
 */
@Slf4j
public class DatabendSinkAggregatedCommitter
        implements SinkAggregatedCommitter<
                DatabendSinkCommitterInfo, DatabendSinkAggregatedCommitInfo> {

    // Add a unique identifier for each instance
    private static final AtomicLong INSTANCE_COUNTER = new AtomicLong(0);
    private final long instanceId = INSTANCE_COUNTER.getAndIncrement();

    private final DatabendSinkConfig databendSinkConfig;
    private final String database;
    private final String table;
    private final String rawTableName;
    private final String streamName;

    private Connection connection;
    private boolean isCdcMode;
    // Store catalog table to access schema information
    private CatalogTable catalogTable;

    // Add a setter for catalogTable
    public void setCatalogTable(CatalogTable catalogTable) {
        this.catalogTable = catalogTable;
    }

    public DatabendSinkAggregatedCommitter(
            DatabendSinkConfig databendSinkConfig,
            String database,
            String table,
            String rawTableName,
            String streamName) {
        this.databendSinkConfig = databendSinkConfig;
        this.database = database;
        this.table = table;
        this.rawTableName = rawTableName;
        this.streamName = streamName;
        this.isCdcMode = databendSinkConfig.isCdcMode();
    }

    @Override
    public void init() {
        try {
            log.info("[Instance {}] Initializing DatabendSinkAggregatedCommitter", instanceId);
            log.info("[Instance {}] DatabendSinkConfig: {}", instanceId, databendSinkConfig);
            log.info("[Instance {}] Database: {}", instanceId, database);
            log.info("[Instance {}] Table: {}", instanceId, table);
            log.info("[Instance {}] Is CDC mode: {}", instanceId, isCdcMode);

            this.connection = DatabendUtil.createConnection(databendSinkConfig);
            log.info(
                    "[Instance {}] Databend connection created successfully: {}",
                    instanceId,
                    connection);

            // CDC infrastructure is now initialized in DatabendSink.setJobContext
            // Just log that we're in CDC mode
            if (isCdcMode) {
                log.info("[Instance {}] Running in CDC mode", instanceId);
            }
        } catch (SQLException e) {
            log.error(
                    "[Instance {}] Failed to initialize DatabendSinkAggregatedCommitter: {}",
                    instanceId,
                    e.getMessage(),
                    e);
            throw new DatabendConnectorException(
                    DatabendConnectorErrorCode.CONNECT_FAILED,
                    "Failed to initialize DatabendSinkAggregatedCommitter: " + e.getMessage(),
                    e);
        } catch (Exception e) {
            log.error(
                    "[Instance {}] Unexpected error during initialization: {}",
                    instanceId,
                    e.getMessage(),
                    e);
            throw e;
        }
    }

    private String getCurrentTimestamp() {
        return LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMddHHmmssSSS"));
    }

    @Override
    public List<DatabendSinkAggregatedCommitInfo> commit(
            List<DatabendSinkAggregatedCommitInfo> aggregatedCommitInfos) throws IOException {
        // Perform final merge operation in CDC mode only when necessary
        if (isCdcMode) {
            performMerge(aggregatedCommitInfos);
        }

        // Return empty list as there's no need to retry
        return new ArrayList<>();
    }

    private void performMerge(List<DatabendSinkAggregatedCommitInfo> aggregatedCommitInfos) {
        // Merge all the data from raw table to target table
        String mergeSql = generateMergeSql();
        log.info("[Instance {}] Executing MERGE INTO statement: {}", instanceId, mergeSql);

        try (Statement stmt = connection.createStatement()) {
            stmt.execute(mergeSql);
            log.info("[Instance {}] Merge operation completed successfully", instanceId);
        } catch (SQLException e) {
            log.error(
                    "[Instance {}] Failed to execute merge operation: {}",
                    instanceId,
                    e.getMessage(),
                    e);
            throw new DatabendConnectorException(
                    DatabendConnectorErrorCode.SQL_OPERATION_FAILED,
                    "Failed to execute merge operation: " + e.getMessage(),
                    e);
        }
    }

    private String generateMergeSql() {
        StringBuilder sql = new StringBuilder();
        sql.append(String.format("MERGE INTO %s.%s a ", database, table));
        sql.append("USING (SELECT ");

        // Add all columns from raw_data
        if (catalogTable != null && catalogTable.getSeaTunnelRowType() != null) {
            String[] fieldNames = catalogTable.getSeaTunnelRowType().getFieldNames();
            for (int i = 0; i < fieldNames.length; i++) {
                if (i > 0) {
                    sql.append(", ");
                }
                sql.append(String.format("raw_data:%s as %s", fieldNames[i], fieldNames[i]));
            }
        } else {
            // Fallback to generic raw_data if schema is not available
            sql.append("raw_data");
        }

        sql.append(", action FROM ")
                .append(database)
                .append(".")
                // In the new approach, we don't have streamName in this class
                // The stream name should be passed from DatabendSink or retrieved differently
                .append(streamName) // Placeholder, will be replaced properly
                .append(" QUALIFY ROW_NUMBER() OVER(PARTITION BY ")
                .append(databendSinkConfig.getConflictKey())
                .append(" ORDER BY add_time DESC) = 1) b ");

        sql.append("ON a.")
                .append(databendSinkConfig.getConflictKey())
                .append(" = b.")
                .append(databendSinkConfig.getConflictKey())
                .append(" ");

        sql.append("WHEN MATCHED AND b.action = 'update' THEN UPDATE * ");

        if (databendSinkConfig.isEnableDelete()) {
            sql.append("WHEN MATCHED AND b.action = 'delete' THEN DELETE ");
        }

        sql.append("WHEN NOT MATCHED AND b.action!='delete' THEN INSERT *");

        return sql.toString();
    }

    @Override
    public DatabendSinkAggregatedCommitInfo combine(List<DatabendSinkCommitterInfo> commitInfos) {
        // Just combine all commit infos into one aggregated commit info
        // In the new approach, rawTableName and streamName are not needed here
        return new DatabendSinkAggregatedCommitInfo(commitInfos, null, null);
    }

    @Override
    public void abort(List<DatabendSinkAggregatedCommitInfo> aggregatedCommitInfos)
            throws IOException {
        // In case of abort, we might want to clean up the raw table and stream
        log.info("[Instance {}] Aborting Databend sink operations", instanceId);
        try {
            if (isCdcMode && connection != null && !connection.isClosed()) {
                // In the new approach, raw table and stream names are not stored in this class
                // Cleanup would need to be handled differently or at the DatabendSink level
                log.info(
                        "[Instance {}] CDC mode abort - cleanup handled at DatabendSink level",
                        instanceId);
            }
        } catch (Exception e) {
            log.warn(
                    "[Instance {}] Failed to clean up during abort: {}",
                    instanceId,
                    e.getMessage(),
                    e);
        }
    }

    @Override
    public void close() throws IOException {
        try {
            if (connection != null && !connection.isClosed()) {
                connection.close();
            }
        } catch (SQLException e) {
            throw new DatabendConnectorException(
                    DatabendConnectorErrorCode.CONNECT_FAILED,
                    "[Instance {}] Failed to close connection in DatabendSinkAggregatedCommitter: "
                            + e.getMessage(),
                    e);
        }
    }
}
