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

package org.apache.seatunnel.connectors.cdc.dameng.source.reader.fetch.snapshot;

import org.apache.seatunnel.connectors.cdc.base.relational.JdbcSourceEventDispatcher;
import org.apache.seatunnel.connectors.cdc.base.source.split.SnapshotSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.wartermark.WatermarkKind;
import org.apache.seatunnel.connectors.cdc.dameng.source.offset.LogMinerOffset;
import org.apache.seatunnel.connectors.cdc.dameng.utils.DamengConncetionUtils;

import org.apache.kafka.connect.errors.ConnectException;

import io.debezium.DebeziumException;
import io.debezium.connector.dameng.DamengConnection;
import io.debezium.connector.dameng.DamengConnectorConfig;
import io.debezium.connector.dameng.DamengDatabaseSchema;
import io.debezium.connector.dameng.DamengOffsetContext;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.AbstractSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.ChangeRecordEmitter;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.relational.RelationalSnapshotChangeEventSource;
import io.debezium.relational.SnapshotChangeRecordEmitter;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;
import io.debezium.util.ColumnUtils;
import io.debezium.util.Strings;
import io.debezium.util.Threads;
import lombok.extern.slf4j.Slf4j;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Duration;

@Slf4j
public class DamengSnapshotSplitReadTask extends AbstractSnapshotChangeEventSource {
    private static final Duration LOG_INTERVAL = Duration.ofMillis(10_000);
    private final DamengConnectorConfig connectorConfig;
    private final DamengOffsetContext offsetContext;
    private final SnapshotProgressListener snapshotProgressListener;
    private final DamengDatabaseSchema databaseSchema;
    private final DamengConnection jdbcConnection;
    private final JdbcSourceEventDispatcher eventDispatcher;
    private final SnapshotSplit snapshotSplit;
    private final Clock clock;

    public DamengSnapshotSplitReadTask(
            DamengConnectorConfig connectorConfig,
            DamengOffsetContext previousOffset,
            SnapshotProgressListener snapshotProgressListener,
            DamengDatabaseSchema databaseSchema,
            DamengConnection jdbcConnection,
            JdbcSourceEventDispatcher eventDispatcher,
            SnapshotSplit snapshotSplit) {
        super(connectorConfig, snapshotProgressListener);
        this.connectorConfig = connectorConfig;
        this.offsetContext = previousOffset;
        this.snapshotProgressListener = snapshotProgressListener;
        this.databaseSchema = databaseSchema;
        this.jdbcConnection = jdbcConnection;
        this.eventDispatcher = eventDispatcher;
        this.snapshotSplit = snapshotSplit;
        this.clock = Clock.SYSTEM;
    }

    @Override
    public SnapshotResult execute(
            ChangeEventSource.ChangeEventSourceContext context, OffsetContext previousOffset)
            throws InterruptedException {
        SnapshottingTask snapshottingTask = getSnapshottingTask(previousOffset);
        final SnapshotContext ctx;
        try {
            ctx = prepare(context);
        } catch (Exception e) {
            log.error("Failed to initialize snapshot context.", e);
            throw new RuntimeException(e);
        }
        try {
            return doExecute(context, previousOffset, ctx, snapshottingTask);
        } catch (InterruptedException e) {
            log.warn("Snapshot was interrupted before completion");
            throw e;
        } catch (Exception t) {
            throw new DebeziumException(t);
        }
    }

    @Override
    protected SnapshotResult doExecute(
            ChangeEventSourceContext context,
            OffsetContext previousOffset,
            SnapshotContext snapshotContext,
            SnapshottingTask snapshottingTask)
            throws Exception {
        RelationalSnapshotChangeEventSource.RelationalSnapshotContext ctx =
                (RelationalSnapshotChangeEventSource.RelationalSnapshotContext) snapshotContext;
        ctx.offset = offsetContext;

        LogMinerOffset lowWatermark = new LogMinerOffset(jdbcConnection.currentCheckpointLsn());
        log.info(
                "Snapshot step 1 - Determining low watermark {} for split {}",
                lowWatermark,
                snapshotSplit);
        ((DamengSnapshotSplitChangeEventSourceContext) context).setLowWatermark(lowWatermark);
        eventDispatcher.dispatchWatermarkEvent(
                offsetContext.getPartition(), snapshotSplit, lowWatermark, WatermarkKind.LOW);

        log.info("Snapshot step 2 - Snapshotting data");
        createDataEvents(ctx, snapshotSplit.getTableId());

        LogMinerOffset highWatermark = new LogMinerOffset(jdbcConnection.currentCheckpointLsn());
        log.info(
                "Snapshot step 3 - Determining high watermark {} for split {}",
                highWatermark,
                snapshotSplit);
        ((DamengSnapshotSplitChangeEventSourceContext) context).setHighWatermark(highWatermark);
        eventDispatcher.dispatchWatermarkEvent(
                offsetContext.getPartition(), snapshotSplit, highWatermark, WatermarkKind.HIGH);
        return SnapshotResult.completed(ctx.offset);
    }

    @Override
    protected SnapshottingTask getSnapshottingTask(OffsetContext previousOffset) {
        return new SnapshottingTask(false, true);
    }

    @Override
    protected SnapshotContext prepare(ChangeEventSourceContext changeEventSourceContext)
            throws Exception {
        return new DamengSnapshotContext();
    }

    private void createDataEvents(
            RelationalSnapshotChangeEventSource.RelationalSnapshotContext snapshotContext,
            TableId tableId)
            throws Exception {
        EventDispatcher.SnapshotReceiver snapshotReceiver =
                eventDispatcher.getSnapshotChangeEventReceiver();
        log.debug("Snapshotting table {}", tableId);
        createDataEventsForTable(
                snapshotContext, snapshotReceiver, databaseSchema.tableFor(tableId));
        snapshotReceiver.completeSnapshot();
    }

    private void createDataEventsForTable(
            RelationalSnapshotChangeEventSource.RelationalSnapshotContext snapshotContext,
            EventDispatcher.SnapshotReceiver snapshotReceiver,
            Table table)
            throws InterruptedException {
        long exportStart = clock.currentTimeInMillis();
        log.info("Exporting data from split '{}' of table {}", snapshotSplit.splitId(), table.id());

        String selectSql =
                DamengConncetionUtils.buildSplitQuery(
                        snapshotSplit.getTableId(),
                        snapshotSplit.getSplitKeyType(),
                        snapshotSplit.getSplitStart() == null,
                        snapshotSplit.getSplitEnd() == null);
        log.info(
                "For split '{}' of table {} using select statement: '{}'",
                snapshotSplit.splitId(),
                table.id(),
                selectSql);

        try (PreparedStatement selectStatement =
                        DamengConncetionUtils.createTableSplitDataStatement(
                                jdbcConnection,
                                selectSql,
                                snapshotSplit.getSplitStart() == null,
                                snapshotSplit.getSplitEnd() == null,
                                new Object[] {snapshotSplit.getSplitStart()},
                                new Object[] {snapshotSplit.getSplitEnd()},
                                snapshotSplit.getSplitKeyType().getTotalFields(),
                                connectorConfig.getQueryFetchSize());
                ResultSet rs = selectStatement.executeQuery()) {

            ColumnUtils.ColumnArray columnArray = ColumnUtils.toArray(rs, table);
            Threads.Timer logTimer = getTableScanLogTimer();
            long rows = 0;

            while (rs.next()) {
                rows++;
                Object[] row = new Object[columnArray.getGreatestColumnPosition()];
                for (int i = 0; i < columnArray.getColumns().length; i++) {
                    row[columnArray.getColumns()[i].position() - 1] = readField(rs, i + 1);
                }

                if (logTimer.expired()) {
                    log.info(
                            "Exported {} records for split '{}' after {}",
                            rows,
                            snapshotSplit.splitId(),
                            Strings.duration(clock.currentTimeInMillis() - exportStart));
                    snapshotProgressListener.rowsScanned(table.id(), rows);
                    logTimer = getTableScanLogTimer();
                }
                eventDispatcher.dispatchSnapshotEvent(
                        table.id(),
                        getChangeRecordEmitter(snapshotContext, table.id(), row),
                        snapshotReceiver);
            }
            log.info(
                    "Finished exporting {} records for split '{}', total duration '{}'",
                    rows,
                    snapshotSplit.splitId(),
                    Strings.duration(clock.currentTimeInMillis() - exportStart));
        } catch (SQLException e) {
            throw new ConnectException("Snapshotting of table " + table.id() + " failed", e);
        }
    }

    private Threads.Timer getTableScanLogTimer() {
        return Threads.timer(clock, LOG_INTERVAL);
    }

    private Object readField(ResultSet rs, int columnIndex) throws SQLException {
        ResultSetMetaData metaData = rs.getMetaData();
        int columnType = metaData.getColumnType(columnIndex);

        if (columnType == Types.TIME) {
            return rs.getTimestamp(columnIndex);
        } else {
            return rs.getObject(columnIndex);
        }
    }

    protected ChangeRecordEmitter getChangeRecordEmitter(
            AbstractSnapshotChangeEventSource.SnapshotContext snapshotContext,
            TableId tableId,
            Object[] row) {
        snapshotContext.offset.event(tableId, clock.currentTime());
        return new SnapshotChangeRecordEmitter(snapshotContext.offset, row, clock);
    }

    private static class DamengSnapshotContext
            extends RelationalSnapshotChangeEventSource.RelationalSnapshotContext {
        public DamengSnapshotContext() throws SQLException {
            super("");
        }
    }
}
