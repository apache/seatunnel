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

package org.apache.searunnel.connectors.cdc.db2.source.reader.fetch.scan;

import org.apache.seatunnel.connectors.cdc.base.relational.JdbcSourceEventDispatcher;
import org.apache.seatunnel.connectors.cdc.base.source.split.SnapshotSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.wartermark.WatermarkKind;

import org.apache.kafka.connect.errors.ConnectException;
import org.apache.searunnel.connectors.cdc.db2.source.offset.LsnOffset;
import org.apache.searunnel.connectors.cdc.db2.utils.Db2Utils;

import io.debezium.DebeziumException;
import io.debezium.connector.db2.Db2Connection;
import io.debezium.connector.db2.Db2ConnectorConfig;
import io.debezium.connector.db2.Db2DatabaseSchema;
import io.debezium.connector.db2.Db2OffsetContext;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.AbstractSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.ChangeRecordEmitter;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.relational.Column;
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
public class Db2SnapshotSplitReadTask extends AbstractSnapshotChangeEventSource {

    /** Interval for showing a log statement with the progress while scanning a single table. */
    private static final Duration LOG_INTERVAL = Duration.ofMillis(10_000);

    private final Db2ConnectorConfig connectorConfig;
    private final Db2DatabaseSchema databaseSchema;
    private final Db2Connection jdbcConnection;
    private final JdbcSourceEventDispatcher dispatcher;
    private final Clock clock;
    private final SnapshotSplit snapshotSplit;
    private final Db2OffsetContext offsetContext;
    private final SnapshotProgressListener snapshotProgressListener;

    public Db2SnapshotSplitReadTask(
            Db2ConnectorConfig connectorConfig,
            Db2OffsetContext previousOffset,
            SnapshotProgressListener snapshotProgressListener,
            Db2DatabaseSchema databaseSchema,
            Db2Connection jdbcConnection,
            JdbcSourceEventDispatcher dispatcher,
            SnapshotSplit snapshotSplit) {
        super(connectorConfig, snapshotProgressListener);
        this.offsetContext = previousOffset;
        this.connectorConfig = connectorConfig;
        this.databaseSchema = databaseSchema;
        this.jdbcConnection = jdbcConnection;
        this.dispatcher = dispatcher;
        this.clock = Clock.SYSTEM;
        this.snapshotSplit = snapshotSplit;
        this.snapshotProgressListener = snapshotProgressListener;
    }

    @Override
    public SnapshotResult execute(ChangeEventSourceContext context, OffsetContext previousOffset)
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
        final SqlSeverSnapshotContext ctx = (SqlSeverSnapshotContext) snapshotContext;
        ctx.offset = offsetContext;

        final LsnOffset lowWatermark = Db2Utils.currentLsn(jdbcConnection);
        log.info(
                "Snapshot step 1 - Determining low watermark {} for split {}",
                lowWatermark,
                snapshotSplit);
        ((SnapshotSplitChangeEventSourceContext) context).setLowWatermark(lowWatermark);
        dispatcher.dispatchWatermarkEvent(
                offsetContext.getPartition(), snapshotSplit, lowWatermark, WatermarkKind.LOW);

        log.info("Snapshot step 2 - Snapshotting data");
        createDataEvents(ctx, snapshotSplit.getTableId());

        final LsnOffset highWatermark = Db2Utils.currentLsn(jdbcConnection);
        log.info(
                "Snapshot step 3 - Determining high watermark {} for split {}",
                highWatermark,
                snapshotSplit);
        ((SnapshotSplitChangeEventSourceContext) context).setHighWatermark(highWatermark);
        dispatcher.dispatchWatermarkEvent(
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
        return new SqlSeverSnapshotContext();
    }

    private void createDataEvents(SqlSeverSnapshotContext snapshotContext, TableId tableId)
            throws Exception {
        EventDispatcher.SnapshotReceiver snapshotReceiver =
                dispatcher.getSnapshotChangeEventReceiver();
        log.debug("Snapshotting table {}", tableId);
        createDataEventsForTable(
                snapshotContext, snapshotReceiver, databaseSchema.tableFor(tableId));
        snapshotReceiver.completeSnapshot();
    }

    /** Dispatches the data change events for the records of a single table. */
    private void createDataEventsForTable(
            SqlSeverSnapshotContext snapshotContext,
            EventDispatcher.SnapshotReceiver snapshotReceiver,
            Table table)
            throws InterruptedException {

        long exportStart = clock.currentTimeInMillis();
        log.info("Exporting data from split '{}' of table {}", snapshotSplit.splitId(), table.id());

        final String selectSql =
                Db2Utils.buildSplitScanQuery(
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
                        Db2Utils.readTableSplitDataStatement(
                                jdbcConnection,
                                selectSql,
                                snapshotSplit.getSplitStart() == null,
                                snapshotSplit.getSplitEnd() == null,
                                snapshotSplit.getSplitStart(),
                                snapshotSplit.getSplitEnd(),
                                snapshotSplit.getSplitKeyType().getTotalFields(),
                                connectorConfig.getSnapshotFetchSize());
                ResultSet rs = selectStatement.executeQuery()) {

            ColumnUtils.ColumnArray columnArray = ColumnUtils.toArray(rs, table);
            long rows = 0;
            Threads.Timer logTimer = getTableScanLogTimer();

            while (rs.next()) {
                rows++;
                final Object[] row = new Object[columnArray.getGreatestColumnPosition()];
                for (int i = 0; i < columnArray.getColumns().length; i++) {
                    Column actualColumn = table.columns().get(i);
                    row[columnArray.getColumns()[i].position() - 1] = readField(rs, i + 1);
                }
                if (logTimer.expired()) {
                    long stop = clock.currentTimeInMillis();
                    log.info(
                            "Exported {} records for split '{}' after {}",
                            rows,
                            snapshotSplit.splitId(),
                            Strings.duration(stop - exportStart));
                    snapshotProgressListener.rowsScanned(table.id(), rows);
                    logTimer = getTableScanLogTimer();
                }
                dispatcher.dispatchSnapshotEvent(
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

    protected ChangeRecordEmitter getChangeRecordEmitter(
            SqlSeverSnapshotContext snapshotContext, TableId tableId, Object[] row) {
        snapshotContext.offset.event(tableId, clock.currentTime());
        return new SnapshotChangeRecordEmitter(snapshotContext.offset, row, clock);
    }

    private Threads.Timer getTableScanLogTimer() {
        return Threads.timer(clock, LOG_INTERVAL);
    }

    private Object readField(ResultSet rs, int columnIndex) throws SQLException {
        final ResultSetMetaData metaData = rs.getMetaData();
        final int columnType = metaData.getColumnType(columnIndex);

        if (columnType == Types.TIME) {
            return rs.getTimestamp(columnIndex);
        } else {
            return rs.getObject(columnIndex);
        }
    }

    private static class SqlSeverSnapshotContext
            extends RelationalSnapshotChangeEventSource.RelationalSnapshotContext {

        public SqlSeverSnapshotContext() throws SQLException {
            super("");
        }
    }
}
