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

package org.apache.seatunnel.connectors.seatunnel.cdc.mariadb.source.reader.fetch.scan;

import org.apache.seatunnel.connectors.cdc.base.relational.JdbcSourceEventDispatcher;
import org.apache.seatunnel.connectors.cdc.base.source.split.SnapshotSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.wartermark.WatermarkKind;
import org.apache.seatunnel.connectors.seatunnel.cdc.mariadb.offset.MariaDbBinlogOffset;

import org.apache.kafka.connect.errors.ConnectException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.mysql.MySqlConnection;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.MySqlDatabaseSchema;
import io.debezium.connector.mysql.MySqlOffsetContext;
import io.debezium.connector.mysql.MySqlPartition;
import io.debezium.connector.mysql.MySqlValueConverters;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.AbstractSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.ChangeRecordEmitter;
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

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.time.Duration;
import java.util.Calendar;

import static org.apache.seatunnel.connectors.seatunnel.cdc.mariadb.utils.MariaDbConnectionUtils.currentBinlogOffset;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mariadb.utils.MariaDbUtils.buildSplitScanQuery;
import static org.apache.seatunnel.connectors.seatunnel.cdc.mariadb.utils.MariaDbUtils.readTableSplitDataStatement;

public class MariaDbSnapshotSplitReadTask
        extends AbstractSnapshotChangeEventSource<MySqlPartition, MySqlOffsetContext> {

    private static final Logger LOG = LoggerFactory.getLogger(MariaDbSnapshotSplitReadTask.class);

    /** Interval for showing a log statement with the progress while scanning a single table. */
    private static final Duration LOG_INTERVAL = Duration.ofMillis(10_000);

    private final MySqlConnectorConfig connectorConfig;
    private final MySqlDatabaseSchema databaseSchema;
    private final MySqlConnection jdbcConnection;
    private final JdbcSourceEventDispatcher<MySqlPartition> dispatcher;
    private final Clock clock;
    private final SnapshotSplit snapshotSplit;
    private final MySqlOffsetContext offsetContext;
    private final SnapshotProgressListener<MySqlPartition> snapshotProgressListener;

    public MariaDbSnapshotSplitReadTask(
            MySqlConnectorConfig connectorConfig,
            MySqlOffsetContext previousOffset,
            SnapshotProgressListener<MySqlPartition> snapshotProgressListener,
            MySqlDatabaseSchema databaseSchema,
            MySqlConnection jdbcConnection,
            JdbcSourceEventDispatcher<MySqlPartition> dispatcher,
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
    public SnapshotResult<MySqlOffsetContext> execute(
            ChangeEventSource.ChangeEventSourceContext context,
            MySqlPartition partition,
            MySqlOffsetContext previousOffset)
            throws InterruptedException {
        SnapshottingTask snapshottingTask = getSnapshottingTask(partition, previousOffset);
        final SnapshotContext<MySqlPartition, MySqlOffsetContext> ctx;
        try {
            ctx = prepare(partition);
        } catch (Exception e) {
            LOG.error("Failed to initialize snapshot context.", e);
            throw new RuntimeException(e);
        }
        try {
            return doExecute(context, previousOffset, ctx, snapshottingTask);
        } catch (InterruptedException e) {
            LOG.warn("Snapshot was interrupted before completion");
            throw e;
        } catch (Exception t) {
            throw new DebeziumException(t);
        }
    }

    @Override
    protected SnapshotResult<MySqlOffsetContext> doExecute(
            ChangeEventSource.ChangeEventSourceContext context,
            MySqlOffsetContext previousOffset,
            AbstractSnapshotChangeEventSource.SnapshotContext<MySqlPartition, MySqlOffsetContext>
                    snapshotContext,
            AbstractSnapshotChangeEventSource.SnapshottingTask snapshottingTask)
            throws Exception {
        final MariaDbSnapshotContext ctx = (MariaDbSnapshotContext) snapshotContext;
        ctx.offset = offsetContext;

        final MariaDbBinlogOffset lowWatermark = currentBinlogOffset(jdbcConnection);
        LOG.info(
                "Snapshot step 1 - Determining low watermark {} for split {}",
                lowWatermark,
                snapshotSplit);
        ((SnapshotSplitChangeEventSourceContext) context).setLowWatermark(lowWatermark);
        dispatcher.dispatchWatermarkEvent(
                ctx.partition.getSourcePartition(), snapshotSplit, lowWatermark, WatermarkKind.LOW);

        LOG.info("Snapshot step 2 - Snapshotting data");
        createDataEvents(ctx, snapshotSplit.getTableId());

        final MariaDbBinlogOffset highWatermark = currentBinlogOffset(jdbcConnection);
        LOG.info(
                "Snapshot step 3 - Determining high watermark {} for split {}",
                highWatermark,
                snapshotSplit);
        ((SnapshotSplitChangeEventSourceContext) context).setHighWatermark(highWatermark);
        dispatcher.dispatchWatermarkEvent(
                ctx.partition.getSourcePartition(),
                snapshotSplit,
                highWatermark,
                WatermarkKind.HIGH);
        return SnapshotResult.completed(ctx.offset);
    }

    @Override
    protected SnapshottingTask getSnapshottingTask(
            MySqlPartition partition, MySqlOffsetContext previousOffset) {
        return new SnapshottingTask(false, true);
    }

    @Override
    protected MariaDbSnapshotContext prepare(MySqlPartition partition) throws Exception {
        return new MariaDbSnapshotContext(partition);
    }

    private void createDataEvents(MariaDbSnapshotContext snapshotContext, TableId tableId)
            throws Exception {
        EventDispatcher.SnapshotReceiver snapshotReceiver =
                dispatcher.getSnapshotChangeEventReceiver();
        LOG.debug("Snapshotting table {}", tableId);
        createDataEventsForTable(
                snapshotContext, snapshotReceiver, databaseSchema.tableFor(tableId));
        snapshotReceiver.completeSnapshot();
    }

    /** Dispatches the data change events for the records of a single table. */
    private void createDataEventsForTable(
            MariaDbSnapshotContext snapshotContext,
            EventDispatcher.SnapshotReceiver<MySqlPartition> snapshotReceiver,
            Table table)
            throws InterruptedException {

        long exportStart = clock.currentTimeInMillis();
        LOG.info("Exporting data from split '{}' of table {}", snapshotSplit.splitId(), table.id());

        final String selectSql =
                buildSplitScanQuery(
                        snapshotSplit.getTableId(),
                        snapshotSplit.getSplitKeyType(),
                        snapshotSplit.getSplitStart() == null,
                        snapshotSplit.getSplitEnd() == null);
        LOG.info(
                "For split '{}' of table {} using select statement: '{}'",
                snapshotSplit.splitId(),
                table.id(),
                selectSql);

        try (PreparedStatement selectStatement =
                        readTableSplitDataStatement(
                                jdbcConnection,
                                selectSql,
                                snapshotSplit.getSplitStart() == null,
                                snapshotSplit.getSplitEnd() == null,
                                snapshotSplit.getSplitStart(),
                                snapshotSplit.getSplitEnd(),
                                snapshotSplit.getSplitKeyType(),
                                connectorConfig.getSnapshotFetchSize());
                ResultSet rs = selectStatement.executeQuery()) {

            ColumnUtils.ColumnArray columnArray = ColumnUtils.toArray(rs, table);
            long rows = 0;
            Threads.Timer logTimer = getTableScanLogTimer();

            while (rs.next()) {
                rows++;
                final Object[] row = new Object[columnArray.getGreatestColumnPosition()];
                for (int i = 0; i < columnArray.getColumns().length; i++) {
                    Column actualColumn = columnArray.getColumns()[i];
                    row[columnArray.getColumns()[i].position() - 1] =
                            readField(rs, i + 1, actualColumn, table);
                }
                if (logTimer.expired()) {
                    long stop = clock.currentTimeInMillis();
                    LOG.info(
                            "Exported {} records for split '{}' after {}",
                            rows,
                            snapshotSplit.splitId(),
                            Strings.duration(stop - exportStart));
                    snapshotProgressListener.rowsScanned(
                            snapshotContext.partition, table.id(), rows);
                    logTimer = getTableScanLogTimer();
                }
                dispatcher.dispatchSnapshotEvent(
                        snapshotContext.partition,
                        table.id(),
                        getChangeRecordEmitter(snapshotContext, table.id(), row),
                        snapshotReceiver);
            }
            LOG.info(
                    "Finished exporting {} records for split '{}', total duration '{}'",
                    rows,
                    snapshotSplit.splitId(),
                    Strings.duration(clock.currentTimeInMillis() - exportStart));
        } catch (SQLException e) {
            throw new ConnectException("Snapshotting of table " + table.id() + " failed", e);
        }
    }

    protected ChangeRecordEmitter<MySqlPartition> getChangeRecordEmitter(
            MariaDbSnapshotContext snapshotContext, TableId tableId, Object[] row) {
        snapshotContext.offset.event(tableId, clock.currentTime());
        return new SnapshotChangeRecordEmitter<>(
                snapshotContext.partition, snapshotContext.offset, row, clock);
    }

    private Threads.Timer getTableScanLogTimer() {
        return Threads.timer(clock, LOG_INTERVAL);
    }

    private static class MariaDbSnapshotContext
            extends RelationalSnapshotChangeEventSource.RelationalSnapshotContext<
                    MySqlPartition, MySqlOffsetContext> {

        public MariaDbSnapshotContext(MySqlPartition partition) throws SQLException {
            super(partition, "");
        }
    }

    private Object readField(ResultSet rs, int fieldNo, Column actualColumn, Table actualTable)
            throws SQLException {
        if (actualColumn.jdbcType() == Types.TIME) {
            return readTimeField(rs, fieldNo);
        } else if (actualColumn.jdbcType() == Types.DATE) {
            return readDateField(rs, fieldNo, actualColumn, actualTable);
        } else if (actualColumn.jdbcType() == Types.TIMESTAMP) {
            return readTimestampField(rs, fieldNo, actualColumn, actualTable);
        } else if (actualColumn.jdbcType() == Types.TINYINT
                || actualColumn.jdbcType() == Types.SMALLINT) {
            return rs.getObject(fieldNo) == null ? null : rs.getInt(fieldNo);
        } else if (actualColumn.jdbcType() == Types.BLOB
                || actualColumn.jdbcType() == Types.LONGVARBINARY
                || actualColumn.jdbcType() == Types.BINARY
                || actualColumn.jdbcType() == Types.VARBINARY) {
            return rs.getBytes(fieldNo);
        } else {
            return rs.getObject(fieldNo);
        }
    }

    private Object readTimeField(ResultSet rs, int fieldNo) throws SQLException {
        String s = rs.getString(fieldNo);
        if (s == null) {
            return null;
        }
        return MySqlValueConverters.stringToDuration(s);
    }

    private Object readDateField(ResultSet rs, int fieldNo, Column column, Table table)
            throws SQLException {
        String s = rs.getString(fieldNo);
        if (s == null) {
            return null;
        }
        return MySqlValueConverters.stringToLocalDate(s, column, table);
    }

    private Object readTimestampField(ResultSet rs, int fieldNo, Column column, Table table)
            throws SQLException {
        String s = rs.getString(fieldNo);
        if (s == null) {
            return null;
        }
        return MySqlValueConverters.containsZeroValuesInDatePart(s, column, table)
                ? null
                : rs.getTimestamp(fieldNo, Calendar.getInstance());
    }
}
