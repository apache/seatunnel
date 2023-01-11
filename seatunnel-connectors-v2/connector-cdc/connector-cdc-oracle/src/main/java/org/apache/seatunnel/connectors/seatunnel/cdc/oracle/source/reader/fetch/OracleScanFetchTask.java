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

package org.apache.seatunnel.connectors.seatunnel.cdc.oracle.source.reader.fetch;

import static org.apache.seatunnel.connectors.seatunnel.cdc.oracle.utils.OracleConnectionUtils.createOracleConnection;
import static org.apache.seatunnel.connectors.seatunnel.cdc.oracle.utils.OracleConnectionUtils.currentRedoLogOffset;
import static org.apache.seatunnel.connectors.seatunnel.cdc.oracle.utils.OracleUtils.buildSplitScanQuery;
import static org.apache.seatunnel.connectors.seatunnel.cdc.oracle.utils.OracleUtils.readTableSplitDataStatement;

import org.apache.seatunnel.connectors.cdc.base.relational.JdbcSourceEventDispatcher;
import org.apache.seatunnel.connectors.cdc.base.source.reader.external.FetchTask;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SnapshotSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;
import org.apache.seatunnel.connectors.cdc.base.source.split.wartermark.WatermarkKind;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle.source.offset.RedoLogOffset;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OracleValueConverters;
import io.debezium.connector.oracle.logminer.LogMinerOracleOffsetContextLoader;
import io.debezium.heartbeat.Heartbeat;
import io.debezium.pipeline.EventDispatcher;
import io.debezium.pipeline.source.AbstractSnapshotChangeEventSource;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.pipeline.source.spi.SnapshotProgressListener;
import io.debezium.pipeline.spi.ChangeRecordEmitter;
import io.debezium.pipeline.spi.OffsetContext;
import io.debezium.pipeline.spi.SnapshotResult;
import io.debezium.relational.Column;
import io.debezium.relational.RelationalSnapshotChangeEventSource;
import io.debezium.relational.SnapshotChangeRecordEmitter;
import io.debezium.relational.Table;
import io.debezium.relational.TableId;
import io.debezium.relational.ValueConverter;
import io.debezium.util.Clock;
import io.debezium.util.ColumnUtils;
import io.debezium.util.Strings;
import io.debezium.util.Threads;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.errors.ConnectException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Map;

/** The task to work for fetching data of Oracle table snapshot split. */
public class OracleScanFetchTask implements FetchTask<SourceSplitBase> {

    private final SnapshotSplit split;
    private volatile boolean taskRunning = false;

    private OracleSnapshotSplitReadTask snapshotSplitReadTask;

    public OracleScanFetchTask(SnapshotSplit split) {
        this.split = split;
    }

    @Override
    public SnapshotSplit getSplit() {
        return split;
    }

    @Override
    public boolean isRunning() {
        return taskRunning;
    }

    @Override
    public void execute(FetchTask.Context context) throws Exception {
        OracleSourceFetchTaskContext sourceFetchContext = (OracleSourceFetchTaskContext) context;
        taskRunning = true;
        snapshotSplitReadTask =
                new OracleSnapshotSplitReadTask(
                        sourceFetchContext.getDbzConnectorConfig(),
                        sourceFetchContext.getOffsetContext(),
                        sourceFetchContext.getSnapshotChangeEventSourceMetrics(),
                        sourceFetchContext.getDatabaseSchema(),
                        sourceFetchContext.getConnection(),
                        sourceFetchContext.getDispatcher(),
                        split);
        SnapshotSplitChangeEventSourceContext changeEventSourceContext =
                new SnapshotSplitChangeEventSourceContext();
        SnapshotResult snapshotResult =
                snapshotSplitReadTask.execute(
                        changeEventSourceContext, sourceFetchContext.getOffsetContext());

        final IncrementalSplit backfillBinlogSplit =
                createBackfillRedoLogSplit(changeEventSourceContext);
        // optimization that skip the binlog read when the low watermark equals high
        // watermark

        final boolean binlogBackfillRequired =
                backfillBinlogSplit
                    .getStopOffset()
                        .isAfter(backfillBinlogSplit.getStartupOffset());

        if (!binlogBackfillRequired) {
            dispatchBinlogEndEvent(
                    backfillBinlogSplit,
                    ((OracleSourceFetchTaskContext) context).getOffsetContext().getPartition(),
                    ((OracleSourceFetchTaskContext) context).getDispatcher());
            taskRunning = false;
            return;
        }
        // execute redoLog read task
        if (snapshotResult.isCompletedOrSkipped()) {
            final OracleStreamFetchTask.RedoLogSplitReadTask backfillBinlogReadTask =
                    createBackfillRedoLogReadTask(backfillBinlogSplit, sourceFetchContext);

            OracleConnectorConfig oracleConnectorConfig =
                sourceFetchContext.getSourceConfig().getDbzConnectorConfig();
            final OffsetContext.Loader<OracleOffsetContext> loader =
                new LogMinerOracleOffsetContextLoader(oracleConnectorConfig);
            final OracleOffsetContext oracleOffsetContext =
                loader.load(backfillBinlogSplit.getStartupOffset().getOffset());
            backfillBinlogReadTask.execute(
                    new SnapshotBinlogSplitChangeEventSourceContext(), oracleOffsetContext);
        } else {
            taskRunning = false;
            throw new IllegalStateException(
                    String.format("Read snapshot for oracle split %s fail", split));
        }
    }

    private IncrementalSplit createBackfillRedoLogSplit(
            SnapshotSplitChangeEventSourceContext sourceContext) {
        return new IncrementalSplit(
            split.splitId(),
            Collections.singletonList(split.getTableId()),
            sourceContext.getLowWatermark(),
            sourceContext.getHighWatermark(),
            new ArrayList<>());
    }

    private OracleStreamFetchTask.RedoLogSplitReadTask createBackfillRedoLogReadTask(
            IncrementalSplit backfillBinlogSplit, OracleSourceFetchTaskContext context) {
        // we should only capture events for the current table,
        // otherwise, we may can't find corresponding schema
        Configuration dezConf =
                context.getSourceConfig()
                        .getDbzConfiguration()
                        .edit()
                        .with("table.include.list",
                            split.getTableId().toString().substring(split.getTableId().toString().indexOf(".") + 1))
                        // Disable heartbeat event in snapshot split fetcher
                        .with(Heartbeat.HEARTBEAT_INTERVAL, 0)
                        .build();
        // task to read binlog and backfill for current split
        return new OracleStreamFetchTask.RedoLogSplitReadTask(
                new OracleConnectorConfig(dezConf),
                createOracleConnection(context.getSourceConfig().getDbzConfiguration()),
                context.getDispatcher(),
                context.getErrorHandler(),
                context.getDatabaseSchema(),
                context.getSourceConfig().getOriginDbzConnectorConfig(),
                context.getStreamingChangeEventSourceMetrics(),
                backfillBinlogSplit);
    }

    private void dispatchBinlogEndEvent(
            IncrementalSplit backFillBinlogSplit,
            Map<String, ?> sourcePartition,
            JdbcSourceEventDispatcher eventDispatcher)
            throws InterruptedException {
        eventDispatcher.dispatchWatermarkEvent(
                sourcePartition,
                backFillBinlogSplit,
                backFillBinlogSplit.getStopOffset(),
                WatermarkKind.END);
    }

    /** A wrapped task to fetch snapshot split of table. */
    public static class OracleSnapshotSplitReadTask extends AbstractSnapshotChangeEventSource {

        private static final Logger LOG =
                LoggerFactory.getLogger(OracleSnapshotSplitReadTask.class);

        /** Interval for showing a log statement with the progress while scanning a single table. */
        private static final Duration LOG_INTERVAL = Duration.ofMillis(10_000);

        private final OracleConnectorConfig connectorConfig;
        private final OracleDatabaseSchema databaseSchema;
        private final OracleConnection jdbcConnection;
        private final JdbcSourceEventDispatcher dispatcher;
        private final Clock clock;
        private final SnapshotSplit snapshotSplit;
        private final OracleOffsetContext offsetContext;
        private final SnapshotProgressListener snapshotProgressListener;

        public OracleSnapshotSplitReadTask(
                OracleConnectorConfig connectorConfig,
                OracleOffsetContext previousOffset,
                SnapshotProgressListener snapshotProgressListener,
                OracleDatabaseSchema databaseSchema,
                OracleConnection jdbcConnection,
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
        public SnapshotResult execute(
                ChangeEventSourceContext context, OffsetContext previousOffset)
                throws InterruptedException {
            SnapshottingTask snapshottingTask = getSnapshottingTask(previousOffset);
            final SnapshotContext ctx;
            try {
                ctx = prepare(context);
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
        protected SnapshotResult doExecute(
                ChangeEventSourceContext context,
                OffsetContext previousOffset,
                SnapshotContext snapshotContext,
                SnapshottingTask snapshottingTask)
                throws Exception {
            final RelationalSnapshotChangeEventSource.RelationalSnapshotContext ctx =
                    (RelationalSnapshotChangeEventSource.RelationalSnapshotContext) snapshotContext;
            ctx.offset = offsetContext;

            final RedoLogOffset lowWatermark = currentRedoLogOffset(jdbcConnection);
            LOG.info(
                    "Snapshot step 1 - Determining low watermark {} for split {}",
                    lowWatermark,
                    snapshotSplit);
            ((SnapshotSplitChangeEventSourceContext) context).setLowWatermark(lowWatermark);
            dispatcher.dispatchWatermarkEvent(
                    offsetContext.getPartition(), snapshotSplit, lowWatermark, WatermarkKind.LOW);

            LOG.info("Snapshot step 2 - Snapshotting data");
            createDataEvents(ctx, snapshotSplit.getTableId());

            final RedoLogOffset highWatermark = currentRedoLogOffset(jdbcConnection);
            LOG.info(
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
            return new MySqlSnapshotContext();
        }

        private void createDataEvents(
                RelationalSnapshotChangeEventSource.RelationalSnapshotContext snapshotContext,
                TableId tableId)
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
                RelationalSnapshotChangeEventSource.RelationalSnapshotContext snapshotContext,
                EventDispatcher.SnapshotReceiver snapshotReceiver,
                Table table)
                throws InterruptedException {

            long exportStart = clock.currentTimeInMillis();
            LOG.info(
                    "Exporting data from split '{}' of table {}",
                    snapshotSplit.splitId(),
                    table.id());

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
                                new Object[]{snapshotSplit.getSplitStart()},
                                new Object[]{snapshotSplit.getSplitEnd()},
                                snapshotSplit.getSplitKeyType().getTotalFields(),
                                connectorConfig.getQueryFetchSize());
                    ResultSet rs = selectStatement.executeQuery()) {

                ColumnUtils.ColumnArray columnArray = ColumnUtils.toArray(rs, table);
                long rows = 0;
                Threads.Timer logTimer = getTableScanLogTimer();

                while (rs.next()) {
                    rows++;
                    final Object[] row = new Object[columnArray.getGreatestColumnPosition()];
                    for (int i = 0; i < columnArray.getColumns().length; i++) {
                        Column actualColumn = table.columns().get(i);
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
                        snapshotProgressListener.rowsScanned(table.id(), rows);
                        logTimer = getTableScanLogTimer();
                    }
                    dispatcher.dispatchSnapshotEvent(
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

        protected ChangeRecordEmitter getChangeRecordEmitter(
                SnapshotContext snapshotContext, TableId tableId, Object[] row) {
            snapshotContext.offset.event(tableId, clock.currentTime());
            return new SnapshotChangeRecordEmitter(snapshotContext.offset, row, clock);
        }

        private Threads.Timer getTableScanLogTimer() {
            return Threads.timer(clock, LOG_INTERVAL);
        }

        /**
         * copied from
         * io.debezium.connector.oracle.antlr.listener.ParserUtils#convertValueToSchemaType.
         */
        private Object readField(ResultSet rs, int fieldNo, Column actualColumn, Table actualTable)
                throws SQLException {

            OracleValueConverters oracleValueConverters =
                    new OracleValueConverters(connectorConfig, jdbcConnection);

            final SchemaBuilder schemaBuilder = oracleValueConverters.schemaBuilder(actualColumn);
            if (schemaBuilder == null) {
                return null;
            }
            Schema schema = schemaBuilder.build();
            Field field = new Field(actualColumn.name(), 1, schema);
            final ValueConverter valueConverter =
                    oracleValueConverters.converter(actualColumn, field);
            return valueConverter.convert(rs.getObject(fieldNo));
        }

        private static class MySqlSnapshotContext
            extends RelationalSnapshotChangeEventSource.RelationalSnapshotContext {

            public MySqlSnapshotContext() throws SQLException {
                super("");
            }
        }
    }

    /**
     * {@link ChangeEventSource.ChangeEventSourceContext} implementation that keeps low/high
     * watermark for each {@link SnapshotSplit}.
     */
    public class SnapshotSplitChangeEventSourceContext
            implements ChangeEventSource.ChangeEventSourceContext {

        private RedoLogOffset lowWatermark;
        private RedoLogOffset highWatermark;

        public RedoLogOffset getLowWatermark() {
            return lowWatermark;
        }

        public void setLowWatermark(RedoLogOffset lowWatermark) {
            this.lowWatermark = lowWatermark;
        }

        public RedoLogOffset getHighWatermark() {
            return highWatermark;
        }

        public void setHighWatermark(RedoLogOffset highWatermark) {
            this.highWatermark = highWatermark;
        }

        @Override
        public boolean isRunning() {
            return lowWatermark != null && highWatermark != null;
        }
    }

    /**
     * The {@link ChangeEventSource.ChangeEventSourceContext} implementation for bounded binlog task
     * of a snapshot split task.
     */
    public class SnapshotBinlogSplitChangeEventSourceContext
            implements ChangeEventSource.ChangeEventSourceContext {

        public void finished() {
            taskRunning = false;
        }

        @Override
        public boolean isRunning() {
            return taskRunning;
        }
    }
}
