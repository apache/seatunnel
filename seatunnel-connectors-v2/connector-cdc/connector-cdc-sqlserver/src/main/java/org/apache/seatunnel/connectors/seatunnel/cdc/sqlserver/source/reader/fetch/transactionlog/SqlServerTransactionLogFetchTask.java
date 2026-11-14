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

package org.apache.seatunnel.connectors.seatunnel.cdc.sqlserver.source.reader.fetch.transactionlog;

import org.apache.seatunnel.connectors.cdc.base.relational.JdbcSourceEventDispatcher;
import org.apache.seatunnel.connectors.cdc.base.source.reader.external.FetchTask;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;
import org.apache.seatunnel.connectors.cdc.base.source.split.wartermark.WatermarkKind;
import org.apache.seatunnel.connectors.seatunnel.cdc.sqlserver.source.offset.LsnOffset;
import org.apache.seatunnel.connectors.seatunnel.cdc.sqlserver.source.reader.fetch.SqlServerSourceFetchTaskContext;
import org.apache.seatunnel.connectors.seatunnel.cdc.sqlserver.source.reader.fetch.scan.SqlServerSnapshotFetchTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.connector.sqlserver.Lsn;
import io.debezium.connector.sqlserver.SqlServerChangeTable;
import io.debezium.connector.sqlserver.SqlServerConnection;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig;
import io.debezium.connector.sqlserver.SqlServerDatabaseSchema;
import io.debezium.connector.sqlserver.SqlServerOffsetContext;
import io.debezium.connector.sqlserver.SqlServerPartition;
import io.debezium.connector.sqlserver.SqlServerStreamingChangeEventSource;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.relational.TableId;
import io.debezium.util.Clock;

import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.apache.seatunnel.connectors.seatunnel.cdc.sqlserver.source.offset.LsnOffset.NO_STOPPING_OFFSET;

public class SqlServerTransactionLogFetchTask implements FetchTask<SourceSplitBase> {
    private final IncrementalSplit split;
    private volatile boolean taskRunning = false;

    public SqlServerTransactionLogFetchTask(IncrementalSplit split) {
        this.split = split;
    }

    @Override
    public void execute(FetchTask.Context context) throws Exception {
        SqlServerSourceFetchTaskContext sourceFetchContext =
                (SqlServerSourceFetchTaskContext) context;
        taskRunning = true;

        TransactionLogSplitReadTask transactionLogSplitReadTask =
                new TransactionLogSplitReadTask(
                        sourceFetchContext.getDbzConnectorConfig(),
                        sourceFetchContext.getDataConnection(),
                        sourceFetchContext.getMetadataConnection(),
                        sourceFetchContext.getDispatcher(),
                        sourceFetchContext.getErrorHandler(),
                        sourceFetchContext.getDatabaseSchema(),
                        split);

        TransactionLogSplitChangeEventSourceContext changeEventSourceContext =
                new TransactionLogSplitChangeEventSourceContext();

        transactionLogSplitReadTask.execute(
                changeEventSourceContext,
                sourceFetchContext.getPartition(),
                sourceFetchContext.getOffsetContext());
    }

    @Override
    public boolean isRunning() {
        return taskRunning;
    }

    @Override
    public void shutdown() {
        taskRunning = false;
    }

    @Override
    public SourceSplitBase getSplit() {
        return split;
    }

    /**
     * A wrapped task to read all binlog for table and also supports read bounded (from lowWatermark
     * to highWatermark) binlog.
     */
    public static class TransactionLogSplitReadTask extends SqlServerStreamingChangeEventSource {

        private static final Logger LOG =
                LoggerFactory.getLogger(TransactionLogSplitReadTask.class);
        private final IncrementalSplit lsnSplit;
        private final JdbcSourceEventDispatcher dispatcher;
        private final ErrorHandler errorHandler;
        private ChangeEventSourceContext context;

        public TransactionLogSplitReadTask(
                SqlServerConnectorConfig connectorConfig,
                SqlServerConnection connection,
                SqlServerConnection metadataConnection,
                JdbcSourceEventDispatcher dispatcher,
                ErrorHandler errorHandler,
                SqlServerDatabaseSchema schema,
                IncrementalSplit lsnSplit) {
            super(
                    connectorConfig,
                    connection,
                    metadataConnection,
                    dispatcher,
                    errorHandler,
                    Clock.system(),
                    schema);
            this.lsnSplit = lsnSplit;
            this.dispatcher = dispatcher;
            this.errorHandler = errorHandler;
        }

        @Override
        public void afterHandleLsn(SqlServerPartition partition, Map<String, ?> offset) {
            // Row-event offsets cannot prove completion of an empty bounded query interval.
            super.afterHandleLsn(partition, offset);
        }

        @Override
        protected Lsn limitQueryUpperBound(Lsn toLsn) {
            if (isBoundedRead() && toLsn.isAvailable()) {
                final Lsn highWatermark = ((LsnOffset) lsnSplit.getStopOffset()).getCommitLsn();
                if (toLsn.compareTo(highWatermark) > 0) {
                    return highWatermark;
                }
            }
            return toLsn;
        }

        @Override
        protected void validateQueryTables(SqlServerChangeTable[] tables, Lsn fromLsn, Lsn toLsn) {
            if (!isBoundedRead()) {
                return;
            }
            for (TableId tableId : lsnSplit.getTableIds()) {
                final List<SqlServerChangeTable> captures =
                        Arrays.stream(tables)
                                .filter(table -> tableId.equals(table.getSourceTableId()))
                                .collect(Collectors.toList());
                for (SqlServerChangeTable capture : captures) {
                    if (capture.getStartLsn() == null || !capture.getStartLsn().isAvailable()) {
                        throw new DebeziumException(
                                "Cannot complete bounded backfill: capture start LSN is unavailable for "
                                        + tableId);
                    }
                }
                captures.sort(Comparator.comparing(SqlServerChangeTable::getStartLsn));
                Lsn coveredUntil = fromLsn;
                boolean covered = false;
                for (SqlServerChangeTable capture : captures) {
                    if (capture.getStartLsn().compareTo(coveredUntil) > 0) {
                        break;
                    }
                    final Lsn stopLsn = capture.getStopLsn();
                    if (stopLsn == null || !stopLsn.isAvailable()) {
                        covered = true;
                        break;
                    }
                    if (stopLsn.compareTo(coveredUntil) > 0) {
                        coveredUntil = stopLsn;
                    }
                    // Capture stop is exclusive; an instance ending exactly at HIGH is
                    // insufficient.
                    if (coveredUntil.compareTo(toLsn) > 0) {
                        covered = true;
                        break;
                    }
                }
                if (!covered) {
                    throw new DebeziumException(
                            "Cannot complete bounded backfill: active capture instances do not cover "
                                    + tableId
                                    + " from "
                                    + fromLsn
                                    + " through "
                                    + toLsn);
                }
            }
            if (lsnSplit.getTableIds().isEmpty()) {
                throw new DebeziumException(
                        "Cannot complete bounded backfill without a source table");
            }
        }

        @Override
        protected void afterQueryCompleted(
                SqlServerPartition partition, Map<String, ?> offset, Lsn completedLsn)
                throws InterruptedException {
            if (!isBoundedRead()) {
                super.afterQueryCompleted(partition, offset, completedLsn);
                return;
            }
            final Lsn highWatermark = ((LsnOffset) lsnSplit.getStopOffset()).getCommitLsn();
            if (completedLsn.compareTo(highWatermark) >= 0) {
                // An empty result still proves this entire bounded interval was queried
                // successfully.
                // Keep row-event offsets untouched and emit END only after the complete HIGH
                // transaction.
                dispatcher.dispatchWatermarkEvent(
                        partition.getSourcePartition(),
                        lsnSplit,
                        lsnSplit.getStopOffset(),
                        WatermarkKind.END);
                ((SqlServerSnapshotFetchTask.SnapshotBinlogSplitChangeEventSourceContext) context)
                        .finished();
            }
        }

        private boolean isBoundedRead() {
            return !NO_STOPPING_OFFSET.equals(lsnSplit.getStopOffset());
        }

        @Override
        public void execute(
                ChangeEventSourceContext context,
                SqlServerPartition partition,
                SqlServerOffsetContext offsetContext)
                throws InterruptedException {
            this.context = context;
            super.execute(context, partition, offsetContext);
        }
    }

    private class TransactionLogSplitChangeEventSourceContext
            implements ChangeEventSource.ChangeEventSourceContext {
        @Override
        public boolean isRunning() {
            return taskRunning;
        }
    }
}
