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
import io.debezium.connector.sqlserver.SqlServerConnection;
import io.debezium.connector.sqlserver.SqlServerConnectorConfig;
import io.debezium.connector.sqlserver.SqlServerDatabaseSchema;
import io.debezium.connector.sqlserver.SqlServerOffsetContext;
import io.debezium.connector.sqlserver.SqlServerPartition;
import io.debezium.connector.sqlserver.SqlServerStreamingChangeEventSource;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.util.Clock;

import java.util.Map;

import static org.apache.seatunnel.connectors.seatunnel.cdc.sqlserver.source.offset.LsnOffset.NO_STOPPING_OFFSET;
import static org.apache.seatunnel.connectors.seatunnel.cdc.sqlserver.utils.SqlServerUtils.getLsnPosition;

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
            // check do we need to stop for fetch binlog for snapshot split.
            if (isBoundedRead()) {
                final LsnOffset currentRedoLogOffset = getLsnPosition(offset);
                // reach the high watermark, the binlog fetcher should be finished
                if (currentRedoLogOffset.isAtOrAfter(lsnSplit.getStopOffset())) {
                    // send binlog end event
                    try {
                        dispatcher.dispatchWatermarkEvent(
                                partition.getSourcePartition(),
                                lsnSplit,
                                currentRedoLogOffset,
                                WatermarkKind.END);
                    } catch (InterruptedException e) {
                        LOG.error("Send signal event error.", e);
                        errorHandler.setProducerThrowable(
                                new DebeziumException("Error processing binlog signal event", e));
                    }
                    // tell fetcher the binlog task finished
                    ((SqlServerSnapshotFetchTask.SnapshotBinlogSplitChangeEventSourceContext)
                                    context)
                            .finished();
                }
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
