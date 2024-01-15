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

package org.apache.seatunnel.connectors.seatunnel.cdc.oracle.source.reader.fetch.logminer;

import org.apache.seatunnel.connectors.cdc.base.relational.JdbcSourceEventDispatcher;
import org.apache.seatunnel.connectors.cdc.base.source.reader.external.FetchTask;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;
import org.apache.seatunnel.connectors.cdc.base.source.split.wartermark.WatermarkKind;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle.source.offset.RedoLogOffset;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle.source.reader.fetch.OracleSourceFetchTaskContext;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle.source.reader.fetch.scan.OracleSnapshotFetchTask;
import org.apache.seatunnel.connectors.seatunnel.cdc.oracle.utils.OracleUtils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.debezium.DebeziumException;
import io.debezium.config.Configuration;
import io.debezium.connector.oracle.OracleConnection;
import io.debezium.connector.oracle.OracleConnectorConfig;
import io.debezium.connector.oracle.OracleDatabaseSchema;
import io.debezium.connector.oracle.OracleOffsetContext;
import io.debezium.connector.oracle.OracleStreamingChangeEventSourceMetrics;
import io.debezium.connector.oracle.logminer.LogMinerStreamingChangeEventSource;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.util.Clock;

import static org.apache.seatunnel.connectors.seatunnel.cdc.oracle.source.offset.RedoLogOffset.NO_STOPPING_OFFSET;

/** The task to work for fetching data of Oracle table stream split. */
public class OracleRedoLogFetchTask implements FetchTask<SourceSplitBase> {

    private final IncrementalSplit split;
    private volatile boolean taskRunning = false;

    public OracleRedoLogFetchTask(IncrementalSplit split) {
        this.split = split;
    }

    @Override
    public void execute(FetchTask.Context context) throws Exception {
        OracleSourceFetchTaskContext sourceFetchContext = (OracleSourceFetchTaskContext) context;
        taskRunning = true;
        RedoLogSplitReadTask redoLogSplitReadTask =
                new RedoLogSplitReadTask(
                        sourceFetchContext.getDbzConnectorConfig(),
                        sourceFetchContext.getConnection(),
                        sourceFetchContext.getDispatcher(),
                        sourceFetchContext.getErrorHandler(),
                        sourceFetchContext.getDatabaseSchema(),
                        sourceFetchContext.getSourceConfig().getOriginDbzConnectorConfig(),
                        sourceFetchContext.getStreamingChangeEventSourceMetrics(),
                        split);
        RedoLogSplitChangeEventSourceContext changeEventSourceContext =
                new RedoLogSplitChangeEventSourceContext();
        redoLogSplitReadTask.execute(
                changeEventSourceContext, sourceFetchContext.getOffsetContext());
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
    public IncrementalSplit getSplit() {
        return split;
    }

    /**
     * A wrapped task to read all redoLog for table and also supports read bounded (from
     * lowWatermark to highWatermark) redoLog.
     */
    public static class RedoLogSplitReadTask extends LogMinerStreamingChangeEventSource {

        private static final Logger LOG = LoggerFactory.getLogger(RedoLogSplitReadTask.class);
        private final IncrementalSplit redoLogSplit;
        private final JdbcSourceEventDispatcher dispatcher;
        private final ErrorHandler errorHandler;
        private ChangeEventSourceContext context;

        public RedoLogSplitReadTask(
                OracleConnectorConfig connectorConfig,
                OracleConnection connection,
                JdbcSourceEventDispatcher dispatcher,
                ErrorHandler errorHandler,
                OracleDatabaseSchema schema,
                Configuration jdbcConfig,
                OracleStreamingChangeEventSourceMetrics metrics,
                IncrementalSplit redoLogSplit) {
            super(
                    connectorConfig,
                    connection,
                    dispatcher,
                    errorHandler,
                    Clock.SYSTEM,
                    schema,
                    jdbcConfig,
                    metrics);
            this.redoLogSplit = redoLogSplit;
            this.dispatcher = dispatcher;
            this.errorHandler = errorHandler;
        }

        @Override
        public void execute(ChangeEventSourceContext context, OracleOffsetContext offsetContext) {
            this.context = context;
            super.execute(context, offsetContext);
        }

        @Override
        public void afterHandleScn(OracleOffsetContext offsetContext) {
            super.afterHandleScn(offsetContext);
            // check do we need to stop for fetch redoLog for snapshot split.
            if (isBoundedRead()) {
                final RedoLogOffset currentRedoLogOffset =
                        OracleUtils.getRedoLogPosition(offsetContext.getOffset());
                // reach the high watermark, the redoLog fetcher should be finished
                if (currentRedoLogOffset.isAtOrAfter(redoLogSplit.getStopOffset())) {
                    // send redoLog end event
                    try {
                        dispatcher.dispatchWatermarkEvent(
                                offsetContext.getPartition(),
                                redoLogSplit,
                                currentRedoLogOffset,
                                WatermarkKind.END);
                    } catch (InterruptedException e) {
                        LOG.error("Send signal event error.", e);
                        errorHandler.setProducerThrowable(
                                new DebeziumException("Error processing redoLog signal event", e));
                    }
                    // tell fetcher the redoLog task finished
                    ((OracleSnapshotFetchTask.SnapshotRedoLogSplitChangeEventSourceContext) context)
                            .finished();
                }
            }
        }

        private boolean isBoundedRead() {
            return !NO_STOPPING_OFFSET.equals(redoLogSplit.getStopOffset());
        }
    }

    /**
     * The {@link ChangeEventSource.ChangeEventSourceContext} implementation for redoLog split task.
     */
    private class RedoLogSplitChangeEventSourceContext
            implements ChangeEventSource.ChangeEventSourceContext {
        @Override
        public boolean isRunning() {
            return taskRunning;
        }
    }
}
