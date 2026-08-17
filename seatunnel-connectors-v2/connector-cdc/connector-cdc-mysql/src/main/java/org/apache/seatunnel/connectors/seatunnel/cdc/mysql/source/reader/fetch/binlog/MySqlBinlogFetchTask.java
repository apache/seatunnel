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

package org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source.reader.fetch.binlog;

import org.apache.seatunnel.connectors.cdc.base.config.StartupConfig;
import org.apache.seatunnel.connectors.cdc.base.option.StartupMode;
import org.apache.seatunnel.connectors.cdc.base.relational.JdbcSourceEventDispatcher;
import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.source.reader.external.FetchTask;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;
import org.apache.seatunnel.connectors.cdc.base.source.split.wartermark.WatermarkKind;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source.offset.BinlogOffset;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source.reader.fetch.MySqlSourceFetchTaskContext;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source.reader.fetch.scan.MySqlSnapshotFetchTask;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeader;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import io.debezium.DebeziumException;
import io.debezium.connector.mysql.MySqlConnection;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import io.debezium.connector.mysql.MySqlOffsetContext;
import io.debezium.connector.mysql.MySqlPartition;
import io.debezium.connector.mysql.MySqlStreamingChangeEventSource;
import io.debezium.connector.mysql.MySqlStreamingChangeEventSourceMetrics;
import io.debezium.connector.mysql.MySqlTaskContext;
import io.debezium.pipeline.ErrorHandler;
import io.debezium.pipeline.source.spi.ChangeEventSource;
import io.debezium.util.Clock;
import lombok.extern.slf4j.Slf4j;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import static org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source.offset.BinlogOffset.NO_STOPPING_OFFSET;

@Slf4j
public class MySqlBinlogFetchTask implements FetchTask<SourceSplitBase> {
    private final IncrementalSplit split;
    private volatile boolean taskRunning = false;

    public MySqlBinlogFetchTask(IncrementalSplit split) {
        this.split = split;
    }

    @Override
    public void execute(FetchTask.Context context) throws Exception {
        MySqlSourceFetchTaskContext sourceFetchContext = (MySqlSourceFetchTaskContext) context;
        taskRunning = true;
        MySqlStreamingChangeEventSource mySqlStreamingChangeEventSource;

        StartupConfig startupConfig = sourceFetchContext.getSourceConfig().getStartupConfig();

        StartupMode startupMode = startupConfig.getStartupMode();
        // Check if we need bounded read (stop at specific position or timestamp)
        boolean isBoundedRead = !NO_STOPPING_OFFSET.equals(split.getStopOffset());

        if (shouldFilterByTimestamp(startupMode, split.getStartupOffset())) {
            if (isBoundedRead) {
                // Both timestamp startup and a specific stop offset: apply the timestamp filter
                // inside the bounded reader so the job still terminates at the stop offset
                // (instead of running forever).
                log.info(
                        "Starting MySQL binlog reader with timestamp filter {} and bounded read, stop offset: {}",
                        startupConfig.getTimestamp(),
                        split.getStopOffset());
                mySqlStreamingChangeEventSource =
                        new BoundedMySqlStreamingChangeEventSource(
                                sourceFetchContext.getDbzConnectorConfig(),
                                sourceFetchContext.getConnection(),
                                sourceFetchContext.getDispatcher(),
                                sourceFetchContext.getErrorHandler(),
                                Clock.SYSTEM,
                                sourceFetchContext.getTaskContext(),
                                sourceFetchContext.getStreamingChangeEventSourceMetrics(),
                                split,
                                startupConfig.getTimestamp());
            } else {
                log.info(
                        "Starting MySQL binlog reader,with timestamp filter {}",
                        startupConfig.getTimestamp());

                mySqlStreamingChangeEventSource =
                        new TimestampFilterMySqlStreamingChangeEventSource(
                                sourceFetchContext.getDbzConnectorConfig(),
                                sourceFetchContext.getConnection(),
                                sourceFetchContext.getDispatcher(),
                                sourceFetchContext.getErrorHandler(),
                                Clock.SYSTEM,
                                sourceFetchContext.getTaskContext(),
                                sourceFetchContext.getStreamingChangeEventSourceMetrics(),
                                startupConfig.getTimestamp());
            }
        } else if (isBoundedRead) {
            // Bounded read: stop at specific offset
            log.info(
                    "Starting MySQL binlog reader with bounded read, stop offset: {}",
                    split.getStopOffset());
            mySqlStreamingChangeEventSource =
                    new BoundedMySqlStreamingChangeEventSource(
                            sourceFetchContext.getDbzConnectorConfig(),
                            sourceFetchContext.getConnection(),
                            sourceFetchContext.getDispatcher(),
                            sourceFetchContext.getErrorHandler(),
                            Clock.SYSTEM,
                            sourceFetchContext.getTaskContext(),
                            sourceFetchContext.getStreamingChangeEventSourceMetrics(),
                            split,
                            null);
        } else {
            mySqlStreamingChangeEventSource =
                    new MySqlStreamingChangeEventSource(
                            sourceFetchContext.getDbzConnectorConfig(),
                            sourceFetchContext.getConnection(),
                            sourceFetchContext.getDispatcher(),
                            sourceFetchContext.getErrorHandler(),
                            Clock.SYSTEM,
                            sourceFetchContext.getTaskContext(),
                            sourceFetchContext.getStreamingChangeEventSourceMetrics());
        }

        BinlogSplitChangeEventSourceContext changeEventSourceContext =
                new BinlogSplitChangeEventSourceContext();

        sourceFetchContext
                .getBinaryLogClient()
                .registerLifecycleListener(
                        new BinaryLogClient.AbstractLifecycleListener() {
                            @Override
                            public void onConnect(BinaryLogClient client) {
                                try {
                                    sourceFetchContext.getConnection().close();
                                    log.info(
                                            "Binlog client connected, closed idle jdbc connection.");
                                } catch (SQLException e) {
                                    throw new RuntimeException(e);
                                }
                            }
                        });

        mySqlStreamingChangeEventSource.execute(
                changeEventSourceContext,
                sourceFetchContext.getPartition(),
                sourceFetchContext.getOffsetContext());
        taskRunning = false;
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

    static boolean shouldFilterByTimestamp(StartupMode startupMode, Offset startupOffset) {
        return startupMode.equals(StartupMode.TIMESTAMP)
                && startupOffset instanceof BinlogOffset
                && ((BinlogOffset) startupOffset).isTimestampOffset();
    }

    /**
     * A wrapped task to read all binlog for table and also supports read bounded (from lowWatermark
     * to highWatermark) binlog.
     */
    public static class MySqlBinlogSplitReadTask extends MySqlStreamingChangeEventSource {

        private static final Logger LOG = LoggerFactory.getLogger(MySqlBinlogSplitReadTask.class);
        private final IncrementalSplit binlogSplit;
        private final MySqlOffsetContext offsetContext;
        private final JdbcSourceEventDispatcher<MySqlPartition> dispatcher;
        private final ErrorHandler errorHandler;
        private ChangeEventSourceContext context;
        private long eventCount = 0;
        private long lastLogTime = System.currentTimeMillis();
        private static final long LOG_INTERVAL_MS = 10000;
        private BinlogOffset lastLoggedOffset = null;

        public MySqlBinlogSplitReadTask(
                MySqlConnectorConfig connectorConfig,
                MySqlOffsetContext offsetContext,
                MySqlConnection connection,
                JdbcSourceEventDispatcher<MySqlPartition> dispatcher,
                ErrorHandler errorHandler,
                MySqlTaskContext taskContext,
                MySqlStreamingChangeEventSourceMetrics metrics,
                IncrementalSplit binlogSplit) {
            super(
                    connectorConfig,
                    connection,
                    dispatcher,
                    errorHandler,
                    Clock.SYSTEM,
                    taskContext,
                    metrics);
            this.binlogSplit = binlogSplit;
            this.dispatcher = dispatcher;
            this.offsetContext = offsetContext;
            this.errorHandler = errorHandler;
        }

        @Override
        public void execute(
                ChangeEventSourceContext context,
                MySqlPartition partition,
                MySqlOffsetContext offsetContext)
                throws InterruptedException {
            this.context = context;
            super.execute(context, partition, this.offsetContext);
        }

        @Override
        protected void handleEvent(
                MySqlPartition partition, MySqlOffsetContext offsetContext, Event event) {
            super.handleEvent(partition, offsetContext, event);
            eventCount++;
            logBinlogProgress(offsetContext);
            // check do we need to stop for fetch binlog for snapshot split.
            if (isBoundedRead()) {
                final BinlogOffset currentBinlogOffset =
                        getBinlogPosition(offsetContext.getOffset());
                // reach the high watermark, the binlog fetcher should be finished
                if (currentBinlogOffset.isAtOrAfter(binlogSplit.getStopOffset())) {
                    // send binlog end event
                    try {
                        dispatcher.dispatchWatermarkEvent(
                                partition.getSourcePartition(),
                                binlogSplit,
                                currentBinlogOffset,
                                WatermarkKind.END);
                    } catch (InterruptedException e) {
                        LOG.error("Send signal event error.", e);
                        errorHandler.setProducerThrowable(
                                new DebeziumException("Error processing binlog signal event", e));
                    }
                    // tell fetcher the binlog task finished
                    ((MySqlSnapshotFetchTask.SnapshotBinlogSplitChangeEventSourceContext) context)
                            .finished();
                }
            }
        }

        private void logBinlogProgress(MySqlOffsetContext offsetContext) {
            long currentTime = System.currentTimeMillis();
            if (currentTime - lastLogTime >= LOG_INTERVAL_MS) {
                BinlogOffset currentOffset = getBinlogPosition(offsetContext.getOffset());
                if (lastLoggedOffset == null
                        || !currentOffset.getFilename().equals(lastLoggedOffset.getFilename())
                        || currentOffset.getPosition() != lastLoggedOffset.getPosition()) {
                    LOG.info(
                            "MySQL CDC binlog progress - file: {}, position: {}, events processed: {}",
                            currentOffset.getFilename(),
                            currentOffset.getPosition(),
                            eventCount);
                    lastLoggedOffset = currentOffset;
                }
                lastLogTime = currentTime;
            }
        }

        private boolean isBoundedRead() {
            return !NO_STOPPING_OFFSET.equals(binlogSplit.getStopOffset());
        }

        public static BinlogOffset getBinlogPosition(Map<String, ?> offset) {
            Map<String, String> offsetStrMap = new HashMap<>();
            for (Map.Entry<String, ?> entry : offset.entrySet()) {
                offsetStrMap.put(
                        entry.getKey(),
                        entry.getValue() == null ? null : entry.getValue().toString());
            }
            return new BinlogOffset(offsetStrMap);
        }
    }

    private class TimestampFilterMySqlStreamingChangeEventSource
            extends MySqlStreamingChangeEventSource {

        private final Long targetTimestamp;
        private long logTimestamp;
        private boolean loggedWaitingMessage;
        private final long LOG_INTERVAL_MS = 10000;
        private long eventCount = 0;
        private long lastLogTime = System.currentTimeMillis();
        private BinlogOffset lastLoggedOffset = null;

        public TimestampFilterMySqlStreamingChangeEventSource(
                MySqlConnectorConfig connectorConfig,
                MySqlConnection connection,
                JdbcSourceEventDispatcher<MySqlPartition> dispatcher,
                ErrorHandler errorHandler,
                Clock clock,
                MySqlTaskContext taskContext,
                MySqlStreamingChangeEventSourceMetrics metrics,
                Long targetTimestamp) {
            super(
                    connectorConfig,
                    connection,
                    dispatcher,
                    errorHandler,
                    clock,
                    taskContext,
                    metrics);
            this.targetTimestamp = targetTimestamp;
        }

        @Override
        protected void handleEvent(
                MySqlPartition partition, MySqlOffsetContext offsetContext, Event event) {
            if (event == null) {
                super.handleEvent(partition, offsetContext, event);
                return;
            }

            eventCount++;
            logBinlogProgress(offsetContext);

            long eventTs = event.getHeader().getTimestamp();
            if (eventTs == 0 || targetTimestamp == null || targetTimestamp == 0) {
                super.handleEvent(partition, offsetContext, event);
                return;
            }
            boolean shouldSkip = eventTs < targetTimestamp;
            if (shouldSkip) {
                if (!loggedWaitingMessage) {
                    log.info(
                            "skip binlog, currentTime:{}, filterTime:{}", eventTs, targetTimestamp);
                    loggedWaitingMessage = true;
                    logTimestamp = eventTs;
                }
                if (eventTs - logTimestamp >= LOG_INTERVAL_MS) {
                    loggedWaitingMessage = false;
                }
                updateOffsetPosition(offsetContext, event.getHeader());
                return;
            }

            super.handleEvent(partition, offsetContext, event);
        }

        private void logBinlogProgress(MySqlOffsetContext offsetContext) {
            long currentTime = System.currentTimeMillis();
            if (currentTime - lastLogTime >= LOG_INTERVAL_MS) {
                BinlogOffset currentOffset =
                        MySqlBinlogSplitReadTask.getBinlogPosition(offsetContext.getOffset());
                if (lastLoggedOffset == null
                        || !currentOffset.getFilename().equals(lastLoggedOffset.getFilename())
                        || currentOffset.getPosition() != lastLoggedOffset.getPosition()) {
                    log.info(
                            "MySQL CDC binlog progress - file: {}, position: {}, events processed: {}",
                            currentOffset.getFilename(),
                            currentOffset.getPosition(),
                            eventCount);
                    lastLoggedOffset = currentOffset;
                }
                lastLogTime = currentTime;
            }
        }

        private void updateOffsetPosition(
                MySqlOffsetContext offsetContext, EventHeader eventHeader) {
            try {
                if (eventHeader instanceof EventHeaderV4) {
                    EventHeaderV4 headerV4 = (EventHeaderV4) eventHeader;
                    offsetContext.setEventPosition(
                            headerV4.getPosition(), headerV4.getEventLength());
                }
                offsetContext.setBinlogServerId(eventHeader.getServerId());
                offsetContext.completeEvent();
            } catch (Exception e) {
                log.warn("Failed to update offset for skipped event: {}", e.getMessage());
            }
        }
    }

    /**
     * A bounded MySQL streaming change event source that stops at a specific offset. Used when
     * stop.mode = "specific" is configured.
     */
    private class BoundedMySqlStreamingChangeEventSource extends MySqlStreamingChangeEventSource {

        private final IncrementalSplit binlogSplit;
        private final JdbcSourceEventDispatcher<MySqlPartition> dispatcher;
        private final ErrorHandler errorHandler;
        private final Long targetTimestamp;
        private BoundedBinlogChangeEventSourceContext boundedContext;
        private long eventCount = 0;
        private long lastLogTime = System.currentTimeMillis();
        private static final long LOG_INTERVAL_MS = 10000;
        private BinlogOffset lastLoggedOffset = null;
        private boolean loggedWaitingMessage;
        private long logTimestamp;

        public BoundedMySqlStreamingChangeEventSource(
                MySqlConnectorConfig connectorConfig,
                MySqlConnection connection,
                JdbcSourceEventDispatcher<MySqlPartition> dispatcher,
                ErrorHandler errorHandler,
                Clock clock,
                MySqlTaskContext taskContext,
                MySqlStreamingChangeEventSourceMetrics metrics,
                IncrementalSplit binlogSplit,
                Long targetTimestamp) {
            super(
                    connectorConfig,
                    connection,
                    dispatcher,
                    errorHandler,
                    clock,
                    taskContext,
                    metrics);
            this.binlogSplit = binlogSplit;
            this.dispatcher = dispatcher;
            this.errorHandler = errorHandler;
            this.targetTimestamp = targetTimestamp;
        }

        @Override
        protected void handleEvent(
                MySqlPartition partition, MySqlOffsetContext offsetContext, Event event) {
            try {
                if (shouldSkipByTimestamp(offsetContext, event)) {
                    return;
                }
                super.handleEvent(partition, offsetContext, event);
            } finally {
                eventCount++;
                logBinlogProgress(offsetContext);
                checkStopOffset(partition, offsetContext);
            }
        }

        private boolean shouldSkipByTimestamp(MySqlOffsetContext offsetContext, Event event) {
            if (event == null || targetTimestamp == null || targetTimestamp == 0) {
                return false;
            }
            long eventTs = event.getHeader().getTimestamp();
            if (eventTs == 0) {
                return false;
            }
            boolean shouldSkip = eventTs < targetTimestamp;
            if (shouldSkip) {
                if (!loggedWaitingMessage) {
                    log.info(
                            "skip binlog, currentTime:{}, filterTime:{}", eventTs, targetTimestamp);
                    loggedWaitingMessage = true;
                    logTimestamp = eventTs;
                }
                if (eventTs - logTimestamp >= LOG_INTERVAL_MS) {
                    loggedWaitingMessage = false;
                }
                updateOffsetPosition(offsetContext, event.getHeader());
            }
            return shouldSkip;
        }

        private void updateOffsetPosition(
                MySqlOffsetContext offsetContext, EventHeader eventHeader) {
            try {
                if (eventHeader instanceof EventHeaderV4) {
                    EventHeaderV4 headerV4 = (EventHeaderV4) eventHeader;
                    offsetContext.setEventPosition(
                            headerV4.getPosition(), headerV4.getEventLength());
                }
                offsetContext.setBinlogServerId(eventHeader.getServerId());
                offsetContext.completeEvent();
            } catch (Exception e) {
                log.warn("Failed to update offset for skipped event: {}", e.getMessage());
            }
        }

        private void logBinlogProgress(MySqlOffsetContext offsetContext) {
            long currentTime = System.currentTimeMillis();
            if (currentTime - lastLogTime >= LOG_INTERVAL_MS) {
                BinlogOffset currentOffset =
                        MySqlBinlogSplitReadTask.getBinlogPosition(offsetContext.getOffset());
                if (lastLoggedOffset == null
                        || !currentOffset.getFilename().equals(lastLoggedOffset.getFilename())
                        || currentOffset.getPosition() != lastLoggedOffset.getPosition()) {
                    log.info(
                            "MySQL CDC binlog progress - file: {}, position: {}, events processed: {}",
                            currentOffset.getFilename(),
                            currentOffset.getPosition(),
                            eventCount);
                    lastLoggedOffset = currentOffset;
                }
                lastLogTime = currentTime;
            }
        }

        private void checkStopOffset(MySqlPartition partition, MySqlOffsetContext offsetContext) {
            final BinlogOffset currentBinlogOffset =
                    MySqlBinlogSplitReadTask.getBinlogPosition(offsetContext.getOffset());

            if (currentBinlogOffset.isAtOrAfter(binlogSplit.getStopOffset())) {
                log.info(
                        "Reached stop offset {} at current position {}. Stopping binlog reader.",
                        binlogSplit.getStopOffset(),
                        currentBinlogOffset);

                // Send end watermark event
                try {
                    dispatcher.dispatchWatermarkEvent(
                            partition.getSourcePartition(),
                            binlogSplit,
                            currentBinlogOffset,
                            WatermarkKind.END);
                } catch (InterruptedException e) {
                    log.error("Error sending binlog end watermark event", e);
                    errorHandler.setProducerThrowable(
                            new DebeziumException("Error processing binlog end event", e));
                }

                // Stop the task
                if (boundedContext != null) {
                    boundedContext.finished();
                }
            }
        }

        @Override
        public void execute(
                ChangeEventSourceContext context,
                MySqlPartition partition,
                MySqlOffsetContext offsetContext)
                throws InterruptedException {
            // Wrap the context to allow stopping
            this.boundedContext = new BoundedBinlogChangeEventSourceContext(context);
            super.execute(boundedContext, partition, offsetContext);
        }
    }

    /** A context wrapper that allows stopping the binlog reader. */
    private class BoundedBinlogChangeEventSourceContext
            implements ChangeEventSource.ChangeEventSourceContext {

        private final ChangeEventSource.ChangeEventSourceContext delegate;
        private volatile boolean running = true;

        public BoundedBinlogChangeEventSourceContext(
                ChangeEventSource.ChangeEventSourceContext delegate) {
            this.delegate = delegate;
        }

        @Override
        public boolean isRunning() {
            return running && delegate.isRunning();
        }

        public void finished() {
            running = false;
        }
    }

    private class BinlogSplitChangeEventSourceContext
            implements ChangeEventSource.ChangeEventSourceContext {
        @Override
        public boolean isRunning() {
            return taskRunning;
        }
    }
}
