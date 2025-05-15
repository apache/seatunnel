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

import org.apache.seatunnel.connectors.cdc.base.relational.JdbcSourceEventDispatcher;
import org.apache.seatunnel.connectors.cdc.base.source.reader.external.FetchTask;
import org.apache.seatunnel.connectors.cdc.base.source.split.IncrementalSplit;
import org.apache.seatunnel.connectors.cdc.base.source.split.SourceSplitBase;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source.offset.BinlogOffset;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source.reader.fetch.MySqlSourceFetchTaskContext;

import com.github.shyiko.mysql.binlog.BinaryLogClient;
import com.github.shyiko.mysql.binlog.event.Event;
import com.github.shyiko.mysql.binlog.event.EventHeader;
import com.github.shyiko.mysql.binlog.event.EventHeaderV4;
import com.github.shyiko.mysql.binlog.event.EventType;
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
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A {@link FetchTask} implementation that reads the MySQL binlog and supports waiting for future
 * timestamps.
 *
 * <p>This reader extends the standard MySQL binlog reader to add support for filtering events based
 * on a future timestamp. When configured with a future timestamp, it will only start processing
 * events once their timestamp is greater than or equal to the specified timestamp. Meanwhile, it
 * will continue to track the binlog offset to ensure no data is lost.
 */
@Slf4j
public class MySqlBinlogSplitReader implements FetchTask<SourceSplitBase> {
    private static final DateTimeFormatter DATE_TIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private static final long LOG_INTERVAL_MS = 30_000; // 30 seconds

    private final IncrementalSplit split;
    private volatile boolean taskRunning = false;
    private final Long futureTimestamp;
    private boolean loggedWaitingMessage = false;
    private long lastLogTimeMs = 0;
    private final AtomicLong filteredEventsCount = new AtomicLong(0);

    public MySqlBinlogSplitReader(IncrementalSplit split) {
        this.split = split;

        // check If There Is A FutureTimeStamp Setting
        BinlogOffset offset = (BinlogOffset) split.getStartupOffset();
        Map<String, String> offsetMap = offset.getOffset();

        // parsing FutureTimestamp Configurations
        Long parsedTimestamp = null;
        if (offsetMap.containsKey("future_timestamp")
                && "true".equals(offsetMap.get("is_future_timestamp"))) {
            try {
                parsedTimestamp = Long.parseLong(offsetMap.get("future_timestamp"));
                log.info(
                        "Configured with future timestamp filter: {} ({})",
                        parsedTimestamp,
                        formatTimestamp(parsedTimestamp));
            } catch (NumberFormatException e) {
                log.warn(
                        "Invalid future_timestamp format '{}', timestamp filtering will be disabled",
                        offsetMap.get("future_timestamp"));
            }
        }
        this.futureTimestamp = parsedTimestamp;
    }

    @Override
    public void execute(FetchTask.Context context) throws Exception {
        MySqlSourceFetchTaskContext sourceFetchContext = (MySqlSourceFetchTaskContext) context;
        taskRunning = true;

        // Create a custom event listener to filter events before a specific timestamp
        TimestampFilteringMySqlStreamingChangeEventSource changeEventSource =
                new TimestampFilteringMySqlStreamingChangeEventSource(
                        sourceFetchContext.getDbzConnectorConfig(),
                        sourceFetchContext.getConnection(),
                        sourceFetchContext.getDispatcher(),
                        sourceFetchContext.getErrorHandler(),
                        Clock.SYSTEM,
                        sourceFetchContext.getTaskContext(),
                        sourceFetchContext.getStreamingChangeEventSourceMetrics(),
                        futureTimestamp);

        BinlogSplitChangeEventSourceContext changeEventSourceContext =
                new BinlogSplitChangeEventSourceContext();

        // Register a connection callback, close the idle JDBC connection after the connection is
        // established to save resources
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
                                    throw new RuntimeException(
                                            "Failed to close JDBC connection", e);
                                }
                            }
                        });

        // start Performing Event Source Reading
        log.info(
                "Starting MySQL binlog reader{}",
                futureTimestamp != null
                        ? " with timestamp filter: " + formatTimestamp(futureTimestamp)
                        : "");

        changeEventSource.execute(
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
        if (taskRunning) {
            log.info(
                    "Shutting down MySQL binlog reader. Filtered events count: {}",
                    filteredEventsCount.get());
            taskRunning = false;
        }
    }

    @Override
    public SourceSplitBase getSplit() {
        return split;
    }

    /** Format the timestamp to a human-readable date-time string. */
    private String formatTimestamp(long timestamp) {
        LocalDateTime dateTime =
                LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.systemDefault());
        return dateTime.format(DATE_TIME_FORMATTER);
    }

    /** Inner context for passing running status. */
    private class BinlogSplitChangeEventSourceContext
            implements ChangeEventSource.ChangeEventSourceContext {
        @Override
        public boolean isRunning() {
            return taskRunning;
        }
    }

    /**
     * Extended MySQL streaming change event source that supports filtering events based on
     * timestamp.
     */
    private class TimestampFilteringMySqlStreamingChangeEventSource
            extends MySqlStreamingChangeEventSource {
        private final Long targetTimestamp;
        private Long lastProcessedEventTimestamp = null;

        public TimestampFilteringMySqlStreamingChangeEventSource(
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
                return;
            }

            // Get the timestamp of the event
            final EventHeader eventHeader = event.getHeader();
            long eventTs = 0;
            if (!eventHeader.getEventType().equals(EventType.HEARTBEAT)) {
                eventTs = eventHeader.getTimestamp();
            }

            // If there is no timestamp information, process the event directly
            if (eventTs == 0 || targetTimestamp == null) {
                super.handleEvent(partition, offsetContext, event);
                return;
            }

            // For future timestamp settings, check if the target timestamp has been reached
            boolean shouldSkip = eventTs < targetTimestamp;

            if (shouldSkip) {
                // Log the first waiting message
                if (!loggedWaitingMessage) {
                    long currentTimeMillis = System.currentTimeMillis();
                    log.info(
                            "Waiting for events with timestamp >= {} (current event: {}, current time: {})",
                            formatTimestamp(targetTimestamp),
                            formatTimestamp(eventTs),
                            formatTimestamp(currentTimeMillis));
                    loggedWaitingMessage = true;
                    lastLogTimeMs = System.currentTimeMillis();
                }

                // Periodically print waiting status (to avoid excessive logging)
                long currentTimeMs = System.currentTimeMillis();
                if (currentTimeMs - lastLogTimeMs >= LOG_INTERVAL_MS) {
                    log.info(
                            "Still waiting for events with timestamp >= {} (latest event: {}, current time: {}, filtered events: {})",
                            formatTimestamp(targetTimestamp),
                            formatTimestamp(eventTs),
                            formatTimestamp(currentTimeMs),
                            filteredEventsCount.incrementAndGet());
                    lastLogTimeMs = currentTimeMs;
                } else {
                    // Do not print log but increase the count
                    filteredEventsCount.incrementAndGet();
                }

                // Skip processing this event, only update the offset to track the position
                updateOffsetPosition(offsetContext, eventHeader);
                return;
            }

            // When the target timestamp is reached or exceeded, print a notification message
            if (loggedWaitingMessage) {
                long currentTimeMillis = System.currentTimeMillis();
                log.info(
                        "Target timestamp {} reached! Event timestamp: {}, current time: {}, filtered events: {}",
                        formatTimestamp(targetTimestamp),
                        formatTimestamp(eventTs),
                        formatTimestamp(currentTimeMillis),
                        filteredEventsCount.get());
                loggedWaitingMessage = false;
            }

            // Record the timestamp of the last processed event (for debugging)
            lastProcessedEventTimestamp = eventTs;

            // Process the event normally
            super.handleEvent(partition, offsetContext, event);
        }

        /** Update the offset position information without processing the event content. */
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
}
