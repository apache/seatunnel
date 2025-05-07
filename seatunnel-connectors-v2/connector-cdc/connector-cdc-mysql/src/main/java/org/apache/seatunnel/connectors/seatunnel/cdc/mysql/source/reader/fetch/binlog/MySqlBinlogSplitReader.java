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

        // 检查是否有未来时间戳设置
        BinlogOffset offset = (BinlogOffset) split.getStartupOffset();
        Map<String, String> offsetMap = offset.getOffset();

        // 解析未来时间戳配置
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

        // 创建一个自定义的事件监听器，用于过滤特定时间戳之前的事件
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

        // 注册连接回调，在连接建立后关闭空闲的JDBC连接以节省资源
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

        // 开始执行事件源读取
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

    /** 格式化时间戳为人类可读的日期时间字符串 */
    private String formatTimestamp(long timestamp) {
        LocalDateTime dateTime =
                LocalDateTime.ofInstant(Instant.ofEpochMilli(timestamp), ZoneId.systemDefault());
        return dateTime.format(DATE_TIME_FORMATTER);
    }

    /** 内部上下文，用于传递运行状态 */
    private class BinlogSplitChangeEventSourceContext
            implements ChangeEventSource.ChangeEventSourceContext {
        @Override
        public boolean isRunning() {
            return taskRunning;
        }
    }

    /** 扩展的MySQL流式事件源，支持基于时间戳过滤事件 */
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

            // 获取事件的时间戳
            final EventHeader eventHeader = event.getHeader();
            long eventTs = 0;
            if (!eventHeader.getEventType().equals(EventType.HEARTBEAT)) {
                eventTs = eventHeader.getTimestamp();
            }

            // 如果没有时间戳信息，直接处理事件
            if (eventTs == 0 || targetTimestamp == null) {
                super.handleEvent(partition, offsetContext, event);
                return;
            }

            // 对于未来时间戳设置，检查是否已达到目标时间点
            boolean shouldSkip = eventTs < targetTimestamp;

            if (shouldSkip) {
                // 记录第一条等待消息
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

                // 周期性打印等待状态（避免日志过多）
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
                    // 不打印日志但要增加计数
                    filteredEventsCount.incrementAndGet();
                }

                // 跳过处理这个事件，只更新偏移量来跟踪位置
                updateOffsetPosition(offsetContext, eventHeader);
                return;
            }

            // 当到达或超过目标时间戳时，打印通知消息
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

            // 记录最后处理的事件时间戳（用于调试）
            lastProcessedEventTimestamp = eventTs;

            // 正常处理事件
            super.handleEvent(partition, offsetContext, event);
        }

        /** 更新偏移量位置信息，但不处理事件内容 */
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
