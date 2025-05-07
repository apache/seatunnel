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

package org.apache.seatunnel.connectors.seatunnel.cdc.mysql.source.offset;

import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.cdc.base.dialect.JdbcDataSourceDialect;
import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;
import org.apache.seatunnel.connectors.cdc.base.source.offset.OffsetFactory;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.config.MySqlSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.config.MySqlSourceConfigFactory;
import org.apache.seatunnel.connectors.seatunnel.cdc.mysql.utils.MySqlConnectionUtils;

import io.debezium.jdbc.JdbcConnection;
import lombok.extern.slf4j.Slf4j;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** An offset factory class create {@link BinlogOffset} instance. */
@Slf4j
public class BinlogOffsetFactory extends OffsetFactory {

    private final MySqlSourceConfig sourceConfig;

    private final JdbcDataSourceDialect dialect;

    public BinlogOffsetFactory(
            MySqlSourceConfigFactory configFactory, JdbcDataSourceDialect dialect) {
        this.sourceConfig = configFactory.create(0);
        this.dialect = dialect;
    }

    @Override
    public Offset earliest() {
        try (JdbcConnection jdbcConnection = dialect.openJdbcConnection(sourceConfig)) {
            return MySqlConnectionUtils.earliestBinlogOffset(jdbcConnection);
        } catch (Exception e) {
            throw new RuntimeException("Read the binlog offset error", e);
        }
    }

    @Override
    public Offset neverStop() {
        return BinlogOffset.NO_STOPPING_OFFSET;
    }

    @Override
    public Offset latest() {
        try (JdbcConnection jdbcConnection = dialect.openJdbcConnection(sourceConfig)) {
            return MySqlConnectionUtils.currentBinlogOffset(jdbcConnection);
        } catch (Exception e) {
            throw new RuntimeException("Read the binlog offset error", e);
        }
    }

    @Override
    public Offset specific(Map<String, String> offset) {
        return new BinlogOffset(offset);
    }

    @Override
    public Offset specific(String filename, Long position) {
        return new BinlogOffset(filename, position);
    }

    @Override
    public Offset timestamp(long timestamp) {
        try (JdbcConnection jdbcConnection = dialect.openJdbcConnection(sourceConfig)) {
            BinlogOffset earliestOffset = (BinlogOffset) earliest();
            BinlogOffset latestOffset = (BinlogOffset) latest();

            // 检查请求的时间戳是否早于最早的可用binlog
            long earliestTimestamp = getEarliestTimestampInBinlog(jdbcConnection);
            if (timestamp < earliestTimestamp) {
                String earliestTimeStr = formatTimestamp(earliestTimestamp);
                log.error(
                        "Requested start time ({}) is earlier than earliest available binlog timestamp ({})",
                        formatTimestamp(timestamp),
                        earliestTimeStr);
                throw new SeaTunnelException(
                        String.format(
                                "Requested start time is earlier than earliest available binlog timestamp. Earliest available: %s",
                                earliestTimeStr));
            }

            // 检查请求的时间戳是否晚于当前时间
            long currentTimestamp = System.currentTimeMillis();
            if (timestamp > currentTimestamp) {
                log.info(
                        "Requested start time {} is in the future (current time: {}). Creating a future timestamp offset.",
                        formatTimestamp(timestamp),
                        formatTimestamp(currentTimestamp));
                // 创建一个带有未来时间戳标记的特殊偏移量，而不是简单返回最新位置
                return createFutureTimestampOffset(latestOffset, timestamp);
            }

            // 查找指定时间点的binlog位置
            log.info("Finding binlog position for timestamp: {}", formatTimestamp(timestamp));
            BinlogOffset result = findOffsetByTimestamp(jdbcConnection, timestamp);
            log.info(
                    "Found binlog position for timestamp {}: {}",
                    formatTimestamp(timestamp),
                    result);
            return result;
        } catch (SQLException e) {
            log.error("Failed to read binlog offset by timestamp: {}", e.getMessage());
            throw new SeaTunnelException("Failed to read binlog offset by timestamp", e);
        }
    }

    /** 创建一个带有未来时间戳标记的特殊偏移量 */
    private BinlogOffset createFutureTimestampOffset(
            BinlogOffset latestOffset, long targetTimestamp) {
        // 创建一个新的偏移量，并添加目标时间戳信息
        Map<String, String> offsetMap = new HashMap<>(latestOffset.getOffset());
        // 添加特殊标记，表示这是一个未来时间戳偏移量
        offsetMap.put("future_timestamp", String.valueOf(targetTimestamp));
        offsetMap.put("is_future_timestamp", "true");
        return new BinlogOffset(offsetMap);
    }

    /** 获取最早的binlog时间戳 */
    private long getEarliestTimestampInBinlog(JdbcConnection jdbcConnection) throws SQLException {
        String firstBinlogFilename = null;

        // 获取第一个binlog文件名
        try (Statement statement = jdbcConnection.connection().createStatement()) {
            try (ResultSet rs = statement.executeQuery("SHOW BINARY LOGS")) {
                if (rs.next()) {
                    firstBinlogFilename = rs.getString("Log_name");
                } else {
                    log.warn("No binary logs found on the server");
                    throw new SeaTunnelException("No binary logs available on the MySQL server");
                }
            }
        }

        // 首先尝试从information_schema.files获取binlog文件的创建时间
        try (Statement statement = jdbcConnection.connection().createStatement()) {
            try {
                String query =
                        String.format(
                                "SELECT UNIX_TIMESTAMP(CREATED) * 1000 as create_time "
                                        + "FROM information_schema.files "
                                        + "WHERE FILE_NAME LIKE '%%/%s'",
                                firstBinlogFilename);

                try (ResultSet rs = statement.executeQuery(query)) {
                    if (rs.next() && rs.getObject("create_time") != null) {
                        long timestamp = rs.getLong("create_time");
                        log.debug(
                                "Found creation time for binlog file {}: {}",
                                firstBinlogFilename,
                                formatTimestamp(timestamp));
                        return timestamp;
                    }
                }
            } catch (SQLException e) {
                log.warn(
                        "Failed to get creation time from information_schema.files: {}",
                        e.getMessage());
            }
        }

        // 如果无法从information_schema.files获取，尝试从binlog事件获取
        try (Statement statement = jdbcConnection.connection().createStatement()) {
            try {
                String query =
                        String.format("SHOW BINLOG EVENTS IN '%s' LIMIT 1", firstBinlogFilename);
                try (ResultSet rs = statement.executeQuery(query)) {
                    if (rs.next()) {
                        String eventTime = rs.getString("Timestamp");
                        if (eventTime != null) {
                            try {
                                SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                Date eventDate = sdf.parse(eventTime);
                                long timestamp = eventDate.getTime();
                                log.debug(
                                        "Found first event time for binlog file {}: {}",
                                        firstBinlogFilename,
                                        formatTimestamp(timestamp));
                                return timestamp;
                            } catch (ParseException e) {
                                log.warn(
                                        "Failed to parse binlog event timestamp: {}",
                                        e.getMessage());
                            }
                        }
                    }
                }
            } catch (SQLException e) {
                log.warn("Failed to get first binlog event: {}", e.getMessage());
            }
        }

        // 如果所有尝试都失败，返回一个保守的估计时间（默认假设binlog保留时间为1天）
        long defaultBinlogRetentionHours = 24;
        long estimatedTime =
                System.currentTimeMillis() - (defaultBinlogRetentionHours * 3600 * 1000);
        log.warn(
                "Could not determine earliest binlog timestamp, using conservative estimate: {}",
                formatTimestamp(estimatedTime));
        return estimatedTime;
    }

    /** 格式化时间戳为可读字符串 */
    private String formatTimestamp(long timestamp) {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(timestamp));
    }

    private BinlogOffset findOffsetByTimestamp(JdbcConnection jdbcConnection, long timestamp)
            throws SQLException {
        // 获取所有可用的binlog文件
        Statement statement = jdbcConnection.connection().createStatement();
        List<BinlogInfo> binlogInfoList = new ArrayList<>();

        try (ResultSet rs = statement.executeQuery("SHOW BINARY LOGS")) {
            while (rs.next()) {
                String binlogFilename = rs.getString("Log_name");
                long fileSize = rs.getLong("File_size");
                binlogInfoList.add(new BinlogInfo(binlogFilename, fileSize));
            }
        }

        if (binlogInfoList.isEmpty()) {
            throw new SeaTunnelException("No binary logs available on the MySQL server");
        }

        log.info("Found {} binlog files to search", binlogInfoList.size());

        // 检查MySQL是否支持 SHOW BINLOG EVENTS 语句的时间戳过滤
        boolean supportsTimestampQuery =
                checkTimestampQuerySupport(
                        jdbcConnection.connection(), binlogInfoList.get(0).filename);

        if (supportsTimestampQuery) {
            // 使用 SHOW BINLOG EVENTS 直接查找对应时间戳的位置
            log.info("MySQL server supports timestamp filtering. Using direct timestamp query.");
            return findOffsetUsingBinlogEvents(
                    jdbcConnection.connection(), binlogInfoList, timestamp);
        } else {
            // 如果不支持时间戳过滤，使用更基础的方法
            log.info(
                    "MySQL server doesn't support timestamp filtering in SHOW BINLOG EVENTS. Using a fallback approach.");

            // 获取binlog文件的创建和修改时间信息
            BinlogTimestampMapper timestampMapper =
                    buildBinlogTimestampMapper(
                            jdbcConnection.connection(), binlogInfoList, timestamp);

            if (timestampMapper.hasFoundExactPosition()) {
                // 已经找到了准确的位置
                log.info(
                        "Found exact position for timestamp {} in binlog",
                        formatTimestamp(timestamp));
                return timestampMapper.getExactPosition();
            }

            // 如果未找到准确位置，确定开始读取的binlog文件
            if (timestampMapper.isTargetTimeInFuture()) {
                // 目标时间点在当前最新binlog之后，返回最新位置等待新数据
                BinlogOffset latestOffset = (BinlogOffset) latest();
                log.info(
                        "Target timestamp {} is after the latest binlog event. Using latest position {} and waiting for future events.",
                        formatTimestamp(timestamp),
                        latestOffset);
                return latestOffset;
            } else {
                // 找到第一个可能包含目标时间戳之后数据的binlog文件
                BinlogOffset estimatedPosition = timestampMapper.getEstimatedPosition();
                log.info(
                        "Using estimated position for timestamp {}: file={}, position={}",
                        formatTimestamp(timestamp),
                        estimatedPosition.getFilename(),
                        estimatedPosition.getPosition());
                return estimatedPosition;
            }
        }
    }

    /** 检查MySQL是否支持带时间戳过滤的SHOW BINLOG EVENTS */
    private boolean checkTimestampQuerySupport(Connection connection, String sampleBinlogFile) {
        try (Statement statement = connection.createStatement()) {
            // 执行一个测试查询来检查是否支持带时间戳过滤的SHOW BINLOG EVENTS
            try {
                String query =
                        String.format(
                                "SHOW BINLOG EVENTS IN '%s' WHERE UNIX_TIMESTAMP(TIMESTAMP) >= UNIX_TIMESTAMP('2000-01-01 00:00:00') LIMIT 1",
                                sampleBinlogFile);
                statement.executeQuery(query);
                log.debug("MySQL server supports timestamp filtering in SHOW BINLOG EVENTS");
                return true;
            } catch (SQLException e) {
                log.debug(
                        "MySQL server does not support timestamp filtering in SHOW BINLOG EVENTS: {}",
                        e.getMessage());
                return false;
            }
        } catch (SQLException e) {
            log.warn("Failed to check timestamp query support: {}", e.getMessage());
            return false;
        }
    }

    private BinlogOffset findOffsetUsingBinlogEvents(
            Connection connection, List<BinlogInfo> binlogInfoList, long timestamp)
            throws SQLException {
        // 将时间戳转换为MySQL日期时间字符串
        String timestampStr = formatTimestampForMysql(timestamp);

        log.info("Searching for binlog position with timestamp >= {}", timestampStr);

        // 按照时间顺序从早到晚遍历binlog文件
        for (int i = 0; i < binlogInfoList.size(); i++) {
            BinlogInfo binlogInfo = binlogInfoList.get(i);

            try (Statement statement = connection.createStatement()) {
                // 设置语句超时，避免大文件查询超时
                statement.setQueryTimeout(60); // 60秒超时

                // 查询binlog文件中第一个时间戳大于等于目标时间戳的事件
                String query =
                        String.format(
                                "SHOW BINLOG EVENTS IN '%s' WHERE UNIX_TIMESTAMP(TIMESTAMP) >= UNIX_TIMESTAMP('%s') ORDER BY Position ASC LIMIT 1",
                                binlogInfo.filename, timestampStr);

                try (ResultSet rs = statement.executeQuery(query)) {
                    if (rs.next()) {
                        // 找到匹配的事件
                        long position = rs.getLong("Position");
                        String eventTimestamp = rs.getString("Timestamp");
                        log.info(
                                "Found matching event in binlog file {} at position {} (event time: {})",
                                binlogInfo.filename,
                                position,
                                eventTimestamp);
                        return new BinlogOffset(binlogInfo.filename, position);
                    } else {
                        // 此binlog文件没有满足条件的事件，继续检查下一个文件
                        log.debug(
                                "No events found after {} in file {}",
                                timestampStr,
                                binlogInfo.filename);
                    }
                } catch (SQLException e) {
                    // MySQL 5.6及之前的版本可能不支持直接在WHERE子句中使用TIMESTAMP比较
                    // 使用另一种方式查询
                    log.warn(
                            "Failed to query binlog events with timestamp comparison. Trying manual approach: {}",
                            e.getMessage());

                    // 手动遍历所有事件并比较时间戳
                    try {
                        return findPositionByManualSearch(connection, binlogInfo, timestamp);
                    } catch (SQLException fallbackException) {
                        log.warn(
                                "Also failed with manual approach for file {}: {}",
                                binlogInfo.filename,
                                fallbackException.getMessage());
                    }
                }
            }
        }

        // 如果所有文件中都没有找到匹配的事件，则使用最新的binlog位置
        // 这意味着指定的时间戳在最新的binlog之后，需要等待新数据产生
        try (JdbcConnection jdbcConnection = dialect.openJdbcConnection(sourceConfig)) {
            BinlogOffset latestOffset = (BinlogOffset) latest();
            log.info(
                    "Could not find any events after timestamp {} in existing binlog files. Using latest binlog position {} and will wait for future events.",
                    timestampStr,
                    latestOffset);
            return latestOffset;
        } catch (Exception e) {
            log.error("Failed to get latest binlog position", e);
            // 如果获取最新位置失败，使用最后一个binlog文件的末尾位置
            String lastBinlogFile = binlogInfoList.get(binlogInfoList.size() - 1).filename;
            long lastPosition = binlogInfoList.get(binlogInfoList.size() - 1).fileSize;
            log.info(
                    "Using end of last binlog file as fallback: {} position {}",
                    lastBinlogFile,
                    lastPosition);
            return new BinlogOffset(lastBinlogFile, lastPosition);
        }
    }

    /** 手动遍历binlog事件查找匹配的时间戳位置 */
    private BinlogOffset findPositionByManualSearch(
            Connection connection, BinlogInfo binlogInfo, long searchTimestamp)
            throws SQLException {
        log.info(
                "Manually searching binlog file {} for events matching timestamp {}",
                binlogInfo.filename,
                formatTimestamp(searchTimestamp));

        try (Statement statement = connection.createStatement()) {
            String query = String.format("SHOW BINLOG EVENTS IN '%s'", binlogInfo.filename);

            try (ResultSet rs = statement.executeQuery(query)) {
                while (rs.next()) {
                    String eventTime = rs.getString("Timestamp");
                    long position = rs.getLong("Position");

                    try {
                        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        long eventTimestamp = sdf.parse(eventTime).getTime();

                        if (eventTimestamp >= searchTimestamp) {
                            log.info(
                                    "Found matching event at position {} with timestamp {}",
                                    position,
                                    eventTime);
                            return new BinlogOffset(binlogInfo.filename, position);
                        }
                    } catch (ParseException e) {
                        log.warn(
                                "Failed to parse event timestamp '{}': {}",
                                eventTime,
                                e.getMessage());
                    }
                }
            }
        }

        // 如果在此文件中没有找到匹配的事件，返回下一个binlog文件的起始位置（如果有）
        log.debug("No matching events found in {}", binlogInfo.filename);
        return new BinlogOffset(binlogInfo.filename, 4L); // 4是binlog文件头部后的初始位置
    }

    private String formatTimestampForMysql(long timestamp) {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(timestamp));
    }

    private BinlogTimestampMapper buildBinlogTimestampMapper(
            Connection connection, List<BinlogInfo> binlogInfoList, long targetTimestamp) {
        BinlogTimestampMapper mapper = new BinlogTimestampMapper(targetTimestamp);

        // 1. 从information_schema.files获取binlog文件的创建和修改时间
        Map<String, BinlogTimestampInfo> binlogTimestamps =
                getBinlogTimestamps(connection, binlogInfoList);

        // 2. 通过分析每个binlog文件中的第一个和最后一个事件获取更精确的时间信息
        enrichBinlogTimestampsWithEvents(connection, binlogInfoList, binlogTimestamps);

        // 3. 如果仍有不足的时间信息，进行估算
        if (hasIncompleteTimestamps(binlogTimestamps, binlogInfoList)) {
            estimateRemainingTimestamps(binlogTimestamps, binlogInfoList);
        }

        // 4. 精确找到目标时间戳所在的位置
        try {
            for (BinlogInfo binlogInfo : binlogInfoList) {
                BinlogTimestampInfo timestampInfo = binlogTimestamps.get(binlogInfo.filename);
                if (timestampInfo == null) continue;

                // 检查当前binlog文件是否可能包含目标时间戳的事件
                if (timestampInfo.hasTimestampRange()) {
                    if (targetTimestamp >= timestampInfo.firstEventTime
                            && targetTimestamp <= timestampInfo.lastEventTime) {
                        // 目标时间戳在此binlog文件的时间范围内，找到精确位置
                        BinlogOffset position =
                                findExactPositionInFile(
                                        connection, binlogInfo.filename, targetTimestamp);
                        if (position != null) {
                            mapper.setExactPosition(position);
                            return mapper;
                        }
                    } else if (targetTimestamp < timestampInfo.firstEventTime) {
                        // 目标时间戳早于此binlog文件的第一个事件，应该从这个文件开始读取
                        mapper.setEstimatedPosition(new BinlogOffset(binlogInfo.filename, 4L));
                        return mapper;
                    }
                    // 如果目标时间戳晚于此binlog文件的最后一个事件，继续检查下一个文件
                }
            }

            // 如果遍历完所有文件仍未找到合适的文件，说明目标时间戳在最新binlog之后
            BinlogInfo lastBinlog = binlogInfoList.get(binlogInfoList.size() - 1);
            BinlogTimestampInfo lastTimestampInfo = binlogTimestamps.get(lastBinlog.filename);

            if (lastTimestampInfo != null && lastTimestampInfo.lastEventTime < targetTimestamp) {
                mapper.setTargetTimeInFuture(true);
            } else {
                // 回退策略：使用最后一个binlog文件
                mapper.setEstimatedPosition(new BinlogOffset(lastBinlog.filename, 4L));
            }
        } catch (Exception e) {
            log.warn(
                    "Error while trying to find exact position for timestamp {}: {}",
                    formatTimestamp(targetTimestamp),
                    e.getMessage());
            // 回退策略：使用第一个binlog文件
            mapper.setEstimatedPosition(new BinlogOffset(binlogInfoList.get(0).filename, 4L));
        }

        return mapper;
    }

    private BinlogOffset findExactPositionInFile(
            Connection connection, String binlogFilename, long targetTimestamp) {
        String timestampStr = formatTimestampForMysql(targetTimestamp);

        try (Statement statement = connection.createStatement()) {
            // 分析该binlog文件中的所有事件，找到第一个时间戳大于等于目标时间戳的事件
            String query = String.format("SHOW BINLOG EVENTS IN '%s'", binlogFilename);
            try (ResultSet rs = statement.executeQuery(query)) {
                while (rs.next()) {
                    String eventTime = rs.getString("Timestamp");
                    long eventPosition = rs.getLong("Position");

                    // 手动比较时间戳
                    try {
                        java.text.SimpleDateFormat sdf =
                                new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                        java.util.Date eventDate = sdf.parse(eventTime);
                        java.util.Date targetDate = new java.util.Date(targetTimestamp);

                        if (eventDate.getTime() >= targetTimestamp) {
                            log.info(
                                    "Found first event after timestamp {} in file {} at position {} (event time: {})",
                                    timestampStr,
                                    binlogFilename,
                                    eventPosition,
                                    eventTime);
                            return new BinlogOffset(binlogFilename, eventPosition);
                        }
                    } catch (Exception e) {
                        log.warn("Failed to parse timestamp: {}", e.getMessage());
                    }
                }
            }
        } catch (SQLException e) {
            log.warn(
                    "Failed to query binlog events for file {}: {}",
                    binlogFilename,
                    e.getMessage());
        }

        // 没有找到精确位置
        return null;
    }

    private boolean hasIncompleteTimestamps(
            Map<String, BinlogTimestampInfo> binlogTimestamps, List<BinlogInfo> binlogInfoList) {
        for (BinlogInfo binlogInfo : binlogInfoList) {
            BinlogTimestampInfo timestampInfo = binlogTimestamps.get(binlogInfo.filename);
            if (timestampInfo == null || !timestampInfo.hasTimestampRange()) {
                return true;
            }
        }
        return false;
    }

    private void estimateRemainingTimestamps(
            Map<String, BinlogTimestampInfo> binlogTimestamps, List<BinlogInfo> binlogInfoList) {
        // 找出所有有完整时间信息的binlog文件
        List<BinlogTimestampInfo> completeInfos = new ArrayList<>();
        for (BinlogInfo binlogInfo : binlogInfoList) {
            BinlogTimestampInfo info = binlogTimestamps.get(binlogInfo.filename);
            if (info != null && info.hasTimestampRange()) {
                completeInfos.add(info);
            }
        }

        if (completeInfos.isEmpty()) {
            // 如果没有完整信息的binlog文件，使用保守估计
            long currentTime = System.currentTimeMillis();
            long defaultRetentionHours = 24;
            long earliestEstimatedTime = currentTime - (defaultRetentionHours * 3600 * 1000);

            // 假设binlog文件均匀分布在时间线上
            long timeRange = currentTime - earliestEstimatedTime;
            long timeStep = binlogInfoList.size() > 1 ? timeRange / (binlogInfoList.size() - 1) : 0;

            for (int i = 0; i < binlogInfoList.size(); i++) {
                BinlogInfo binlogInfo = binlogInfoList.get(i);
                long estimatedFirstTime = earliestEstimatedTime + (i * timeStep);
                long estimatedLastTime =
                        i < binlogInfoList.size() - 1
                                ? earliestEstimatedTime + ((i + 1) * timeStep) - 1
                                : currentTime;

                binlogTimestamps.put(
                        binlogInfo.filename,
                        new BinlogTimestampInfo(
                                binlogInfo.filename, estimatedFirstTime, estimatedLastTime));
            }
        } else {
            // 根据现有完整信息进行插值估计
            // 这里使用简单的线性插值

            // 先排序已有的完整信息
            completeInfos.sort(Comparator.comparing(info -> info.binlogFilename));

            // 为缺失信息的binlog文件填充估计值
            for (int i = 0; i < binlogInfoList.size(); i++) {
                BinlogInfo binlogInfo = binlogInfoList.get(i);
                if (!binlogTimestamps.containsKey(binlogInfo.filename)
                        || !binlogTimestamps.get(binlogInfo.filename).hasTimestampRange()) {

                    // 找到最近的前后有完整信息的binlog文件
                    final BinlogTimestampInfo[] beforeRef = new BinlogTimestampInfo[1];
                    final BinlogTimestampInfo[] afterRef = new BinlogTimestampInfo[1];

                    for (int j = i - 1; j >= 0 && beforeRef[0] == null; j--) {
                        BinlogTimestampInfo info =
                                binlogTimestamps.get(binlogInfoList.get(j).filename);
                        if (info != null && info.hasTimestampRange()) {
                            beforeRef[0] = info;
                        }
                    }

                    for (int j = i + 1; j < binlogInfoList.size() && afterRef[0] == null; j++) {
                        BinlogTimestampInfo info =
                                binlogTimestamps.get(binlogInfoList.get(j).filename);
                        if (info != null && info.hasTimestampRange()) {
                            afterRef[0] = info;
                        }
                    }

                    final BinlogTimestampInfo before = beforeRef[0];
                    final BinlogTimestampInfo after = afterRef[0];

                    // 进行估计
                    long estimatedFirstTime;
                    long estimatedLastTime;

                    if (before != null && after != null) {
                        // 有前后信息，进行线性插值
                        int positionDiff =
                                binlogInfoList.indexOf(
                                                binlogInfoList.stream()
                                                        .filter(
                                                                info ->
                                                                        info.filename.equals(
                                                                                after.binlogFilename))
                                                        .findFirst()
                                                        .orElse(null))
                                        - binlogInfoList.indexOf(
                                                binlogInfoList.stream()
                                                        .filter(
                                                                info ->
                                                                        info.filename.equals(
                                                                                before.binlogFilename))
                                                        .findFirst()
                                                        .orElse(null));
                        int currentPosition =
                                i
                                        - binlogInfoList.indexOf(
                                                binlogInfoList.stream()
                                                        .filter(
                                                                info ->
                                                                        info.filename.equals(
                                                                                before.binlogFilename))
                                                        .findFirst()
                                                        .orElse(null));

                        double ratio = (double) currentPosition / positionDiff;
                        estimatedFirstTime =
                                (long)
                                        (before.lastEventTime
                                                + ratio
                                                        * (after.firstEventTime
                                                                - before.lastEventTime));
                        estimatedLastTime =
                                (long)
                                        (before.lastEventTime
                                                + (ratio + 1.0 / positionDiff)
                                                        * (after.firstEventTime
                                                                - before.lastEventTime));
                    } else if (before != null) {
                        // 只有前面的信息
                        estimatedFirstTime = before.lastEventTime + 1;
                        estimatedLastTime = System.currentTimeMillis(); // 保守估计用当前时间
                    } else if (after != null) {
                        // 只有后面的信息
                        estimatedLastTime = after.firstEventTime - 1;
                        // 保守估计用24小时前
                        estimatedFirstTime = Math.max(0, estimatedLastTime - (24 * 3600 * 1000));
                    } else {
                        // 没有任何信息，使用保守估计
                        estimatedLastTime = System.currentTimeMillis();
                        estimatedFirstTime = Math.max(0, estimatedLastTime - (24 * 3600 * 1000));
                    }

                    binlogTimestamps.put(
                            binlogInfo.filename,
                            new BinlogTimestampInfo(
                                    binlogInfo.filename, estimatedFirstTime, estimatedLastTime));
                }
            }
        }
    }

    private void enrichBinlogTimestampsWithEvents(
            Connection connection,
            List<BinlogInfo> binlogInfoList,
            Map<String, BinlogTimestampInfo> binlogTimestamps) {
        for (BinlogInfo binlogInfo : binlogInfoList) {
            BinlogTimestampInfo timestampInfo = binlogTimestamps.get(binlogInfo.filename);
            if (timestampInfo == null) {
                timestampInfo = new BinlogTimestampInfo(binlogInfo.filename);
                binlogTimestamps.put(binlogInfo.filename, timestampInfo);
            }

            // 读取文件中的第一个和最后一个事件的时间戳
            try (Statement statement = connection.createStatement()) {
                // 第一个事件
                try (ResultSet rs =
                        statement.executeQuery(
                                String.format(
                                        "SHOW BINLOG EVENTS IN '%s' LIMIT 1",
                                        binlogInfo.filename))) {
                    if (rs.next()) {
                        String eventTime = rs.getString("Timestamp");
                        try {
                            java.text.SimpleDateFormat sdf =
                                    new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                            java.util.Date eventDate = sdf.parse(eventTime);
                            timestampInfo.firstEventTime = eventDate.getTime();
                        } catch (Exception e) {
                            log.warn(
                                    "Failed to parse first event timestamp for {}: {}",
                                    binlogInfo.filename,
                                    e.getMessage());
                        }
                    }
                } catch (SQLException e) {
                    log.warn(
                            "Failed to query first binlog event for {}: {}",
                            binlogInfo.filename,
                            e.getMessage());
                }

                // 最后一个事件
                try (ResultSet rs =
                        statement.executeQuery(
                                String.format(
                                        "SHOW BINLOG EVENTS IN '%s' LIMIT 1000000000 OFFSET 1000000000",
                                        binlogInfo.filename))) {
                    if (rs.next()) {
                        String eventTime = rs.getString("Timestamp");
                        try {
                            java.text.SimpleDateFormat sdf =
                                    new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                            java.util.Date eventDate = sdf.parse(eventTime);
                            timestampInfo.lastEventTime = eventDate.getTime();
                        } catch (Exception e) {
                            log.warn(
                                    "Failed to parse last event timestamp for {}: {}",
                                    binlogInfo.filename,
                                    e.getMessage());
                        }
                    }
                } catch (SQLException e) {
                    // 另一种方式：从末尾读取少量事件
                    try (ResultSet rs =
                            statement.executeQuery(
                                    String.format(
                                            "SHOW BINLOG EVENTS IN '%s' LIMIT 10 OFFSET 1000000",
                                            binlogInfo.filename))) {
                        String latestEventTime = null;
                        while (rs.next()) {
                            latestEventTime = rs.getString("Timestamp");
                        }
                        if (latestEventTime != null) {
                            try {
                                java.text.SimpleDateFormat sdf =
                                        new java.text.SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                                java.util.Date eventDate = sdf.parse(latestEventTime);
                                timestampInfo.lastEventTime = eventDate.getTime();
                            } catch (Exception e2) {
                                log.warn(
                                        "Failed to parse last event timestamp for {}: {}",
                                        binlogInfo.filename,
                                        e2.getMessage());
                            }
                        }
                    } catch (SQLException e2) {
                        log.warn(
                                "Failed to query last binlog event for {}: {}",
                                binlogInfo.filename,
                                e2.getMessage());
                    }
                }
            } catch (SQLException e) {
                log.warn("Failed to create statement: {}", e.getMessage());
            }
        }
    }

    private Map<String, BinlogTimestampInfo> getBinlogTimestamps(
            Connection connection, List<BinlogInfo> binlogInfoList) {
        Map<String, BinlogTimestampInfo> timestamps = new HashMap<>();

        try (Statement statement = connection.createStatement()) {
            // 从information_schema.files获取binlog文件的创建时间
            for (BinlogInfo binlogInfo : binlogInfoList) {
                String query =
                        String.format(
                                "SELECT "
                                        + "UNIX_TIMESTAMP(CREATED) * 1000 as create_time, "
                                        + "UNIX_TIMESTAMP(LAST_UPDATE) * 1000 as update_time "
                                        + "FROM information_schema.files "
                                        + "WHERE FILE_NAME LIKE '%%/%s'",
                                binlogInfo.filename);

                try (ResultSet rs = statement.executeQuery(query)) {
                    if (rs.next() && rs.getObject("create_time") != null) {
                        long createTime = rs.getLong("create_time");
                        long updateTime =
                                rs.getObject("update_time") != null
                                        ? rs.getLong("update_time")
                                        : createTime;

                        timestamps.put(
                                binlogInfo.filename,
                                new BinlogTimestampInfo(
                                        binlogInfo.filename, createTime, updateTime));
                    }
                } catch (SQLException e) {
                    log.warn(
                            "Failed to get timestamp for binlog file {}: {}",
                            binlogInfo.filename,
                            e.getMessage());
                }
            }
        } catch (SQLException e) {
            log.warn(
                    "Failed to query binlog timestamps from information_schema: {}",
                    e.getMessage());
        }

        return timestamps;
    }

    private static class BinlogTimestampInfo {
        final String binlogFilename;
        Long createTime;
        Long updateTime;
        Long firstEventTime;
        Long lastEventTime;

        public BinlogTimestampInfo(String binlogFilename) {
            this.binlogFilename = binlogFilename;
        }

        public BinlogTimestampInfo(String binlogFilename, Long createTime, Long updateTime) {
            this.binlogFilename = binlogFilename;
            this.createTime = createTime;
            this.updateTime = updateTime;
        }

        public boolean hasTimestampRange() {
            return firstEventTime != null && lastEventTime != null;
        }
    }

    private static class BinlogTimestampMapper {
        private final long targetTimestamp;
        private BinlogOffset exactPosition;
        private BinlogOffset estimatedPosition;
        private boolean targetTimeInFuture;

        public BinlogTimestampMapper(long targetTimestamp) {
            this.targetTimestamp = targetTimestamp;
        }

        public boolean hasFoundExactPosition() {
            return exactPosition != null;
        }

        public BinlogOffset getExactPosition() {
            return exactPosition;
        }

        public void setExactPosition(BinlogOffset exactPosition) {
            this.exactPosition = exactPosition;
        }

        public BinlogOffset getEstimatedPosition() {
            return estimatedPosition;
        }

        public void setEstimatedPosition(BinlogOffset estimatedPosition) {
            this.estimatedPosition = estimatedPosition;
        }

        public boolean isTargetTimeInFuture() {
            return targetTimeInFuture;
        }

        public void setTargetTimeInFuture(boolean targetTimeInFuture) {
            this.targetTimeInFuture = targetTimeInFuture;
        }
    }

    private static class BinlogInfo {
        private final String filename;
        private final long fileSize;

        public BinlogInfo(String filename, long fileSize) {
            this.filename = filename;
            this.fileSize = fileSize;
        }
    }
}
