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

            // Check if the requested timestamp is earlier than the earliest available binlog
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

            // Check if the requested timestamp is later than the current time
            long currentTimestamp = System.currentTimeMillis();
            if (timestamp > currentTimestamp) {
                log.info(
                        "Requested start time {} is in the future (current time: {}). Creating a future timestamp offset.",
                        formatTimestamp(timestamp),
                        formatTimestamp(currentTimestamp));
                // Create a special offset with a future timestamp marker instead of simply
                // returning the latest position
                return createFutureTimestampOffset(latestOffset, timestamp);
            }

            // Find the binlog position for the specified timestamp
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

    /** Create a special offset with a future timestamp marker */
    private BinlogOffset createFutureTimestampOffset(
            BinlogOffset latestOffset, long targetTimestamp) {
        // Create a new offset and add the target timestamp information
        Map<String, String> offsetMap = new HashMap<>(latestOffset.getOffset());
        // Add a special marker to indicate that this is a future timestamp offset
        offsetMap.put("future_timestamp", String.valueOf(targetTimestamp));
        offsetMap.put("is_future_timestamp", "true");
        return new BinlogOffset(offsetMap);
    }

    /** Get the earliest timestamp in the binlog */
    private long getEarliestTimestampInBinlog(JdbcConnection jdbcConnection) throws SQLException {
        String firstBinlogFilename = null;

        // Get the name of the first binlog file
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

        // First try to get the creation time of the binlog file from information_schema.files
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

        // If unable to get from information_schema.files, try to get from binlog events
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

        // If all attempts fail, return a conservative estimate time (default assumption is binlog
        // retention time of 1 day)
        long defaultBinlogRetentionHours = 24;
        long estimatedTime =
                System.currentTimeMillis() - (defaultBinlogRetentionHours * 3600 * 1000);
        log.warn(
                "Could not determine earliest binlog timestamp, using conservative estimate: {}",
                formatTimestamp(estimatedTime));
        return estimatedTime;
    }

    /** Format the timestamp to a readable string */
    private String formatTimestamp(long timestamp) {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(timestamp));
    }

    private BinlogOffset findOffsetByTimestamp(JdbcConnection jdbcConnection, long timestamp)
            throws SQLException {
        // Get all available binlog files
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

        // Check if the MySQL server supports timestamp filtering in SHOW BINLOG EVENTS
        boolean supportsTimestampQuery =
                checkTimestampQuerySupport(
                        jdbcConnection.connection(), binlogInfoList.get(0).filename);

        if (supportsTimestampQuery) {
            // Use SHOW BINLOG EVENTS directly to find the corresponding position for the timestamp
            log.info("MySQL server supports timestamp filtering. Using direct timestamp query.");
            return findOffsetUsingBinlogEvents(
                    jdbcConnection.connection(), binlogInfoList, timestamp);
        } else {
            // If timestamp filtering is not supported, use a fallback approach
            log.info(
                    "MySQL server doesn't support timestamp filtering in SHOW BINLOG EVENTS. Using a fallback approach.");

            // Get the creation and modification time information of the binlog files
            BinlogTimestampMapper timestampMapper =
                    buildBinlogTimestampMapper(
                            jdbcConnection.connection(), binlogInfoList, timestamp);

            if (timestampMapper.hasFoundExactPosition()) {
                // The exact position has been found
                log.info(
                        "Found exact position for timestamp {} in binlog",
                        formatTimestamp(timestamp));
                return timestampMapper.getExactPosition();
            }

            // If the exact position is not found, determine the starting binlog file for reading
            if (timestampMapper.isTargetTimeInFuture()) {
                // The target timestamp is after the latest binlog event, return the latest position
                // and wait for future events
                BinlogOffset latestOffset = (BinlogOffset) latest();
                log.info(
                        "Target timestamp {} is after the latest binlog event. Using latest position {} and waiting for future events.",
                        formatTimestamp(timestamp),
                        latestOffset);
                return latestOffset;
            } else {
                // Find the first binlog file that may contain data after the target timestamp
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

    /** Check if the MySQL server supports timestamp filtering in SHOW BINLOG EVENTS */
    private boolean checkTimestampQuerySupport(Connection connection, String sampleBinlogFile) {
        try (Statement statement = connection.createStatement()) {
            // Execute a test query to check if timestamp filtering is supported
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
        // Convert the timestamp to a MySQL date-time string
        String timestampStr = formatTimestampForMysql(timestamp);

        log.info("Searching for binlog position with timestamp >= {}", timestampStr);

        // Traverse the binlog files in chronological order from earliest to latest
        for (int i = 0; i < binlogInfoList.size(); i++) {
            BinlogInfo binlogInfo = binlogInfoList.get(i);

            try (Statement statement = connection.createStatement()) {
                // Set the statement timeout to avoid timeout for large files
                statement.setQueryTimeout(60); // 60-second timeout

                // Query the first event in the binlog file with a timestamp greater than or equal
                // to the target timestamp
                String query =
                        String.format(
                                "SHOW BINLOG EVENTS IN '%s' WHERE UNIX_TIMESTAMP(TIMESTAMP) >= UNIX_TIMESTAMP('%s') ORDER BY Position ASC LIMIT 1",
                                binlogInfo.filename, timestampStr);

                try (ResultSet rs = statement.executeQuery(query)) {
                    if (rs.next()) {
                        // Found a matching event
                        long position = rs.getLong("Position");
                        String eventTimestamp = rs.getString("Timestamp");
                        log.info(
                                "Found matching event in binlog file {} at position {} (event time: {})",
                                binlogInfo.filename,
                                position,
                                eventTimestamp);
                        return new BinlogOffset(binlogInfo.filename, position);
                    } else {
                        // No events found in this binlog file, continue checking the next file
                        log.debug(
                                "No events found after {} in file {}",
                                timestampStr,
                                binlogInfo.filename);
                    }
                } catch (SQLException e) {
                    // MySQL 5.6 and earlier versions may not support direct timestamp comparison in
                    // the WHERE clause
                    // Use an alternative approach to query
                    log.warn(
                            "Failed to query binlog events with timestamp comparison. Trying manual approach: {}",
                            e.getMessage());

                    // Manually traverse all events and compare timestamps
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

        // If no matching events are found in all files, use the latest binlog position
        // This means the specified timestamp is after the latest binlog, and we need to wait for
        // new data to be produced
        try (JdbcConnection jdbcConnection = dialect.openJdbcConnection(sourceConfig)) {
            BinlogOffset latestOffset = (BinlogOffset) latest();
            log.info(
                    "Could not find any events after timestamp {} in existing binlog files. Using latest binlog position {} and will wait for future events.",
                    timestampStr,
                    latestOffset);
            return latestOffset;
        } catch (Exception e) {
            log.error("Failed to get latest binlog position", e);
            // If getting the latest position fails, use the end of the last binlog file
            String lastBinlogFile = binlogInfoList.get(binlogInfoList.size() - 1).filename;
            long lastPosition = binlogInfoList.get(binlogInfoList.size() - 1).fileSize;
            log.info(
                    "Using end of last binlog file as fallback: {} position {}",
                    lastBinlogFile,
                    lastPosition);
            return new BinlogOffset(lastBinlogFile, lastPosition);
        }
    }

    /** Manually search the binlog file for events matching the timestamp */
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

        // If no matching events are found in this file, return the starting position of the next
        // binlog file (if any)
        log.debug("No matching events found in {}", binlogInfo.filename);
        return new BinlogOffset(
                binlogInfo.filename, 4L); // 4 is the initial position after the binlog header
    }

    private String formatTimestampForMysql(long timestamp) {
        return new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(new Date(timestamp));
    }

    private BinlogTimestampMapper buildBinlogTimestampMapper(
            Connection connection, List<BinlogInfo> binlogInfoList, long targetTimestamp) {
        BinlogTimestampMapper mapper = new BinlogTimestampMapper(targetTimestamp);

        // 1. Get the creation and modification times of the binlog files from
        // information_schema.files
        Map<String, BinlogTimestampInfo> binlogTimestamps =
                getBinlogTimestamps(connection, binlogInfoList);

        // 2. Enrich the timestamp information with the first and last events in each binlog file
        enrichBinlogTimestampsWithEvents(connection, binlogInfoList, binlogTimestamps);

        // 3. If there is still incomplete timestamp information, make estimates
        if (hasIncompleteTimestamps(binlogTimestamps, binlogInfoList)) {
            estimateRemainingTimestamps(binlogTimestamps, binlogInfoList);
        }

        // 4. Precisely find the position of the target timestamp
        try {
            for (BinlogInfo binlogInfo : binlogInfoList) {
                BinlogTimestampInfo timestampInfo = binlogTimestamps.get(binlogInfo.filename);
                if (timestampInfo == null) continue;

                // Check if the current binlog file may contain events for the target timestamp
                if (timestampInfo.hasTimestampRange()) {
                    if (targetTimestamp >= timestampInfo.firstEventTime
                            && targetTimestamp <= timestampInfo.lastEventTime) {
                        // The target timestamp is within the time range of this binlog file, find
                        // the exact position
                        BinlogOffset position =
                                findExactPositionInFile(
                                        connection, binlogInfo.filename, targetTimestamp);
                        if (position != null) {
                            mapper.setExactPosition(position);
                            return mapper;
                        }
                    } else if (targetTimestamp < timestampInfo.firstEventTime) {
                        // The target timestamp is earlier than the first event in this binlog file,
                        // start reading from this file
                        mapper.setEstimatedPosition(new BinlogOffset(binlogInfo.filename, 4L));
                        return mapper;
                    }
                    // If the target timestamp is later than the last event in this binlog file,
                    // continue checking the next file
                }
            }

            // If no suitable file is found after traversing all files, the target timestamp is
            // after the latest binlog
            BinlogInfo lastBinlog = binlogInfoList.get(binlogInfoList.size() - 1);
            BinlogTimestampInfo lastTimestampInfo = binlogTimestamps.get(lastBinlog.filename);

            if (lastTimestampInfo != null && lastTimestampInfo.lastEventTime < targetTimestamp) {
                mapper.setTargetTimeInFuture(true);
            } else {
                // Fallback strategy: use the last binlog file
                mapper.setEstimatedPosition(new BinlogOffset(lastBinlog.filename, 4L));
            }
        } catch (Exception e) {
            log.warn(
                    "Error while trying to find exact position for timestamp {}: {}",
                    formatTimestamp(targetTimestamp),
                    e.getMessage());
            // Fallback strategy: use the first binlog file
            mapper.setEstimatedPosition(new BinlogOffset(binlogInfoList.get(0).filename, 4L));
        }

        return mapper;
    }

    private BinlogOffset findExactPositionInFile(
            Connection connection, String binlogFilename, long targetTimestamp) {
        String timestampStr = formatTimestampForMysql(targetTimestamp);

        try (Statement statement = connection.createStatement()) {
            // Analyze all events in this binlog file to find the first event with a timestamp
            // greater than or equal to the target timestamp
            String query = String.format("SHOW BINLOG EVENTS IN '%s'", binlogFilename);
            try (ResultSet rs = statement.executeQuery(query)) {
                while (rs.next()) {
                    String eventTime = rs.getString("Timestamp");
                    long eventPosition = rs.getLong("Position");

                    // Manually compare timestamps
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

        // No exact position found
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
        // Find all binlog files with complete timestamp information
        List<BinlogTimestampInfo> completeInfos = new ArrayList<>();
        for (BinlogInfo binlogInfo : binlogInfoList) {
            BinlogTimestampInfo info = binlogTimestamps.get(binlogInfo.filename);
            if (info != null && info.hasTimestampRange()) {
                completeInfos.add(info);
            }
        }

        if (completeInfos.isEmpty()) {
            // If no binlog files have complete information, use a conservative estimate
            long currentTime = System.currentTimeMillis();
            long defaultRetentionHours = 24;
            long earliestEstimatedTime = currentTime - (defaultRetentionHours * 3600 * 1000);

            // Assume binlog files are uniformly distributed over time
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
            // Interpolate based on existing complete information
            // Here we use simple linear interpolation

            // Sort the existing complete information
            completeInfos.sort(Comparator.comparing(info -> info.binlogFilename));

            // Fill in estimated values for binlog files with missing information
            for (int i = 0; i < binlogInfoList.size(); i++) {
                BinlogInfo binlogInfo = binlogInfoList.get(i);
                if (!binlogTimestamps.containsKey(binlogInfo.filename)
                        || !binlogTimestamps.get(binlogInfo.filename).hasTimestampRange()) {

                    // Find the nearest binlog files with complete information before and after
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

                    // Make estimates
                    long estimatedFirstTime;
                    long estimatedLastTime;

                    if (before != null && after != null) {
                        // Both before and after information available, use linear interpolation
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
                        // Only before information available
                        estimatedFirstTime = before.lastEventTime + 1;
                        estimatedLastTime =
                                System.currentTimeMillis(); // Conservative estimate using current
                        // time
                    } else if (after != null) {
                        // Only after information available
                        estimatedLastTime = after.firstEventTime - 1;
                        // Conservative estimate using 24 hours ago
                        estimatedFirstTime = Math.max(0, estimatedLastTime - (24 * 3600 * 1000));
                    } else {
                        // No information available, use conservative estimate
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

            // Read the timestamp of the first and last events in the file
            try (Statement statement = connection.createStatement()) {
                // First event
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

                // Last event
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
                    // Alternative approach: read a few events from the end
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
            // Get the creation time of the binlog files from information_schema.files
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
