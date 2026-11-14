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

package org.apache.seatunnel.connectors.seatunnel.cdc.mariadb.offset;

import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;

import java.util.HashMap;
import java.util.Map;

/**
 * A structure describes a fine grained offset in a MariaDB binlog event including binlog position
 * and gtid set etc.
 */
public class MariaDbBinlogOffset extends Offset {

    private static final long serialVersionUID = 1L;

    public static final String BINLOG_FILENAME_OFFSET_KEY = "file";
    public static final String BINLOG_POSITION_OFFSET_KEY = "pos";
    public static final String EVENTS_TO_SKIP_OFFSET_KEY = "event";
    public static final String ROWS_TO_SKIP_OFFSET_KEY = "row";
    public static final String GTID_SET_KEY = "gtids";
    public static final String TIMESTAMP_KEY = "ts_sec";
    public static final String SERVER_ID_KEY = "server_id";

    public static final MariaDbBinlogOffset INITIAL_OFFSET = new MariaDbBinlogOffset("", 0);
    public static final MariaDbBinlogOffset NO_STOPPING_OFFSET =
            new MariaDbBinlogOffset("", Long.MIN_VALUE);

    public MariaDbBinlogOffset(Map<String, String> offset) {
        this.offset = offset;
    }

    public MariaDbBinlogOffset(String filename, long position) {
        this(filename, position, 0L, 0L, 0L, null, null);
    }

    public MariaDbBinlogOffset(
            String filename,
            long position,
            long restartSkipEvents,
            long restartSkipRows,
            long binlogEpochSecs,
            String restartGtidSet,
            Integer serverId) {
        Map<String, String> offsetMap = new HashMap<>();
        offsetMap.put(BINLOG_FILENAME_OFFSET_KEY, filename);
        offsetMap.put(BINLOG_POSITION_OFFSET_KEY, String.valueOf(position));
        offsetMap.put(EVENTS_TO_SKIP_OFFSET_KEY, String.valueOf(restartSkipEvents));
        offsetMap.put(ROWS_TO_SKIP_OFFSET_KEY, String.valueOf(restartSkipRows));
        offsetMap.put(TIMESTAMP_KEY, String.valueOf(binlogEpochSecs));
        if (restartGtidSet != null) {
            offsetMap.put(GTID_SET_KEY, restartGtidSet);
        }
        if (serverId != null) {
            offsetMap.put(SERVER_ID_KEY, String.valueOf(serverId));
        }
        this.offset = offsetMap;
    }

    public MariaDbBinlogOffset(long timestamp) {
        Map<String, String> offsetMap = new HashMap<>();
        offsetMap.put(TIMESTAMP_KEY, String.valueOf(timestamp));
        this.offset = offsetMap;
    }

    public String getFilename() {
        return offset.get(BINLOG_FILENAME_OFFSET_KEY);
    }

    public long getPosition() {
        return longOffsetValue(offset, BINLOG_POSITION_OFFSET_KEY);
    }

    public long getRestartSkipEvents() {
        return longOffsetValue(offset, EVENTS_TO_SKIP_OFFSET_KEY);
    }

    public long getRestartSkipRows() {
        return longOffsetValue(offset, ROWS_TO_SKIP_OFFSET_KEY);
    }

    public String getGtidSet() {
        return offset.get(GTID_SET_KEY);
    }

    public long getTimestamp() {
        return longOffsetValue(offset, TIMESTAMP_KEY);
    }

    public boolean isTimestampOffset() {
        return offset.containsKey(TIMESTAMP_KEY)
                && !offset.containsKey(BINLOG_FILENAME_OFFSET_KEY)
                && !offset.containsKey(BINLOG_POSITION_OFFSET_KEY);
    }

    public Long getServerId() {
        return longOffsetValue(offset, SERVER_ID_KEY);
    }

    @Override
    public boolean isNeverStop() {
        return NO_STOPPING_OFFSET.equals(this);
    }

    @Override
    public int compareTo(Offset offset) {
        MariaDbBinlogOffset that = (MariaDbBinlogOffset) offset;
        if (NO_STOPPING_OFFSET.equals(that) && NO_STOPPING_OFFSET.equals(this)) {
            return 0;
        }
        if (NO_STOPPING_OFFSET.equals(this)) {
            return 1;
        }
        if (NO_STOPPING_OFFSET.equals(that)) {
            return -1;
        }

        String gtidSetStr = this.getGtidSet();
        String targetGtidSetStr = that.getGtidSet();
        if (StringUtils.isNotEmpty(targetGtidSetStr)) {
            if (StringUtils.isNotEmpty(gtidSetStr)) {
                MariaDbGtidSet gtidSet = new MariaDbGtidSet(gtidSetStr);
                MariaDbGtidSet targetGtidSet = new MariaDbGtidSet(targetGtidSetStr);
                if (gtidSet.equals(targetGtidSet)) {
                    String thisFilename = this.getFilename();
                    String thatFilename = that.getFilename();
                    if (StringUtils.isNotEmpty(thisFilename)
                            && StringUtils.isNotEmpty(thatFilename)) {
                        int filenameCompare = thisFilename.compareToIgnoreCase(thatFilename);
                        if (filenameCompare != 0) {
                            return filenameCompare;
                        }
                        int posCompare = Long.compare(this.getPosition(), that.getPosition());
                        if (posCompare != 0) {
                            return posCompare;
                        }
                    }
                    int eventCompare =
                            Long.compare(this.getRestartSkipEvents(), that.getRestartSkipEvents());
                    if (eventCompare != 0) {
                        return eventCompare;
                    }
                    return Long.compare(this.getRestartSkipRows(), that.getRestartSkipRows());
                }
                return gtidSet.isContainedWithin(targetGtidSet) ? -1 : 1;
            }
            String thisFilename = this.getFilename();
            String thatFilename = that.getFilename();
            if (StringUtils.isNotEmpty(thisFilename) && StringUtils.isNotEmpty(thatFilename)) {
                int filenameCompare = thisFilename.compareToIgnoreCase(thatFilename);
                if (filenameCompare != 0) {
                    return filenameCompare;
                }
                return Long.compare(this.getPosition(), that.getPosition());
            }
            return -1;
        } else if (StringUtils.isNotEmpty(gtidSetStr)) {
            String thisFilename = this.getFilename();
            String thatFilename = that.getFilename();
            if (StringUtils.isNotEmpty(thisFilename) && StringUtils.isNotEmpty(thatFilename)) {
                int filenameCompare = thisFilename.compareToIgnoreCase(thatFilename);
                if (filenameCompare != 0) {
                    return filenameCompare;
                }
                return Long.compare(this.getPosition(), that.getPosition());
            }
            return 1;
        }

        Long serverId = this.getServerId();
        Long targetServerId = that.getServerId();

        if (serverId != null && targetServerId != null && !serverId.equals(targetServerId)) {
            long timestamp = this.getTimestamp();
            long targetTimestamp = that.getTimestamp();
            if (timestamp != 0 && targetTimestamp != 0) {
                return Long.compare(timestamp, targetTimestamp);
            }
        }

        if (this.getFilename().compareToIgnoreCase(that.getFilename()) != 0) {
            return this.getFilename().compareToIgnoreCase(that.getFilename());
        }

        if (this.getPosition() != that.getPosition()) {
            return Long.compare(this.getPosition(), that.getPosition());
        }

        if (this.getRestartSkipEvents() != that.getRestartSkipEvents()) {
            return Long.compare(this.getRestartSkipEvents(), that.getRestartSkipEvents());
        }

        return Long.compare(this.getRestartSkipRows(), that.getRestartSkipRows());
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof MariaDbBinlogOffset)) {
            return false;
        }
        MariaDbBinlogOffset that = (MariaDbBinlogOffset) o;
        return offset.equals(that.offset);
    }
}
