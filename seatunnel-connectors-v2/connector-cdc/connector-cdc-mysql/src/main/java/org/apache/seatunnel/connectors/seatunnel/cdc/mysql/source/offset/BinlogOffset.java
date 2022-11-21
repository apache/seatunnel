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

import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;

import io.debezium.connector.mysql.GtidSet;
import org.apache.commons.lang3.StringUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * A structure describes a fine grained offset in a binlog event including binlog position and gtid
 * set etc.
 *
 * <p>This structure can also be used to deal the binlog event in transaction, a transaction may
 * contains multiple change events, and each change event may contain multiple rows. When restart
 * from a specific {@link BinlogOffset}, we need to skip the processed change events and the
 * processed rows.
 */
public class BinlogOffset extends Offset {

    private static final long serialVersionUID = 1L;

    public static final String BINLOG_FILENAME_OFFSET_KEY = "file";
    public static final String BINLOG_POSITION_OFFSET_KEY = "pos";
    public static final String EVENTS_TO_SKIP_OFFSET_KEY = "event";
    public static final String ROWS_TO_SKIP_OFFSET_KEY = "row";
    public static final String GTID_SET_KEY = "gtids";
    public static final String TIMESTAMP_KEY = "ts_sec";
    public static final String SERVER_ID_KEY = "server_id";

    public static final BinlogOffset INITIAL_OFFSET = new BinlogOffset("", 0);
    public static final BinlogOffset NO_STOPPING_OFFSET = new BinlogOffset("", Long.MIN_VALUE);

    public BinlogOffset(Map<String, String> offset) {
        this.offset = offset;
    }

    public BinlogOffset(String filename, long position) {
        this(filename, position, 0L, 0L, 0L, null, null);
    }

    public BinlogOffset(
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

    public Long getServerId() {
        return longOffsetValue(offset, SERVER_ID_KEY);
    }

    /**
     * This method is inspired by {@link io.debezium.relational.history.HistoryRecordComparator}.
     */
    @Override
    public int compareTo(Offset offset) {
        BinlogOffset that = (BinlogOffset) offset;
        // the NO_STOPPING_OFFSET is the max offset
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
            // The target offset uses GTIDs, so we ideally compare using GTIDs ...
            if (StringUtils.isNotEmpty(gtidSetStr)) {
                // Both have GTIDs, so base the comparison entirely on the GTID sets.
                GtidSet gtidSet = new GtidSet(gtidSetStr);
                GtidSet targetGtidSet = new GtidSet(targetGtidSetStr);
                if (gtidSet.equals(targetGtidSet)) {
                    long restartSkipEvents = this.getRestartSkipEvents();
                    long targetRestartSkipEvents = that.getRestartSkipEvents();
                    return Long.compare(restartSkipEvents, targetRestartSkipEvents);
                }
                // The GTIDs are not an exact match, so figure out if this is a subset of the target
                // offset
                // ...
                return gtidSet.isContainedWithin(targetGtidSet) ? -1 : 1;
            }
            // The target offset did use GTIDs while this did not use GTIDs. So, we assume
            // that this offset is older since GTIDs are often enabled but rarely disabled.
            // And if they are disabled,
            // it is likely that this offset would not include GTIDs as we would be trying
            // to read the binlog of a
            // server that no longer has GTIDs. And if they are enabled, disabled, and re-enabled,
            // per
            // https://dev.mysql.com/doc/refman/5.7/en/replication-gtids-failover.html all properly
            // configured slaves that
            // use GTIDs should always have the complete set of GTIDs copied from the master, in
            // which case
            // again we know that this offset not having GTIDs is before the target offset ...
            return -1;
        } else if (StringUtils.isNotEmpty(gtidSetStr)) {
            // This offset has a GTID but the target offset does not, so per the previous paragraph
            // we
            // assume that previous
            // is not at or before ...
            return 1;
        }

        // Both offsets are missing GTIDs. Look at the servers ...
        long serverId = this.getServerId();
        long targetServerId = that.getServerId();

        if (serverId != targetServerId) {
            // These are from different servers, and their binlog coordinates are not related. So
            // the only thing we can do
            // is compare timestamps, and we have to assume that the server timestamps can be
            // compared ...
            long timestamp = this.getTimestamp();
            long targetTimestamp = that.getTimestamp();
            return Long.compare(timestamp, targetTimestamp);
        }

        // First compare the MySQL binlog filenames
        if (this.getFilename().compareToIgnoreCase(that.getFilename()) != 0) {
            return this.getFilename().compareToIgnoreCase(that.getFilename());
        }

        // The filenames are the same, so compare the positions
        if (this.getPosition() != that.getPosition()) {
            return Long.compare(this.getPosition(), that.getPosition());
        }

        // The positions are the same, so compare the completed events in the transaction ...
        if (this.getRestartSkipEvents() != that.getRestartSkipEvents()) {
            return Long.compare(this.getRestartSkipEvents(), that.getRestartSkipEvents());
        }

        // The completed events are the same, so compare the row number ...
        return Long.compare(this.getRestartSkipRows(), that.getRestartSkipRows());
    }

    @SuppressWarnings("checkstyle:EqualsHashCode")
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof BinlogOffset)) {
            return false;
        }
        BinlogOffset that = (BinlogOffset) o;
        return offset.equals(that.offset);
    }
}
