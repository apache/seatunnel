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

package org.apache.seatunnel.connectors.seatunnel.cdc.sqlserver.source.offset;

import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;

import io.debezium.connector.sqlserver.Lsn;
import io.debezium.connector.sqlserver.SourceInfo;

import java.util.HashMap;
import java.util.Map;

public class LsnOffset extends Offset {

    private static final long serialVersionUID = 1L;

    public static final LsnOffset INITIAL_OFFSET = new LsnOffset(null, null, null);
    public static final LsnOffset NO_STOPPING_OFFSET =
            valueOf(Lsn.valueOf(new byte[] {Byte.MAX_VALUE}).toString());

    public static LsnOffset valueOf(String commitLsn) {
        return new LsnOffset(Lsn.valueOf(commitLsn), null, null);
    }

    /**
     * Creates an offset from the full SQL Server position reported by Debezium.
     *
     * <p>SQL Server can emit multiple change events for the same commit LSN. The change LSN and
     * event serial number are therefore required to resume without skipping records.
     */
    public static LsnOffset valueOf(Map<String, ?> offset) {
        Object eventSerialNo = offset.get(SourceInfo.EVENT_SERIAL_NO_KEY);
        Object commitLsn = offset.get(SourceInfo.COMMIT_LSN_KEY);
        Object changeLsn = offset.get(SourceInfo.CHANGE_LSN_KEY);
        return new LsnOffset(
                Lsn.valueOf(commitLsn == null ? null : commitLsn.toString()),
                Lsn.valueOf(changeLsn == null ? null : changeLsn.toString()),
                eventSerialNo == null ? null : Long.valueOf(eventSerialNo.toString()));
    }

    /**
     * Creates a boundary offset suitable for {@code startup.mode=timestamp}.
     *
     * <p>{@code sys.fn_cdc_map_time_to_lsn('smallest greater than or equal', ts)} returns the
     * COMMIT lsn of the first transaction whose commit time is at or after the requested timestamp.
     * Rows belonging to that transaction must be emitted, so the boundary must order itself BEFORE
     * any real change event at the same commit. This is achieved by leaving the commit LSN intact
     * while using the smallest available change LSN and event serial number as the in-commit
     * position. With {@link #compareTo(Offset)}, every real in-commit event then compares as {@code
     * isAfter(this)}.
     *
     * <p>Compare with {@link #valueOf(String)} (commit-only), which is used for {@code
     * startup.mode=latest} and intentionally orders AFTER same-commit events to avoid replaying
     * rows that already existed before startup.
     */
    public static LsnOffset timestampBoundary(String commitLsn) {
        return new LsnOffset(
                Lsn.valueOf(commitLsn), Lsn.valueOf(new byte[] {0, 0, 0, 0, 0, 0, 0, 0, 0, 1}), 0L);
    }

    private LsnOffset(Lsn commitLsn, Lsn changeLsn, Long eventSerialNo) {
        Map<String, String> offsetMap = new HashMap<>();

        if (commitLsn != null && commitLsn.isAvailable()) {
            offsetMap.put(SourceInfo.COMMIT_LSN_KEY, commitLsn.toString());
        }
        if (changeLsn != null && changeLsn.isAvailable()) {
            offsetMap.put(SourceInfo.CHANGE_LSN_KEY, changeLsn.toString());
        }
        if (eventSerialNo != null) {
            offsetMap.put(SourceInfo.EVENT_SERIAL_NO_KEY, String.valueOf(eventSerialNo));
        }

        this.offset = offsetMap;
    }

    public Lsn getChangeLsn() {
        return Lsn.valueOf(offset.get(SourceInfo.CHANGE_LSN_KEY));
    }

    public Lsn getCommitLsn() {
        return Lsn.valueOf(offset.get(SourceInfo.COMMIT_LSN_KEY));
    }

    public Object getEventSerialNo() {
        return offset.get(SourceInfo.EVENT_SERIAL_NO_KEY);
    }

    public int compareTo(Offset o) {
        LsnOffset that = (LsnOffset) o;
        final int comparison = getCommitLsn().compareTo(that.getCommitLsn());
        if (comparison != 0) {
            return comparison;
        }
        // A commit-only offset carries no in-commit position. It can represent the latest
        // startup boundary, a timestamp-derived boundary, a legacy coarse checkpoint or a
        // snapshot watermark, all of which address a whole commit (everything in that commit
        // is treated as already processed before the boundary takes effect).
        //
        // Comparing a commit-only boundary against a same-commit complete event position must
        // therefore order the complete event BEFORE the boundary, otherwise:
        //   * the non-exactly-once path would skip same-commit rows (acceptable, but stricter
        //     than needed),
        //   * the exactly-once pure-binlog transition (`isAtOrAfter`) would flip a table into
        //     "emit everything" mode on the very first record of the boundary commit and
        //     replay rows already committed before startup. That is a data-correctness
        //     regression on the normal streaming path for `startup.mode=latest`.
        final boolean thisComplete = hasCompletePosition();
        final boolean thatComplete = that.hasCompletePosition();
        if (!thisComplete && !thatComplete) {
            return 0;
        }
        if (!thisComplete) {
            return 1;
        }
        if (!thatComplete) {
            return -1;
        }
        final int changeLsnComparison = getChangeLsn().compareTo(that.getChangeLsn());
        if (changeLsnComparison != 0) {
            return changeLsnComparison;
        }
        return Long.compare(eventSerialNo(), that.eventSerialNo());
    }

    private boolean hasCompletePosition() {
        return getChangeLsn().isAvailable() && getEventSerialNo() != null;
    }

    private long eventSerialNo() {
        Object eventSerialNo = getEventSerialNo();
        return eventSerialNo == null ? 0L : Long.parseLong(eventSerialNo.toString());
    }

    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        LsnOffset other = (LsnOffset) obj;
        return offset.equals(other.offset);
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((getCommitLsn() == null) ? 0 : getCommitLsn().hashCode());
        result = prime * result + ((getChangeLsn() == null) ? 0 : getChangeLsn().hashCode());
        result =
                prime * result + ((getEventSerialNo() == null) ? 0 : getEventSerialNo().hashCode());
        return result;
    }

    @Override
    public boolean isNeverStop() {
        return NO_STOPPING_OFFSET.equals(this);
    }
}
