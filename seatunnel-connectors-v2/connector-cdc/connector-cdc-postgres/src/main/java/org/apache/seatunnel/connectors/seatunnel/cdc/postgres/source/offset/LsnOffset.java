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

package org.apache.seatunnel.connectors.seatunnel.cdc.postgres.source.offset;

import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;

import io.debezium.connector.postgresql.SourceInfo;
import io.debezium.connector.postgresql.connection.Lsn;

import java.util.HashMap;
import java.util.Map;

public class LsnOffset extends Offset {

    private static final long serialVersionUID = 1L;

    public static final LsnOffset INITIAL_OFFSET = new LsnOffset(Lsn.valueOf(Long.MIN_VALUE));
    public static final LsnOffset NO_STOPPING_OFFSET = new LsnOffset(Lsn.valueOf(Long.MAX_VALUE));

    /**
     * the position in the server WAL for a particular event; may be null indicating that this
     * information is not available.
     */
    private Lsn lsn;

    /**
     * the ID of the transaction that generated the transaction; may be null if this information is
     * not available.
     */
    private Long txId;

    /** the xmin of the slot, may be null. */
    private Long xmin;

    public LsnOffset(Map<String, String> offset) {
        this.offset = offset;
    }

    public LsnOffset(Lsn lsn) {
        this(lsn, null, null);
    }

    public LsnOffset(Lsn lsn, Long txId, Long xmin) {
        Map<String, String> offsetMap = new HashMap<>();

        if (lsn != null && lsn.isValid()) {
            offsetMap.put(SourceInfo.LSN_KEY, String.valueOf(lsn.asLong()));
        }
        if (txId != null) {
            offsetMap.put(SourceInfo.TXID_KEY, String.valueOf(txId));
        }
        if (xmin != null) {
            offsetMap.put(SourceInfo.XMIN_KEY, String.valueOf(xmin));
        }

        this.offset = offsetMap;
    }

    public Lsn getLsn() {
        return Lsn.valueOf(Long.valueOf(offset.get(SourceInfo.LSN_KEY)));
    }

    public Long getTxId() {
        return Long.parseLong(offset.get(SourceInfo.TXID_KEY));
    }

    public Long getXmin() {
        return Long.parseLong(offset.get(SourceInfo.XMIN_KEY));
    }

    @Override
    public int compareTo(Offset o) {
        LsnOffset that = (LsnOffset) o;
        if (NO_STOPPING_OFFSET.equals(that) && NO_STOPPING_OFFSET.equals(this)) {
            return 0;
        }
        if (NO_STOPPING_OFFSET.equals(this)) {
            return 1;
        }
        if (NO_STOPPING_OFFSET.equals(that)) {
            return -1;
        }

        Lsn thisLsn = this.getLsn();
        Lsn thatLsn = that.getLsn();
        if (thatLsn.isValid()) {
            if (thisLsn.isValid()) {
                return thisLsn.compareTo(thatLsn);
            }
            return -1;
        } else if (thisLsn.isValid()) {
            return 1;
        }
        return 0;
    }

    @SuppressWarnings("checkstyle:EqualsHashCode")
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof LsnOffset)) {
            return false;
        }
        LsnOffset that = (LsnOffset) o;
        return offset.equals(that.offset);
    }
}
