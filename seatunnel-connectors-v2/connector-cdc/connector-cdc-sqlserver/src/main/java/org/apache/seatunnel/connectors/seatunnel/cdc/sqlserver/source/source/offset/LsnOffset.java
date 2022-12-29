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

package org.apache.seatunnel.connectors.seatunnel.cdc.sqlserver.source.source.offset;

import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;

import io.debezium.connector.sqlserver.Lsn;
import io.debezium.connector.sqlserver.SourceInfo;

import java.util.HashMap;
import java.util.Map;

public class LsnOffset extends Offset {

    private static final long serialVersionUID = 1L;

    public static final LsnOffset INITIAL_OFFSET = new LsnOffset(Lsn.valueOf(new byte[] {Byte.MIN_VALUE}));
    public static final LsnOffset NO_STOPPING_OFFSET = new LsnOffset(Lsn.valueOf(new byte[] {Byte.MAX_VALUE}));

    public LsnOffset(Lsn changeLsn){
        this(changeLsn, null, null);
    }

    public LsnOffset(Lsn changeLsn, Lsn commitLsn, Long eventSerialNo){
        Map<String, String> offsetMap = new HashMap<>();

        if (changeLsn != null && changeLsn.isAvailable()) {
            offsetMap.put(SourceInfo.CHANGE_LSN_KEY, changeLsn.toString());
        }
        if (commitLsn != null && commitLsn.isAvailable()) {
            offsetMap.put(SourceInfo.COMMIT_LSN_KEY, commitLsn.toString());
        }
        if (eventSerialNo != null) {
            offsetMap.put(SourceInfo.EVENT_SERIAL_NO_KEY, String.valueOf(eventSerialNo));
        }

        this.offset = offsetMap;
    }

    public Lsn getChangeLsn() {
        return Lsn.valueOf(offset.get(SourceInfo.CHANGE_LSN_KEY));
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

        Lsn thisChangeLsn = this.getChangeLsn();
        Lsn thatChangeLsn = that.getChangeLsn();
        if (thatChangeLsn.isAvailable()) {
            if (thisChangeLsn.isAvailable()) {
                return thisChangeLsn.compareTo(thatChangeLsn);
            }
            return -1;
        } else if (thisChangeLsn.isAvailable()) {
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
