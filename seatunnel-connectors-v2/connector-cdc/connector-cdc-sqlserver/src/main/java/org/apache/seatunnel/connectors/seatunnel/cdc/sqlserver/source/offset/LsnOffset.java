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

    public static final LsnOffset INITIAL_OFFSET = valueOf(Lsn.valueOf(new byte[] {0}).toString());
    public static final LsnOffset NO_STOPPING_OFFSET =
            valueOf(Lsn.valueOf(new byte[] {Byte.MAX_VALUE}).toString());

    public static LsnOffset valueOf(String commitLsn) {
        return new LsnOffset(Lsn.valueOf(commitLsn), null, null);
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
        return comparison == 0 ? getChangeLsn().compareTo(that.getChangeLsn()) : comparison;
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
}
