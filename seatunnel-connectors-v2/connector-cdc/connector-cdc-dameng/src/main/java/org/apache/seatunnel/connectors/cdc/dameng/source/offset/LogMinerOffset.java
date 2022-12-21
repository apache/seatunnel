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

package org.apache.seatunnel.connectors.cdc.dameng.source.offset;

import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;

import io.debezium.connector.dameng.Scn;
import io.debezium.connector.dameng.SourceInfo;

import java.util.HashMap;
import java.util.Map;

public class LogMinerOffset extends Offset {
    public static final LogMinerOffset INITIAL_OFFSET = new LogMinerOffset(Scn.valueOf(0));
    public static final LogMinerOffset NO_STOPPING_OFFSET = new LogMinerOffset(Scn.valueOf(Long.MAX_VALUE));

    public LogMinerOffset(Scn scn) {
        this(scn, null, null);
    }

    public LogMinerOffset(Scn changeScn, Scn commitScn, Long eventSerialNo){
        this(createOffsetMap(changeScn, commitScn, eventSerialNo));
    }

    public LogMinerOffset(Map<String, String> offsetMap) {
        this.offset = offsetMap;
    }

    public Scn getScn() {
        return Scn.valueOf(offset.get(SourceInfo.SCN_KEY));
    }

    public Scn getCommitScn() {
        return Scn.valueOf(offset.get(SourceInfo.COMMIT_SCN_KEY));
    }

    @Override
    public int compareTo(Offset o) {
        LogMinerOffset that = (LogMinerOffset) o;
        if (NO_STOPPING_OFFSET.equals(that) && NO_STOPPING_OFFSET.equals(this)) {
            return 0;
        }
        if (NO_STOPPING_OFFSET.equals(this)) {
            return 1;
        }
        if (NO_STOPPING_OFFSET.equals(that)) {
            return -1;
        }

        Scn thisChangeScn = this.getScn();
        Scn thatChangeScn = that.getScn();
        return thisChangeScn.compareTo(thatChangeScn);
    }

    @SuppressWarnings("checkstyle:EqualsHashCode")
    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof LogMinerOffset)) {
            return false;
        }
        LogMinerOffset that = (LogMinerOffset) o;
        return offset.equals(that.offset);
    }

    private static Map<String, String> createOffsetMap(Scn changeScn, Scn commitScn, Long eventSerialNo) {
        Map<String, String> offsetMap = new HashMap<>();
        if (changeScn != null) {
            offsetMap.put(SourceInfo.SCN_KEY, changeScn.toString());
        }
        if (commitScn != null) {
            offsetMap.put(SourceInfo.COMMIT_SCN_KEY, commitScn.toString());
        }
        if (eventSerialNo != null) {
            offsetMap.put(SourceInfo.EVENT_SERIAL_NO_KEY, String.valueOf(eventSerialNo));
        }
        return offsetMap;
    }
}
