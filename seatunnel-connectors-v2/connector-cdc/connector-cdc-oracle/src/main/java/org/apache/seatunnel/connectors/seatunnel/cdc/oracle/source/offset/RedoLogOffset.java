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

package org.apache.seatunnel.connectors.seatunnel.cdc.oracle.source.offset;

import org.apache.seatunnel.connectors.cdc.base.source.offset.Offset;

import org.apache.commons.lang3.StringUtils;

import io.debezium.connector.oracle.Scn;

import javax.annotation.Nullable;

import java.util.HashMap;
import java.util.Map;

/** A structure describes an offset in a redo log event. */
public class RedoLogOffset extends Offset {

    private static final long serialVersionUID = 1L;

    public static final String SCN_KEY = "scn";
    public static final String COMMIT_SCN_KEY = "commit_scn";
    public static final String LCR_POSITION_KEY = "lcr_position";

    public static final RedoLogOffset INITIAL_OFFSET = new RedoLogOffset(0L);
    public static final RedoLogOffset NO_STOPPING_OFFSET = new RedoLogOffset(Long.MIN_VALUE);

    public RedoLogOffset(Map<String, String> offset) {
        this.offset = offset;
    }

    public RedoLogOffset(Long scn) {
        this(scn, 0L, null);
    }

    public RedoLogOffset(Long scn, Long commitScn, @Nullable String lcrPosition) {
        Map<String, String> offsetMap = new HashMap<>();
        offsetMap.put(SCN_KEY, String.valueOf(scn));
        offsetMap.put(COMMIT_SCN_KEY, String.valueOf(commitScn));
        offsetMap.put(LCR_POSITION_KEY, lcrPosition);
        this.offset = offsetMap;
    }

    public String getScn() {
        return offset.get(SCN_KEY);
    }

    public String getCommitScn() {
        return offset.get(COMMIT_SCN_KEY);
    }

    public String getLcrPosition() {
        return offset.get(LCR_POSITION_KEY);
    }

    @Override
    public int compareTo(Offset offset) {
        RedoLogOffset that = (RedoLogOffset) offset;
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

        String scnStr = this.getScn();
        String targetScnStr = that.getScn();
        if (StringUtils.isNotEmpty(targetScnStr)) {
            if (StringUtils.isNotEmpty(scnStr)) {
                Scn scn = Scn.valueOf(scnStr);
                Scn targetScn = Scn.valueOf(targetScnStr);
                return scn.compareTo(targetScn);
            }
            return -1;
        } else if (StringUtils.isNotEmpty(scnStr)) {
            return 1;
        }
        return 0;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((getScn() == null) ? 0 : getScn().hashCode());
        result = prime * result + ((getCommitScn() == null) ? 0 : getCommitScn().hashCode());
        result = prime * result + ((getLcrPosition() == null) ? 0 : getLcrPosition().hashCode());
        return result;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof RedoLogOffset)) {
            return false;
        }
        RedoLogOffset that = (RedoLogOffset) o;
        return offset.equals(that.offset);
    }
}
