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

package org.apache.seatunnel.connectors.seatunnel.jdbc.source;

import org.apache.seatunnel.api.source.SourceSplit;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;

import lombok.Data;
import lombok.ToString;

@Data
@ToString
public class JdbcSourceSplit implements SourceSplit {
    private static final String CLOSE_TABLE_MARKER_PREFIX = "__close_table_marker__";
    private static final long serialVersionUID = -815542654355310611L;
    private final TablePath tablePath;
    private final String splitId;
    private final String splitQuery;
    private final String splitKeyName;
    private final SeaTunnelDataType splitKeyType;
    private final Object splitStart;
    private final Object splitEnd;
    /**
     * Marker splits are synthetic reader-state entries used to keep close-table protocol state
     * checkpoint-safe even though the reader snapshot can only persist split objects.
     */
    private final boolean closeTableMarker;
    /**
     * A positive value means the reader has already received the global close signal and still
     * needs to emit the downstream {@code CloseTableEvent}. Zero means the reader is only waiting
     * for that global close signal.
     */
    private final int expectedCloseEventCount;

    public JdbcSourceSplit(
            TablePath tablePath,
            String splitId,
            String splitQuery,
            String splitKeyName,
            SeaTunnelDataType splitKeyType,
            Object splitStart,
            Object splitEnd) {
        this(
                tablePath,
                splitId,
                splitQuery,
                splitKeyName,
                splitKeyType,
                splitStart,
                splitEnd,
                false,
                0);
    }

    public JdbcSourceSplit(
            TablePath tablePath,
            String splitId,
            String splitQuery,
            String splitKeyName,
            SeaTunnelDataType splitKeyType,
            Object splitStart,
            Object splitEnd,
            boolean closeTableMarker,
            int expectedCloseEventCount) {
        this.tablePath = tablePath;
        this.splitId = splitId;
        this.splitQuery = splitQuery;
        this.splitKeyName = splitKeyName;
        this.splitKeyType = splitKeyType;
        this.splitStart = splitStart;
        this.splitEnd = splitEnd;
        this.closeTableMarker = closeTableMarker;
        this.expectedCloseEventCount = expectedCloseEventCount;
    }

    /** Creates a synthetic split that preserves a waiting or pending close-table state. */
    public static JdbcSourceSplit forCloseTableState(
            TablePath tablePath, int expectedCloseEventCount) {
        return new JdbcSourceSplit(
                tablePath,
                CLOSE_TABLE_MARKER_PREFIX + tablePath.getFullName(),
                null,
                null,
                null,
                null,
                null,
                true,
                expectedCloseEventCount);
    }

    public boolean isPendingCloseTableEvent() {
        return closeTableMarker && expectedCloseEventCount > 0;
    }

    @Override
    public String splitId() {
        return splitId;
    }
}
