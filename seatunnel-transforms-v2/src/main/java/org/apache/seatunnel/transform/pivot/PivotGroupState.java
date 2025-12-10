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

package org.apache.seatunnel.transform.pivot;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.Serializable;
import java.util.Map;

/**
 * State for PivotTransform that can be serialized for checkpointing.
 *
 * <p>This state captures:
 *
 * <ul>
 *   <li>The group key (combination of group_by column values)
 *   <li>The pivoted values for each pivot column
 *   <li>The last update timestamp for timeout-based flushing
 * </ul>
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class PivotGroupState implements Serializable {

    private static final long serialVersionUID = 1L;

    /** The composite key formed from group_by columns, serialized as a string */
    private String groupKey;

    /** Map from pivot value to the corresponding value column data */
    private Map<String, Object> pivotedValues;

    /** Values of the group_by columns for this group */
    private Object[] groupByValues;

    /** Timestamp of the last update to this group */
    private long lastUpdateTime;

    /** The table ID (for multi-table support) */
    private String tableId;

    public PivotGroupState(
            String groupKey,
            Map<String, Object> pivotedValues,
            Object[] groupByValues,
            String tableId) {
        this.groupKey = groupKey;
        this.pivotedValues = pivotedValues;
        this.groupByValues = groupByValues;
        this.lastUpdateTime = System.currentTimeMillis();
        this.tableId = tableId;
    }

    /** Update the last update time to now */
    public void touch() {
        this.lastUpdateTime = System.currentTimeMillis();
    }

    /**
     * Check if this group has timed out.
     *
     * @param timeoutMs The timeout in milliseconds
     * @return true if the group has timed out
     */
    public boolean isTimedOut(long timeoutMs) {
        if (timeoutMs <= 0) {
            return false;
        }
        return System.currentTimeMillis() - lastUpdateTime > timeoutMs;
    }
}
