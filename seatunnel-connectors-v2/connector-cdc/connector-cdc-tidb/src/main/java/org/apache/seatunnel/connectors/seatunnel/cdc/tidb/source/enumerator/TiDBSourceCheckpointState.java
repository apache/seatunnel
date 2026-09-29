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

package org.apache.seatunnel.connectors.seatunnel.cdc.tidb.source.enumerator;

import org.apache.seatunnel.connectors.seatunnel.cdc.tidb.source.split.TiDBSourceSplit;

import lombok.Getter;
import lombok.ToString;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Getter
@ToString
public class TiDBSourceCheckpointState implements Serializable {
    private static final long serialVersionUID = 6292978509042158791L;
    private boolean shouldEnumerate;
    private Map<Integer, List<TiDBSourceSplit>> pendingSplit;
    private int assignCount;

    public TiDBSourceCheckpointState(boolean shouldEnumerate, Map<Integer, ?> pendingSplit) {
        this(shouldEnumerate, pendingSplit, 0);
    }

    public TiDBSourceCheckpointState(
            boolean shouldEnumerate, Map<Integer, ?> pendingSplit, int assignCount) {
        this.shouldEnumerate = shouldEnumerate;
        this.pendingSplit = normalizePendingSplit(pendingSplit);
        this.assignCount = assignCount;
    }

    public void setShouldEnumerate(boolean shouldEnumerate) {
        this.shouldEnumerate = shouldEnumerate;
    }

    public void setPendingSplit(Map<Integer, ?> pendingSplit) {
        this.pendingSplit = normalizePendingSplit(pendingSplit);
    }

    public Map<Integer, List<TiDBSourceSplit>> getPendingSplit() {
        pendingSplit = normalizePendingSplit(pendingSplit);
        return pendingSplit;
    }

    private static Map<Integer, List<TiDBSourceSplit>> normalizePendingSplit(
            Map<Integer, ?> pendingSplit) {
        Map<Integer, List<TiDBSourceSplit>> normalizedPendingSplit = new HashMap<>();
        if (pendingSplit == null) {
            return normalizedPendingSplit;
        }
        for (Map.Entry<Integer, ?> entry : pendingSplit.entrySet()) {
            Object value = entry.getValue();
            if (value instanceof TiDBSourceSplit) {
                normalizedPendingSplit.put(
                        entry.getKey(),
                        new ArrayList<>(Collections.singletonList((TiDBSourceSplit) value)));
            } else if (value instanceof List) {
                normalizedPendingSplit.put(entry.getKey(), copyPendingSplits((List<?>) value));
            } else if (value == null) {
                normalizedPendingSplit.put(entry.getKey(), new ArrayList<>());
            } else {
                throw new IllegalArgumentException(
                        String.format(
                                "Unsupported pending split value type %s for reader %s.",
                                value.getClass().getName(), entry.getKey()));
            }
        }
        return normalizedPendingSplit;
    }

    private static List<TiDBSourceSplit> copyPendingSplits(List<?> pendingSplits) {
        List<TiDBSourceSplit> copiedPendingSplits = new ArrayList<>();
        for (Object pendingSplit : pendingSplits) {
            if (!(pendingSplit instanceof TiDBSourceSplit)) {
                throw new IllegalArgumentException(
                        String.format(
                                "Unsupported pending split list value type %s.",
                                pendingSplit == null ? "null" : pendingSplit.getClass().getName()));
            }
            copiedPendingSplits.add((TiDBSourceSplit) pendingSplit);
        }
        return copiedPendingSplits;
    }
}
