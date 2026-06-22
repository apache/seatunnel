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

import java.io.IOException;
import java.io.ObjectInputStream;
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

    public TiDBSourceCheckpointState(boolean shouldEnumerate, Map<Integer, ?> pendingSplit) {
        this.shouldEnumerate = shouldEnumerate;
        this.pendingSplit = normalizePendingSplit(pendingSplit);
    }

    public void setShouldEnumerate(boolean shouldEnumerate) {
        this.shouldEnumerate = shouldEnumerate;
    }

    public void setPendingSplit(Map<Integer, ?> pendingSplit) {
        this.pendingSplit = normalizePendingSplit(pendingSplit);
    }

    private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
        in.defaultReadObject();
        try {
            pendingSplit = normalizePendingSplit(pendingSplit);
        } catch (IllegalArgumentException e) {
            throw new IOException("Unsupported TiDB CDC pending split checkpoint state.", e);
        }
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

    @SuppressWarnings("unchecked")
    private static List<TiDBSourceSplit> copyPendingSplits(List<?> pendingSplits) {
        return new ArrayList<>((List<TiDBSourceSplit>) pendingSplits);
    }
}
