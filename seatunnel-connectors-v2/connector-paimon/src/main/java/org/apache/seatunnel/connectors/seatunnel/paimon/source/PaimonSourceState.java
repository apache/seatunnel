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

package org.apache.seatunnel.connectors.seatunnel.paimon.source;

import javax.annotation.Nullable;

import java.io.Serializable;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.Map;

/** Paimon connector source state, saves the splits has assigned to readers. */
public class PaimonSourceState implements Serializable {

    private static final long serialVersionUID = 1L;

    private final Deque<PaimonSourceSplit> assignedSplits;

    private final @Nullable Long currentSnapshotId;

    private final Map<String, Long> currentSnapshotIds;

    public PaimonSourceState(
            Deque<PaimonSourceSplit> assignedSplits, @Nullable Long currentSnapshotId) {
        this.assignedSplits = assignedSplits;
        this.currentSnapshotId = currentSnapshotId;
        this.currentSnapshotIds = Collections.emptyMap();
    }

    public PaimonSourceState(
            Deque<PaimonSourceSplit> assignedSplits, Map<String, Long> currentSnapshotIds) {
        this.assignedSplits = assignedSplits;
        this.currentSnapshotIds = new HashMap<>(currentSnapshotIds);
        this.currentSnapshotId = getSingleSnapshotId(this.currentSnapshotIds);
    }

    public Deque<PaimonSourceSplit> getAssignedSplits() {
        return assignedSplits;
    }

    public @Nullable Long getCurrentSnapshotId() {
        return currentSnapshotId;
    }

    public Map<String, Long> getCurrentSnapshotIds() {
        return currentSnapshotIds == null ? Collections.emptyMap() : currentSnapshotIds;
    }

    private static @Nullable Long getSingleSnapshotId(Map<String, Long> currentSnapshotIds) {
        if (currentSnapshotIds.size() != 1) {
            return null;
        }
        return currentSnapshotIds.values().iterator().next();
    }
}
