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

package org.apache.seatunnel.api.cdc;

import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

import java.io.Serializable;
import java.util.Collections;
import java.util.EnumMap;
import java.util.Map;

/** Latest CDC progress snapshot for one source, table, or split. */
@Getter
@ToString
@EqualsAndHashCode
public class CdcProgressSnapshot implements Serializable {
    private static final long serialVersionUID = 1L;

    private final Long jobId;
    private final Integer pipelineId;
    private final String vertexId;
    private final String connectorType;
    private final String tablePath;
    private final String splitId;
    private final CdcProgressPhase phase;
    private final CdcSnapshotProgress snapshotProgress;
    private final CdcIncrementalProgress incrementalProgress;
    private final CdcCheckpointProgress checkpointProgress;
    private final CdcProgressPosition rawPosition;
    private final Map<CdcProgressSupportGroup, CdcProgressSupportLevel> supportLevels;
    private final long lastProgressTime;
    private final CdcStalledStatus stalledStatus;

    @Builder
    public CdcProgressSnapshot(
            Long jobId,
            Integer pipelineId,
            String vertexId,
            String connectorType,
            String tablePath,
            String splitId,
            CdcProgressPhase phase,
            CdcSnapshotProgress snapshotProgress,
            CdcIncrementalProgress incrementalProgress,
            CdcCheckpointProgress checkpointProgress,
            CdcProgressPosition rawPosition,
            Map<CdcProgressSupportGroup, CdcProgressSupportLevel> supportLevels,
            long lastProgressTime,
            CdcStalledStatus stalledStatus) {
        this.jobId = jobId;
        this.pipelineId = pipelineId;
        this.vertexId = vertexId;
        this.connectorType = connectorType;
        this.tablePath = tablePath;
        this.splitId = splitId;
        this.phase = phase == null ? CdcProgressPhase.UNKNOWN : phase;
        this.snapshotProgress = snapshotProgress;
        this.incrementalProgress = incrementalProgress;
        this.checkpointProgress = checkpointProgress;
        this.rawPosition = rawPosition == null ? CdcProgressPosition.empty() : rawPosition;
        this.supportLevels = immutableSupportLevels(supportLevels);
        this.lastProgressTime = lastProgressTime;
        this.stalledStatus = stalledStatus;
    }

    private static Map<CdcProgressSupportGroup, CdcProgressSupportLevel> immutableSupportLevels(
            Map<CdcProgressSupportGroup, CdcProgressSupportLevel> supportLevels) {
        if (supportLevels == null || supportLevels.isEmpty()) {
            return Collections.emptyMap();
        }
        return Collections.unmodifiableMap(new EnumMap<>(supportLevels));
    }
}
