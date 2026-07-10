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

/** Snapshot split progress reported by a CDC source. */
@Getter
@ToString
@EqualsAndHashCode
public class CdcSnapshotProgress implements Serializable {
    private static final long serialVersionUID = 1L;

    private final int assignedSplitCount;
    private final int completedSplitCount;
    private final int runningSplitCount;
    private final int remainingSplitCount;
    private final String currentTable;
    private final String currentSplitId;
    private final CdcProgressPosition lowWatermark;
    private final CdcProgressPosition highWatermark;
    private final CdcProgressSupportLevel supportLevel;

    @Builder
    public CdcSnapshotProgress(
            int assignedSplitCount,
            int completedSplitCount,
            int runningSplitCount,
            int remainingSplitCount,
            String currentTable,
            String currentSplitId,
            CdcProgressPosition lowWatermark,
            CdcProgressPosition highWatermark,
            CdcProgressSupportLevel supportLevel) {
        this.assignedSplitCount = assignedSplitCount;
        this.completedSplitCount = completedSplitCount;
        this.runningSplitCount = runningSplitCount;
        this.remainingSplitCount = remainingSplitCount;
        this.currentTable = currentTable;
        this.currentSplitId = currentSplitId;
        this.lowWatermark = lowWatermark == null ? CdcProgressPosition.empty() : lowWatermark;
        this.highWatermark = highWatermark == null ? CdcProgressPosition.empty() : highWatermark;
        this.supportLevel =
                supportLevel == null ? CdcProgressSupportLevel.UNSUPPORTED : supportLevel;
    }
}
