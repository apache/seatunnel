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

import org.apache.seatunnel.api.annotation.Experimental;

import java.util.Objects;

/**
 * Progress detail for one active snapshot split.
 *
 * <p>Low and high watermarks always belong to the same {@link #getSplitId() split}. A watermark is
 * unavailable when that split has not established it yet. This value is deeply immutable: its
 * fields are final, {@link CdcProgressValue} is immutable, and any contained {@link
 * CdcProgressPosition} defensively copies and exposes an unmodifiable position map.
 */
@Experimental
public final class CdcSnapshotSplitProgress {

    /** Stable source split identifier. */
    private final String splitId;

    /** Connector table identifier associated with this split. */
    private final String tablePath;

    /** Split low watermark, when established. */
    private final CdcProgressValue<CdcProgressPosition> lowWatermark;

    /** Split high watermark, when established. */
    private final CdcProgressValue<CdcProgressPosition> highWatermark;

    public CdcSnapshotSplitProgress(
            String splitId,
            String tablePath,
            CdcProgressValue<CdcProgressPosition> lowWatermark,
            CdcProgressValue<CdcProgressPosition> highWatermark) {
        this.splitId = Objects.requireNonNull(splitId, "splitId must not be null");
        this.tablePath = Objects.requireNonNull(tablePath, "tablePath must not be null");
        this.lowWatermark = Objects.requireNonNull(lowWatermark, "lowWatermark must not be null");
        this.highWatermark =
                Objects.requireNonNull(highWatermark, "highWatermark must not be null");
    }

    public String getSplitId() {
        return splitId;
    }

    public String getTablePath() {
        return tablePath;
    }

    public CdcProgressValue<CdcProgressPosition> getLowWatermark() {
        return lowWatermark;
    }

    public CdcProgressValue<CdcProgressPosition> getHighWatermark() {
        return highWatermark;
    }
}
