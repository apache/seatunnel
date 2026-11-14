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
 * Immutable facts owned by one CDC source reader. Runtime identity and observation time are added
 * by the engine when the report is collected.
 *
 * <p>The consumed position is the latest position this reader has observed, not a completed
 * checkpoint position. Checkpoint and restore positions are not part of this report; they belong to
 * a later lifecycle-backed extension of the contract.
 */
@Experimental
public final class CdcReaderProgressReport implements CdcProgressReport {

    /** Connector identifier returned by the source plugin. */
    private final String connectorType;

    /** Reader-local lifecycle at observation time. */
    private final CdcProgressLifecycle lifecycle;

    /** Current split identifier, or {@code null} before a split is initialized. */
    private final String activeSplitId;

    /** Latest position consumed by this reader. */
    private final CdcProgressValue<CdcProgressPosition> currentConsumedPosition;

    /** Epoch milliseconds when the current consumed position last changed, or {@code 0}. */
    private final long lastPositionChangeAt;

    /** Latest source event timestamp in epoch milliseconds, or {@code null}. */
    private final Long lastSourceEventAt;

    public CdcReaderProgressReport(
            String connectorType,
            CdcProgressLifecycle lifecycle,
            String activeSplitId,
            CdcProgressValue<CdcProgressPosition> currentConsumedPosition,
            long lastPositionChangeAt,
            Long lastSourceEventAt) {
        this.connectorType =
                Objects.requireNonNull(connectorType, "connectorType must not be null");
        this.lifecycle = Objects.requireNonNull(lifecycle, "lifecycle must not be null");
        this.activeSplitId = activeSplitId;
        this.currentConsumedPosition =
                Objects.requireNonNull(
                        currentConsumedPosition, "currentConsumedPosition must not be null");
        this.lastPositionChangeAt = lastPositionChangeAt;
        this.lastSourceEventAt = lastSourceEventAt;
    }

    public String getConnectorType() {
        return connectorType;
    }

    public CdcProgressLifecycle getLifecycle() {
        return lifecycle;
    }

    public String getActiveSplitId() {
        return activeSplitId;
    }

    public CdcProgressValue<CdcProgressPosition> getCurrentConsumedPosition() {
        return currentConsumedPosition;
    }

    public long getLastPositionChangeAt() {
        return lastPositionChangeAt;
    }

    /**
     * Returns the source event timestamp in epoch milliseconds, or {@code null} when the connector
     * cannot extract one from the latest record.
     */
    public Long getLastSourceEventAt() {
        return lastSourceEventAt;
    }
}
