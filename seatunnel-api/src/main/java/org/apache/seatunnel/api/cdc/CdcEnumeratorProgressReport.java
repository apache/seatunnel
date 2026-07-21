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

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/**
 * Immutable split-assignment facts owned by one CDC source enumerator.
 *
 * <p>Assigned count includes running and completed assigned splits. Prepared remaining count covers
 * splits that have already been created but not assigned. Remaining unchunked table count covers
 * tables that have not been converted into snapshot splits yet. These categories are intentionally
 * separate so lazy chunk creation is not hidden behind one ambiguous remaining count.
 */
@Experimental
public final class CdcEnumeratorProgressReport implements Serializable {

    private static final long serialVersionUID = 1L;

    /** Connector identifier returned by the source plugin. */
    private final String connectorType;

    /** Enumerator-local lifecycle at observation time. */
    private final CdcProgressLifecycle lifecycle;

    /** Total number of splits assigned during the current snapshot lifecycle. */
    private final CdcProgressValue<Integer> assignedSplitCount;

    /** Number of assigned splits with a completed watermark. */
    private final CdcProgressValue<Integer> completedSplitCount;

    /** Number of assigned splits without a completed watermark. */
    private final CdcProgressValue<Integer> runningSplitCount;

    /** Number of already-created splits waiting for assignment. */
    private final CdcProgressValue<Integer> preparedRemainingSplitCount;

    /** Number of captured tables that have not been chunked into splits. */
    private final CdcProgressValue<Integer> remainingUnchunkedTableCount;

    /** Immutable per-split details for assigned splits that are not completed. */
    private final List<CdcSnapshotSplitProgress> activeSplits;

    public CdcEnumeratorProgressReport(
            String connectorType,
            CdcProgressLifecycle lifecycle,
            CdcProgressValue<Integer> assignedSplitCount,
            CdcProgressValue<Integer> completedSplitCount,
            CdcProgressValue<Integer> runningSplitCount,
            CdcProgressValue<Integer> preparedRemainingSplitCount,
            CdcProgressValue<Integer> remainingUnchunkedTableCount,
            List<CdcSnapshotSplitProgress> activeSplits) {
        this.connectorType =
                Objects.requireNonNull(connectorType, "connectorType must not be null");
        this.lifecycle = Objects.requireNonNull(lifecycle, "lifecycle must not be null");
        this.assignedSplitCount =
                Objects.requireNonNull(assignedSplitCount, "assignedSplitCount must not be null");
        this.completedSplitCount =
                Objects.requireNonNull(completedSplitCount, "completedSplitCount must not be null");
        this.runningSplitCount =
                Objects.requireNonNull(runningSplitCount, "runningSplitCount must not be null");
        this.preparedRemainingSplitCount =
                Objects.requireNonNull(
                        preparedRemainingSplitCount,
                        "preparedRemainingSplitCount must not be null");
        this.remainingUnchunkedTableCount =
                Objects.requireNonNull(
                        remainingUnchunkedTableCount,
                        "remainingUnchunkedTableCount must not be null");
        this.activeSplits =
                Collections.unmodifiableList(
                        new ArrayList<>(
                                Objects.requireNonNull(
                                        activeSplits, "activeSplits must not be null")));
    }

    public String getConnectorType() {
        return connectorType;
    }

    public CdcProgressLifecycle getLifecycle() {
        return lifecycle;
    }

    public CdcProgressValue<Integer> getAssignedSplitCount() {
        return assignedSplitCount;
    }

    public CdcProgressValue<Integer> getCompletedSplitCount() {
        return completedSplitCount;
    }

    public CdcProgressValue<Integer> getRunningSplitCount() {
        return runningSplitCount;
    }

    public CdcProgressValue<Integer> getPreparedRemainingSplitCount() {
        return preparedRemainingSplitCount;
    }

    public CdcProgressValue<Integer> getRemainingUnchunkedTableCount() {
        return remainingUnchunkedTableCount;
    }

    public List<CdcSnapshotSplitProgress> getActiveSplits() {
        return activeSplits;
    }
}
