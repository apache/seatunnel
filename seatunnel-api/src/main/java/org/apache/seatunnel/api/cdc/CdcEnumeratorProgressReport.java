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
public final class CdcEnumeratorProgressReport implements CdcProgressReport {

    /** Maximum number of active split details retained in one report. */
    public static final int MAX_ACTIVE_SPLITS = 100;

    /** Connector identifier returned by the source plugin. */
    private final String connectorType;

    /** Snapshot assignment state owned by the enumerator. */
    private final CdcSnapshotAssignmentStatus snapshotAssignmentStatus;

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

    /** Whether active split details were truncated to {@link #MAX_ACTIVE_SPLITS}. */
    private final boolean activeSplitsTruncated;

    public CdcEnumeratorProgressReport(
            String connectorType,
            CdcSnapshotAssignmentStatus snapshotAssignmentStatus,
            CdcProgressValue<Integer> assignedSplitCount,
            CdcProgressValue<Integer> completedSplitCount,
            CdcProgressValue<Integer> runningSplitCount,
            CdcProgressValue<Integer> preparedRemainingSplitCount,
            CdcProgressValue<Integer> remainingUnchunkedTableCount,
            List<CdcSnapshotSplitProgress> activeSplits) {
        this(
                connectorType,
                snapshotAssignmentStatus,
                assignedSplitCount,
                completedSplitCount,
                runningSplitCount,
                preparedRemainingSplitCount,
                remainingUnchunkedTableCount,
                activeSplits,
                activeSplits != null && activeSplits.size() > MAX_ACTIVE_SPLITS);
    }

    public CdcEnumeratorProgressReport(
            String connectorType,
            CdcSnapshotAssignmentStatus snapshotAssignmentStatus,
            CdcProgressValue<Integer> assignedSplitCount,
            CdcProgressValue<Integer> completedSplitCount,
            CdcProgressValue<Integer> runningSplitCount,
            CdcProgressValue<Integer> preparedRemainingSplitCount,
            CdcProgressValue<Integer> remainingUnchunkedTableCount,
            List<CdcSnapshotSplitProgress> activeSplits,
            boolean activeSplitsTruncated) {
        this.connectorType =
                Objects.requireNonNull(connectorType, "connectorType must not be null");
        this.snapshotAssignmentStatus =
                Objects.requireNonNull(
                        snapshotAssignmentStatus, "snapshotAssignmentStatus must not be null");
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
        validateCount("assignedSplitCount", assignedSplitCount);
        validateCount("completedSplitCount", completedSplitCount);
        validateCount("runningSplitCount", runningSplitCount);
        validateCount("preparedRemainingSplitCount", preparedRemainingSplitCount);
        validateCount("remainingUnchunkedTableCount", remainingUnchunkedTableCount);
        validateExactSplitCounts(assignedSplitCount, completedSplitCount, runningSplitCount);
        List<CdcSnapshotSplitProgress> splitDetails =
                Objects.requireNonNull(activeSplits, "activeSplits must not be null");
        int retainedSplitCount = Math.min(splitDetails.size(), MAX_ACTIVE_SPLITS);
        this.activeSplits =
                Collections.unmodifiableList(
                        new ArrayList<>(splitDetails.subList(0, retainedSplitCount)));
        this.activeSplitsTruncated =
                activeSplitsTruncated || splitDetails.size() > MAX_ACTIVE_SPLITS;
    }

    public String getConnectorType() {
        return connectorType;
    }

    public CdcSnapshotAssignmentStatus getSnapshotAssignmentStatus() {
        return snapshotAssignmentStatus;
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

    public boolean isActiveSplitsTruncated() {
        return activeSplitsTruncated;
    }

    private static void validateCount(String name, CdcProgressValue<Integer> count) {
        if (count.getValue() != null && count.getValue() < 0) {
            throw new IllegalArgumentException(name + " must not be negative");
        }
    }

    private static void validateExactSplitCounts(
            CdcProgressValue<Integer> assigned,
            CdcProgressValue<Integer> completed,
            CdcProgressValue<Integer> running) {
        if (assigned.getAccuracy() == CdcProgressAccuracy.EXACT
                && completed.getAccuracy() == CdcProgressAccuracy.EXACT
                && running.getAccuracy() == CdcProgressAccuracy.EXACT
                && assigned.getValue().longValue()
                        != completed.getValue().longValue() + running.getValue().longValue()) {
            throw new IllegalArgumentException(
                    "assignedSplitCount must equal completedSplitCount plus runningSplitCount");
        }
    }
}
