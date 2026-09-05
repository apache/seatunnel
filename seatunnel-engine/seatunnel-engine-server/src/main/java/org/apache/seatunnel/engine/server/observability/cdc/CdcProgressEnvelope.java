/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
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

package org.apache.seatunnel.engine.server.observability.cdc;

import org.apache.seatunnel.api.cdc.CdcEnumeratorProgressReport;
import org.apache.seatunnel.api.cdc.CdcProgressReport;
import org.apache.seatunnel.api.cdc.CdcReaderProgressReport;
import org.apache.seatunnel.engine.server.execution.TaskLocation;

import java.util.Objects;

/** Engine identity and ordering metadata for one connector-owned CDC progress report. */
public final class CdcProgressEnvelope<R extends CdcProgressReport> {

    private final CdcProgressOwner owner;
    private final TaskLocation taskLocation;
    private final long sourceVertexId;
    private final long executionAttemptId;
    private final long reportSequence;
    private final long observedAt;
    private final R report;

    public CdcProgressEnvelope(
            CdcProgressOwner owner,
            TaskLocation taskLocation,
            long sourceVertexId,
            long executionAttemptId,
            long reportSequence,
            long observedAt,
            R report) {
        this.owner = Objects.requireNonNull(owner, "owner must not be null");
        this.taskLocation = Objects.requireNonNull(taskLocation, "taskLocation must not be null");
        this.sourceVertexId = sourceVertexId;
        this.executionAttemptId = executionAttemptId;
        this.reportSequence = reportSequence;
        this.observedAt = observedAt;
        this.report = Objects.requireNonNull(report, "report must not be null");
        validateOwner(owner, report);
    }

    private static void validateOwner(CdcProgressOwner owner, CdcProgressReport report) {
        boolean valid =
                owner == CdcProgressOwner.READER
                        ? report instanceof CdcReaderProgressReport
                        : report instanceof CdcEnumeratorProgressReport;
        if (!valid) {
            throw new IllegalArgumentException(
                    "CDC progress owner "
                            + owner
                            + " does not match report type "
                            + report.getClass().getSimpleName());
        }
    }

    public CdcProgressOwner getOwner() {
        return owner;
    }

    public TaskLocation getTaskLocation() {
        return taskLocation;
    }

    public long getSourceVertexId() {
        return sourceVertexId;
    }

    public long getExecutionAttemptId() {
        return executionAttemptId;
    }

    public long getReportSequence() {
        return reportSequence;
    }

    public long getObservedAt() {
        return observedAt;
    }

    public R getReport() {
        return report;
    }
}
