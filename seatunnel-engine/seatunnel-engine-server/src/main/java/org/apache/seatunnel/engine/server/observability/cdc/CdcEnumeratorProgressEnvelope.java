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

package org.apache.seatunnel.engine.server.observability.cdc;

import org.apache.seatunnel.api.cdc.CdcEnumeratorProgressReport;
import org.apache.seatunnel.engine.server.execution.TaskLocation;

import java.io.Serializable;
import java.util.Objects;

/** Engine identity metadata for one coordinator-owned CDC enumerator report. */
public final class CdcEnumeratorProgressEnvelope implements Serializable {

    private static final long serialVersionUID = 1L;

    private final TaskLocation taskLocation;
    private final long sourceVertexId;
    private final long executionAttemptId;
    private final long observedAt;
    private final CdcEnumeratorProgressReport report;

    public CdcEnumeratorProgressEnvelope(
            TaskLocation taskLocation,
            long sourceVertexId,
            long executionAttemptId,
            long observedAt,
            CdcEnumeratorProgressReport report) {
        this.taskLocation = Objects.requireNonNull(taskLocation, "taskLocation must not be null");
        this.sourceVertexId = sourceVertexId;
        this.executionAttemptId = executionAttemptId;
        this.observedAt = observedAt;
        this.report = Objects.requireNonNull(report, "report must not be null");
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

    public long getObservedAt() {
        return observedAt;
    }

    public CdcEnumeratorProgressReport getReport() {
        return report;
    }
}
