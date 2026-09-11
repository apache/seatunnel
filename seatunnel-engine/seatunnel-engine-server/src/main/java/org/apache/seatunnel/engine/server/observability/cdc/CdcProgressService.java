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
import org.apache.seatunnel.api.cdc.CdcReaderProgressReport;
import org.apache.seatunnel.engine.server.dag.physical.PipelineLocation;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/** Coordinator-side latest-only store for experimental CDC progress reports. */
public class CdcProgressService {

    private final ConcurrentMap<ReportKey, CdcProgressEnvelope<?>> reports =
            new ConcurrentHashMap<>();

    public void updateReports(Collection<? extends CdcProgressEnvelope<?>> candidates) {
        candidates.forEach(
                report ->
                        reports.compute(
                                ReportKey.from(report),
                                (key, current) -> newerReport(current, report)));
    }

    public List<CdcProgressEnvelope<CdcReaderProgressReport>> getReaderReports(
            long jobId, int pipelineId, long sourceVertexId) {
        List<CdcProgressEnvelope<CdcReaderProgressReport>> result = new ArrayList<>();
        reports.forEach(
                (key, value) -> {
                    if (key.owner == CdcProgressOwner.READER
                            && key.matches(jobId, pipelineId, sourceVertexId)) {
                        result.add(readerEnvelope(value));
                    }
                });
        return Collections.unmodifiableList(result);
    }

    public CdcProgressEnvelope<CdcEnumeratorProgressReport> getEnumeratorReport(
            long jobId, int pipelineId, long sourceVertexId) {
        CdcProgressEnvelope<?> report =
                reports.get(
                        new ReportKey(
                                CdcProgressOwner.ENUMERATOR,
                                jobId,
                                pipelineId,
                                sourceVertexId,
                                -1));
        return report == null ? null : enumeratorEnvelope(report);
    }

    public void removePipeline(PipelineLocation pipelineLocation) {
        reports.keySet()
                .removeIf(
                        key ->
                                key.jobId == pipelineLocation.getJobId()
                                        && key.pipelineId == pipelineLocation.getPipelineId());
    }

    private CdcProgressEnvelope<?> newerReport(
            CdcProgressEnvelope<?> current, CdcProgressEnvelope<?> candidate) {
        if (current == null
                || candidate.getExecutionAttemptId() > current.getExecutionAttemptId()
                || (candidate.getExecutionAttemptId() == current.getExecutionAttemptId()
                        && candidate.getReportSequence() > current.getReportSequence())) {
            return candidate;
        }
        return current;
    }

    @SuppressWarnings("unchecked")
    private CdcProgressEnvelope<CdcReaderProgressReport> readerEnvelope(
            CdcProgressEnvelope<?> envelope) {
        if (envelope.getOwner() != CdcProgressOwner.READER) {
            throw new IllegalArgumentException("Expected a reader CDC progress report");
        }
        return (CdcProgressEnvelope<CdcReaderProgressReport>) envelope;
    }

    @SuppressWarnings("unchecked")
    private CdcProgressEnvelope<CdcEnumeratorProgressReport> enumeratorEnvelope(
            CdcProgressEnvelope<?> envelope) {
        if (envelope.getOwner() != CdcProgressOwner.ENUMERATOR) {
            throw new IllegalArgumentException("Expected an enumerator CDC progress report");
        }
        return (CdcProgressEnvelope<CdcEnumeratorProgressReport>) envelope;
    }

    private static final class ReportKey {
        final CdcProgressOwner owner;
        final long jobId;
        final int pipelineId;
        final long sourceVertexId;
        final int taskIndex;

        private ReportKey(
                CdcProgressOwner owner,
                long jobId,
                int pipelineId,
                long sourceVertexId,
                int taskIndex) {
            this.owner = owner;
            this.jobId = jobId;
            this.pipelineId = pipelineId;
            this.sourceVertexId = sourceVertexId;
            this.taskIndex = taskIndex;
        }

        private static ReportKey from(CdcProgressEnvelope<?> envelope) {
            return new ReportKey(
                    envelope.getOwner(),
                    envelope.getTaskLocation().getJobId(),
                    envelope.getTaskLocation().getPipelineId(),
                    envelope.getSourceVertexId(),
                    envelope.getOwner() == CdcProgressOwner.READER
                            ? envelope.getTaskLocation().getTaskIndex()
                            : -1);
        }

        final boolean matches(long jobId, int pipelineId, long sourceVertexId) {
            return this.jobId == jobId
                    && this.pipelineId == pipelineId
                    && this.sourceVertexId == sourceVertexId;
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) {
                return true;
            }
            if (!(object instanceof ReportKey)) {
                return false;
            }
            ReportKey reportKey = (ReportKey) object;
            return jobId == reportKey.jobId
                    && pipelineId == reportKey.pipelineId
                    && sourceVertexId == reportKey.sourceVertexId
                    && taskIndex == reportKey.taskIndex
                    && owner == reportKey.owner;
        }

        @Override
        public int hashCode() {
            return Objects.hash(owner, jobId, pipelineId, sourceVertexId, taskIndex);
        }
    }
}
