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

    private final ConcurrentMap<ReaderKey, CdcReaderProgressEnvelope> readerReports =
            new ConcurrentHashMap<>();
    private final ConcurrentMap<SourceKey, CdcEnumeratorProgressEnvelope> enumeratorReports =
            new ConcurrentHashMap<>();

    public void updateReaderReports(Collection<CdcReaderProgressEnvelope> reports) {
        reports.forEach(
                report ->
                        readerReports.compute(
                                ReaderKey.from(report),
                                (key, current) -> newerReaderReport(current, report)));
    }

    public void updateEnumeratorReports(Collection<CdcEnumeratorProgressEnvelope> reports) {
        reports.forEach(
                report ->
                        enumeratorReports.compute(
                                SourceKey.from(report),
                                (key, current) -> newerEnumeratorReport(current, report)));
    }

    public List<CdcReaderProgressEnvelope> getReaderReports(
            long jobId, int pipelineId, long sourceVertexId) {
        List<CdcReaderProgressEnvelope> result = new ArrayList<>();
        readerReports.forEach(
                (key, value) -> {
                    if (key.matches(jobId, pipelineId, sourceVertexId)) {
                        result.add(value);
                    }
                });
        return Collections.unmodifiableList(result);
    }

    public CdcEnumeratorProgressEnvelope getEnumeratorReport(
            long jobId, int pipelineId, long sourceVertexId) {
        return enumeratorReports.get(new SourceKey(jobId, pipelineId, sourceVertexId));
    }

    public void removePipeline(PipelineLocation pipelineLocation) {
        readerReports
                .keySet()
                .removeIf(
                        key ->
                                key.jobId == pipelineLocation.getJobId()
                                        && key.pipelineId == pipelineLocation.getPipelineId());
        enumeratorReports
                .keySet()
                .removeIf(
                        key ->
                                key.jobId == pipelineLocation.getJobId()
                                        && key.pipelineId == pipelineLocation.getPipelineId());
    }

    private CdcReaderProgressEnvelope newerReaderReport(
            CdcReaderProgressEnvelope current, CdcReaderProgressEnvelope candidate) {
        if (current == null
                || candidate.getExecutionAttemptId() > current.getExecutionAttemptId()
                || (candidate.getExecutionAttemptId() == current.getExecutionAttemptId()
                        && candidate.getReportSequence() > current.getReportSequence())) {
            return candidate;
        }
        return current;
    }

    private CdcEnumeratorProgressEnvelope newerEnumeratorReport(
            CdcEnumeratorProgressEnvelope current, CdcEnumeratorProgressEnvelope candidate) {
        if (current == null
                || candidate.getExecutionAttemptId() > current.getExecutionAttemptId()
                || (candidate.getExecutionAttemptId() == current.getExecutionAttemptId()
                        && candidate.getReportSequence() > current.getReportSequence())) {
            return candidate;
        }
        return current;
    }

    private static class SourceKey {
        final long jobId;
        final int pipelineId;
        final long sourceVertexId;

        private SourceKey(long jobId, int pipelineId, long sourceVertexId) {
            this.jobId = jobId;
            this.pipelineId = pipelineId;
            this.sourceVertexId = sourceVertexId;
        }

        private static SourceKey from(CdcEnumeratorProgressEnvelope envelope) {
            return new SourceKey(
                    envelope.getTaskLocation().getJobId(),
                    envelope.getTaskLocation().getPipelineId(),
                    envelope.getSourceVertexId());
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
            if (!(object instanceof SourceKey)) {
                return false;
            }
            SourceKey sourceKey = (SourceKey) object;
            return jobId == sourceKey.jobId
                    && pipelineId == sourceKey.pipelineId
                    && sourceVertexId == sourceKey.sourceVertexId;
        }

        @Override
        public int hashCode() {
            return Objects.hash(jobId, pipelineId, sourceVertexId);
        }
    }

    private static final class ReaderKey extends SourceKey {
        private final int taskIndex;

        private ReaderKey(long jobId, int pipelineId, long sourceVertexId, int taskIndex) {
            super(jobId, pipelineId, sourceVertexId);
            this.taskIndex = taskIndex;
        }

        private static ReaderKey from(CdcReaderProgressEnvelope envelope) {
            return new ReaderKey(
                    envelope.getTaskLocation().getJobId(),
                    envelope.getTaskLocation().getPipelineId(),
                    envelope.getSourceVertexId(),
                    envelope.getTaskLocation().getTaskIndex());
        }

        @Override
        public boolean equals(Object object) {
            if (this == object) {
                return true;
            }
            if (!(object instanceof ReaderKey)) {
                return false;
            }
            ReaderKey readerKey = (ReaderKey) object;
            return taskIndex == readerKey.taskIndex && super.equals(object);
        }

        @Override
        public int hashCode() {
            return Objects.hash(super.hashCode(), taskIndex);
        }
    }
}
