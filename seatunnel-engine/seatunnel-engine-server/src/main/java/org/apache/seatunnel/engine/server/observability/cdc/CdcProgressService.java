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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Coordinator-side latest-only store for experimental CDC progress reports. */
public class CdcProgressService {

    private final Map<PipelineLocation, PipelineReports> pipelines = new HashMap<>();
    private volatile Generation generation = new Generation();
    private final Owner directOwner = new Owner(0L);
    private long nextOwnerSequence;

    /** Opaque coordinator ownership identity, captured before scheduling asynchronous work. */
    public static final class Generation {
        private boolean closed;

        private Generation() {}
    }

    /** Coordinator-local submission order; an older initializer cannot replace a newer owner. */
    public static final class Owner {
        private final long sequence;

        private Owner(long sequence) {
            this.sequence = sequence;
        }
    }

    /** Reserves ownership before asynchronous initialization is submitted. */
    public synchronized Owner newOwner() {
        return new Owner(++nextOwnerSequence);
    }

    /** Captures the current identity; a cleared identity can never become active again. */
    public Generation getGeneration() {
        return generation;
    }

    /** Opens a new generation before restoring jobs on coordinator activation. */
    public synchronized void activate() {
        if (generation.closed) {
            generation = new Generation();
        }
    }

    /**
     * Opens an observation scope before deploying a pipeline; reports cannot open one themselves.
     */
    public void registerPipeline(PipelineLocation pipelineLocation) {
        registerPipeline(generation, pipelineLocation, directOwner);
    }

    /**
     * Registers only for the captured generation and keeps repeated owner registration idempotent.
     */
    public synchronized void registerPipeline(
            Generation expected, PipelineLocation pipelineLocation, Owner owner) {
        if (expected != generation || generation.closed) {
            return;
        }
        PipelineReports current = pipelines.get(pipelineLocation);
        if (current == null || current.owner.sequence < owner.sequence) {
            pipelines.put(pipelineLocation, new PipelineReports(owner));
        }
    }

    /** Accepts latest reports only while their pipeline observation scope remains open. */
    public void updateReports(Collection<? extends CdcProgressEnvelope<?>> candidates) {
        updateReports(generation, candidates);
    }

    /** Checks ownership and publishes under the same monitor used by clear and registration. */
    public synchronized void updateReports(
            Generation expected, Collection<? extends CdcProgressEnvelope<?>> candidates) {
        if (expected != generation || generation.closed) {
            return;
        }
        candidates.forEach(
                report ->
                        pipelines.computeIfPresent(
                                new PipelineLocation(
                                        report.getTaskLocation().getJobId(),
                                        report.getTaskLocation().getPipelineId()),
                                (pipeline, reports) -> {
                                    reports.values.compute(
                                            ReportKey.from(report),
                                            (key, current) -> newerReport(current, report));
                                    return reports;
                                }));
    }

    /** Returns the independently sampled readers for one source vertex, without aggregation. */
    public synchronized List<CdcProgressEnvelope<CdcReaderProgressReport>> getReaderReports(
            long jobId, int pipelineId, long sourceVertexId) {
        List<CdcProgressEnvelope<CdcReaderProgressReport>> result = new ArrayList<>();
        pipelineReports(jobId, pipelineId)
                .forEach(
                        (key, value) -> {
                            if (key.owner == CdcProgressOwner.READER
                                    && key.matches(jobId, pipelineId, sourceVertexId)) {
                                result.add(readerEnvelope(value));
                            }
                        });
        return Collections.unmodifiableList(result);
    }

    /** Returns the latest assignment report, or null before collection or after cleanup. */
    public synchronized CdcProgressEnvelope<CdcEnumeratorProgressReport> getEnumeratorReport(
            long jobId, int pipelineId, long sourceVertexId) {
        CdcProgressEnvelope<?> report =
                pipelineReports(jobId, pipelineId)
                        .get(
                                new ReportKey(
                                        CdcProgressOwner.ENUMERATOR,
                                        jobId,
                                        pipelineId,
                                        sourceVertexId,
                                        -1));
        return report == null ? null : enumeratorEnvelope(report);
    }

    /** Closes the scope atomically with report insertion; delayed reports cannot reopen it. */
    public synchronized void removePipeline(PipelineLocation pipelineLocation) {
        pipelines.remove(pipelineLocation);
    }

    /** Removes only a scope still owned by this job master. */
    public synchronized void removePipeline(
            Generation expected, PipelineLocation pipelineLocation, Owner owner) {
        if (expected == generation) {
            PipelineReports current = pipelines.get(pipelineLocation);
            if (current != null && current.owner == owner) {
                pipelines.remove(pipelineLocation);
            }
        }
    }

    /** Rolls back all scopes opened by an unsuccessful initialization or submission. */
    public synchronized void removePipelines(Generation expected, Owner owner) {
        if (expected == generation) {
            pipelines.values().removeIf(reports -> reports.owner == owner);
        }
    }

    /** Drops coordinator-local observation state on master deactivation. */
    public synchronized void clear() {
        generation.closed = true;
        pipelines.clear();
    }

    private Map<ReportKey, CdcProgressEnvelope<?>> pipelineReports(long jobId, int pipelineId) {
        PipelineReports reports = pipelines.get(new PipelineLocation(jobId, pipelineId));
        return reports == null ? Collections.emptyMap() : reports.values;
    }

    private static final class PipelineReports {
        private final Owner owner;
        private final Map<ReportKey, CdcProgressEnvelope<?>> values = new HashMap<>();

        private PipelineReports(Owner owner) {
            this.owner = owner;
        }
    }

    private CdcProgressEnvelope<?> newerReport(
            CdcProgressEnvelope<?> current, CdcProgressEnvelope<?> candidate) {
        // Attempts supersede sequences; wall-clock observation time never orders reports.
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
