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

package org.apache.seatunnel.engine.server.master;

import org.apache.seatunnel.api.common.metrics.JobMetrics;
import org.apache.seatunnel.api.common.metrics.Measurement;
import org.apache.seatunnel.api.common.metrics.MetricNames;
import org.apache.seatunnel.api.sink.multitablesink.MultiTableSink;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.core.dag.actions.Action;
import org.apache.seatunnel.engine.core.dag.actions.SinkAction;
import org.apache.seatunnel.engine.core.dag.actions.SourceAction;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.dag.logical.LogicalEdge;
import org.apache.seatunnel.engine.core.dag.logical.LogicalVertex;
import org.apache.seatunnel.lineage.LineageConfig;
import org.apache.seatunnel.lineage.LineageDataset;
import org.apache.seatunnel.lineage.LineageEvent;
import org.apache.seatunnel.lineage.LineageEventType;
import org.apache.seatunnel.lineage.LineageOutputStatistics;
import org.apache.seatunnel.lineage.LineageRunIds;
import org.apache.seatunnel.lineage.LineageRuntime;

import com.hazelcast.logging.ILogger;
import com.hazelcast.logging.Logger;

import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/** Builds Zeta lineage events from the restored logical DAG and job metrics. */
public final class ZetaLineageReporter {
    private static final ILogger LOGGER = Logger.getLogger(ZetaLineageReporter.class);

    private ZetaLineageReporter() {}

    /**
     * Reports a Zeta job lifecycle event when the job enters a lineage-relevant state.
     *
     * <p>The events are built on the calling thread so that they describe the job as it was at the
     * transition, but they are sent through {@link JobMaster#submitLineageEmission(Runnable)}: this
     * method runs inside {@code PhysicalPlan.updateJobState}, which holds the plan monitor, and a
     * blocking send would hold it for the lifetime of the HTTP request.
     *
     * @param jobMaster job master that owns the logical DAG and job configuration
     * @param status current job status
     */
    public static void reportJobState(JobMaster jobMaster, JobStatus status) {
        if (status != JobStatus.RUNNING && !status.isEndState()) {
            return;
        }
        LineageConfig config = jobMaster.resolveLineageConfig();
        if (!config.enabled()) {
            return;
        }
        LineageEventType eventType = eventType(status);
        List<LineageEvent> events = events(jobMaster, config, eventType);
        if (events.isEmpty()) {
            return;
        }
        jobMaster.submitLineageEmission(() -> emit(config, events));
    }

    /**
     * Reports a throttled lineage heartbeat after a completed streaming checkpoint.
     *
     * <p>Everything past the throttle runs off the calling thread. The caller is the checkpoint
     * coordinator finishing a checkpoint, and both halves of the work block: collecting the current
     * metrics issues an RPC to every worker, and sending performs an HTTP request. Doing either on
     * the coordinator thread lets an unreachable lineage receiver stall checkpointing.
     *
     * <p>A non-positive interval disables the heartbeat, which is what the option means on Flink
     * too. Without this the comparison below would read every completed checkpoint as overdue and
     * report on each one, so the value that turns heartbeats off on one engine would produce the
     * heaviest possible reporting on the other.
     *
     * @param jobMaster job master that owns the pipeline
     */
    public static void reportHeartbeat(JobMaster jobMaster) {
        try {
            LineageConfig config = jobMaster.resolveLineageConfig();
            if (!config.enabled()
                    || config.heartbeatMinIntervalMs() <= 0
                    || jobMaster.getJobStatus().isEndState()) {
                return;
            }
            if (jobMaster.getJobImmutableInformation().getJobConfig().getJobContext().getJobMode()
                    != JobMode.STREAMING) {
                return;
            }
            if (!shouldReportHeartbeat(jobMaster, config.heartbeatMinIntervalMs())) {
                return;
            }
            jobMaster.submitLineageEmission(
                    () -> {
                        try {
                            // Checked again here, not only above: the job can reach an end state
                            // between the check above and this submission, and the terminal event
                            // is submitted from the plan thread. Without this, a heartbeat that
                            // lost that race is drained after the terminal event and leaves the
                            // run marked running forever, which is what the heartbeat exists to
                            // prevent.
                            if (jobMaster.getJobStatus().isEndState()) {
                                return;
                            }
                            emit(config, events(jobMaster, config, LineageEventType.RUNNING));
                        } catch (Throwable error) {
                            LOGGER.warning("Failed to emit Zeta lineage heartbeat", error);
                        }
                    });
        } catch (Throwable error) {
            LOGGER.warning("Failed to emit Zeta lineage heartbeat", error);
        }
    }

    private static boolean shouldReportHeartbeat(JobMaster jobMaster, long minimumIntervalMs) {
        // Checkpoint callbacks are per pipeline, but lineage heartbeat expiry is job-wide, so a
        // single timestamp covers every pipeline of the job.
        AtomicLong lastTime = jobMaster.getLineageHeartbeatTime();
        long now = System.currentTimeMillis();
        while (true) {
            long previous = lastTime.get();
            if (previous != 0 && now - previous < minimumIntervalMs) {
                return false;
            }
            if (lastTime.compareAndSet(previous, now)) {
                return true;
            }
        }
    }

    private static List<LineageEvent> events(
            JobMaster jobMaster, LineageConfig config, LineageEventType eventType) {
        LogicalDag logicalDag = jobMaster.getLogicalDag();
        if (logicalDag == null) {
            return Collections.emptyList();
        }
        // Only a run that is still producing, or one that finished, carries meaningful counts: an
        // aborted or failed run's metrics describe work that was rolled back.
        boolean includeStatistics =
                eventType == LineageEventType.COMPLETE || eventType == LineageEventType.RUNNING;
        JobMetrics metrics = includeStatistics ? outputMetrics(jobMaster, eventType) : null;
        Map<Long, List<Long>> upstreamsByVertex = upstreamIndex(logicalDag);
        List<LineageEvent> events = new ArrayList<>();
        int sinkCount = 0;
        Map<TablePath, Integer> tableOwners = new HashMap<>();
        for (LogicalVertex vertex : logicalDag.getLogicalVertexMap().values()) {
            if (vertex.getAction() instanceof SinkAction) {
                SinkAction<?, ?, ?, ?> sink = (SinkAction<?, ?, ?, ?>) vertex.getAction();
                sinkCount++;
                if (includeStatistics) {
                    for (TablePath table : metricTablePaths(sink)) {
                        tableOwners.merge(table, 1, Integer::sum);
                    }
                }
            }
        }
        for (LogicalVertex vertex : logicalDag.getLogicalVertexMap().values()) {
            Action action = vertex.getAction();
            if (!(action instanceof SinkAction)) {
                continue;
            }
            SinkAction<?, ?, ?, ?> sinkAction = (SinkAction<?, ?, ?, ?>) action;
            List<LineageDataset> inputs =
                    sourceDatasets(logicalDag, upstreamsByVertex, vertex.getVertexId());
            List<LineageDataset> outputs = sinkDatasets(sinkAction);
            LineageOutputStatistics statistics =
                    includeStatistics
                            ? outputStatistics(
                                    metrics, sinkAction, outputs.size(), sinkCount, tableOwners)
                            : null;
            for (LineageDataset output : outputs) {
                LineageDataset eventOutput = output;
                if (statistics != null) {
                    eventOutput = output.withOutputStatistics(statistics);
                }
                Map<String, Object> properties = new HashMap<>(config.runProperties());
                properties.put("engine", "zeta");
                properties.put("sink_action", sinkAction.getName());
                events.add(
                        LineageEvent.builder()
                                .runId(
                                        LineageRunIds.forJob(
                                                jobMaster.getJobId(),
                                                output.namespace(),
                                                output.name(),
                                                jobMaster.getLineageAttempt()))
                                .eventTime(ZonedDateTime.now(ZoneOffset.UTC))
                                .eventType(eventType)
                                .jobNamespace(config.namespace())
                                .jobName(
                                        jobName(
                                                config,
                                                jobMaster
                                                        .getJobImmutableInformation()
                                                        .getJobConfig()
                                                        .getName(),
                                                output))
                                .producer(config.producer())
                                .runFacet(config.runFacet())
                                .runProperties(properties)
                                .inputs(inputs)
                                .outputs(Collections.singletonList(eventOutput))
                                .build());
            }
        }
        if (events.isEmpty()) {
            // Reporting is enabled, so producing nothing is a result the operator needs to see;
            // otherwise a job with unsupported or unidentifiable sinks looks identical to a
            // working one that simply has no lineage yet.
            LOGGER.info(
                    "No lineage events built for job "
                            + jobMaster.getJobId()
                            + ": "
                            + logicalDag.getLogicalVertexMap().size()
                            + " vertices, "
                            + sinkCount
                            + " sink actions, none of which resolved to a supported dataset");
        }
        return events;
    }

    /**
     * Returns the OpenLineage job name of the run reporting one output.
     *
     * <p>Every output is reported as its own run, and a receiver that keeps one current run per job
     * then shows only the last one to arrive. With {@link LineageConfig#jobNamePerOutput()} each
     * output gets a job of its own, named after the output in the OpenLineage {@code
     * namespace/name} form. The suffix is applied to a single-output job too, so that adding a sink
     * later does not rename the existing output's job.
     */
    static String jobName(LineageConfig config, String jobName, LineageDataset output) {
        if (!config.jobNamePerOutput()) {
            return jobName;
        }
        return jobName + "::" + output.namespace() + "/" + output.name();
    }

    private static void emit(LineageConfig config, List<LineageEvent> events) {
        try {
            for (LineageEvent event : events) {
                LineageRuntime.emit(config, event);
            }
        } catch (Throwable error) {
            LOGGER.warning("Failed to emit Zeta lineage event", error);
        }
    }

    private static LineageEventType eventType(JobStatus status) {
        switch (status) {
            case FINISHED:
            case SAVEPOINT_DONE:
                return LineageEventType.COMPLETE;
            case CANCELED:
                return LineageEventType.ABORT;
            case FAILED:
            case UNKNOWABLE:
                return LineageEventType.FAIL;
            case RUNNING:
                return LineageEventType.START;
            default:
                throw new IllegalArgumentException("Unsupported lineage job status: " + status);
        }
    }

    /** Indexes the DAG edges by target vertex so a backwards walk does not rescan them. */
    private static Map<Long, List<Long>> upstreamIndex(LogicalDag logicalDag) {
        Map<Long, List<Long>> upstreamsByVertex = new HashMap<>();
        for (LogicalEdge edge : logicalDag.getEdges()) {
            if (edge.getTargetVertexId() == null || edge.getInputVertexId() == null) {
                continue;
            }
            upstreamsByVertex
                    .computeIfAbsent(edge.getTargetVertexId(), ignored -> new ArrayList<>())
                    .add(edge.getInputVertexId());
        }
        return upstreamsByVertex;
    }

    /**
     * Walks the logical DAG backwards from a sink vertex and collects the datasets of every source
     * reachable from it.
     *
     * <p>The traversal deliberately uses {@link LogicalDag#getEdges()} rather than {@link
     * Action#getUpstream()}. {@code AbstractAction.upstreams} is {@code transient}, so after the
     * logical DAG is deserialized on the master the accessor returns {@code null} even though its
     * declaration is annotated {@code @NonNull} — the annotation is documentation, not a runtime
     * check. Edges survive because {@code LogicalDag} serializes them explicitly, carrying vertex
     * IDs rather than vertex objects to avoid a serialization cycle.
     *
     * @param logicalDag restored logical DAG
     * @param upstreamsByVertex edge index built once per event set by {@link #upstreamIndex}
     * @param sinkVertexId vertex ID of the sink to walk back from
     * @return datasets of all reachable sources, without duplicates
     */
    private static List<LineageDataset> sourceDatasets(
            LogicalDag logicalDag, Map<Long, List<Long>> upstreamsByVertex, Long sinkVertexId) {
        if (sinkVertexId == null) {
            return Collections.emptyList();
        }
        Set<LineageDataset> datasets = new LinkedHashSet<>();
        Set<Long> visited = new HashSet<>();
        Deque<Long> pending = new ArrayDeque<>();
        pending.add(sinkVertexId);
        while (!pending.isEmpty()) {
            Long vertexId = pending.poll();
            if (!visited.add(vertexId)) {
                continue;
            }
            LogicalVertex vertex = logicalDag.getLogicalVertexMap().get(vertexId);
            if (vertex != null && vertex.getAction() instanceof SourceAction) {
                datasets.addAll(((SourceAction<?, ?, ?>) vertex.getAction()).getLineageDatasets());
                continue;
            }
            List<Long> upstreams = upstreamsByVertex.get(vertexId);
            if (upstreams != null) {
                pending.addAll(upstreams);
            }
        }
        return new ArrayList<>(datasets);
    }

    /**
     * Returns the datasets this sink writes to.
     *
     * <p>The datasets come from the connector options, which name the write target directly. They
     * are deliberately not cross-checked against {@link MultiTableSink#getSinks()}: that map is
     * keyed by the <em>upstream</em> table identity feeding each writer, not by the table being
     * written. For a job such as {@code FakeSource -> Paimon} the key is the source's {@code
     * plugin_output} while the target is the configured {@code database.table}, so filtering on it
     * discards every dataset and the job reports no lineage at all.
     */
    private static List<LineageDataset> sinkDatasets(SinkAction<?, ?, ?, ?> action) {
        List<LineageDataset> configured = action.getLineageDatasets();
        if (configured.isEmpty()) {
            LOGGER.info(
                    "Sink action "
                            + action.getName()
                            + " has no lineage datasets; its connector options did not identify a"
                            + " supported table");
        }
        return configured;
    }

    private static JobMetrics outputMetrics(JobMaster jobMaster, LineageEventType eventType) {
        if (eventType == LineageEventType.RUNNING) {
            return jobMaster.getCurrentJobMetricsForLineage();
        }
        return jobMaster.getJobHistoryService().getJobMetrics(jobMaster.getJobId());
    }

    private static LineageOutputStatistics outputStatistics(
            JobMetrics metrics,
            SinkAction<?, ?, ?, ?> sinkAction,
            int outputCount,
            int sinkCount,
            Map<TablePath, Integer> tableOwners) {
        List<TablePath> tablePaths = metricTablePaths(sinkAction);
        if (outputCount != 1 || tablePaths.stream().anyMatch(table -> tableOwners.get(table) > 1)) {
            return null;
        }
        boolean allowTotal = sinkCount == 1;
        Long committedCount =
                metricValue(metrics, MetricNames.SINK_COMMITTED_COUNT, allowTotal, tablePaths);
        Long committedBytes =
                metricValue(metrics, MetricNames.SINK_COMMITTED_BYTES, allowTotal, tablePaths);
        if (committedCount != null && committedCount > 0) {
            return new LineageOutputStatistics(committedCount, committedBytes, "committed");
        }
        Long writeCount =
                metricValue(metrics, MetricNames.SINK_WRITE_COUNT, allowTotal, tablePaths);
        Long writeBytes =
                metricValue(metrics, MetricNames.SINK_WRITE_BYTES, allowTotal, tablePaths);
        if (writeCount != null && writeCount > 0) {
            return new LineageOutputStatistics(writeCount, writeBytes, "attempted");
        }
        return null;
    }

    private static List<TablePath> metricTablePaths(SinkAction<?, ?, ?, ?> action) {
        if (action.getSink() instanceof MultiTableSink) {
            // Counters use resolved write targets, not the upstream keys of getSinks().
            return ((MultiTableSink) action.getSink())
                    .getSinkTables().stream().distinct().collect(Collectors.toList());
        }
        return Collections.emptyList();
    }

    /** Reads counters for uniquely owned write tables, using job totals only for a single sink. */
    private static Long metricValue(
            JobMetrics metrics, String metricName, boolean allowTotal, List<TablePath> tablePaths) {
        if (metrics == null) {
            return null;
        }
        long total = 0;
        for (TablePath tablePath : tablePaths) {
            Long value = sum(metrics, metricName + "#" + tablePath.getFullName());
            if (value == null) {
                // An incomplete table slice must not turn into another sink's job-wide total.
                return allowTotal ? sum(metrics, metricName) : null;
            }
            total += value;
        }
        if (!tablePaths.isEmpty()) {
            return total;
        }
        return allowTotal ? sum(metrics, metricName) : null;
    }

    private static Long sum(JobMetrics metrics, String metricName) {
        List<Measurement> measurements = metrics.get(metricName);
        if (measurements.isEmpty()) {
            return null;
        }
        long total = 0;
        boolean hasNumber = false;
        for (Measurement measurement : measurements) {
            if (measurement.value() instanceof Number) {
                total += ((Number) measurement.value()).longValue();
                hasNumber = true;
            }
        }
        return hasNumber ? total : null;
    }
}
