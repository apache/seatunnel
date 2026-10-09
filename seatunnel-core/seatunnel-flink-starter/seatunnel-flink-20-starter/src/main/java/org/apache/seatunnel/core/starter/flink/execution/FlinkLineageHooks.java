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

package org.apache.seatunnel.core.starter.flink.execution;

import org.apache.seatunnel.api.common.metrics.MetricNames;
import org.apache.seatunnel.lineage.LineageConfig;
import org.apache.seatunnel.lineage.LineageEvent;
import org.apache.seatunnel.lineage.LineageEventType;
import org.apache.seatunnel.lineage.LineageOutputStatistics;
import org.apache.seatunnel.lineage.LineageRuntime;
import org.apache.seatunnel.lineage.flink.FlinkClusterOptions;
import org.apache.seatunnel.lineage.flink.LineageJobStatusHook;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.core.execution.JobListener;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * The Flink 1.20 lineage registration, overriding the unsupported one in the common starter.
 *
 * <p>This class exists per Flink version rather than in the common starter because {@code
 * JobStatusHook} is a 1.16+ API: a module compiled against 1.15 can neither name the interface nor
 * call {@code StreamGraph.registerJobStatusHook}. Only this half is version-specific — event
 * construction, configuration resolution and the deployment diagnostics stay in {@link
 * FlinkLineageSupport}.
 */
final class FlinkLineageHooks {
    private static final Logger LOGGER = LoggerFactory.getLogger(FlinkLineageHooks.class);

    private FlinkLineageHooks() {}

    /** Returns whether this starter can register a job status hook. */
    static boolean isSupported() {
        return true;
    }

    /**
     * Registers the JobManager-side status hook and the client-side statistics listener.
     *
     * @param streamGraph the graph the hook is attached to
     * @param environment the environment the result listener is registered on
     * @param config lineage configuration with the auth token already removed
     * @param events the start events whose runs the hook closes
     * @param clientReportsTerminalEvent whether the client reports the successful terminal event
     * @return whether the hook was registered
     */
    static boolean register(
            StreamGraph streamGraph,
            StreamExecutionEnvironment environment,
            LineageConfig config,
            List<LineageEvent> events,
            boolean clientReportsTerminalEvent) {
        streamGraph.registerJobStatusHook(
                new LineageJobStatusHook(config, events, clientReportsTerminalEvent));
        try {
            environment.registerJobListener(new OutputStatisticsListener(config, events));
        } catch (Throwable error) {
            LOGGER.warn("Failed to register Flink lineage result listener", error);
        }
        return true;
    }

    /**
     * Reports the terminal events only the submitting client can report.
     *
     * <p>Two of them. The output statistics live in the job accumulators, which only the client can
     * read, so an attached submission reports the successful terminal event here and the JobManager
     * hook is told to stay silent so the two cannot race. A submission that fails is the other: no
     * hook runs for a job that never reached the cluster, so nothing else would close the runs
     * whose start events have already been sent.
     */
    private static final class OutputStatisticsListener implements JobListener {
        private final LineageConfig config;
        private final List<LineageEvent> events;

        private OutputStatisticsListener(LineageConfig config, List<LineageEvent> events) {
            this.config = config;
            this.events = events;
        }

        /**
         * Closes the runs whose start events were already sent when the submission fails.
         *
         * <p>The start events describe the job the caller is about to run and are sent before the
         * graph reaches the cluster, so a submission that never starts would otherwise leave every
         * one of those runs open on the receiver until its abandoned-run timeout. That is not a
         * corner case here: a JobManager without {@code seatunnel-lineage-flink} in its {@code
         * lib/} directory fails the submission by design, and this integration is what makes it
         * fail.
         *
         * <p>Flink calls this with a null client and the failure on that path, which is the first
         * point at which the client knows the job will not run. A submission whose reply was lost
         * after the JobManager accepted the job reports FAIL here and is later corrected by the
         * hook's own terminal event, which the receiver sees last.
         */
        @Override
        public void onJobSubmitted(JobClient jobClient, Throwable throwable) {
            if (throwable == null) {
                return;
            }
            try {
                emit(LineageEventType.FAIL, null, null);
            } catch (Throwable error) {
                LOGGER.warn("Failed to close the Flink lineage runs of a failed submission", error);
            }
        }

        @Override
        public void onJobExecuted(JobExecutionResult result, Throwable executionError) {
            try {
                if (!FlinkLineageSupport.isAttachedJobExecutionResult(result, executionError)) {
                    return;
                }
                Map<String, Object> accumulators = result.getAllAccumulatorResults();
                Long rowCount = number(accumulators.get(MetricNames.SINK_WRITE_COUNT));
                Long size = number(accumulators.get(MetricNames.SINK_WRITE_BYTES));
                LineageOutputStatistics statistics =
                        positive(rowCount) || positive(size)
                                ? new LineageOutputStatistics(rowCount, size, "attempted")
                                : null;
                String flinkJobId = result.getJobID() == null ? null : result.getJobID().toString();
                // Emitted even without statistics: an attached submission silences the JobManager
                // hook, so skipping here would leave the run with no terminal event at all rather
                // than one that is merely missing its row counts.
                emit(LineageEventType.COMPLETE, flinkJobId, statistics);
            } catch (Throwable error) {
                LOGGER.warn("Failed to emit Flink output statistics", error);
            }
        }

        /** Sends one terminal event for every run this listener owns. */
        private void emit(
                LineageEventType eventType, String flinkJobId, LineageOutputStatistics statistics) {
            LineageConfig runtimeConfig =
                    config.withAuthToken(
                            LineageConfig.resolveToken(
                                    FlinkClusterOptions.load(), System.getenv()));
            for (LineageEvent event : events) {
                LineageEvent terminal =
                        statistics == null ? event : event.withOutputStatistics(statistics);
                LineageRuntime.emit(
                        runtimeConfig,
                        terminal.withEventType(eventType)
                                .withRunProperty(
                                        LineageJobStatusHook.FLINK_JOB_ID_PROPERTY, flinkJobId));
            }
        }

        private static Long number(Object value) {
            return value instanceof Number ? ((Number) value).longValue() : null;
        }

        private static boolean positive(Long value) {
            return value != null && value > 0;
        }
    }
}
