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

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.common.metrics.JobMetrics;
import org.apache.seatunnel.api.common.metrics.Measurement;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.engine.common.config.JobConfig;
import org.apache.seatunnel.engine.common.job.JobStatus;
import org.apache.seatunnel.engine.core.dag.actions.SinkAction;
import org.apache.seatunnel.engine.core.dag.actions.SourceAction;
import org.apache.seatunnel.engine.core.dag.logical.LogicalDag;
import org.apache.seatunnel.engine.core.dag.logical.LogicalEdge;
import org.apache.seatunnel.engine.core.dag.logical.LogicalVertex;
import org.apache.seatunnel.engine.core.job.JobImmutableInformation;
import org.apache.seatunnel.engine.core.job.RestoreMode;
import org.apache.seatunnel.lineage.LineageConfig;
import org.apache.seatunnel.lineage.LineageDataset;
import org.apache.seatunnel.lineage.LineageEventType;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class ZetaLineageReporterTest {

    @Test
    void shouldUseDistinctAttemptsForRestartAndCheckpointRestores() throws Exception {
        JobMaster jobMaster = mock(JobMaster.class, CALLS_REAL_METHODS);
        setField(jobMaster, "initializationTimestamp", 42L);

        JobImmutableInformation savepointInfo = mock(JobImmutableInformation.class);
        doReturn(true).when(savepointInfo).isRestoreJob();
        doReturn(RestoreMode.SAVEPOINT).when(savepointInfo).getRestoreMode();
        setField(jobMaster, "jobImmutableInformation", savepointInfo);
        setField(jobMaster, "restart", false);
        String savepointAttempt = jobMaster.getLineageAttempt();

        JobImmutableInformation checkpointInfo = mock(JobImmutableInformation.class);
        doReturn(true).when(checkpointInfo).isRestoreJob();
        doReturn(RestoreMode.CHECKPOINT).when(checkpointInfo).getRestoreMode();
        setField(jobMaster, "jobImmutableInformation", checkpointInfo);
        String checkpointAttempt = jobMaster.getLineageAttempt();

        setField(jobMaster, "restart", true);
        String restartAttempt = jobMaster.getLineageAttempt();

        Assertions.assertNotNull(savepointAttempt);
        Assertions.assertNotNull(checkpointAttempt);
        Assertions.assertNotNull(restartAttempt);
        Assertions.assertNotEquals(savepointAttempt, checkpointAttempt);
        Assertions.assertNotEquals(savepointAttempt, restartAttempt);
        Assertions.assertNotEquals(checkpointAttempt, restartAttempt);
    }

    /**
     * The restart attempt must be a pure function of the initialization timestamp so that a run ID
     * can be recomputed offline from job facts. A random discriminator would satisfy uniqueness but
     * would make the run ID unrecoverable outside the emitted events.
     */
    @Test
    void shouldDeriveRecomputableRestartAttemptFromInitializationTimestamp() throws Exception {
        JobMaster jobMaster = mock(JobMaster.class, CALLS_REAL_METHODS);
        setField(jobMaster, "restart", true);

        setField(jobMaster, "initializationTimestamp", 42L);
        String firstAttempt = jobMaster.getLineageAttempt();
        String recomputedAttempt = jobMaster.getLineageAttempt();

        setField(jobMaster, "initializationTimestamp", 43L);
        String laterAttempt = jobMaster.getLineageAttempt();

        Assertions.assertEquals("restart-42", firstAttempt);
        Assertions.assertEquals(firstAttempt, recomputedAttempt);
        Assertions.assertEquals("restart-43", laterAttempt);
        Assertions.assertNotEquals(firstAttempt, laterAttempt);
    }

    @Test
    void shouldResolveDisabledConfigWhenEngineConfigIsAbsent() throws Exception {
        JobMaster jobMaster = mock(JobMaster.class, CALLS_REAL_METHODS);
        JobImmutableInformation jobInformation = mock(JobImmutableInformation.class);
        doReturn(new JobConfig()).when(jobInformation).getJobConfig();
        setField(jobMaster, "jobImmutableInformation", jobInformation);

        LineageConfig config = Assertions.assertDoesNotThrow(jobMaster::resolveLineageConfig);

        Assertions.assertFalse(config.enabled());
        // Reached from the checkpoint completion callback, where none of the three configuration
        // layers can have changed since the first resolution.
        Assertions.assertSame(
                config, jobMaster.resolveLineageConfig(), "the resolved config must be reused");
    }

    @Test
    void shouldUseCurrentMetricsForHeartbeatAndHistoryForTerminalEvent() throws Exception {
        JobMetrics history = metrics("history");
        JobMetrics current = metrics("current");

        Assertions.assertSame(
                current,
                invokeOutputMetrics(
                        mockCurrentMetricsJobMaster(current), LineageEventType.RUNNING));
        Assertions.assertSame(
                history,
                invokeOutputMetrics(
                        mockHistoryMetricsJobMaster(history), LineageEventType.COMPLETE));
    }

    @Test
    void shouldCollectCurrentMetricsWithoutPersistingOrCleaning() {
        JobMaster jobMaster = mock(JobMaster.class, CALLS_REAL_METHODS);
        doReturn(Collections.emptyList()).when(jobMaster).getCurrJobMetrics();

        JobMetrics current = jobMaster.getCurrentJobMetricsForLineage();

        Assertions.assertTrue(current.metrics().isEmpty());
        verify(jobMaster).getCurrJobMetrics();
        verify(jobMaster, never()).savePipelineMetricsToHistory(org.mockito.ArgumentMatchers.any());
    }

    @Test
    void shouldThrottleHeartbeatsAcrossPipelinesForOneJob() throws Exception {
        JobMaster jobMaster = mock(JobMaster.class, CALLS_REAL_METHODS);
        doReturn(new AtomicLong()).when(jobMaster).getLineageHeartbeatTime();

        Assertions.assertTrue(invokeShouldReportHeartbeat(jobMaster, Long.MAX_VALUE));
        Assertions.assertFalse(invokeShouldReportHeartbeat(jobMaster, Long.MAX_VALUE));
    }

    /**
     * A non-positive interval disables the heartbeat, matching what the option means on Flink. Zeta
     * reports from the checkpoint completion callback, so reading zero as "always overdue" would
     * turn the value that switches heartbeats off into one metrics RPC and one HTTP request per
     * completed checkpoint.
     */
    @Test
    void shouldNotReportHeartbeatsWhenTheIntervalDisablesThem() {
        JobMaster disabled = streamingJobMaster(0L);

        ZetaLineageReporter.reportHeartbeat(disabled);

        verify(disabled, never()).submitLineageEmission(any());

        // The same wiring with a usable interval must report, so the assertion above cannot pass
        // merely because the mock never reached the throttle.
        JobMaster reporting = streamingJobMaster(1L);

        ZetaLineageReporter.reportHeartbeat(reporting);

        verify(reporting).submitLineageEmission(any());
    }

    /**
     * The heartbeat leaves the checkpoint thread through a queue, while the terminal event is
     * submitted from the plan thread. A heartbeat that passed its own end-state check just before
     * the job finished would otherwise be drained after the terminal event and leave the run marked
     * running forever.
     */
    @Test
    void shouldDropAHeartbeatThatLostTheRaceWithTheTerminalEvent() {
        JobMaster jobMaster = streamingJobMaster(1L);
        ArgumentCaptor<Runnable> emission = ArgumentCaptor.forClass(Runnable.class);

        ZetaLineageReporter.reportHeartbeat(jobMaster);
        verify(jobMaster).submitLineageEmission(emission.capture());

        // The plan thread reached the end state after the heartbeat passed its own check.
        doReturn(JobStatus.FINISHED).when(jobMaster).getJobStatus();
        emission.getValue().run();
        verify(jobMaster, never()).getLogicalDag();

        // The same queued emission on a job that is still running does build its events, so the
        // assertion above cannot pass merely because the emission does nothing.
        doReturn(JobStatus.RUNNING).when(jobMaster).getJobStatus();
        emission.getValue().run();
        verify(jobMaster).getLogicalDag();
    }

    /**
     * A running streaming job master whose lineage is enabled with the given heartbeat interval.
     */
    private static JobMaster streamingJobMaster(long heartbeatIntervalMs) {
        JobMaster jobMaster = mock(JobMaster.class, CALLS_REAL_METHODS);
        Map<String, Object> options = new HashMap<>();
        options.put(LineageConfig.ENABLED, true);
        options.put(LineageConfig.HEARTBEAT_MIN_INTERVAL_MS, heartbeatIntervalMs);
        doReturn(LineageConfig.resolve(options, Collections.emptyMap(), Collections.emptyMap()))
                .when(jobMaster)
                .resolveLineageConfig();
        doReturn(JobStatus.RUNNING).when(jobMaster).getJobStatus();

        JobConfig jobConfig = new JobConfig();
        jobConfig.setJobContext(new JobContext().setJobMode(JobMode.STREAMING));
        JobImmutableInformation information = mock(JobImmutableInformation.class);
        doReturn(jobConfig).when(information).getJobConfig();
        doReturn(information).when(jobMaster).getJobImmutableInformation();

        doReturn(new AtomicLong()).when(jobMaster).getLineageHeartbeatTime();
        doNothing().when(jobMaster).submitLineageEmission(any());
        return jobMaster;
    }

    /**
     * Emission blocks on an HTTP request whose worst case is the configured timeout times one more
     * than the retry count. Its callers hold a completed checkpoint and the physical plan monitor,
     * so the send has to leave the calling thread; it must still reach the receiver in submission
     * order, because a COMPLETE that overtakes its own START leaves the run in the wrong state.
     */
    @Test
    void shouldRunLineageEmissionsOffTheCallerThreadInSubmissionOrder() throws Exception {
        JobMaster jobMaster = mock(JobMaster.class, CALLS_REAL_METHODS);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        setField(jobMaster, "executorService", executor);
        setField(jobMaster, "lineageEmissions", new ArrayDeque<Runnable>());
        setField(jobMaster, "lineageEmissionLock", new Object());
        try {
            List<String> emitted = Collections.synchronizedList(new ArrayList<>());
            CountDownLatch releaseFirst = new CountDownLatch(1);
            CountDownLatch lastEmitted = new CountDownLatch(1);

            jobMaster.submitLineageEmission(
                    () -> {
                        awaitQuietly(releaseFirst);
                        emitted.add("first");
                    });
            // A failing emission must not break the chain for the events queued behind it.
            jobMaster.submitLineageEmission(
                    () -> {
                        throw new IllegalStateException("receiver rejected the event");
                    });
            jobMaster.submitLineageEmission(
                    () -> {
                        emitted.add("third");
                        lastEmitted.countDown();
                    });

            Assertions.assertTrue(
                    emitted.isEmpty(), "the calling thread must not run the emission itself");
            releaseFirst.countDown();
            Assertions.assertTrue(
                    lastEmitted.await(30, TimeUnit.SECONDS), "queued emissions must still run");
            Assertions.assertEquals(Arrays.asList("first", "third"), emitted);
        } finally {
            executor.shutdownNow();
        }
    }

    private static void awaitQuietly(CountDownLatch latch) {
        try {
            latch.await(30, TimeUnit.SECONDS);
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Regression for a failure that only appears once the logical DAG has crossed a serialization
     * boundary: {@code AbstractAction.upstreams} is {@code transient}, so on the master it is null
     * and walking {@code Action.getUpstream()} throws. The traversal must go through the DAG edges,
     * which are serialized as vertex IDs, so this test clears the action upstreams to reproduce the
     * deserialized shape rather than the freshly built one.
     */
    @Test
    void shouldWalkSourcesThroughDagEdgesWhenActionUpstreamsAreLost() throws Exception {
        LineageDataset sourceDataset = LineageDataset.of("mysql://host:3306", "db.src");

        SourceAction<?, ?, ?> sourceAction =
                new SourceAction<>(
                        1L,
                        "source",
                        mock(SeaTunnelSource.class),
                        new HashSet<>(),
                        new HashSet<>());
        sourceAction.setLineageDatasets(Collections.singletonList(sourceDataset));
        SinkAction<?, ?, ?, ?> sinkAction =
                new SinkAction<>(
                        3L, "sink", mock(SeaTunnelSink.class), new HashSet<>(), new HashSet<>());

        LogicalVertex sourceVertex = new LogicalVertex(1L, sourceAction, 1);
        LogicalVertex transformVertex =
                new LogicalVertex(
                        2L, mock(org.apache.seatunnel.engine.core.dag.actions.Action.class), 1);
        LogicalVertex sinkVertex = new LogicalVertex(3L, sinkAction, 1);

        LogicalDag dag = new LogicalDag();
        dag.addLogicalVertex(sourceVertex);
        dag.addLogicalVertex(transformVertex);
        dag.addLogicalVertex(sinkVertex);
        dag.addEdge(new LogicalEdge(1L, 2L));
        dag.addEdge(new LogicalEdge(2L, 3L));

        // Reproduce the deserialized state: the transient upstream links are gone.
        clearUpstreams(sourceAction);
        clearUpstreams(sinkAction);

        List<LineageDataset> inputs = invokeSourceDatasets(dag, 3L);

        Assertions.assertEquals(
                Collections.singletonList(sourceDataset),
                inputs,
                "the source must be reached through the transform via DAG edges");
        Assertions.assertEquals(
                Collections.emptyList(),
                invokeSourceDatasets(dag, 99L),
                "an unknown vertex yields no inputs rather than failing");
        Assertions.assertEquals(
                Collections.emptyList(),
                invokeSourceDatasets(dag, null),
                "a missing vertex id yields no inputs rather than failing");
    }

    private static void clearUpstreams(Object action) throws Exception {
        Class<?> type = action.getClass();
        while (type != null) {
            try {
                Field field = type.getDeclaredField("upstreams");
                field.setAccessible(true);
                field.set(action, null);
                return;
            } catch (NoSuchFieldException ignored) {
                type = type.getSuperclass();
            }
        }
        throw new IllegalStateException("upstreams field not found");
    }

    @SuppressWarnings("unchecked")
    private static List<LineageDataset> invokeSourceDatasets(LogicalDag dag, Long sinkVertexId)
            throws Exception {
        Method method =
                ZetaLineageReporter.class.getDeclaredMethod(
                        "sourceDatasets", LogicalDag.class, Map.class, Long.class);
        method.setAccessible(true);
        Method index =
                ZetaLineageReporter.class.getDeclaredMethod("upstreamIndex", LogicalDag.class);
        index.setAccessible(true);
        return (List<LineageDataset>)
                method.invoke(null, dag, index.invoke(null, dag), sinkVertexId);
    }

    private static JobMetrics invokeOutputMetrics(JobMaster jobMaster, LineageEventType eventType)
            throws Exception {
        Method method =
                ZetaLineageReporter.class.getDeclaredMethod(
                        "outputMetrics", JobMaster.class, LineageEventType.class);
        method.setAccessible(true);
        return (JobMetrics) method.invoke(null, jobMaster, eventType);
    }

    private static JobMaster mockCurrentMetricsJobMaster(JobMetrics current) {
        JobMaster jobMaster = mock(JobMaster.class);
        doReturn(current).when(jobMaster).getCurrentJobMetricsForLineage();
        return jobMaster;
    }

    private static JobMaster mockHistoryMetricsJobMaster(JobMetrics history) {
        JobMaster jobMaster = mock(JobMaster.class);
        JobHistoryService historyService = mock(JobHistoryService.class);
        doReturn(historyService).when(jobMaster).getJobHistoryService();
        doReturn(1L).when(jobMaster).getJobId();
        doReturn(history).when(historyService).getJobMetrics(1L);
        return jobMaster;
    }

    private static boolean invokeShouldReportHeartbeat(JobMaster jobMaster, long minimumIntervalMs)
            throws Exception {
        Method method =
                ZetaLineageReporter.class.getDeclaredMethod(
                        "shouldReportHeartbeat", JobMaster.class, long.class);
        method.setAccessible(true);
        return (boolean) method.invoke(null, jobMaster, minimumIntervalMs);
    }

    private static JobMetrics metrics(String value) {
        Map<String, List<Measurement>> values = new HashMap<>();
        values.put(
                "marker",
                Collections.singletonList(
                        Measurement.of("marker", value, 1L, Collections.emptyMap())));
        return JobMetrics.of(values);
    }

    private static void setField(Object target, String fieldName, Object value) throws Exception {
        Field field = JobMaster.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        field.set(target, value);
    }
}
