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
import org.apache.seatunnel.api.common.metrics.MetricNames;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.multitablesink.MultiTableSink;
import org.apache.seatunnel.api.source.SeaTunnelSource;
import org.apache.seatunnel.api.table.catalog.TablePath;
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
import org.apache.seatunnel.engine.server.dag.physical.PipelineLocation;
import org.apache.seatunnel.engine.server.execution.TaskGroupLocation;
import org.apache.seatunnel.engine.server.resourcemanager.resource.SlotProfile;
import org.apache.seatunnel.engine.server.utils.NodeEngineUtil;
import org.apache.seatunnel.lineage.LineageConfig;
import org.apache.seatunnel.lineage.LineageDataset;
import org.apache.seatunnel.lineage.LineageEvent;
import org.apache.seatunnel.lineage.LineageEventType;
import org.apache.seatunnel.lineage.LineageOutputStatistics;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import com.hazelcast.cluster.Address;
import com.hazelcast.map.IMap;
import com.hazelcast.spi.impl.NodeEngine;
import com.hazelcast.spi.impl.operationservice.impl.InvocationFuture;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

class ZetaLineageReporterTest {

    @Test
    void shouldOmitStatisticsWhenAWorkerMetricsRequestTimesOut() throws Exception {
        JobMaster master = mock(JobMaster.class, CALLS_REAL_METHODS);
        JobImmutableInformation information = mock(JobImmutableInformation.class);
        doReturn(42L).when(information).getJobId();
        setField(master, "jobImmutableInformation", information);
        doReturn(LineageConfig.defaults()).when(master).resolveLineageConfig();
        Address address = new Address("127.0.0.1", 5701);
        SlotProfile slot = mock(SlotProfile.class);
        doReturn(address).when(slot).getWorker();
        @SuppressWarnings("unchecked")
        IMap<PipelineLocation, Map<TaskGroupLocation, SlotProfile>> slots = mock(IMap.class);
        Mockito.doAnswer(
                        invocation -> {
                            BiConsumer<PipelineLocation, Map<TaskGroupLocation, SlotProfile>>
                                    consumer = invocation.getArgument(0);
                            consumer.accept(
                                    new PipelineLocation(42L, 1),
                                    Collections.singletonMap(
                                            new TaskGroupLocation(42L, 1, 1L), slot));
                            return null;
                        })
                .when(slots)
                .forEach(Mockito.any(BiConsumer.class));
        setField(master, "ownedSlotProfilesIMap", slots);
        NodeEngine node = mock(NodeEngine.class, Mockito.RETURNS_DEEP_STUBS);
        setField(master, "nodeEngine", node);
        @SuppressWarnings("unchecked")
        InvocationFuture<Object> future = mock(InvocationFuture.class);
        Mockito.when(future.get()).thenThrow(new AssertionError("unbounded metrics wait"));
        Mockito.when(future.get(Mockito.anyLong(), eq(TimeUnit.NANOSECONDS)))
                .thenThrow(new TimeoutException("fixture timeout"));
        try (MockedStatic<NodeEngineUtil> rpc = Mockito.mockStatic(NodeEngineUtil.class)) {
            rpc.when(() -> NodeEngineUtil.sendOperationToMemberNode(eq(node), any(), eq(address)))
                    .thenReturn(future);
            Assertions.assertNull(master.getCurrentJobMetricsForLineage());
            verify(future).get(Mockito.anyLong(), eq(TimeUnit.NANOSECONDS));
        }
    }

    @Test
    void shouldRejectJobTimeoutAndRetryBudgetsAboveClusterLimits() throws Exception {
        for (String key : Arrays.asList(LineageConfig.TIMEOUT_MS, LineageConfig.RETRY_TIMES)) {
            JobMaster master = mock(JobMaster.class, CALLS_REAL_METHODS);
            JobConfig config = new JobConfig();
            config.getEnvOptions().put(key, Integer.MAX_VALUE);
            JobImmutableInformation information = mock(JobImmutableInformation.class);
            doReturn(config).when(information).getJobConfig();
            setField(master, "jobImmutableInformation", information);
            IllegalArgumentException failure =
                    Assertions.assertThrows(
                            IllegalArgumentException.class, master::resolveLineageConfig);
            Assertions.assertTrue(failure.getMessage().contains(key));
        }
    }

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
    void shouldCollectCurrentMetricsWithoutPersistingOrCleaning() throws Exception {
        JobMaster jobMaster = mock(JobMaster.class, CALLS_REAL_METHODS);
        setField(jobMaster, "ownedSlotProfilesIMap", mock(IMap.class));
        doReturn(LineageConfig.defaults()).when(jobMaster).resolveLineageConfig();

        JobMetrics current = jobMaster.getCurrentJobMetricsForLineage();

        Assertions.assertTrue(current.metrics().isEmpty());
        verify(jobMaster, never()).getCurrJobMetrics();
        verify(jobMaster, never()).savePipelineMetricsToHistory(org.mockito.ArgumentMatchers.any());
    }

    @Test
    void shouldThrottleHeartbeatsAcrossPipelinesForOneJob() throws Exception {
        JobMaster jobMaster = mock(JobMaster.class, CALLS_REAL_METHODS);
        doReturn(new AtomicLong()).when(jobMaster).getLineageHeartbeatTime();

        Assertions.assertTrue(invokeShouldReportHeartbeat(jobMaster, Long.MAX_VALUE));
        Assertions.assertFalse(invokeShouldReportHeartbeat(jobMaster, Long.MAX_VALUE));
    }

    @Test
    void shouldNotSpendTheHeartbeatIntervalBeforeTheQueuedEmissionRuns() {
        JobMaster jobMaster = streamingJobMaster(Long.MAX_VALUE);
        ZetaLineageReporter.reportHeartbeat(jobMaster);
        Assertions.assertEquals(0L, jobMaster.getLineageHeartbeatTime().get());
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

        verify(disabled, never()).submitLineageEmission(any(), eq(LineageEventType.RUNNING));

        // The same wiring with a usable interval must report, so the assertion above cannot pass
        // merely because the mock never reached the throttle.
        JobMaster reporting = streamingJobMaster(1L);

        ZetaLineageReporter.reportHeartbeat(reporting);

        verify(reporting).submitLineageEmission(any(), eq(LineageEventType.RUNNING));
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
        verify(jobMaster).submitLineageEmission(emission.capture(), eq(LineageEventType.RUNNING));

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
        doNothing().when(jobMaster).submitLineageEmission(any(), eq(LineageEventType.RUNNING));
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
        setField(jobMaster, "lineageEmissionExecutor", executor);
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

    @Test
    void shouldCoalesceHeartbeatsAndKeepTerminalEmission() throws Exception {
        JobMaster jobMaster = mock(JobMaster.class, CALLS_REAL_METHODS);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        setField(jobMaster, "lineageEmissionExecutor", executor);
        setField(jobMaster, "lineageEmissions", new ArrayDeque<>());
        setField(jobMaster, "lineageEmissionLock", new Object());
        setField(jobMaster, "jobId", 42L);
        // Pretend a drain is already running so submit only enqueues and never calls execute.
        setField(jobMaster, "drainingLineageEmissions", true);
        try {
            jobMaster.submitLineageEmission(() -> {});
            for (int i = 0; i < 63; i++) {
                jobMaster.submitLineageEmission(() -> {}, LineageEventType.RUNNING);
            }
            Field field = JobMaster.class.getDeclaredField("lineageEmissions");
            field.setAccessible(true);
            @SuppressWarnings("unchecked")
            Deque<?> emissions = (Deque<?>) field.get(jobMaster);
            Assertions.assertEquals(2, emissions.size());

            AtomicBoolean terminalRan = new AtomicBoolean();
            jobMaster.submitLineageEmission(() -> terminalRan.set(true));
            Assertions.assertEquals(2, emissions.size());
            setField(jobMaster, "drainingLineageEmissions", false);
            CountDownLatch drained = new CountDownLatch(1);
            jobMaster.submitLineageEmission(drained::countDown);
            Assertions.assertTrue(drained.await(30, TimeUnit.SECONDS));
            Assertions.assertTrue(terminalRan.get());
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void shouldSendOnlyTheLatestPendingHeartbeat() throws Exception {
        JobMaster job = mock(JobMaster.class, CALLS_REAL_METHODS);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        setField(job, "lineageEmissionExecutor", executor);
        setField(job, "lineageEmissions", new ArrayDeque<>());
        setField(job, "lineageEmissionLock", new Object());
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch heartbeatSent = new CountDownLatch(1);
        List<String> emitted = Collections.synchronizedList(new ArrayList<>());
        try {
            job.submitLineageEmission(
                    () -> {
                        started.countDown();
                        awaitQuietly(release);
                    });
            Assertions.assertTrue(started.await(30, TimeUnit.SECONDS));
            job.submitLineageEmission(() -> emitted.add("old"), LineageEventType.RUNNING);
            job.submitLineageEmission(
                    () -> {
                        emitted.add("latest");
                        heartbeatSent.countDown();
                    },
                    LineageEventType.RUNNING);
            release.countDown();
            Assertions.assertTrue(heartbeatSent.await(30, TimeUnit.SECONDS));
            Assertions.assertEquals(Collections.singletonList("latest"), emitted);
        } finally {
            release.countDown();
            executor.shutdownNow();
        }
    }

    @Test
    void shouldFinishAStartedJobWhenTheSharedQueueBecomesFull() throws Exception {
        ExecutorService executor =
                new ThreadPoolExecutor(1, 1, 0, TimeUnit.MILLISECONDS, new ArrayBlockingQueue<>(1));
        JobMaster first = mock(JobMaster.class, CALLS_REAL_METHODS);
        JobMaster second = mock(JobMaster.class, CALLS_REAL_METHODS);
        for (JobMaster job : Arrays.asList(first, second)) {
            setField(job, "lineageEmissionExecutor", executor);
            setField(job, "lineageEmissions", new ArrayDeque<>());
            setField(job, "lineageEmissionLock", new Object());
        }
        CountDownLatch firstStarted = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        CountDownLatch complete = new CountDownLatch(1);
        CountDownLatch secondComplete = new CountDownLatch(1);
        List<String> emitted = Collections.synchronizedList(new ArrayList<>());
        try {
            first.submitLineageEmission(
                    () -> {
                        emitted.add("first START");
                        firstStarted.countDown();
                        awaitQuietly(release);
                    });
            Assertions.assertTrue(firstStarted.await(30, TimeUnit.SECONDS));
            first.submitLineageEmission(
                    () -> {
                        emitted.add("first COMPLETE");
                        complete.countDown();
                    });
            second.submitLineageEmission(
                    () -> {
                        emitted.add("second START");
                        secondComplete.countDown();
                    });
            release.countDown();
            Assertions.assertTrue(complete.await(30, TimeUnit.SECONDS));
            Assertions.assertTrue(secondComplete.await(30, TimeUnit.SECONDS));
            Assertions.assertEquals(
                    Arrays.asList("first START", "first COMPLETE", "second START"), emitted);
        } finally {
            release.countDown();
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

    /**
     * A job whose two sources each feed their own sink reports one run per output. A receiver that
     * keeps one current run per job name loses all but one of them unless the outputs are told
     * apart, so the per-output name is opt-in and the default must leave the job name untouched.
     */
    @Test
    void shouldNameTheJobPerOutputOnlyWhenConfigured() throws Exception {
        JobMaster jobMaster = twoOutputJobMaster("orders_sync");

        Assertions.assertEquals(
                Arrays.asList("orders_sync", "orders_sync"),
                jobNames(jobMaster, Collections.emptyMap()));

        Map<String, Object> options = new HashMap<>();
        options.put(LineageConfig.JOB_NAME_PER_OUTPUT, true);
        Assertions.assertEquals(
                Arrays.asList(
                        "orders_sync::doris://fe:9030/ods.orders",
                        "orders_sync::doris://fe:9030/ods.refunds"),
                jobNames(jobMaster, options));
    }

    private static List<String> jobNames(JobMaster jobMaster, Map<String, Object> options)
            throws Exception {
        options = new HashMap<>(options);
        options.put(LineageConfig.ENABLED, true);
        LineageConfig config =
                LineageConfig.resolve(options, Collections.emptyMap(), Collections.emptyMap());
        Method method =
                ZetaLineageReporter.class.getDeclaredMethod(
                        "events", JobMaster.class, LineageConfig.class, LineageEventType.class);
        method.setAccessible(true);
        List<String> names = new ArrayList<>();
        for (Object event :
                (List<?>) method.invoke(null, jobMaster, config, LineageEventType.START)) {
            names.add(((LineageEvent) event).jobName());
        }
        Collections.sort(names);
        return names;
    }

    /** Two independent source-to-sink chains, each writing one Doris table. */
    private static JobMaster twoOutputJobMaster(String jobName) {
        LogicalDag dag = new LogicalDag();
        String[][] chains = {{"ods_src.orders", "ods.orders"}, {"ods_src.refunds", "ods.refunds"}};
        long vertexId = 1L;
        for (String[] chain : chains) {
            SourceAction<?, ?, ?> source =
                    new SourceAction<>(
                            vertexId,
                            "source",
                            mock(SeaTunnelSource.class),
                            new HashSet<>(),
                            new HashSet<>());
            source.setLineageDatasets(
                    Collections.singletonList(LineageDataset.of("mysql://db:3306", chain[0])));
            SinkAction<?, ?, ?, ?> sink =
                    new SinkAction<>(
                            vertexId + 1,
                            "sink",
                            mock(SeaTunnelSink.class),
                            new HashSet<>(),
                            new HashSet<>());
            sink.setLineageDatasets(
                    Collections.singletonList(LineageDataset.of("doris://fe:9030", chain[1])));
            dag.addLogicalVertex(new LogicalVertex(vertexId, source, 1));
            dag.addLogicalVertex(new LogicalVertex(vertexId + 1, sink, 1));
            dag.addEdge(new LogicalEdge(vertexId, vertexId + 1));
            vertexId += 2;
        }

        JobConfig jobConfig = new JobConfig();
        jobConfig.setName(jobName);
        JobImmutableInformation information = mock(JobImmutableInformation.class);
        doReturn(jobConfig).when(information).getJobConfig();
        JobMaster jobMaster = mock(JobMaster.class);
        doReturn(dag).when(jobMaster).getLogicalDag();
        doReturn(information).when(jobMaster).getJobImmutableInformation();
        doReturn(7L).when(jobMaster).getJobId();
        doReturn("attempt").when(jobMaster).getLineageAttempt();
        return jobMaster;
    }

    @Test
    void shouldIsolateEqualCountsAndBytesForTwoOutputs() throws Exception {
        assertTwoOutputStatistics(1000, 1000, false);
    }

    @Test
    void shouldIsolateUnequalCountsAndBytesForBranchedOutputs() throws Exception {
        assertTwoOutputStatistics(700, 1300, true);
    }

    private static void assertTwoOutputStatistics(long first, long second, boolean branched)
            throws Exception {
        JobMaster master = twoOutputJobMaster("orders_sync");
        LogicalDag dag = master.getLogicalDag();
        if (branched) {
            dag.getEdges().clear();
            dag.addEdge(new LogicalEdge(1L, 2L));
            dag.addEdge(new LogicalEdge(1L, 4L));
        }
        configureMetricTables(dag, "ods.orders", "ods.refunds");
        Map<String, List<Measurement>> values = new HashMap<>();
        counter(values, MetricNames.SINK_COMMITTED_COUNT, first + second);
        counter(values, MetricNames.SINK_COMMITTED_BYTES, 11 * first + 29 * second);
        counter(
                values,
                MetricNames.SINK_COMMITTED_COUNT + "#ods.orders",
                first / 2,
                first - first / 2);
        counter(values, MetricNames.SINK_COMMITTED_COUNT + "#ods.refunds", second);
        counter(values, MetricNames.SINK_COMMITTED_BYTES + "#ods.orders", 11 * first);
        counter(values, MetricNames.SINK_COMMITTED_BYTES + "#ods.refunds", 29 * second);
        // Attempts must not override committed counters, or supply committed bytes.
        counter(values, MetricNames.SINK_WRITE_COUNT + "#ods.orders", first + 50);
        counter(values, MetricNames.SINK_WRITE_BYTES + "#ods.orders", 99999L);
        List<LineageEvent> events = completionEvents(master, JobMetrics.of(values));
        for (LineageEvent event : events) {
            LineageDataset output = event.outputs().get(0);
            boolean orders = output.name().equals("ods.orders");
            assertStatistics(
                    output.outputStatistics(),
                    orders ? first : second,
                    orders ? 11 * first : 29 * second,
                    "committed");
            Assertions.assertEquals(1, event.inputs().size());
            Assertions.assertEquals(
                    orders || branched ? "ods_src.orders" : "ods_src.refunds",
                    event.inputs().get(0).name());
        }
        Assertions.assertEquals(2, events.size());
        Assertions.assertNotEquals(events.get(0).runId(), events.get(1).runId());
    }

    @Test
    void shouldUseAttemptedCountersWhenCommittedIsZero() throws Exception {
        JobMaster master = twoOutputJobMaster("attempts");
        configureMetricTables(master.getLogicalDag(), "ods.orders", "ods.refunds");
        Map<String, List<Measurement>> values = new HashMap<>();
        counter(values, MetricNames.SINK_COMMITTED_COUNT + "#ods.orders", 0);
        counter(values, MetricNames.SINK_WRITE_COUNT + "#ods.orders", 700);
        counter(values, MetricNames.SINK_WRITE_BYTES + "#ods.orders", 7700);
        counter(values, MetricNames.SINK_WRITE_COUNT, 2000);
        for (LineageEvent event : completionEvents(master, JobMetrics.of(values))) {
            LineageDataset output = event.outputs().get(0);
            if (output.name().equals("ods.orders")) {
                assertStatistics(output.outputStatistics(), 700, 7700L, "attempted");
            } else {
                Assertions.assertNull(output.outputStatistics());
            }
        }
    }

    @Test
    void shouldNotUseJobTotalsForMissingTableCountersOrBytes() throws Exception {
        JobMaster master = twoOutputJobMaster("missing");
        configureMetricTables(master.getLogicalDag(), "ods.orders", "ods.refunds");
        Map<String, List<Measurement>> values = new HashMap<>();
        counter(values, MetricNames.SINK_COMMITTED_COUNT, 2000);
        counter(values, MetricNames.SINK_COMMITTED_BYTES, 40000);
        counter(values, MetricNames.SINK_COMMITTED_COUNT + "#ods.orders", 700);
        for (LineageEvent event : completionEvents(master, JobMetrics.of(values))) {
            LineageDataset output = event.outputs().get(0);
            if (output.name().equals("ods.orders")) {
                assertStatistics(output.outputStatistics(), 700, null, "committed");
            } else {
                Assertions.assertNull(output.outputStatistics());
            }
        }
        values.put(
                MetricNames.SINK_COMMITTED_COUNT + "#ods.orders",
                Collections.singletonList(
                        Measurement.of("invalid", "unknown", 1L, Collections.emptyMap())));
        for (LineageEvent event : completionEvents(master, JobMetrics.of(values))) {
            Assertions.assertNull(event.outputs().get(0).outputStatistics());
        }
    }

    @Test
    void shouldOnlyUseJobTotalsForASingleSink() throws Exception {
        JobMaster master = twoOutputJobMaster("single");
        Map<String, List<Measurement>> values = new HashMap<>();
        counter(values, MetricNames.SINK_COMMITTED_COUNT, 1000);
        counter(values, MetricNames.SINK_COMMITTED_BYTES, 13000);
        // Plain sinks have no per-table counters. Multiple such sinks cannot share the total.
        for (LineageEvent event : completionEvents(master, JobMetrics.of(values))) {
            Assertions.assertNull(event.outputs().get(0).outputStatistics());
        }
        master.getLogicalDag().getLogicalVertexMap().remove(4L);
        assertStatistics(
                completionEvents(master, JobMetrics.of(values))
                        .get(0)
                        .outputs()
                        .get(0)
                        .outputStatistics(),
                1000,
                13000L,
                "committed");
        Assertions.assertNull(
                completionEvents(master, JobMetrics.of(Collections.emptyMap()))
                        .get(0)
                        .outputs()
                        .get(0)
                        .outputStatistics());
    }

    @Test
    void shouldOmitStatisticsForTargetsSharedBySinks() throws Exception {
        JobMaster master = twoOutputJobMaster("shared");
        configureMetricTables(master.getLogicalDag(), "same.target", "same.target");
        Map<String, List<Measurement>> values = new HashMap<>();
        counter(values, MetricNames.SINK_COMMITTED_COUNT + "#same.target", 700, 1300);
        counter(values, MetricNames.SINK_COMMITTED_COUNT, 2000);
        for (LineageEvent event : completionEvents(master, JobMetrics.of(values))) {
            Assertions.assertNull(event.outputs().get(0).outputStatistics());
            Assertions.assertEquals(1, event.inputs().size());
        }
    }

    @Test
    void shouldSumAllUniqueTablesForOneTargetWithoutDuplicateSlices() throws Exception {
        JobMaster master = twoOutputJobMaster("tables");
        configureMetricTables(master.getLogicalDag(), "ods.orders", "ods.refunds");
        master.getLogicalDag().getLogicalVertexMap().remove(4L);
        MultiTableSink sink =
                (MultiTableSink)
                        ((SinkAction<?, ?, ?, ?>)
                                        master.getLogicalDag()
                                                .getLogicalVertexMap()
                                                .get(2L)
                                                .getAction())
                                .getSink();
        doReturn(
                        Arrays.asList(
                                TablePath.of("ods.orders"),
                                TablePath.of("ods.refunds"),
                                TablePath.of("ods.orders")))
                .when(sink)
                .getSinkTables();
        Map<String, List<Measurement>> values = new HashMap<>();
        counter(values, MetricNames.SINK_COMMITTED_COUNT + "#ods.orders", 700);
        counter(values, MetricNames.SINK_COMMITTED_COUNT + "#ods.refunds", 1300);
        counter(values, MetricNames.SINK_COMMITTED_BYTES + "#ods.orders", 7700);
        counter(values, MetricNames.SINK_COMMITTED_BYTES + "#ods.refunds", 37700);
        counter(values, MetricNames.SINK_COMMITTED_COUNT, 9999);
        assertStatistics(
                completionEvents(master, JobMetrics.of(values))
                        .get(0)
                        .outputs()
                        .get(0)
                        .outputStatistics(),
                2000,
                45400L,
                "committed");
    }

    @Test
    void shouldKeepMultiOutputSinkConservativeAndFailedOrAbortedRunsUncounted() throws Exception {
        JobMaster master = twoOutputJobMaster("multi");
        master.getLogicalDag().getLogicalVertexMap().remove(4L);
        SinkAction<?, ?, ?, ?> sink =
                (SinkAction<?, ?, ?, ?>)
                        master.getLogicalDag().getLogicalVertexMap().get(2L).getAction();
        sink.setLineageDatasets(
                Arrays.asList(
                        LineageDataset.of("doris://fe:9030", "ods.orders"),
                        LineageDataset.of("doris://fe:9030", "ods.refunds")));
        Map<String, List<Measurement>> values = new HashMap<>();
        counter(values, MetricNames.SINK_COMMITTED_COUNT, 2000);
        for (LineageEventType type :
                Arrays.asList(
                        LineageEventType.COMPLETE, LineageEventType.FAIL, LineageEventType.ABORT)) {
            List<LineageEvent> events =
                    type == LineageEventType.COMPLETE
                            ? completionEvents(master, JobMetrics.of(values))
                            : invokeEvents(master, type);
            Assertions.assertEquals(2, events.size());
            Assertions.assertNotEquals(events.get(0).runId(), events.get(1).runId());
            for (LineageEvent event : events) {
                Assertions.assertNull(event.outputs().get(0).outputStatistics());
                Assertions.assertEquals(1, event.inputs().size());
            }
        }
    }

    private static void configureMetricTables(LogicalDag dag, String first, String second) {
        String[] tables = {first, second};
        for (int i = 0; i < 2; i++) {
            SinkAction<?, ?, ?, ?> previous =
                    (SinkAction<?, ?, ?, ?>) dag.getLogicalVertexMap().get(2L + 2 * i).getAction();
            MultiTableSink sink = mock(MultiTableSink.class);
            doReturn(Collections.singletonList(TablePath.of(tables[i]))).when(sink).getSinkTables();
            doReturn(
                            Collections.singletonMap(
                                    TablePath.of("source." + i), mock(SeaTunnelSink.class)))
                    .when(sink)
                    .getSinks();
            SinkAction<?, ?, ?, ?> action =
                    new SinkAction<>(
                            previous.getId(), "sink-" + i, sink, new HashSet<>(), new HashSet<>());
            action.setLineageDatasets(previous.getLineageDatasets());
            dag.getLogicalVertexMap()
                    .put(action.getId(), new LogicalVertex(action.getId(), action, 1));
        }
    }

    private static void counter(
            Map<String, List<Measurement>> values, String metric, long... counts) {
        List<Measurement> measurements = new ArrayList<>();
        for (long count : counts) {
            measurements.add(Measurement.of(metric, count, 1L, Collections.emptyMap()));
        }
        values.put(metric, measurements);
    }

    @SuppressWarnings("unchecked")
    private static List<LineageEvent> completionEvents(JobMaster master, JobMetrics metrics)
            throws Exception {
        JobHistoryService history = mock(JobHistoryService.class);
        long jobId = master.getJobId();
        doReturn(metrics).when(history).getJobMetrics(jobId);
        doReturn(history).when(master).getJobHistoryService();
        return invokeEvents(master, LineageEventType.COMPLETE);
    }

    @SuppressWarnings("unchecked")
    private static List<LineageEvent> invokeEvents(JobMaster master, LineageEventType type)
            throws Exception {
        Method method =
                ZetaLineageReporter.class.getDeclaredMethod(
                        "events", JobMaster.class, LineageConfig.class, LineageEventType.class);
        method.setAccessible(true);
        LineageConfig config =
                LineageConfig.resolve(
                        Collections.singletonMap(LineageConfig.ENABLED, true),
                        Collections.emptyMap(),
                        Collections.emptyMap());
        return (List<LineageEvent>) method.invoke(null, master, config, type);
    }

    private static void assertStatistics(
            LineageOutputStatistics statistics, long rows, Long bytes, String semantics) {
        Assertions.assertNotNull(statistics);
        Assertions.assertEquals(rows, statistics.rowCount());
        Assertions.assertEquals(bytes, statistics.size());
        Assertions.assertEquals(semantics, statistics.semantics());
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
