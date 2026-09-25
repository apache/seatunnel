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

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.common.metrics.MetricNames;
import org.apache.seatunnel.lineage.LineageConfig;
import org.apache.seatunnel.lineage.LineageDataset;
import org.apache.seatunnel.lineage.LineageEvent;
import org.apache.seatunnel.lineage.LineageEventType;
import org.apache.seatunnel.lineage.flink.LineageJobStatusHook;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.JobID;
import org.apache.flink.core.execution.DetachedJobExecutionResult;
import org.apache.flink.core.execution.JobListener;
import org.apache.flink.core.execution.JobStatusHook;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.util.OptionalFailure;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Covers the lineage registration path against a Flink runtime that actually has the status-hook
 * API.
 *
 * <p>{@code seatunnel-flink-starter-common} compiles against Flink 1.15, which has no {@code
 * JobStatusHook}: there {@code FlinkLineageHooks} reports lineage as unsupported, so the same
 * assertions could only ever be skipped. This module carries the {@code FlinkLineageHooks} that
 * does the real registration, and is the only place the hook and the result listener are reachable
 * at all.
 */
class FlinkLineageRegistrationTest {

    @BeforeEach
    void resetBackend() {
        RecordingLineageBackend.EVENTS.clear();
    }

    @Test
    void detectsTheStatusHookApiOnFlink20() {
        Assertions.assertTrue(FlinkLineageSupport.isSupported());
    }

    @Test
    void keepsFlinkCallbacksBestEffortWhenTheLineageBackendFails() throws Exception {
        StreamExecutionEnvironment environment = environmentWithOneSink();
        StreamGraph streamGraph = Assertions.assertDoesNotThrow(() -> register(environment));

        Assertions.assertNotNull(streamGraph);
        Assertions.assertEquals(1, environment.getJobListeners().size());
        JobListener listener = environment.getJobListeners().get(0);
        Map<String, OptionalFailure<Object>> accumulatorResults = new HashMap<>();
        accumulatorResults.put(
                MetricNames.SINK_WRITE_COUNT, OptionalFailure.of((Object) Long.valueOf(1L)));
        JobExecutionResult attached = new JobExecutionResult(new JobID(), 1L, accumulatorResults);
        Assertions.assertDoesNotThrow(() -> listener.onJobExecuted(attached, null));
        Assertions.assertDoesNotThrow(
                () -> listener.onJobExecuted(new DetachedJobExecutionResult(new JobID()), null));

        JobStatusHook hook = registeredHook(streamGraph);
        Assertions.assertDoesNotThrow(() -> hook.onFinished(new JobID()));
        Assertions.assertDoesNotThrow(
                () -> hook.onFailed(new JobID(), new IllegalStateException("failed")));
        Assertions.assertDoesNotThrow(() -> hook.onCanceled(new JobID()));
    }

    /**
     * A local environment is an attached submission, so the client reports the successful terminal
     * event with the output statistics it reads from the accumulators. The hook must be told to
     * stay out of that event, or the two would race and the statistics-free one could land last.
     */
    @Test
    void handsTheTerminalEventToTheClientForAnAttachedSubmission() throws Exception {
        StreamGraph streamGraph = register(environmentWithOneSink());

        JobStatusHook hook = registeredHook(streamGraph);
        Assertions.assertTrue(hook instanceof LineageJobStatusHook);
        Field field = LineageJobStatusHook.class.getDeclaredField("clientReportsCompletion");
        field.setAccessible(true);
        Assertions.assertTrue(
                field.getBoolean(hook),
                "an attached submission must leave the successful terminal event to the client");
    }

    /**
     * The no-argument {@code getStreamGraph()} clears the environment's transformations, which
     * would leave the caller with an unrunnable job if registration failed afterwards. Lineage must
     * be droppable, so the transformations have to survive registration.
     */
    @Test
    void leavesTheEnvironmentRunnableAfterRegistration() throws Exception {
        StreamExecutionEnvironment environment = environmentWithOneSink();

        Assertions.assertNotNull(register(environment));

        Assertions.assertFalse(
                environment.getStreamGraph(false).getStreamNodes().isEmpty(),
                "registration must not consume the transformations the job still needs");
    }

    private static StreamExecutionEnvironment environmentWithOneSink() {
        StreamExecutionEnvironment environment =
                StreamExecutionEnvironment.getExecutionEnvironment();
        environment.fromElements(1).print();
        return environment;
    }

    /**
     * A submission that never produces a running job leaves the start events sent at registration
     * describing runs that nothing would ever close: no JobManager hook runs for a job that was
     * never created, so the client is the only side that learns of the failure. The case this
     * exists for is a JobManager without the lineage artifact in its {@code lib/} directory, which
     * this integration deliberately fails the submission over.
     */
    @Test
    void closesTheRunsOfASubmissionThatFailed() {
        StreamExecutionEnvironment environment = environmentWithOneSink();

        Assertions.assertNotNull(register(environment, RecordingLineageBackend.NAME));
        Assertions.assertEquals(
                Collections.singletonList(LineageEventType.START),
                recordedEventTypes(),
                "the start event is sent before the job is submitted");

        environment.getJobListeners().get(0).onJobSubmitted(null, new IllegalStateException("no"));

        Assertions.assertEquals(
                Arrays.asList(LineageEventType.START, LineageEventType.FAIL),
                recordedEventTypes(),
                "a failed submission must close the runs its start events opened");
    }

    /** A submitted job is closed by its terminal callback, so nothing may be reported here. */
    @Test
    void leavesTheRunsOpenWhenTheSubmissionSucceeds() {
        StreamExecutionEnvironment environment = environmentWithOneSink();
        Assertions.assertNotNull(register(environment, RecordingLineageBackend.NAME));

        environment.getJobListeners().get(0).onJobSubmitted(null, null);

        Assertions.assertEquals(
                Collections.singletonList(LineageEventType.START),
                recordedEventTypes(),
                "a successful submission must not close its own runs");
    }

    private static List<LineageEventType> recordedEventTypes() {
        List<LineageEventType> types = new ArrayList<>();
        for (LineageEvent event : RecordingLineageBackend.EVENTS) {
            types.add(event.eventType());
        }
        return types;
    }

    private static StreamGraph register(StreamExecutionEnvironment environment) {
        return register(environment, LineageConfig.DEFAULT_TRANSPORT);
    }

    private static StreamGraph register(StreamExecutionEnvironment environment, String transport) {
        Map<String, Object> options = new LinkedHashMap<>();
        options.put(LineageConfig.ENABLED, true);
        options.put(LineageConfig.TRANSPORT, transport);
        options.put(LineageConfig.URL, "http://127.0.0.1:1");
        options.put(LineageConfig.TIMEOUT_MS, 100);
        options.put(LineageConfig.RETRY_TIMES, 0);
        LineageConfig config =
                FlinkLineageSupport.resolveConfig(
                        options,
                        Collections.<String, Object>emptyMap(),
                        Collections.<String, Object>emptyMap());
        return FlinkLineageSupport.register(
                environment,
                config,
                new JobContext(),
                Collections.<LineageDataset>emptyList(),
                Collections.singletonList(LineageDataset.of("mysql://db", "warehouse.orders")),
                "lineage-test");
    }

    private static JobStatusHook registeredHook(StreamGraph streamGraph) {
        List<JobStatusHook> hooks = streamGraph.getJobStatusHooks();
        Assertions.assertEquals(1, hooks.size());
        return hooks.get(0);
    }
}
