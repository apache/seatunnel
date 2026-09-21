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

package org.apache.seatunnel.benchmark;

import org.junit.jupiter.api.Test;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Threads;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CheckpointSchedulingBenchmarkTest {

    private static final int PIPELINE_NUM = 8;
    private static final long CHECKPOINT_INTERVAL_MILLIS = 20L;
    private static final long TRIGGER_BODY_MICROS = 100L;
    private static final int TRIGGER_COUNT = 200;
    private static final long BACKGROUND_LOAD_TIMEOUT_NANOS = TimeUnit.SECONDS.toNanos(30);

    @Test
    void shouldSampleSchedulingDelayOnASingleThread() {
        assertEquals(
                Mode.SampleTime,
                CheckpointSchedulingBenchmark.class.getAnnotation(BenchmarkMode.class).value()[0]);
        assertEquals(
                TimeUnit.MICROSECONDS,
                CheckpointSchedulingBenchmark.class.getAnnotation(OutputTimeUnit.class).value());
        assertEquals(1, CheckpointSchedulingBenchmark.class.getAnnotation(Threads.class).value());
    }

    @Test
    void shouldRunEveryScheduledTriggerUnderBackgroundLoad() throws Exception {
        CheckpointSchedulingBenchmarkState state =
                new CheckpointSchedulingBenchmarkState(
                        PIPELINE_NUM, CHECKPOINT_INTERVAL_MILLIS, TRIGGER_BODY_MICROS);
        state.setUp();
        long schedulerThreads;
        try {
            for (int i = 0; i < TRIGGER_COUNT; i++) {
                state.scheduleAndAwaitTrigger();
            }
            awaitEveryPipelineTriggered(state);
            schedulerThreads = CheckpointSchedulingBenchmarkState.countSchedulerThreads();
        } finally {
            state.tearDown();
        }

        assertEquals(TRIGGER_COUNT, state.getMeasuredTriggers());
        assertTrue(
                state.getBackgroundTriggers() >= PIPELINE_NUM,
                "every pipeline should have run its periodic trigger at least once, ran "
                        + state.getBackgroundTriggers());
        assertTrue(
                schedulerThreads >= PIPELINE_NUM,
                "the per-pipeline model should hold at least one thread per pipeline, held "
                        + schedulerThreads);
    }

    @Test
    void shouldStopEverySchedulerThreadOnTearDown() throws Exception {
        CheckpointSchedulingBenchmarkState state =
                new CheckpointSchedulingBenchmarkState(
                        PIPELINE_NUM, CHECKPOINT_INTERVAL_MILLIS, TRIGGER_BODY_MICROS);
        state.setUp();
        state.scheduleAndAwaitTrigger();

        state.tearDown();

        assertEquals(0L, CheckpointSchedulingBenchmarkState.countSchedulerThreads());
    }

    /**
     * Waits for the periodic load to cover every pipeline.
     *
     * <p>The measured triggers run far faster than one checkpoint interval, so the periodic
     * triggers of the later pipelines have not come due by the time the loop finishes.
     */
    private static void awaitEveryPipelineTriggered(CheckpointSchedulingBenchmarkState state)
            throws InterruptedException {
        long deadline = System.nanoTime() + BACKGROUND_LOAD_TIMEOUT_NANOS;
        while (state.getBackgroundTriggers() < PIPELINE_NUM && System.nanoTime() < deadline) {
            TimeUnit.MILLISECONDS.sleep(CHECKPOINT_INTERVAL_MILLIS);
        }
    }

    @Test
    void shouldRejectParametersOutsideTheSupportedRange() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new CheckpointSchedulingBenchmarkState(0, 1_000L, 0L).setUp());
        assertThrows(
                IllegalArgumentException.class,
                () -> new CheckpointSchedulingBenchmarkState(1, 9L, 0L).setUp());
        assertThrows(
                IllegalArgumentException.class,
                () -> new CheckpointSchedulingBenchmarkState(1, 1_000L, -1L).setUp());
    }
}
