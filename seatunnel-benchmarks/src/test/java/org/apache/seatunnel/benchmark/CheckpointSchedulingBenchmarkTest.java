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
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CheckpointSchedulingBenchmarkTest {

    private static final int PIPELINE_NUM = 4;
    private static final long CHECKPOINT_INTERVAL_MILLIS = 200L;
    private static final int SAMPLE_COUNT = 20;
    private static final long THREAD_STOP_TIMEOUT_NANOS = TimeUnit.SECONDS.toNanos(30);

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
    void shouldMeasureDueTriggersOfRealCoordinators() throws Exception {
        CheckpointSchedulingFixture fixture =
                new CheckpointSchedulingFixture(PIPELINE_NUM, CHECKPOINT_INTERVAL_MILLIS);
        fixture.setUp();
        try {
            assertTrue(
                    CheckpointSchedulingFixture.countSchedulerThreads() > 0,
                    "the coordinators should be running checkpoint scheduler threads");
            fixture.beginIteration();
            for (int i = 0; i < SAMPLE_COUNT; i++) {
                fixture.awaitNextDueTrigger();
                long due = System.nanoTime();
                long observed = fixture.awaitTrigger();
                assertTrue(
                        observed - due < TimeUnit.MILLISECONDS.toNanos(CHECKPOINT_INTERVAL_MILLIS),
                        "a due trigger should run well within one interval");
            }

            assertEquals(SAMPLE_COUNT, fixture.getSampled());
            fixture.endIteration();
        } finally {
            fixture.tearDown();
        }
    }

    @Test
    void shouldStopEverySchedulerThreadOnTearDown() throws Exception {
        CheckpointSchedulingFixture fixture =
                new CheckpointSchedulingFixture(PIPELINE_NUM, CHECKPOINT_INTERVAL_MILLIS);
        fixture.setUp();

        fixture.tearDown();

        long deadline = System.nanoTime() + THREAD_STOP_TIMEOUT_NANOS;
        while (CheckpointSchedulingFixture.countSchedulerThreads() > 0
                && System.nanoTime() < deadline) {
            TimeUnit.MILLISECONDS.sleep(CHECKPOINT_INTERVAL_MILLIS);
        }
        assertEquals(
                0L,
                CheckpointSchedulingFixture.countSchedulerThreads(),
                () ->
                        "still running: "
                                + Thread.getAllStackTraces().keySet().stream()
                                        .map(Thread::getName)
                                        .filter(
                                                name ->
                                                        name.startsWith(
                                                                CheckpointSchedulingFixture
                                                                        .SCHEDULER_THREAD_NAME_PREFIX))
                                        .collect(Collectors.toList()));
    }

    @Test
    void shouldRejectParametersOutsideTheSupportedRange() {
        assertThrows(
                IllegalArgumentException.class,
                () -> new CheckpointSchedulingFixture(0, 1_000L).setUp());
        assertThrows(
                IllegalArgumentException.class,
                () -> new CheckpointSchedulingFixture(1, 9L).setUp());
    }

    @Test
    void shouldSpaceMeasuredCoordinatorsAtLeastOneHundredMillisApart() {
        long interval = TimeUnit.MILLISECONDS.toNanos(CHECKPOINT_INTERVAL_MILLIS);

        assertEquals(1, CheckpointSchedulingFixture.probeStride(1, interval));
        assertEquals(2, CheckpointSchedulingFixture.probeStride(4, interval));
        assertEquals(5, CheckpointSchedulingFixture.probeStride(10, interval));
        assertEquals(5, CheckpointSchedulingFixture.probeStride(500, TimeUnit.SECONDS.toNanos(10)));
        assertEquals(
                3, CheckpointSchedulingFixture.probeStride(3, TimeUnit.MILLISECONDS.toNanos(10)));
    }

    @Test
    void shouldRejectTooManySkipsOnlyOnceEnoughTriggersWereDue() {
        // A one-second smoke iteration sees a handful of due triggers; one skip there is noise.
        assertFalse(CheckpointSchedulingFixture.isSkipShareTooHigh(7, 1));
        assertFalse(CheckpointSchedulingFixture.isSkipShareTooHigh(49, 49));

        assertFalse(CheckpointSchedulingFixture.isSkipShareTooHigh(100, 10));
        assertTrue(CheckpointSchedulingFixture.isSkipShareTooHigh(100, 11));
        assertTrue(CheckpointSchedulingFixture.isSkipShareTooHigh(50, 50));
    }

    @Test
    void shouldNameTheEngineFieldWhenItNoLongerExists() {
        IllegalStateException failure =
                assertThrows(
                        IllegalStateException.class,
                        () -> BenchmarkReflection.requireField(Object.class, "pendingCounter"));

        assertTrue(failure.getMessage().contains("java.lang.Object#pendingCounter"));
    }
}
