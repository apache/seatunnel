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
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Threads;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class CheckpointSchedulingBenchmarkTest {

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
    void shouldLowerJitThresholdsSoTheTriggerPathCompilesWithinWarmup() {
        assertTrue(
                Arrays.asList(
                                CheckpointSchedulingBenchmark.class
                                        .getAnnotation(Fork.class)
                                        .jvmArgsAppend())
                        .contains("-XX:CompileThresholdScaling=0.05"));
    }

    @Test
    void shouldOnlySweepPipelineCountsTheIntervalFloorDoesNotSlowDown() throws Exception {
        String[] pipelineNums = paramValues("pipelineNum");
        long triggerSpacingMillis = Long.parseLong(paramValues("triggerSpacingMillis")[0]);

        for (String pipelineNum : pipelineNums) {
            assertTrue(
                    Integer.parseInt(pipelineNum) * triggerSpacingMillis
                            >= CheckpointSchedulingBenchmark.MIN_MEASURABLE_INTERVAL_MILLIS,
                    "pipelineNum=" + pipelineNum + " would trigger below the sweep's rate");
        }
    }

    private static String[] paramValues(String field) throws NoSuchFieldException {
        return CheckpointSchedulingBenchmark.CoordinatorsState.class
                .getDeclaredField(field)
                .getAnnotation(Param.class)
                .value();
    }
}
