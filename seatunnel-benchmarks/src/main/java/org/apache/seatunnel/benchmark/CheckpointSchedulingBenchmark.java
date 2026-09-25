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

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Level;
import org.openjdk.jmh.annotations.Measurement;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.annotations.Warmup;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.concurrent.TimeUnit;

/**
 * Measures how late a periodic checkpoint trigger runs after it is due, on a real member with
 * {@code pipelineNum} real checkpoint coordinators.
 *
 * <p>This is the part of checkpointing the scheduling model decides. Checkpoint completion time,
 * which {@link CheckpointingTimeBenchmark} measures, is dominated by the barrier round-trip and is
 * close to blind to how the trigger was scheduled.
 *
 * <p>Each invocation measures one trigger. The untimed setup picks the coordinator due soonest and
 * returns exactly when its trigger is due; the timed body spins until that trigger has created its
 * pending checkpoint. The score is therefore the scheduling delay plus the trigger's own decision
 * logic up to creating the checkpoint, and nothing else. {@code Level.Invocation} is normally
 * discouraged, but here the delay being measured is in microseconds while the setup overhead JMH
 * leaves outside the timed region is tens of nanoseconds. See {@link CheckpointSchedulingFixture}
 * for how due times are known and which triggers are skipped.
 *
 * <p>The checkpoint interval is {@code pipelineNum * triggerSpacingMillis}, so every point of the
 * sweep sees the same rate of due triggers and the same checkpoint load on storage, and only the
 * number of coordinators changes. It is floored at {@link #MIN_MEASURABLE_INTERVAL_MILLIS}, so the
 * smallest pipeline counts sample less often. Each job has one pipeline: many jobs is what "many
 * active pipelines" means on a member, and it keeps per-job checkpoint state from becoming the
 * bottleneck.
 */
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Threads(1)
@Warmup(iterations = 2, time = 10)
@Measurement(iterations = 5, time = 10)
@Fork(
        value = 2,
        jvmArgsAppend = {
            "-Xms4g",
            "-Xmx4g",
            "-XX:+UseG1GC",
            "-XX:+AlwaysPreTouch",
            "-XX:+DisableExplicitGC",
            "-XX:ActiveProcessorCount=4",
            "-Djava.net.preferIPv4Stack=true"
        })
public class CheckpointSchedulingBenchmark extends BenchmarkBase {

    /**
     * Floor for the checkpoint interval. A checkpoint takes a few milliseconds here, and an
     * interval close to that sends triggers down the pending re-arm path instead of measuring them,
     * which is what a 20 ms interval at one pipeline would do. This is a floor for measuring, above
     * the lowest interval SeaTunnel accepts, which the fixture enforces separately.
     */
    static final long MIN_MEASURABLE_INTERVAL_MILLIS = 200L;

    public static void main(String[] args) throws RunnerException {
        Options options =
                new OptionsBuilder()
                        .verbosity(VerboseMode.NORMAL)
                        .include(
                                ".*"
                                        + CheckpointSchedulingBenchmark.class.getCanonicalName()
                                        + ".*")
                        .build();

        new Runner(options).run();
    }

    @Benchmark
    public long periodicTriggerDelay(CoordinatorsState state) {
        return state.fixture.awaitTrigger();
    }

    @State(Scope.Thread)
    public static class CoordinatorsState {

        @Param({"1", "10", "100", "500"})
        private int pipelineNum;

        @Param({"20"})
        private long triggerSpacingMillis;

        private CheckpointSchedulingFixture fixture;

        @Setup(Level.Trial)
        public void setUp() throws Exception {
            fixture =
                    new CheckpointSchedulingFixture(
                            pipelineNum,
                            Math.max(
                                    MIN_MEASURABLE_INTERVAL_MILLIS,
                                    pipelineNum * triggerSpacingMillis));
            fixture.setUp();
            System.out.printf(
                    "# checkpoint scheduler threads for %d pipelines: %d; measuring %d of them%n",
                    pipelineNum, fixture.countSchedulerThreads(), fixture.probeCount());
        }

        @Setup(Level.Iteration)
        public void setUpIteration() {
            fixture.beginIteration();
        }

        @Setup(Level.Invocation)
        public void awaitDueTrigger() {
            fixture.awaitNextDueTrigger();
        }

        @TearDown(Level.Iteration)
        public void tearDownIteration() {
            System.out.println("# " + fixture.iterationReport());
            fixture.endIteration();
        }

        @TearDown(Level.Trial)
        public void tearDown() throws Exception {
            fixture.tearDown();
        }
    }
}
