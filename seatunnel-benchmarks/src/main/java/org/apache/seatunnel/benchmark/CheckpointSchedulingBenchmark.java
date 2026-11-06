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

import org.apache.seatunnel.benchmark.checkpoint.CheckpointSchedulingFixture;

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
 * number of coordinators changes. It is floored at {@link #MIN_MEASURABLE_INTERVAL_MILLIS}, which
 * is why the sweep starts at ten pipelines: below that the floor binds, the member triggers fewer
 * checkpoints a second than at the other points, and the code on the trigger path warms up that
 * much slower. Each job has one pipeline: many jobs is what "many active pipelines" means on a
 * member, and it keeps per-job checkpoint state from becoming the bottleneck.
 *
 * <p>The trigger path runs only 50 times a second, and the JIT compiles by invocation count, so
 * with default thresholds that path is still being compiled minutes into a fork, reaching C2 only
 * after about two minutes. Every iteration in that window scores lower than the one before. Two
 * things keep that out of the measurement. The forks run with {@code
 * -XX:CompileThresholdScaling=0.05}, so the path compiles after a twentieth of the invocations;
 * lower values compile so much at once that triggers come due unmeasured and the skip limit fails
 * the run. And each trial first lets its coordinators trigger {@link #TRIGGER_PATH_WARMUP_TRIGGERS}
 * checkpoints, a warmup counted in triggers rather than seconds, and runs for at least {@link
 * #MIN_WARMUP_SECONDS}. At 50 triggers a second the time is what binds; the count keeps the warmup
 * long enough if a lower trigger rate is added.
 *
 * <p>Scores taken before this warmup are not comparable with scores taken after it, so a PR
 * comparison needs the candidate branch to contain it as well as the baseline.
 */
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Threads(1)
@Warmup(iterations = 2, time = 10)
@Measurement(iterations = 5, time = 10)
@Fork(
        value = 3,
        jvmArgsAppend = {
            "-Xms4g",
            "-Xmx4g",
            "-XX:+UseG1GC",
            "-XX:+AlwaysPreTouch",
            "-XX:+DisableExplicitGC",
            "-XX:ActiveProcessorCount=4",
            "-XX:CompileThresholdScaling=0.05",
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

    /**
     * Checkpoints each trial triggers before JMH's own warmup starts. With the fork's lowered
     * compile thresholds, the trigger path, the scheduler queue and the MDC wrapper around each
     * scheduled task stopped changing tier after about 1,000 triggers on GitHub's Linux runners.
     */
    static final long TRIGGER_PATH_WARMUP_TRIGGERS = 1_000L;

    /**
     * Least time each trial runs its coordinators before JMH's own warmup. At ten pipelines or more
     * the trigger count above is reached in 20 seconds, while the rest of the member is still
     * compiling, and the first measured iteration then caught millisecond outliers.
     */
    static final long MIN_WARMUP_SECONDS = 60L;

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

        @Param({"10", "100", "500"})
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
                                    pipelineNum * triggerSpacingMillis),
                            TRIGGER_PATH_WARMUP_TRIGGERS,
                            TimeUnit.SECONDS.toMillis(MIN_WARMUP_SECONDS));
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
