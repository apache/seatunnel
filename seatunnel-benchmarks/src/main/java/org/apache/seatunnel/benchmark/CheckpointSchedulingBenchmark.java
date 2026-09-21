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
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Param;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.Setup;
import org.openjdk.jmh.annotations.State;
import org.openjdk.jmh.annotations.TearDown;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.concurrent.TimeUnit;

/**
 * Measures how long a due checkpoint trigger waits before its scheduling thread runs it.
 *
 * <p>This is the part of checkpointing that the scheduling model decides. Checkpoint completion
 * time, which {@link CheckpointingTimeBenchmark} measures, is dominated by the barrier round-trip
 * and is close to blind to how the trigger was scheduled.
 *
 * <p>The axis that separates scheduling models is the pipeline count. Every {@code
 * CheckpointCoordinator} builds its own two-thread pool, so a member running P pipelines carries 2P
 * scheduler threads; a shared pool is a fixed width whatever P is. {@code triggerBodyMicros} is a
 * parameter rather than a fixed cost because how long a trigger occupies its thread is what decides
 * whether a fixed width is wide enough, and that cost is not the same for every deployment.
 *
 * <p>{@code checkpointIntervalMillis} defaults far below the production default of 300000 so that a
 * benchmark iteration of a few seconds sees a realistic number of triggers.
 */
@BenchmarkMode(Mode.SampleTime)
@OutputTimeUnit(TimeUnit.MICROSECONDS)
@Threads(1)
@Fork(
        value = 3,
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
    public long perPipelineSchedulerTriggerDelay(PerPipelineSchedulerState state)
            throws InterruptedException {
        return state.scheduleAndAwaitTrigger();
    }

    @State(Scope.Thread)
    public static class PerPipelineSchedulerState {

        @Param({"1", "10", "100", "500"})
        private int pipelineNum;

        @Param({"1000"})
        private long checkpointIntervalMillis;

        @Param({"0", "500"})
        private long triggerBodyMicros;

        private CheckpointSchedulingBenchmarkState delegate;

        @Setup(Level.Trial)
        public void setUp() {
            delegate =
                    new CheckpointSchedulingBenchmarkState(
                            pipelineNum, checkpointIntervalMillis, triggerBodyMicros);
            delegate.setUp();
            System.out.printf(
                    "# checkpoint scheduler threads for %d pipelines: %d%n",
                    pipelineNum, CheckpointSchedulingBenchmarkState.countSchedulerThreads());
        }

        long scheduleAndAwaitTrigger() throws InterruptedException {
            return delegate.scheduleAndAwaitTrigger();
        }

        @TearDown(Level.Trial)
        public void tearDown() throws InterruptedException {
            delegate.tearDown();
        }
    }
}
