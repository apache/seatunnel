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

import org.apache.seatunnel.benchmark.sink.MultiTableSinkFixture;
import org.apache.seatunnel.benchmark.sink.MultiTableTimerFlushFixture;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
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
import org.openjdk.jmh.runner.options.OptionsBuilder;

import java.util.concurrent.TimeUnit;

/** Measures MultiTableSink record handoff and timer flush using in-memory writers. */
@OutputTimeUnit(TimeUnit.SECONDS)
@Threads(1)
public class MultiTableSinkBenchmark extends BenchmarkBase {

    public static void main(String[] args) throws RunnerException {
        new Runner(
                        new OptionsBuilder()
                                .include(MultiTableSinkBenchmark.class.getCanonicalName() + ".*")
                                .build())
                .run();
    }

    @Benchmark
    public long recordHandoff(SinkState state) throws Exception {
        return state.fixture.publish();
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void timerFlush(TimerFlushSinkState state) throws Exception {
        state.fixture.flush();
    }

    @State(Scope.Thread)
    public static class SinkState {

        @Param({"1", "5", "15"})
        private int replicaNum;

        @Param({"1"})
        private int tableCount;

        @Param({"65536"})
        private int recordPoolSize;

        @Param({"HASH"})
        private MultiTableSinkFixture.Routing routing;

        @Param({"1000"})
        private int consumerBatchSize;

        @Param({"0"})
        private long batchDelayMicros;

        private MultiTableSinkFixture fixture;

        @Setup(Level.Trial)
        public void setUp() throws Exception {
            fixture =
                    new MultiTableSinkFixture(
                            replicaNum,
                            tableCount,
                            recordPoolSize,
                            routing,
                            consumerBatchSize,
                            batchDelayMicros);
            fixture.setUp();
        }

        @TearDown(Level.Iteration)
        public void verifyIteration() throws Exception {
            fixture.drainAndVerify();
        }

        @TearDown(Level.Trial)
        public void tearDown() throws Exception {
            if (fixture != null) {
                fixture.close();
            }
        }
    }

    @State(Scope.Thread)
    public static class TimerFlushSinkState {

        @Param({"1", "5", "10"})
        private int flushQueueCount;

        @Param({"100", "500", "1000"})
        private long flushIntervalMillis;

        @Param({"5"})
        private long writerFlushMillis;

        private MultiTableTimerFlushFixture fixture;
        private long intervalNanos;
        private long nextFlushNanos;

        @Setup(Level.Trial)
        public void setUp() throws Exception {
            if (flushIntervalMillis <= 0) {
                throw new IllegalArgumentException("flushIntervalMillis must be positive");
            }
            fixture = new MultiTableTimerFlushFixture(flushQueueCount, writerFlushMillis);
            intervalNanos = TimeUnit.MILLISECONDS.toNanos(flushIntervalMillis);
            nextFlushNanos = System.nanoTime();
        }

        @Setup(Level.Invocation)
        public void awaitFlushSignal() throws InterruptedException {
            TimeUnit.NANOSECONDS.sleep(nextFlushNanos - System.nanoTime());
            nextFlushNanos = System.nanoTime() + intervalNanos;
        }

        @TearDown(Level.Invocation)
        public void verifyRound() {
            fixture.verifyRound();
        }

        @TearDown(Level.Trial)
        public void tearDown() throws Exception {
            if (fixture != null) {
                fixture.close();
            }
        }
    }
}
