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

import org.apache.seatunnel.engine.common.config.server.QueueType;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Level;
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

/** Compares record handoff throughput of the two production intermediate queue implementations. */
@OutputTimeUnit(TimeUnit.SECONDS)
@Threads(1)
public class IntermediateQueueBenchmark extends BenchmarkBase {

    public static void main(String[] args) throws RunnerException {
        Options options =
                new OptionsBuilder()
                        .verbosity(VerboseMode.NORMAL)
                        .include(".*" + IntermediateQueueBenchmark.class.getCanonicalName() + ".*")
                        .build();
        new Runner(options).run();
    }

    @Benchmark
    public long blockingQueueRecordHandoff(BlockingQueueState state) {
        return state.publish();
    }

    @Benchmark
    public long disruptorRecordHandoff(DisruptorQueueState state) {
        return state.publish();
    }

    @State(Scope.Thread)
    public static class BlockingQueueState {

        @Param({"1024"})
        private int capacity;

        @Param({"4096"})
        private int recordPoolSize;

        private IntermediateQueueBenchmarkState delegate;

        @Setup(Level.Trial)
        public void setUp() throws Exception {
            delegate =
                    new IntermediateQueueBenchmarkState(
                            QueueType.BLOCKINGQUEUE, capacity, recordPoolSize);
            delegate.setUp();
        }

        long publish() {
            return delegate.publish();
        }

        @TearDown(Level.Trial)
        public void tearDown() throws Exception {
            delegate.tearDown();
        }
    }

    @State(Scope.Thread)
    public static class DisruptorQueueState {

        @Param({"1024"})
        private int capacity;

        @Param({"4096"})
        private int recordPoolSize;

        private IntermediateQueueBenchmarkState delegate;

        @Setup(Level.Trial)
        public void setUp() throws Exception {
            delegate =
                    new IntermediateQueueBenchmarkState(
                            QueueType.DISRUPTOR, capacity, recordPoolSize);
            delegate.setUp();
        }

        long publish() {
            return delegate.publish();
        }

        @TearDown(Level.Trial)
        public void tearDown() throws Exception {
            delegate.tearDown();
        }
    }
}
