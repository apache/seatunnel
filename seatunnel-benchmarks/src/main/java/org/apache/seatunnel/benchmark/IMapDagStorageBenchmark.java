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

import org.apache.seatunnel.benchmark.storage.imap.IMapDagStorageBenchmarkWorkload;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.concurrent.TimeUnit;

import static org.apache.seatunnel.benchmark.storage.imap.IMapDagStorageBenchmarkWorkload.STORE_OPERATIONS_PER_INVOCATION;

/** Measures JobDAGInfo write and reload across exactly constructed pipeline counts. */
@BenchmarkMode(Mode.AverageTime)
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
public class IMapDagStorageBenchmark extends BenchmarkBase {

    public static void main(String[] args) throws RunnerException {
        Options options =
                new OptionsBuilder()
                        .verbosity(VerboseMode.NORMAL)
                        .include(IMapDagStorageBenchmark.class.getCanonicalName())
                        .build();
        new Runner(options).run();
    }

    /**
     * Measures retained JobDAGInfo growth through the production finished-DAG IMap.
     *
     * <p>The workload builds a real {@code JobDAGInfo} containing 1, 10, or 100 source-to-sink
     * pipelines before measurement and starts with 0 or 100 retained DAGs. The timed phase writes
     * that payload under 100 unique job IDs using the production history TTL, exercising IMap
     * serialization, FileMapStore, and WAL append. Fixture construction, durable sample reloads,
     * and deletion of the measured phase are not timed.
     *
     * <p>{@link Mode#SingleShotTime} fixes each growth phase at 100 DAGs. {@link
     * OperationsPerInvocation} normalizes the phase duration to one persisted DAG and prevents a
     * faster candidate from receiving more writes in the same measurement window.
     */
    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @OperationsPerInvocation(STORE_OPERATIONS_PER_INVOCATION)
    public long finishedJobDagStore(IMapDagStorageBenchmarkWorkload workload) {
        return workload.storeFinishedJobDagBatch();
    }

    /** Loads an evicted JobDAGInfo through FileMapStore and the production IMapStorage WAL. */
    @Benchmark
    public void finishedJobDagLoad(IMapDagStorageBenchmarkWorkload workload) {
        workload.loadFinishedJobDag();
    }
}
