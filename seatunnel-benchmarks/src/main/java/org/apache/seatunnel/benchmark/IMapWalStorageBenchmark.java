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

import org.apache.seatunnel.benchmark.storage.imap.IMapWalAppendBenchmarkWorkload;
import org.apache.seatunnel.benchmark.storage.imap.IMapWalRecoveryBenchmarkWorkload;

import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.BenchmarkMode;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OperationsPerInvocation;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.concurrent.TimeUnit;

import static org.apache.seatunnel.benchmark.storage.imap.IMapWalAppendBenchmarkWorkload.Batch.APPENDS_PER_INVOCATION;

/** Measures production file-backed IMap WAL append and recovery paths. */
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
public class IMapWalStorageBenchmark extends BenchmarkBase {

    public static void main(String[] args) throws RunnerException {
        Options options =
                new OptionsBuilder()
                        .verbosity(VerboseMode.NORMAL)
                        .include(IMapWalStorageBenchmark.class.getCanonicalName())
                        .addProfiler(GCProfiler.class)
                        .build();
        new Runner(options).run();
    }

    /**
     * Measures WAL growth when finished-job DAG mutations use new keys.
     *
     * <p>The timed phase performs 100 TTL-backed puts to the production finished-DAG IMap. Each put
     * uses a new job ID and alternates between two code-built {@code JobDAGInfo} payloads
     * containing 1, 10, or 100 pipelines, exercising IMap serialization and the real FileMapStore
     * WAL writer. WAL byte accounting and durable reload validation happen after measurement.
     *
     * <p>{@link Mode#SingleShotTime} gives every candidate exactly 100 appends per iteration, and
     * {@link OperationsPerInvocation} reports time per append. The auxiliary {@code
     * walBytesPerAppend} counter separately reports persisted WAL growth per append.
     */
    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @OperationsPerInvocation(APPENDS_PER_INVOCATION)
    public void appendNewKey(IMapWalAppendBenchmarkWorkload workload) {
        workload.appendNewKeyBatch();
    }

    /**
     * Measures WAL growth when finished-job DAG mutations repeatedly update one hot key.
     *
     * <p>The timed phase performs 100 TTL-backed puts to the production finished-DAG IMap under the
     * same job ID, alternating between two code-built {@code JobDAGInfo} payloads containing 1, 10,
     * or 100 pipelines. This builds a controlled per-key WAL history through the real FileMapStore
     * writer. WAL byte accounting and validation of the last durably reloaded value are not timed.
     *
     * <p>{@link Mode#SingleShotTime} fixes the added history depth at 100 mutations per iteration.
     * {@link OperationsPerInvocation} normalizes the score to one hot-key append, while {@code
     * walBytesPerAppend} reports its persisted byte growth separately.
     */
    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @OperationsPerInvocation(APPENDS_PER_INVOCATION)
    public void appendHotKey(IMapWalAppendBenchmarkWorkload workload) {
        workload.appendHotKeyBatch();
    }

    /** Replays a controlled WAL history and materializes its latest retained values. */
    @Benchmark
    public void recoverAll(IMapWalRecoveryBenchmarkWorkload workload) {
        workload.recoverAll();
    }
}
