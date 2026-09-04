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

import org.apache.seatunnel.benchmark.storage.checkpoint.CheckpointIdIncrementBenchmarkWorkload;
import org.apache.seatunnel.benchmark.storage.checkpoint.CheckpointOverviewBenchmarkWorkload;
import org.apache.seatunnel.benchmark.storage.checkpoint.CheckpointPersistenceBenchmarkWorkload;

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

import static org.apache.seatunnel.benchmark.storage.checkpoint.CheckpointStorageBenchmarkFixture.CHECKPOINT_OPERATIONS_PER_INVOCATION;

/** Measures checkpoint persistence operations using coordinator-produced fixture state. */
@BenchmarkMode(Mode.SingleShotTime)
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
public class CheckpointStorageBenchmark extends BenchmarkBase {

    public static void main(String[] args) throws RunnerException {
        Options options =
                new OptionsBuilder()
                        .verbosity(VerboseMode.NORMAL)
                        .include(CheckpointStorageBenchmark.class.getCanonicalName())
                        .build();
        new Runner(options).run();
    }

    /**
     * Measures the storage work performed after a checkpoint has already completed.
     *
     * <p>Each measured invocation processes 100 independent, coordinator-produced checkpoints. For
     * every checkpoint it atomically allocates the next checkpoint ID, serializes and writes the
     * completed checkpoint through the HDFS storage plugin, and updates the checkpoint overview
     * through {@code CheckpointMonitorService}. Barrier delivery, task snapshots, and ACK waiting
     * are intentionally excluded; fixture preparation, durability validation, and cleanup run
     * outside measured time.
     *
     * <p>{@link Mode#SingleShotTime} executes exactly one fixed-size phase per measurement
     * iteration. {@link OperationsPerInvocation} then normalizes the phase duration to one logical
     * checkpoint persistence transaction, so every compared candidate writes the same number of
     * checkpoint and WAL records.
     */
    @Benchmark
    @OperationsPerInvocation(CHECKPOINT_OPERATIONS_PER_INVOCATION)
    public void checkpointPersistenceTransaction(CheckpointPersistenceBenchmarkWorkload workload)
            throws Exception {
        workload.persistCheckpointStorageTransaction();
    }

    /**
     * Measures checkpoint-ID allocation through the production checkpoint counter state store.
     *
     * <p>The timed phase calls {@code StateStoreCheckpointIDCounter.getAndIncrement()} for 100
     * independent job/pipeline counters that were initialized before measurement. Counter setup,
     * MapStore reload checks, result validation, and cleanup are not timed.
     *
     * <p>Each increment waits on the write-through file-backed MapStore WAL append, so durable sync
     * cost dominates both the mean latency and the within-run coefficient of variation.
     *
     * <p>{@link Mode#SingleShotTime} keeps the phase at exactly 100 allocations, while {@link
     * OperationsPerInvocation} reports the normalized cost of one atomic checkpoint-ID allocation.
     */
    @Benchmark
    @OperationsPerInvocation(CHECKPOINT_OPERATIONS_PER_INVOCATION)
    public long checkpointIdAtomicIncrement(CheckpointIdIncrementBenchmarkWorkload workload)
            throws Exception {
        return workload.incrementCheckpointId();
    }

    /**
     * Measures the completed-checkpoint update path of the production checkpoint monitor.
     *
     * <p>For each of 100 independent checkpoint fixtures, the timed phase calculates retained state
     * size and calls {@code CheckpointMonitorService.onCheckpointCompleted()}. This exercises the
     * checkpoint-overview IMap update that increments the completed count and records the latest
     * and historical checkpoint metadata. Fixture construction, durable MapStore reload checks, and
     * cleanup are outside measured time.
     *
     * <p>Like the counter benchmark, this path is write-through. Latency variance is dominated by
     * durable WAL sync plus overview serialization; residual CV after WAL-writer cleanup should be
     * interpreted as storage-path noise, not a cross-JDK regression.
     *
     * <p>{@link Mode#SingleShotTime} executes one fixed update phase per measurement iteration, and
     * {@link OperationsPerInvocation} normalizes its duration to one completed-checkpoint overview
     * update.
     */
    @Benchmark
    @OperationsPerInvocation(CHECKPOINT_OPERATIONS_PER_INVOCATION)
    public void checkpointOverviewIncrementalUpdate(CheckpointOverviewBenchmarkWorkload workload) {
        workload.updateCheckpointOverview();
    }
}
