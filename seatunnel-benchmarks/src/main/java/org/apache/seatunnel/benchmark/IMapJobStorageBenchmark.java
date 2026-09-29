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

import org.apache.seatunnel.benchmark.storage.imap.IMapJobGrowthBenchmarkWorkload;
import org.apache.seatunnel.benchmark.storage.imap.IMapJobRecoveryBenchmarkWorkload;
import org.apache.seatunnel.benchmark.storage.imap.IMapJobStorageBenchmarkWorkload;
import org.apache.seatunnel.benchmark.storage.imap.IMapMetricsReportBenchmarkWorkload;

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

import static org.apache.seatunnel.benchmark.storage.imap.IMapJobGrowthBenchmarkWorkload.GROWTH_OPERATIONS_PER_INVOCATION;
import static org.apache.seatunnel.benchmark.storage.imap.IMapJobStorageBenchmarkWorkload.TRANSITION_OPERATIONS_PER_INVOCATION;

/** Measures steady-state and controlled job-storage growth with values produced by Zeta. */
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
public class IMapJobStorageBenchmark extends BenchmarkBase {

    public static void main(String[] args) throws RunnerException {
        Options options =
                new OptionsBuilder()
                        .verbosity(VerboseMode.NORMAL)
                        .include(IMapJobStorageBenchmark.class.getCanonicalName())
                        .addProfiler(GCProfiler.class)
                        .build();
        new Runner(options).run();
    }

    /**
     * Measures the IMap storage operations in a normal task-group transition from CREATED to
     * RUNNING.
     *
     * <p>Before timing, the workload inserts 100 unique task groups in CREATED state and optionally
     * retains 0 or 1,000 additional task groups as storage pressure. For every measured transition
     * it reads and updates the state-timestamp array, checks and writes the running-state entry,
     * and reads the final state. This reproduces the persistent-map portion of {@code
     * PhysicalVertex.updateTaskState()} without timing fixture creation, durable reload validation,
     * or cleanup.
     *
     * <p>{@link Mode#SingleShotTime} ensures one fixed batch is executed per measurement iteration.
     * {@link OperationsPerInvocation} divides that batch duration by 100, so the reported score is
     * the cost of one task-group transition and every candidate creates equal IMap/WAL pressure.
     */
    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @OperationsPerInvocation(TRANSITION_OPERATIONS_PER_INVOCATION)
    public Object taskGroupStateTransition(IMapJobStorageBenchmarkWorkload workload) {
        return workload.transitionTaskGroupStateBatch();
    }

    /**
     * Updates the partitioned running-metrics value used by periodic TaskExecutionService reports.
     */
    @Benchmark
    public void runningMetricsReport(IMapMetricsReportBenchmarkWorkload workload) throws Exception {
        workload.reportMetrics();
    }

    /**
     * Measures growth of the three IMaps that retain an active job.
     *
     * <p>Starting from a controlled baseline of 0 or 1,000 retained jobs, the measured phase adds
     * 100 unique jobs. Each logical operation writes a real {@code JobInfo}, a RUNNING task-group
     * state, and its state-timestamp array, and deliberately keeps all three entries to model
     * long-running job growth. Fixture generation, durability validation, and removal of the phase
     * are outside measured time.
     *
     * <p>{@link Mode#SingleShotTime} fixes the growth phase at 100 jobs, and {@link
     * OperationsPerInvocation} normalizes the result to one retained running job.
     */
    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @OperationsPerInvocation(GROWTH_OPERATIONS_PER_INVOCATION)
    public long runningJobGrowth(IMapJobGrowthBenchmarkWorkload workload) {
        return workload.appendRunningJobBatch();
    }

    /**
     * Measures the IMap writes and deletes performed when jobs move into retained history.
     *
     * <p>Starting from 0 or 1,000 retained jobs, each of 100 measured lifecycles first writes the
     * running {@code JobInfo}, task-group state, and timestamps; stores finished metrics and
     * finished job state through {@code JobHistoryService}; and finally deletes the three transient
     * running entries. This leaves exactly one finished-state and one finished-metrics entry per
     * job. Preparation, durability checks, and phase cleanup are not timed.
     *
     * <p>{@link Mode#SingleShotTime} makes every candidate process the same 100 completed jobs.
     * {@link OperationsPerInvocation} reports the normalized cost of one complete storage
     * lifecycle.
     */
    @Benchmark
    @BenchmarkMode(Mode.SingleShotTime)
    @OperationsPerInvocation(GROWTH_OPERATIONS_PER_INVOCATION)
    public long completedJobHistoryGrowth(IMapJobGrowthBenchmarkWorkload workload) {
        return workload.appendCompletedJobLifecycleBatch();
    }

    /** Restores and scans all persisted running JobInfo values after evicting the in-memory map. */
    @Benchmark
    public int runningJobRecovery(IMapJobRecoveryBenchmarkWorkload workload) {
        return workload.recoverRunningJobs();
    }
}
