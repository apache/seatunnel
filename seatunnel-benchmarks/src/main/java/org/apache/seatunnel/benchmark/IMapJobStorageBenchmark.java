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
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.annotations.Threads;
import org.openjdk.jmh.profile.GCProfiler;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.RunnerException;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;

import java.util.concurrent.TimeUnit;

/** Measures steady-state and continuously growing job storage with values produced by Zeta. */
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

    /** Persists the timestamp and state writes used by a normal task-group state transition. */
    @Benchmark
    public Object taskGroupStateTransition(IMapJobStorageBenchmarkWorkload workload) {
        return workload.transitionTaskGroupState();
    }

    /**
     * Updates the partitioned running-metrics value used by periodic TaskExecutionService reports.
     */
    @Benchmark
    public void runningMetricsReport(IMapMetricsReportBenchmarkWorkload workload) throws Exception {
        workload.reportMetrics();
    }

    /** Adds unique running jobs without deleting prior entries, increasing IMap pressure. */
    @Benchmark
    public long runningJobGrowth(IMapJobGrowthBenchmarkWorkload workload) {
        return workload.appendRunningJob();
    }

    /** Runs completed-job persistence while retained history grows until its production TTL. */
    @Benchmark
    public long completedJobHistoryGrowth(IMapJobGrowthBenchmarkWorkload workload) {
        return workload.appendCompletedJobLifecycle();
    }

    /** Restores and scans all persisted running JobInfo values after evicting the in-memory map. */
    @Benchmark
    public int runningJobRecovery(IMapJobRecoveryBenchmarkWorkload workload) {
        return workload.recoverRunningJobs();
    }
}
