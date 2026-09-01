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

package org.apache.seatunnel.benchmark.storage.imap;

import org.apache.seatunnel.benchmark.IMapJobStorageBenchmark;
import org.apache.seatunnel.benchmark.storage.SeaTunnelStorageEnvironmentContext;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertEquals;

/** Smoke coverage for normal IMap lifecycle and code-built DAG storage scenarios. */
class IMapJobStorageBenchmarkTest {

    @Test
    @Timeout(value = 3, unit = TimeUnit.MINUTES)
    void jobGrowthWorkloadRetainsEntriesAcrossInvocations() throws Exception {
        SeaTunnelStorageEnvironmentContext environment = new SeaTunnelStorageEnvironmentContext();
        IMapJobStorageBenchmark benchmark = new IMapJobStorageBenchmark();
        IMapJobGrowthBenchmarkWorkload workload = new IMapJobGrowthBenchmarkWorkload();
        workload.initialStoredJobCount = 10;
        try {
            environment.setUp();
            workload.setUp(environment);

            int initialRunningJobs = workload.runningJobCount();
            workload.prepareInvocation();
            benchmark.runningJobGrowth(workload);
            workload.prepareInvocation();
            benchmark.runningJobGrowth(workload);
            assertEquals(initialRunningJobs + 2, workload.runningJobCount());

            int initialFinishedJobs = workload.finishedJobCount();
            int initialFinishedMetrics = workload.finishedJobMetricsCount();
            workload.prepareInvocation();
            benchmark.completedJobHistoryGrowth(workload);
            workload.prepareInvocation();
            benchmark.completedJobHistoryGrowth(workload);
            assertEquals(initialFinishedJobs + 2, workload.finishedJobCount());
            assertEquals(initialFinishedMetrics + 2, workload.finishedJobMetricsCount());
        } finally {
            try {
                workload.tearDown();
            } finally {
                environment.tearDown();
            }
        }
    }

    @Test
    @Timeout(value = 3, unit = TimeUnit.MINUTES)
    void hotJobStoragePathsExerciseProductionIMaps() throws Exception {
        SeaTunnelStorageEnvironmentContext environment = new SeaTunnelStorageEnvironmentContext();
        IMapJobStorageBenchmark benchmark = new IMapJobStorageBenchmark();
        IMapJobStorageBenchmarkWorkload workload = new IMapJobStorageBenchmarkWorkload();
        IMapMetricsReportBenchmarkWorkload metricsWorkload =
                new IMapMetricsReportBenchmarkWorkload();
        workload.storedTaskGroupCount = 10;
        metricsWorkload.taskCount = 10;
        try {
            environment.setUp();
            workload.setUp(environment);
            metricsWorkload.setUp(environment);
            workload.prepareInvocation();
            try {
                benchmark.taskGroupStateTransition(workload);
                benchmark.runningMetricsReport(metricsWorkload);
            } finally {
                workload.cleanInvocation();
            }
        } finally {
            environment.tearDown();
        }
    }

    @Test
    @Timeout(value = 3, unit = TimeUnit.MINUTES)
    void runningJobRecoveryReloadsAllPersistedJobInfo() throws Exception {
        SeaTunnelStorageEnvironmentContext environment = new SeaTunnelStorageEnvironmentContext();
        IMapJobStorageBenchmark benchmark = new IMapJobStorageBenchmark();
        IMapJobRecoveryBenchmarkWorkload workload = new IMapJobRecoveryBenchmarkWorkload();
        workload.runningJobCount = 10;
        try {
            environment.setUp();
            workload.setUp(environment);
            workload.prepareInvocation();
            try {
                assertEquals(workload.runningJobCount, benchmark.runningJobRecovery(workload));
                workload.verifyRecovery();
            } finally {
                workload.cleanInvocation();
            }
        } finally {
            try {
                workload.tearDown();
            } finally {
                environment.tearDown();
            }
        }
    }
}
