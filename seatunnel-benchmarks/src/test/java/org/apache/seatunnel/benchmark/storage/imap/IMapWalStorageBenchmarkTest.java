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

import org.apache.seatunnel.benchmark.IMapWalStorageBenchmark;
import org.apache.seatunnel.benchmark.storage.SeaTunnelStorageEnvironmentContext;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;

/** Smoke coverage for standalone production WAL append and recovery scenarios. */
class IMapWalStorageBenchmarkTest {

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    void appendPathsPersistThroughFileMapStore() throws Exception {
        SeaTunnelStorageEnvironmentContext environment = new SeaTunnelStorageEnvironmentContext();
        IMapWalStorageBenchmark benchmark = new IMapWalStorageBenchmark();
        IMapWalAppendBenchmarkWorkload workload = new IMapWalAppendBenchmarkWorkload();
        workload.pipelineCount = 10;
        try {
            environment.setUp();
            workload.setUp(environment);
            benchmark.appendNewKey(workload);
            benchmark.appendHotKey(workload);
            benchmark.appendHotKey(workload);
        } finally {
            environment.tearDown();
        }
    }

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    void recoveryRetainsTheLatestMutationForEveryKey() throws Exception {
        SeaTunnelStorageEnvironmentContext environment = new SeaTunnelStorageEnvironmentContext();
        IMapWalStorageBenchmark benchmark = new IMapWalStorageBenchmark();
        IMapWalRecoveryBenchmarkWorkload workload = new IMapWalRecoveryBenchmarkWorkload();
        workload.uniqueKeyCount = 10;
        workload.mutationsPerKey = 3;
        try {
            environment.setUp();
            workload.setUp(environment);
            workload.prepareInvocation();
            try {
                benchmark.recoverAll(workload);
                workload.verifyRecovery();
            } finally {
                workload.cleanInvocation();
            }
        } finally {
            environment.tearDown();
        }
    }
}
