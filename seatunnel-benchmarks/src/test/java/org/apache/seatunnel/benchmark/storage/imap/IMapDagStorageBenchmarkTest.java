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

import org.apache.seatunnel.benchmark.IMapDagStorageBenchmark;
import org.apache.seatunnel.benchmark.storage.SeaTunnelStorageEnvironmentContext;
import org.apache.seatunnel.engine.core.job.JobDAGInfo;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertNull;

/** Smoke coverage for code-built DAG storage through the production FileMapStore. */
class IMapDagStorageBenchmarkTest {

    @Test
    @Timeout(value = 2, unit = TimeUnit.MINUTES)
    void benchmarkMethodsStoreAndLoadCodeBuiltDag() throws Exception {
        SeaTunnelStorageEnvironmentContext environment = new SeaTunnelStorageEnvironmentContext();
        IMapDagStorageBenchmark benchmark = new IMapDagStorageBenchmark();
        IMapDagStorageBenchmarkWorkload workload = new IMapDagStorageBenchmarkWorkload();
        workload.pipelineCount = 10;
        workload.storedDagCount = 10;
        try {
            environment.setUp();
            workload.setUp(environment);
            workload.prepareInvocation();
            try {
                JobDAGInfo previousDag = benchmark.finishedJobDagStore(workload);
                benchmark.finishedJobDagLoad(workload);
                workload.verifyFinishedJobDagLoaded();
                assertNull(previousDag);
            } finally {
                workload.cleanInvocation();
            }
        } finally {
            environment.tearDown();
        }
    }
}
