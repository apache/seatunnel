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

package org.apache.seatunnel.benchmark.storage.checkpoint;

import org.apache.seatunnel.benchmark.CheckpointStorageBenchmark;
import org.apache.seatunnel.benchmark.storage.SeaTunnelStorageEnvironmentContext;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;

/** Smoke coverage for real-fixture generation and isolated checkpoint storage operations. */
class CheckpointStorageBenchmarkTest {

    @Test
    @Timeout(value = 3, unit = TimeUnit.MINUTES)
    void benchmarkStorageBoundariesAreExecutable() throws Exception {
        SeaTunnelStorageEnvironmentContext environment = new SeaTunnelStorageEnvironmentContext();
        CheckpointStorageBenchmark benchmark = new CheckpointStorageBenchmark();
        CheckpointStorageBenchmarkWorkload workload = new CheckpointStorageBenchmarkWorkload();
        try {
            environment.setUp();
            workload.setUp(environment);

            runInvocation(workload, () -> benchmark.checkpointIdAtomicIncrement(workload));
            runInvocation(workload, () -> benchmark.checkpointOverviewIncrementalUpdate(workload));
            assertCheckpointResultStoredUnderConfiguredNamespace(environment, workload);
            runInvocation(workload, () -> benchmark.checkpointPersistenceTransaction(workload));
        } finally {
            try {
                workload.tearDown();
            } finally {
                environment.tearDown();
            }
        }
    }

    private static void assertCheckpointResultStoredUnderConfiguredNamespace(
            SeaTunnelStorageEnvironmentContext environment,
            CheckpointStorageBenchmarkWorkload workload)
            throws Exception {
        workload.prepareInvocation();
        try {
            workload.storeCheckpointResult();
            Path checkpointDirectory = environment.checkpointDirectory();
            assertTrue(Files.isDirectory(checkpointDirectory));
            try (Stream<Path> files = Files.walk(checkpointDirectory)) {
                assertTrue(
                        files.anyMatch(
                                path ->
                                        Files.isRegularFile(path)
                                                && path.getFileName().toString().endsWith(".ser")));
            }
        } finally {
            workload.cleanInvocation();
        }
    }

    private static void runInvocation(
            CheckpointStorageBenchmarkWorkload workload, CheckedOperation operation)
            throws Exception {
        workload.prepareInvocation();
        try {
            operation.run();
        } finally {
            workload.cleanInvocation();
        }
    }

    @FunctionalInterface
    private interface CheckedOperation {
        void run() throws Exception;
    }
}
