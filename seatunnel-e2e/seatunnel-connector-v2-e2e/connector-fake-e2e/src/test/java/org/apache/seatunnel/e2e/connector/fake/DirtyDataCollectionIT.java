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

package org.apache.seatunnel.e2e.connector.fake;

import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Integration test for dirty data collection functionality. Tests the complete workflow using
 * FakeSource -> Console pipeline where dirty data is defined via {@code dirty.validator} in env
 * config.
 *
 * <p>The {@code AlwaysDirtyDataValidator} marks every record as dirty, causing the configured
 * collector to log them.
 */
@Slf4j
public class DirtyDataCollectionIT extends TestSuiteBase {

    @TestTemplate
    public void testDirtyDataCollectionLogging(TestContainer container)
            throws IOException, InterruptedException {
        String previousServerLogs = container.getServerLogs();
        Container.ExecResult result =
                container.executeJob("/fake_to_console_dirty_collection_only.conf");

        String stdout = result.getStdout();
        String stderr = result.getStderr();
        String allLogs =
                currentExecutionLogs(result, previousServerLogs, container.getServerLogs());

        Assertions.assertEquals(0, result.getExitCode(), "Job should complete: stderr=" + stderr);

        assertCollectedRows(allLogs, 50);

        log.info("Dirty data collection (logging only) test completed!");
    }

    @TestTemplate
    public void testDirtyDataCollectionFailure(TestContainer container)
            throws IOException, InterruptedException {

        String previousServerLogs = container.getServerLogs();
        Container.ExecResult failResult =
                container.executeJob("/fake_to_console_dirty_fail_threshold.conf");

        Assertions.assertNotEquals(
                0, failResult.getExitCode(), "Job should fail when dirty threshold is exceeded");

        String combinedOutput =
                currentExecutionLogs(failResult, previousServerLogs, container.getServerLogs());
        assertCollectedRows(combinedOutput, 10);
        Assertions.assertTrue(
                combinedOutput.contains("Dirty record threshold exceeded: 10 >= 10"),
                "Expected exact threshold failure from this execution: " + combinedOutput);

        log.info("Dirty data threshold failure test completed!");
    }
    /**
     * Removes server lines already present before submission while retaining current client output.
     *
     * <p>Containers are reused between test cases, so historical collection markers cannot prove
     * that this job invoked its validator or collector.
     */
    static String currentExecutionLogs(
            Container.ExecResult result, String previousServerLogs, String currentServerLogs) {
        Map<String, Integer> previousLines = new HashMap<>();
        Arrays.stream(previousServerLogs.split("\\R"))
                .forEach(line -> previousLines.merge(line, 1, Integer::sum));
        StringBuilder newServerLogs = new StringBuilder();
        Arrays.stream(currentServerLogs.split("\\R"))
                .forEach(
                        line -> {
                            int remaining = previousLines.getOrDefault(line, 0);
                            if (remaining > 0) {
                                previousLines.put(line, remaining - 1);
                            } else {
                                newServerLogs.append(line).append('\n');
                            }
                        });
        return result.getStdout() + "\n" + result.getStderr() + "\n" + newServerLogs;
    }

    /**
     * Verifies every expected per-record collection count rather than provider initialization.
     *
     * <p>Single-parallelism fixtures make each count observable and prevent an initialization-only
     * implementation from satisfying the integration test.
     */
    static void assertCollectedRows(String logs, int expectedRows) {
        for (int count = 1; count <= expectedRows; count++) {
            String expectedCount = "Count: " + count + ",";
            Assertions.assertTrue(
                    Arrays.stream(logs.split("\\R"))
                            .anyMatch(
                                    line ->
                                            line.contains("Dirty record collected (user rule)")
                                                    && line.contains(expectedCount)),
                    "Missing collected row " + count + " from this execution: " + logs);
        }
    }
}
