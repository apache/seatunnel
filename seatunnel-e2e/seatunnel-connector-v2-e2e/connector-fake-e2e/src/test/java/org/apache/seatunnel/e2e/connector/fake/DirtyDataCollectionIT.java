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
        Container.ExecResult result =
                container.executeJob("/fake_to_console_dirty_collection_only.conf");

        String stdout = result.getStdout();
        String stderr = result.getStderr();
        String serverLogs = container.getServerLogs();
        String allLogs = stdout + "\n" + stderr + "\n" + serverLogs;

        Assertions.assertEquals(0, result.getExitCode(), "Job should complete: stderr=" + stderr);

        boolean hasDirtyCollected =
                allLogs.contains("Dirty record collected")
                        || allLogs.contains("user rule")
                        || allLogs.contains("[AlwaysDirtyValidator]");
        Assertions.assertTrue(
                hasDirtyCollected,
                "Expected dirty record collection logs. Log tail: "
                        + allLogs.substring(Math.max(0, allLogs.length() - 500)));

        log.info("Dirty data collection (logging only) test completed!");
    }

    @TestTemplate
    public void testDirtyDataCollectionFailure(TestContainer container)
            throws IOException, InterruptedException {

        Container.ExecResult failResult =
                container.executeJob("/fake_to_console_dirty_fail_threshold.conf");

        Assertions.assertNotEquals(
                0, failResult.getExitCode(), "Job should fail when dirty threshold is exceeded");

        String combinedOutput =
                failResult.getStdout()
                        + "\n"
                        + failResult.getStderr()
                        + "\n"
                        + container.getServerLogs();

        boolean hasDirtyCollected =
                combinedOutput.contains("Dirty record collected")
                        || combinedOutput.contains("user rule")
                        || combinedOutput.contains("[AlwaysDirtyValidator]");
        Assertions.assertTrue(
                hasDirtyCollected,
                "Expected dirty collection log in output. Log tail: "
                        + combinedOutput.substring(Math.max(0, combinedOutput.length() - 500)));

        boolean hasThresholdMessage =
                combinedOutput.contains("threshold exceeded")
                        || combinedOutput.contains("Dirty record threshold exceeded");
        Assertions.assertTrue(
                hasThresholdMessage,
                "Expected threshold exceeded message. Log tail: "
                        + combinedOutput.substring(Math.max(0, combinedOutput.length() - 500)));

        log.info("Dirty data threshold failure test completed!");
    }
}
