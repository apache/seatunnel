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
 * Integration tests for dirty data extension points:
 *
 * <ul>
 *   <li>{@link #testCustomCollectorViaSPI} — verifies that a custom {@code
 *       DirtyRecordCollectorProvider} registered via SPI is discovered and used at runtime (the
 *       "counting" type backed by {@code CountingDirtyRecordCollector}).
 *   <li>{@link #testCustomValidatorDefinition} — verifies that a custom {@code DirtyDataValidator}
 *       registered via SPI can define "what counts as dirty" before the write happens (the {@code
 *       AlwaysDirtyDataValidator} marks every record dirty via user-defined rule).
 * </ul>
 *
 * Both tests use the FakeSource → Console pipeline with configuration in {@code env}.
 */
@Slf4j
public class DirtyDataCustomExtensionIT extends TestSuiteBase {

    @TestTemplate
    public void testCustomCollectorViaSPI(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult result =
                container.executeJob("/fake_to_console_custom_collector.conf");

        String stdout = result.getStdout();
        String stderr = result.getStderr();
        String serverLogs = container.getServerLogs();
        String allLogs = stdout + "\n" + stderr + "\n" + serverLogs;

        log.info("Custom collector test exit code: {}", result.getExitCode());
        Assertions.assertEquals(
                0, result.getExitCode(), "Job should complete successfully: stderr=" + stderr);

        boolean hasCountingMarker = allLogs.contains("[CountingCollector]");
        Assertions.assertTrue(
                hasCountingMarker,
                "Expected [CountingCollector] marker in logs, proving the custom SPI collector was used. "
                        + "Log snippet (last 500 chars): "
                        + allLogs.substring(Math.max(0, allLogs.length() - 500)));

        log.info("Custom collector SPI test passed — [CountingCollector] confirmed in logs");
    }

    @TestTemplate
    public void testCustomValidatorDefinition(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult result =
                container.executeJob("/fake_to_console_custom_validator.conf");

        String stdout = result.getStdout();
        String stderr = result.getStderr();
        String serverLogs = container.getServerLogs();
        String allLogs = stdout + "\n" + stderr + "\n" + serverLogs;

        log.info("Custom validator test exit code: {}", result.getExitCode());
        Assertions.assertEquals(
                0, result.getExitCode(), "Job should complete successfully: stderr=" + stderr);

        boolean hasUserRule = allLogs.contains("user rule");
        Assertions.assertTrue(
                hasUserRule,
                "Expected 'user rule' in logs, proving records were caught by validator pre-check. "
                        + "Log snippet (last 500 chars): "
                        + allLogs.substring(Math.max(0, allLogs.length() - 500)));

        boolean hasValidatorMarker = allLogs.contains("[AlwaysDirtyValidator]");
        Assertions.assertTrue(
                hasValidatorMarker,
                "Expected [AlwaysDirtyValidator] marker in logs, proving the custom validator was used. "
                        + "Log snippet (last 500 chars): "
                        + allLogs.substring(Math.max(0, allLogs.length() - 500)));

        log.info(
                "Custom validator test passed — [AlwaysDirtyValidator] + 'user rule' confirmed in logs");
    }
}
