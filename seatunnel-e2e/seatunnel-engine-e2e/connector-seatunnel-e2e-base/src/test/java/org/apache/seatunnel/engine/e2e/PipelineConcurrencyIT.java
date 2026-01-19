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

package org.apache.seatunnel.engine.e2e;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;

/**
 * E2E test for pipeline concurrency control feature. Tests serial, limited, and unlimited
 * concurrency modes.
 */
@Slf4j
public class PipelineConcurrencyIT extends SeaTunnelEngineContainer {

    @Test
    public void testSerialPipelineExecution() throws IOException, InterruptedException {
        Container.ExecResult execResult = executeSeaTunnelJob("/pipeline_concurrency_serial.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        String logs = execResult.getStdout();

        // Verify serial mode logging
        Assertions.assertTrue(
                logs.contains("Parsed pipeline_concurrency from config: 1"),
                "Should parse pipeline_concurrency = 1");
        Assertions.assertTrue(
                logs.contains("Pipeline concurrency: 1"), "Should show pipeline concurrency as 1");
        Assertions.assertTrue(logs.contains("[Serial Mode]"), "Should execute in serial mode");

        log.info("✅ Serial pipeline execution test passed");
    }

    @Test
    public void testLimitedPipelineConcurrency() throws IOException, InterruptedException {
        Container.ExecResult execResult = executeSeaTunnelJob("/pipeline_concurrency_limited.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        String logs = execResult.getStdout();

        // Verify limited concurrency logging
        Assertions.assertTrue(
                logs.contains("Parsed pipeline_concurrency from config: 2"),
                "Should parse pipeline_concurrency = 2");
        Assertions.assertTrue(
                logs.contains("Pipeline concurrency: 2"), "Should show pipeline concurrency as 2");
        Assertions.assertTrue(
                logs.contains("[Limited Concurrency=2]"),
                "Should execute in limited concurrency mode");

        log.info("✅ Limited pipeline concurrency test passed");
    }

    @Test
    public void testUnlimitedPipelineConcurrency() throws IOException, InterruptedException {
        Container.ExecResult execResult =
                executeSeaTunnelJob("/pipeline_concurrency_unlimited.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        String logs = execResult.getStdout();

        // Verify unlimited concurrency (default behavior)
        Assertions.assertTrue(
                logs.contains("pipeline_concurrency not found in env config")
                        || logs.contains("Pipeline concurrency: unlimited"),
                "Should use default unlimited concurrency");
        Assertions.assertTrue(
                logs.contains("[Concurrent Mode]"), "Should execute in concurrent mode");

        log.info("✅ Unlimited pipeline concurrency test passed");
    }

    @Test
    public void testPipelineCompletionLogging() throws IOException, InterruptedException {
        Container.ExecResult execResult = executeSeaTunnelJob("/pipeline_concurrency_serial.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        String logs = execResult.getStdout();

        // Verify pipeline completion logging
        Assertions.assertTrue(
                logs.contains("Pipeline completed:"), "Should log pipeline completion");
        Assertions.assertTrue(logs.contains("Progress:"), "Should show progress information");
        Assertions.assertTrue(logs.contains("Running:"), "Should show running pipeline count");
        Assertions.assertTrue(logs.contains("Queued:"), "Should show queued pipeline count");

        log.info("✅ Pipeline completion logging test passed");
    }
}
