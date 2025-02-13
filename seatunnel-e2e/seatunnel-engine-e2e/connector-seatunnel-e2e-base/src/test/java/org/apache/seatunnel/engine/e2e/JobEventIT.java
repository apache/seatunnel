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

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class JobEventIT extends SeaTunnelEngineContainer {

    private static final String TEST_CONFIG_FILE = "/fakesource_to_console.conf";
    private static final String LIFECYCLE_EVENT = "LIFECYCLE_READER_OPEN";
    private static final String JOB_ID_PATTERN = "job id:\\s*(\\d+)";
    private static final String EVENT_API_ENDPOINT = "localhost:8080/event/";

    @Test
    public void testJobRestoreApplyResources() throws IOException, InterruptedException {
        // Execute job and verify output
        Container.ExecResult execResult = executeJob(server, TEST_CONFIG_FILE);
        String stdout = execResult.getStdout();
        Assertions.assertNotNull(stdout, "Job execution output should not be null");
        Assertions.assertTrue(
                stdout.contains(LIFECYCLE_EVENT), "Job output should contain lifecycle event");

        // Extract job ID from logs
        String jobId = extractJobId(server.getLogs());
        Assertions.assertNotNull(jobId, "Failed to extract job ID from logs");

        // Query job event API
        Container.ExecResult eventResult = queryJobEvent(jobId);
        String eventOutput = eventResult.getStdout();
        Assertions.assertNotNull(eventOutput, "Event API response should not be null");
        Assertions.assertTrue(
                eventOutput.contains(LIFECYCLE_EVENT),
                "Event API response should contain lifecycle event");
    }

    private String extractJobId(String logs) {
        Pattern pattern = Pattern.compile(JOB_ID_PATTERN);
        Matcher matcher = pattern.matcher(logs);
        return matcher.find() ? matcher.group(1) : null;
    }

    private Container.ExecResult queryJobEvent(String jobId)
            throws InterruptedException, IOException {
        return server.execInContainer(
                "sh", "-c", String.format("curl %s%s", EVENT_API_ENDPOINT, jobId));
    }
}
