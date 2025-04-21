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

package org.apache.seatunnel.engine.e2e.joblog;

import org.apache.seatunnel.engine.e2e.SeaTunnelEngineContainer;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.Container;

import java.io.IOException;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

public class TaskExecutionLocationLogIT extends SeaTunnelEngineContainer {

    @Test
    public void testTaskExecutionLocationLogging() throws IOException, InterruptedException {
        // Execute a simple job
        Container.ExecResult execResult = executeSeaTunnelJob("/batch_fakesource_to_console.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        // Get the logs from the container
        String logs = server.getLogs();

        // Wait for logs to be available
        Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            // Check for task execution location logs from AbstractTask.init()
                            Pattern initLogPattern =
                                    Pattern.compile(
                                            "Task \\[\\d+\\] executing on worker \\[.+?\\]");
                            Assertions.assertTrue(
                                    initLogPattern.matcher(logs).find(),
                                    "Log should contain task execution location from init()");

                            // Check for task execution location logs from TaskExecutionService
                            // (start)
                            Pattern startLogPattern =
                                    Pattern.compile(
                                            "Starting task \\[\\d+\\] execution on worker \\[.+?\\]");
                            Assertions.assertTrue(
                                    startLogPattern.matcher(logs).find(),
                                    "Log should contain task start execution location");

                            // Check for task execution location logs from TaskExecutionService
                            // (complete)
                            Pattern completeLogPattern =
                                    Pattern.compile(
                                            "Task \\[\\d+\\] completed on worker \\[.+?\\]");
                            Assertions.assertTrue(
                                    completeLogPattern.matcher(logs).find(),
                                    "Log should contain task completion location");
                        });
    }
}
