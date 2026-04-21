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

package org.apache.seatunnel.engine.server.telemetry.log;

import org.apache.seatunnel.common.utils.ReflectionUtils;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class TaskLogManagerServiceTest {

    @TempDir Path tempDir;

    private TaskLogManagerService service;

    @BeforeEach
    void setUp() {
        service = new TaskLogManagerService(null);
        ReflectionUtils.setField(service, "path", tempDir.toString());
    }

    @Test
    void testCleanDoesNothingWhenJobIdIsZero() throws IOException {
        Path logFile = tempDir.resolve("job-1111022.log");
        Files.write(logFile, "test log content".getBytes(StandardCharsets.UTF_8));

        service.clean(0);

        assertTrue(Files.exists(logFile), "Log file should NOT be deleted when jobId is 0");
    }

    @Test
    void testCleanDeletesMatchingLogFiles() throws IOException {
        long jobId = 12345L;
        Path matchFile1 = tempDir.resolve("seatunnel-" + jobId + "-task-1.log");
        Path matchFile2 = tempDir.resolve("worker-" + jobId + "-task-2.log");
        Path otherFile = tempDir.resolve("seatunnel-99999-task.log");

        Files.write(matchFile1, "log1".getBytes(StandardCharsets.UTF_8));
        Files.write(matchFile2, "log2".getBytes(StandardCharsets.UTF_8));
        Files.write(otherFile, "other".getBytes(StandardCharsets.UTF_8));

        service.clean(jobId);

        assertFalse(Files.exists(matchFile1), "Log file containing jobId should be deleted");
        assertFalse(Files.exists(matchFile2), "Log file containing jobId should be deleted");
        assertTrue(Files.exists(otherFile), "Log file not matching jobId should be preserved");
    }
}
