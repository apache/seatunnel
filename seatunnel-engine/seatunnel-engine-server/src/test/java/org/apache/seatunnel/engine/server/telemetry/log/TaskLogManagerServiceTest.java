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

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Covers terminal job log cleanup with exact job id file matching.
 *
 * <p>The regression target is preventing terminal cleanup from deleting adjacent job files.
 */
public class TaskLogManagerServiceTest {

    @TempDir Path tempDir;

    private TaskLogManagerService service;

    @BeforeEach
    void setUp() {
        service = new TaskLogManagerService(null);
        ReflectionUtils.setField(service, "path", tempDir.toString());
    }

    /**
     * Terminal cleanup must delete only active, rolled, and sidecar files for the exact job id.
     *
     * <p>Files for other jobs or unsupported suffixes must survive cleanup.
     */
    @Test
    void testCleanDeletesOnlyExactJobLogFiles() throws IOException {
        Path activeFile = tempDir.resolve("job-123.log");
        Path rolledFile = tempDir.resolve("job-123.log.2026-07-13-1");
        Path sidecarFile = tempDir.resolve("job-123.log.unclassified");
        Path substringFile = tempDir.resolve("job-1234.log");
        Path unsupportedSuffixFile = tempDir.resolve("job-123.log.tmp");
        Path unrelatedFile = tempDir.resolve("seatunnel.log");

        Files.write(activeFile, "active".getBytes(StandardCharsets.UTF_8));
        Files.write(rolledFile, "rolled".getBytes(StandardCharsets.UTF_8));
        Files.write(sidecarFile, "sidecar".getBytes(StandardCharsets.UTF_8));
        Files.write(substringFile, "substring".getBytes(StandardCharsets.UTF_8));
        Files.write(unsupportedSuffixFile, "tmp".getBytes(StandardCharsets.UTF_8));
        Files.write(unrelatedFile, "unrelated".getBytes(StandardCharsets.UTF_8));

        service.clean(123L);

        assertFalse(Files.exists(activeFile), "Active job log should be deleted");
        assertFalse(Files.exists(rolledFile), "Rolled job log should be deleted");
        assertFalse(Files.exists(sidecarFile), "Unclassified sidecar should be deleted");
        assertTrue(Files.exists(substringFile), "Adjacent job id must not be touched");
        assertTrue(Files.exists(unsupportedSuffixFile), "Unsupported suffix must not be touched");
        assertTrue(Files.exists(unrelatedFile), "Unrelated log must not be touched");
    }

    @Test
    void testCleanSkipsMissingLogDirectory() throws Exception {
        TaskLogManagerService missingDirService = new TaskLogManagerService(null);
        ReflectionUtils.setField(missingDirService, "path", tempDir.resolve("missing").toString());

        assertDoesNotThrow(() -> missingDirService.clean(123L));
    }

    @Test
    void testCleanSkipsNullOrNonPositiveJobId() throws IOException {
        Path activeFile = tempDir.resolve("job-123.log");
        Files.write(activeFile, "active".getBytes(StandardCharsets.UTF_8));

        service.clean(0);
        service.clean(-1);

        assertTrue(Files.exists(activeFile), "Active job log should remain for invalid jobId");
    }
}
