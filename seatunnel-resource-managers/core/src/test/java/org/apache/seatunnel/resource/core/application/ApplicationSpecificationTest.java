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

package org.apache.seatunnel.resource.core.application;

import org.apache.seatunnel.engine.common.runtime.DeployType;
import org.apache.seatunnel.resource.core.config.ApplicationOptions;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ApplicationSpecificationTest {
    @TempDir Path temporary;

    @Test
    void roundTripsLocalizedSpecificationWithoutLosingJobOrPlatformOptions() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("application.name", "sync-job");
        options.put("application.worker-count", "3");
        options.put("application.worker.memory-mb", "2048");
        options.put("application.worker.cpu-cores", "2");
        options.put("application.worker.slots", "4");
        options.put("application.startup-timeout-millis", "90000");
        options.put("yarn.queue", "batch");
        String job = "env { job.mode=BATCH }\nsource { FakeSource { row.num=5 } }\n";
        ApplicationSpecification original =
                ApplicationSpecification.fromOptions(DeployType.YARN, job, options);
        Path file = temporary.resolve("application.properties");
        original.write(file);
        ApplicationSpecification copy = ApplicationSpecification.read(file);
        assertEquals(DeployType.YARN, copy.getDeployType());
        assertEquals("sync-job", copy.getName());
        assertEquals(job, copy.getJobConfig());
        assertEquals(3, copy.getWorkerCount());
        assertEquals(new WorkerSpecification(2048, 2, 4), copy.getWorkerSpecification());
        assertEquals(90000L, copy.getStartupTimeoutMillis());
        assertEquals(original.getOptions(), copy.getOptions());
        assertEquals(original.getJobId(), copy.getJobId());
        assertTrue(copy.getJobId() > 0);
        options.put("yarn.queue", "mutated");
        assertEquals("batch", original.getOptions().get("yarn.queue"));
        assertThrows(
                UnsupportedOperationException.class, () -> original.getOptions().put("x", "y"));
    }

    @Test
    void rejectsInvalidResourcesBeforeSubmittingToPlatform() {
        for (String key :
                new String[] {
                    "application.worker-count",
                    "application.worker.memory-mb",
                    "application.worker.cpu-cores",
                    "application.worker.slots",
                    "application.master.memory-mb",
                    "application.master.cpu-cores",
                    "application.startup-timeout-millis",
                    "application.master.port",
                    "application.job-id",
                    "application.restore-job-id"
                }) {
            assertThrows(
                    IllegalArgumentException.class,
                    () ->
                            ApplicationSpecification.fromOptions(
                                    DeployType.KUBERNETES,
                                    "source {}",
                                    Collections.singletonMap(key, "0")),
                    key);
        }
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        ApplicationSpecification.fromOptions(
                                DeployType.KUBERNETES,
                                "source {}",
                                Collections.singletonMap("application.master.port", "65536")));
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        ApplicationSpecification.fromOptions(
                                DeployType.STANDALONE, "source {}", Collections.emptyMap()));
    }

    @Test
    void validatesRecoveryIdentityWithoutReusingTheSourceJobId() {
        Map<String, String> options = new HashMap<>();
        options.put(ApplicationOptions.JOB_ID.key(), "101");
        options.put(ApplicationOptions.RESTORE_JOB_ID.key(), "100");
        ApplicationSpecification specification =
                ApplicationSpecification.fromOptions(DeployType.YARN, "source {}", options);
        assertEquals(101L, specification.getJobId());
        assertEquals(100L, specification.getOption(ApplicationOptions.RESTORE_JOB_ID).longValue());
        options.put(ApplicationOptions.RESTORE_JOB_ID.key(), "101");
        assertThrows(
                IllegalArgumentException.class,
                () -> ApplicationSpecification.fromOptions(DeployType.YARN, "source {}", options));
    }

    @Test
    void rejectsUnrecognizedLocalizedFormat() throws Exception {
        Path file = temporary.resolve("application.properties");
        Files.write(file, Collections.singletonList("format.version=2"));
        assertThrows(IOException.class, () -> ApplicationSpecification.read(file));
    }
}
