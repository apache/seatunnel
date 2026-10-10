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

package org.apache.seatunnel.engine.common.spec;

import org.apache.seatunnel.shade.com.typesafe.config.ConfigException;
import org.apache.seatunnel.shade.com.typesafe.config.ConfigFactory;

import org.apache.seatunnel.engine.common.config.SeatunnelApplicationConfig;
import org.apache.seatunnel.engine.common.config.server.ApplicationOptions;
import org.apache.seatunnel.engine.common.config.spec.ApplicationSpecification;
import org.apache.seatunnel.engine.common.config.spec.WorkerSpecification;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ApplicationSpecificationTest {
    @TempDir Path temporary;

    @Test
    void loadsNestedApplicationFileAndResolvesOverridesWithoutCli() throws Exception {
        Path application = temporary.resolve("application.config");
        Files.write(
                application,
                ("application { name = demo, worker-count = 2 }\n"
                                + "application.worker.slots = ${application.worker-count}\n"
                                + "yarn { queue = ${queue} }\n")
                        .getBytes(StandardCharsets.UTF_8));
        Map<String, String> overrides = new HashMap<>();
        overrides.put("application.worker-count", "3");
        overrides.put("queue", "analytics");
        Map<String, String> loaded = SeatunnelApplicationConfig.load(application, overrides);
        assertEquals("demo", loaded.get("application.name"));
        assertEquals("3", loaded.get("application.worker-count"));
        assertEquals("3", loaded.get("application.worker.slots"));
        assertEquals("analytics", loaded.get("yarn.queue"));
        assertEquals(2, overrides.size());
        assertFalse(overrides.containsKey("application.name"));
        assertFalse(loaded.containsKey(ApplicationOptions.MASTER_PORT.key()));
    }

    @Test
    void loadsOverridesWithoutDiscoveringAnApplicationFile() {
        Map<String, String> overrides = Collections.singletonMap("yarn.queue", "batch");
        Map<String, String> loaded = SeatunnelApplicationConfig.load(null, overrides);
        assertEquals(overrides, loaded);
        loaded.put("yarn.queue", "changed");
        assertEquals("batch", overrides.get("yarn.queue"));
        assertTrue(SeatunnelApplicationConfig.load(null, Collections.emptyMap()).isEmpty());
    }

    @Test
    void rejectsUnreadableMalformedAndUnresolvedApplicationFiles() throws Exception {
        Path application = temporary.resolve("application.config");
        assertThrows(
                ConfigException.class,
                () -> SeatunnelApplicationConfig.load(application, Collections.emptyMap()));
        Files.write(application, "application { invalid".getBytes(StandardCharsets.UTF_8));
        assertThrows(
                ConfigException.class,
                () -> SeatunnelApplicationConfig.load(application, Collections.emptyMap()));
        Files.write(
                application,
                "application.name = ${missing-application-name}".getBytes(StandardCharsets.UTF_8));
        assertThrows(
                ConfigException.class,
                () -> SeatunnelApplicationConfig.load(application, Collections.emptyMap()));
    }

    @Test
    void readsJobFileIndependentlyOfDeploymentOverrides() throws Exception {
        Path job = temporary.resolve("job.config");
        Files.write(job, "env.parallelism = 1".getBytes(StandardCharsets.UTF_8));
        Map<String, String> options = new HashMap<>();
        options.put("env.parallelism", "99");
        options.put("application.worker-count", "3");
        ApplicationSpecification specification = SeatunnelApplicationConfig.parse(job, options);
        assertEquals(
                1,
                ConfigFactory.parseString(specification.getJobConfig()).getInt("env.parallelism"));
        assertEquals(3, specification.getWorkerCount());
        assertThrows(
                ConfigException.class,
                () ->
                        SeatunnelApplicationConfig.parse(
                                temporary.resolve("missing-job.config"), options));
    }

    @Test
    void roundTripsResolvedApplicationFieldsWithoutPlatformOptions() throws Exception {
        Map<String, String> options = new HashMap<>();
        options.put("application.name", "sync-job");
        options.put("application.worker-count", "3");
        options.put("application.worker.memory-mb", "2048");
        options.put("application.worker.cpu-cores", "2");
        options.put("application.worker.slots", "4");
        options.put("application.startup-timeout-millis", "90000");
        options.put("application.master.memory-mb", "4096");
        options.put("application.master.cpu-cores", "3");
        options.put("application.master.port", "5802");
        options.put("yarn.queue", "batch");
        String job = "env { job.mode=BATCH }\nsource { FakeSource { row.num=5 } }\n";
        ApplicationSpecification original = SeatunnelApplicationConfig.parse(job, options);
        Properties localized = SeatunnelApplicationConfig.toProperties(original);
        assertEquals("V1", localized.getProperty("format.version"));
        ApplicationSpecification copy = SeatunnelApplicationConfig.fromProperties(localized);
        assertEquals("sync-job", copy.getName());
        assertEquals(job, copy.getJobConfig());
        assertEquals(3, copy.getWorkerCount());
        assertEquals(new WorkerSpecification(2048, 2, 4), copy.getWorkerSpecification());
        assertEquals(90000L, copy.getStartupTimeoutMillis());
        assertEquals(4096, copy.getMasterMemoryMb());
        assertEquals(3, copy.getMasterCpuCores());
        assertEquals(5802, copy.getMasterPort());
        assertFalse(localized.containsKey("yarn.queue"));
        assertEquals(original.getJobId(), copy.getJobId());
        assertTrue(copy.getJobId() > 0);
        options.put("application.name", "mutated");
        assertEquals("sync-job", original.getName());
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
                            SeatunnelApplicationConfig.parse(
                                    "source {}", Collections.singletonMap(key, "0")),
                    key);
        }
        assertThrows(
                IllegalArgumentException.class,
                () ->
                        SeatunnelApplicationConfig.parse(
                                "source {}",
                                Collections.singletonMap("application.master.port", "65536")));
    }

    @Test
    void resolvesCommonDefaultsWithoutRequiringPlatformSettings() {
        ApplicationSpecification specification =
                SeatunnelApplicationConfig.parse("source {}", Collections.emptyMap());
        assertEquals("seatunnel", specification.getName());
        assertEquals(1, specification.getWorkerCount());
        assertEquals(5801, specification.getMasterPort());
    }

    @Test
    void validatesRecoveryIdentityWithoutReusingTheSourceJobId() {
        Map<String, String> options = new HashMap<>();
        options.put(ApplicationOptions.JOB_ID.key(), "101");
        options.put(ApplicationOptions.RESTORE_JOB_ID.key(), "100");
        ApplicationSpecification specification =
                SeatunnelApplicationConfig.parse("source {}", options);
        assertEquals(101L, specification.getJobId());
        assertEquals(100L, specification.getRestoreJobId().longValue());
        options.put(ApplicationOptions.RESTORE_JOB_ID.key(), "101");
        assertThrows(
                IllegalArgumentException.class,
                () -> SeatunnelApplicationConfig.parse("source {}", options));
    }

    @Test
    void rejectsUnrecognizedLocalizedFormat() throws Exception {
        Properties properties = new Properties();
        properties.setProperty("format.version", "1");
        assertThrows(
                IOException.class, () -> SeatunnelApplicationConfig.fromProperties(properties));
        properties.setProperty("format.version", "V1");
        assertThrows(
                IOException.class, () -> SeatunnelApplicationConfig.fromProperties(properties));
    }
}
