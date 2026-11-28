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

package org.apache.seatunnel.e2e.connector.opendatadc;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.container.TestContainerId;
import org.apache.seatunnel.e2e.common.container.seatunnel.SeaTunnelContainer;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.Testcontainers;
import org.testcontainers.containers.Container;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.LongStream;

/**
 * Exercises the installed source against the local ArcGIS fixture in Zeta. Tests verify exact
 * request sequences, terminal job outcomes and committed-key restore during throttled page output.
 */
public class OpenDataDcIT extends TestSuiteBase implements TestResource {
    private static final String CONF = "/open_data_dc_to_assert.conf";
    private ArcGisCrashesFixture fixture;

    @BeforeAll
    @Override
    public void startUp() throws IOException {
        fixture = new ArcGisCrashesFixture();
        fixture.start();
        Testcontainers.exposeHostPorts(fixture.getPort());
    }

    @AfterAll
    @Override
    public void tearDown() {
        if (fixture != null) {
            fixture.close();
        }
    }

    @TestTemplate
    public void continuesEmptyWindowsAndRetriesSameOffsetWithoutReplay(TestContainer container)
            throws Exception {
        Assumptions.assumeTrue(container.identifier() == TestContainerId.SEATUNNEL);
        fixture.reset(ArcGisCrashesFixture.Mode.RECOVER);
        String jobId = Long.toString(System.currentTimeMillis());
        Container.ExecResult result = execute(container, jobId);
        Assertions.assertEquals(0, result.getExitCode(), result.getStderr());
        Assertions.assertEquals("FINISHED", container.getJobStatus(jobId));
        Assertions.assertEquals(
                Arrays.asList(
                        "1=1 @0",
                        "1=1 @1",
                        "OBJECTID > 0 AND OBJECTID <= 19 @0",
                        "OBJECTID > 7 AND OBJECTID <= 19 @0",
                        "OBJECTID > 7 AND OBJECTID <= 19 @2",
                        "OBJECTID > 7 AND OBJECTID <= 19 @2",
                        "OBJECTID > 12 AND OBJECTID <= 19 @0"),
                fixture.getQueries());
    }

    @TestTemplate
    public void failsWhenArcGisReturnsAnErrorWithHttp200(TestContainer container) throws Exception {
        Assumptions.assumeTrue(container.identifier() == TestContainerId.SEATUNNEL);
        fixture.reset(ArcGisCrashesFixture.Mode.ARCGIS_ERROR);
        String jobId = Long.toString(System.currentTimeMillis());
        Container.ExecResult result = execute(container, jobId);
        Assertions.assertNotEquals(0, result.getExitCode());
        Assertions.assertEquals("FAILED", container.getJobStatus(jobId));
        Assertions.assertTrue(
                (result.getStdout() + result.getStderr())
                        .contains("Open Data DC ArcGIS error: 400"));
        Assertions.assertEquals(
                Arrays.asList(
                        "1=1 @0",
                        "OBJECTID > 0 AND OBJECTID <= 19 @0",
                        "OBJECTID > 7 AND OBJECTID <= 19 @0"),
                fixture.getQueries());
    }

    @TestTemplate
    public void failsAfterThreeAttemptsForTheSameOffset(TestContainer container) throws Exception {
        Assumptions.assumeTrue(container.identifier() == TestContainerId.SEATUNNEL);
        fixture.reset(ArcGisCrashesFixture.Mode.THROTTLE_EXHAUST);
        String jobId = Long.toString(System.currentTimeMillis());
        Container.ExecResult result = execute(container, jobId);
        Assertions.assertNotEquals(0, result.getExitCode());
        Assertions.assertEquals("FAILED", container.getJobStatus(jobId));
        Assertions.assertTrue(
                (result.getStdout() + result.getStderr())
                        .contains("Open Data DC returned HTTP 429"));
        Assertions.assertEquals(
                Arrays.asList(
                        "1=1 @0",
                        "OBJECTID > 0 AND OBJECTID <= 19 @0",
                        "OBJECTID > 7 AND OBJECTID <= 19 @0",
                        "OBJECTID > 7 AND OBJECTID <= 19 @2",
                        "OBJECTID > 7 AND OBJECTID <= 19 @2",
                        "OBJECTID > 7 AND OBJECTID <= 19 @2"),
                fixture.getQueries());
    }

    @TestTemplate
    public void failsRatherThanTruncatesWhenEmptyWindowBudgetIsExhausted(TestContainer container)
            throws Exception {
        Assumptions.assumeTrue(container.identifier() == TestContainerId.SEATUNNEL);
        fixture.reset(ArcGisCrashesFixture.Mode.EMPTY_EXHAUST);
        String jobId = Long.toString(System.currentTimeMillis());
        Container.ExecResult result = execute(container, jobId);
        Assertions.assertNotEquals(0, result.getExitCode());
        Assertions.assertEquals("FAILED", container.getJobStatus(jobId));
        Assertions.assertTrue(
                (result.getStdout() + result.getStderr())
                        .contains("too many consecutive empty windows"));
        List<String> expected =
                new ArrayList<>(Arrays.asList("1=1 @0", "OBJECTID > 0 AND OBJECTID <= 19 @0"));
        for (int offset = 0; offset <= 64; offset += 2) {
            expected.add("OBJECTID > 7 AND OBJECTID <= 19 @" + offset);
        }
        Assertions.assertEquals(expected, fixture.getQueries());
    }

    @TestTemplate
    public void rejectsRedirectWithoutMakingAnotherRequest(TestContainer container)
            throws Exception {
        Assumptions.assumeTrue(container.identifier() == TestContainerId.SEATUNNEL);
        fixture.reset(ArcGisCrashesFixture.Mode.REDIRECT);
        String jobId = Long.toString(System.currentTimeMillis());
        Container.ExecResult result = execute(container, jobId);
        Assertions.assertNotEquals(0, result.getExitCode());
        Assertions.assertEquals("FAILED", container.getJobStatus(jobId));
        Assertions.assertTrue(
                (result.getStdout() + result.getStderr())
                        .contains("Open Data DC returned HTTP 302"));
        Assertions.assertEquals(Arrays.asList("1=1 @0"), fixture.getQueries());
    }

    @TestTemplate
    public void failsBeforeAcceptingAnOversizedChunkedResponse(TestContainer container)
            throws Exception {
        Assumptions.assumeTrue(container.identifier() == TestContainerId.SEATUNNEL);
        fixture.reset(ArcGisCrashesFixture.Mode.OVERSIZED_RESPONSE);
        String jobId = Long.toString(System.currentTimeMillis());
        Container.ExecResult result = execute(container, jobId);
        Assertions.assertNotEquals(0, result.getExitCode());
        Assertions.assertEquals("FAILED", container.getJobStatus(jobId));
        Assertions.assertTrue(
                (result.getStdout() + result.getStderr())
                        .contains("response exceeds the local byte budget"));
        Assertions.assertEquals(Arrays.asList("1=1 @0"), fixture.getQueries());
    }

    @Test
    @Timeout(value = 5, unit = TimeUnit.MINUTES)
    void checkpointsDecodedThrottledRowsAndRestoresTheExactAcceptedKeyBoundAndCap()
            throws Exception {
        String conf = "/open_data_dc_checkpoint_to_localfile.conf";
        fixture.reset(ArcGisCrashesFixture.Mode.CHECKPOINT_ROWS);
        CheckpointEngine engine = new CheckpointEngine();
        ExecutorService jobs = Executors.newSingleThreadExecutor();
        String original = Long.toString(ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE));
        List<String> variables =
                Arrays.asList(
                        "open_data_dc.test.port=" + fixture.getPort(), "job.output=" + original);
        try {
            engine.startUp();
            Future<Container.ExecResult> first =
                    jobs.submit(
                            () ->
                                    engine.executeJob(
                                            conf, original, variables.toArray(new String[0])));
            Awaitility.await()
                    .atMost(20, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                int count = committedIds(engine, original).size();
                                Assertions.assertTrue(
                                        count >= 5 && count < 400,
                                        "A completed checkpoint must commit a partial decoded page");
                            });
            long baseline = engine.getCompletedCheckpointCount(original);
            Awaitility.await()
                    .atMost(10, TimeUnit.SECONDS)
                    .until(() -> engine.getCompletedCheckpointCount(original) > baseline);
            Assertions.assertEquals(
                    Arrays.asList("1=1 @0 #1", "OBJECTID > 0 AND OBJECTID <= 1000 @0 #400"),
                    fixture.getCheckpointQueries());
            engine.stopJob(original);
            Awaitility.await()
                    .atMost(20, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            "CANCELED", engine.getJobStatus(original)));
            first.get(25, TimeUnit.SECONDS);
            List<Long> committed = committedIds(engine, original);
            int accepted = committed.size();
            Assertions.assertTrue(accepted >= 5 && accepted < 400);
            Assertions.assertEquals(
                    LongStream.rangeClosed(1, accepted).boxed().collect(Collectors.toList()),
                    committed);
            String restored =
                    Long.toString(ThreadLocalRandom.current().nextLong(1, Long.MAX_VALUE));
            Future<Container.ExecResult> resumed =
                    jobs.submit(
                            () ->
                                    engine.restoreJobWithCheckpoint(
                                            conf, original, restored, variables));
            Container.ExecResult result = resumed.get(120, TimeUnit.SECONDS);
            Assertions.assertEquals(0, result.getExitCode(), result.getStderr());
            Awaitility.await()
                    .atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            "FINISHED", engine.getJobStatus(restored)));
            Assertions.assertEquals(
                    LongStream.rangeClosed(1, 400).boxed().collect(Collectors.toList()),
                    committedIds(engine, original));
            Assertions.assertEquals(
                    Arrays.asList(
                            "1=1 @0 #1",
                            "OBJECTID > 0 AND OBJECTID <= 1000 @0 #400",
                            "OBJECTID > "
                                    + accepted
                                    + " AND OBJECTID <= 1000 @0 #"
                                    + (400 - accepted)),
                    fixture.getCheckpointQueries());
        } finally {
            jobs.shutdownNow();
            engine.tearDown();
        }
    }

    /**
     * Reads only committed transaction receipts, so restored output detects replay or skipped keys.
     * Pending checkpoint files remain outside this directory and do not count as accepted progress.
     */
    private static List<Long> committedIds(CheckpointEngine engine, String output)
            throws Exception {
        String directory = "/tmp/seatunnel_mnt/open-data-dc-" + output;
        Container.ExecResult files =
                engine.command(
                        "sh",
                        "-c",
                        "if test -d "
                                + directory
                                + "; then find "
                                + directory
                                + " -maxdepth 1 -name 'receipt_*' -type f "
                                + "-exec sh -c 'for f do cat \"$f\"; printf \"\\n\"; done' sh {} +; fi");
        Assertions.assertEquals(0, files.getExitCode(), files.getStderr());
        List<Long> ids = new ArrayList<>();
        ObjectMapper json = new ObjectMapper();
        for (String line : files.getStdout().split("\\R")) {
            if (!line.trim().isEmpty()) {
                JsonNode row = json.readTree(line);
                ids.add(row.get("OBJECTID").longValue());
            }
        }
        ids.sort(Long::compareTo);
        return ids;
    }

    private Container.ExecResult execute(TestContainer container, String jobId)
            throws IOException, InterruptedException {
        return container.executeJob(CONF, jobId, "open_data_dc.test.port=" + fixture.getPort());
    }

    /**
     * Owns an isolated Zeta worker for transactional receipt inspection during checkpoint restore.
     * It uses the existing packaged connector loading and restore command without shared API
     * changes.
     */
    private static final class CheckpointEngine extends SeaTunnelContainer {
        Container.ExecResult command(String... args) throws IOException, InterruptedException {
            return server.execInContainer(args);
        }
    }
}
