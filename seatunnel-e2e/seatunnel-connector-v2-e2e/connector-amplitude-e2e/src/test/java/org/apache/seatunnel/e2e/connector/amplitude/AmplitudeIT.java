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

package org.apache.seatunnel.e2e.connector.amplitude;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.container.TestContainerId;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.Testcontainers;
import org.testcontainers.containers.Container;

import com.sun.net.httpserver.HttpServer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Base64;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

/** Runs the connector against a local export service; no Amplitude credentials are needed. */
public class AmplitudeIT extends TestSuiteBase implements TestResource {
    private HttpServer fixture;
    private int port;
    private final AtomicInteger requests = new AtomicInteger();
    private volatile int eventCount = 3;
    private volatile boolean corrupt;
    private volatile boolean blockDownload;
    private volatile CountDownLatch bodyStarted;
    private volatile CountDownLatch releaseBody;
    private ExecutorService fixtureThreads;

    @BeforeAll
    @Override
    public void startUp() throws IOException {
        fixture = HttpServer.create(new InetSocketAddress(0), 0);
        fixtureThreads = Executors.newCachedThreadPool();
        fixture.setExecutor(fixtureThreads);
        port = fixture.getAddress().getPort();
        Testcontainers.exposeHostPorts(port);
        fixture.createContext(
                "/api/2/export",
                exchange -> {
                    int attempt = requests.incrementAndGet();
                    String expected =
                            "Basic "
                                    + Base64.getEncoder()
                                            .encodeToString(
                                                    "fixture-api-key:fixture-secret"
                                                            .getBytes(StandardCharsets.UTF_8));
                    if (!"GET".equals(exchange.getRequestMethod())
                            || !expected.equals(
                                    exchange.getRequestHeaders().getFirst("Authorization"))
                            || !"start=20260101T00&end=20260101T01"
                                    .equals(exchange.getRequestURI().getRawQuery())) {
                        exchange.sendResponseHeaders(400, -1);
                        exchange.close();
                        return;
                    }
                    if (attempt < 3) {
                        exchange.getResponseHeaders().set("Retry-After", "0");
                        exchange.sendResponseHeaders(attempt == 1 ? 429 : 503, -1);
                        exchange.close();
                        return;
                    }
                    byte[] zip = archive();
                    if (corrupt) {
                        zip = Arrays.copyOf(zip, zip.length - 22);
                    }
                    exchange.getResponseHeaders().set("Content-Type", "application/zip");
                    exchange.sendResponseHeaders(200, zip.length);
                    try (OutputStream output = exchange.getResponseBody()) {
                        output.write(zip, 0, zip.length / 2);
                        output.flush();
                        if (blockDownload) {
                            bodyStarted.countDown();
                            if (!releaseBody.await(90, TimeUnit.SECONDS)) {
                                throw new IOException("Fixture download release timed out");
                            }
                        }
                        output.write(zip, zip.length / 2, zip.length - zip.length / 2);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IOException("Fixture interrupted");
                    } finally {
                        exchange.close();
                    }
                });
        fixture.start();
    }

    private byte[] archive() throws IOException {
        ByteArrayOutputStream zipBytes = new ByteArrayOutputStream();
        try (ZipOutputStream zip = new ZipOutputStream(zipBytes)) {
            // Multiple gzip entries exercise both archive layers.
            for (int i = 1; i <= 3; i++) {
                ByteArrayOutputStream gzipBytes = new ByteArrayOutputStream();
                try (GZIPOutputStream gzip = new GZIPOutputStream(gzipBytes)) {
                    for (int row = i; row <= eventCount; row += 3) {
                        gzip.write(
                                ("{\"event_type\":\"purchase\",\"uuid\":\""
                                                + row
                                                + "\",\"event_properties\":{\"amount\":42}}\n")
                                        .getBytes(StandardCharsets.UTF_8));
                    }
                }
                zip.putNextEntry(new ZipEntry(i + ".json.gz"));
                zip.write(gzipBytes.toByteArray());
                zip.closeEntry();
            }
        }
        return zipBytes.toByteArray();
    }

    @AfterAll
    @Override
    public void tearDown() throws InterruptedException {
        if (releaseBody != null) {
            releaseBody.countDown();
        }
        if (fixture != null) {
            fixture.stop(0);
        }
        if (fixtureThreads != null) {
            fixtureThreads.shutdownNow();
            Assertions.assertTrue(fixtureThreads.awaitTermination(10, TimeUnit.SECONDS));
        }
    }

    @TestTemplate
    public void retriesThenReadsAllGzipEntries(TestContainer container) throws Exception {
        reset(false, false);
        String jobId = Long.toString(System.currentTimeMillis());
        Container.ExecResult result = execute(container, jobId);
        Assertions.assertEquals(0, result.getExitCode(), result.getStderr());
        if (container.identifier() == TestContainerId.SEATUNNEL) {
            Assertions.assertEquals("FINISHED", container.getJobStatus(jobId));
        }
        Assertions.assertEquals(3, requests.get());
    }

    @TestTemplate
    public void readsRepresentativeExport(TestContainer container) throws Exception {
        reset(false, false);
        eventCount = 20000;
        String jobId = Long.toString(System.currentTimeMillis());
        Container.ExecResult result = execute(container, jobId);
        Assertions.assertEquals(0, result.getExitCode(), result.getStderr());
        if (container.identifier() == TestContainerId.SEATUNNEL) {
            Assertions.assertEquals("FINISHED", container.getJobStatus(jobId));
        }
        Assertions.assertEquals(3, requests.get());
    }

    @TestTemplate
    public void rejectsTruncatedArchiveWithoutSuccessfulCompletion(TestContainer container)
            throws Exception {
        reset(true, false);
        String jobId = Long.toString(System.currentTimeMillis());
        Container.ExecResult result =
                container.identifier() == TestContainerId.SEATUNNEL
                        ? container.executeJob(
                                "/amplitude_source_to_assert.conf", jobId, variables(0))
                        : container.executeJob(
                                "/amplitude_source_to_assert.conf", Arrays.asList(variables(0)));
        Assertions.assertNotEquals(0, result.getExitCode());
        if (container.identifier() == TestContainerId.SEATUNNEL) {
            Assertions.assertEquals("FAILED", container.getJobStatus(jobId));
        }
        Assertions.assertEquals(
                3, requests.get(), "Archive corruption must not be retried as HTTP");
        Assertions.assertTrue(
                (result.getStdout() + result.getStderr()).contains("Amplitude export failed"),
                "Failure must originate in archive validation");
    }

    @TestTemplate
    public void checkpointsDuringDownloadAndReplaysAfterSavepointRestore(TestContainer container)
            throws Exception {
        Assumptions.assumeTrue(container.identifier() == TestContainerId.SEATUNNEL);
        reset(false, true);
        String jobId = Long.toString(System.currentTimeMillis());
        ExecutorService submission = Executors.newSingleThreadExecutor();
        try {
            Future<Container.ExecResult> running =
                    submission.submit(
                            () ->
                                    container.executeJob(
                                            "/amplitude_source_to_assert.conf",
                                            jobId,
                                            variables(0)));
            Assertions.assertTrue(bodyStarted.await(90, TimeUnit.SECONDS));
            long before = container.getCompletedCheckpointCount(jobId);
            Awaitility.await()
                    .atMost(40, TimeUnit.SECONDS)
                    .until(() -> container.getCompletedCheckpointCount(jobId) > before);
            Container.ExecResult savepoint = container.savepointJob(jobId);
            Assertions.assertEquals(0, savepoint.getExitCode(), savepoint.getStderr());
            Container.ExecResult stopped = running.get(60, TimeUnit.SECONDS);
            Assertions.assertEquals(0, stopped.getExitCode(), stopped.getStderr());
            Assertions.assertFalse(
                    container.getServerLogs().contains("transport retry budget exhausted"));
            blockDownload = false;
            releaseBody.countDown();
            Container.ExecResult restored =
                    container.restoreJobWithCheckpoint(
                            "/amplitude_source_to_assert.conf", jobId, variables());
            Assertions.assertEquals(0, restored.getExitCode(), restored.getStderr());
            Matcher finished =
                    Pattern.compile("Job \\((\\d+)\\) end with state FINISHED")
                            .matcher(restored.getStdout() + restored.getStderr());
            Assertions.assertTrue(
                    finished.find(), "Restore must finish under its newly allocated job ID");
            String restoredId = finished.group(1);
            Assertions.assertNotEquals(jobId, restoredId);
            Assertions.assertEquals("FINISHED", container.getJobStatus(restoredId));
            Assertions.assertTrue(container.getServerLogs().contains("restore with CHECKPOINT"));
            Assertions.assertEquals(
                    4, requests.get(), "Restore must request the complete window again");
        } finally {
            releaseBody.countDown();
            submission.shutdownNow();
            Assertions.assertTrue(submission.awaitTermination(30, TimeUnit.SECONDS));
        }
    }

    private String[] variables() {
        return variables(eventCount);
    }

    private String[] variables(int minimum) {
        return new String[] {
            "amplitude.test.port=" + port,
            "amplitude.test.min_rows=" + minimum,
            "amplitude.test.max_rows=" + eventCount
        };
    }

    private Container.ExecResult execute(TestContainer container, String jobId)
            throws IOException, InterruptedException {
        return container.identifier() == TestContainerId.SEATUNNEL
                ? container.executeJob("/amplitude_source_to_assert.conf", jobId, variables())
                : container.executeJob(
                        "/amplitude_source_to_assert.conf", Arrays.asList(variables()));
    }

    private void reset(boolean invalid, boolean blocked) {
        requests.set(0);
        eventCount = 3;
        corrupt = invalid;
        blockDownload = blocked;
        bodyStarted = new CountDownLatch(1);
        releaseBody = new CountDownLatch(1);
    }
}
