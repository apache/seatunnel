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

package org.apache.seatunnel.e2e.connector.mixpanel;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.container.TestContainerId;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.Testcontainers;
import org.testcontainers.containers.Container;

import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/** Offline engine test of the real export reader against a streaming HTTP fixture. */
public class MixpanelIT extends TestSuiteBase implements TestResource {
    private HttpServer fixture;
    private int port;
    private final AtomicInteger requests = new AtomicInteger();
    private volatile TestContainer activeContainer;
    private volatile String activeJobId;
    private volatile boolean checkpointObserved;

    @BeforeAll
    @Override
    public void startUp() throws IOException {
        fixture = HttpServer.create(new InetSocketAddress(0), 0);
        port = fixture.getAddress().getPort();
        Testcontainers.exposeHostPorts(port);
        fixture.createContext(
                "/api/2.0/export",
                exchange -> {
                    int attempt = requests.incrementAndGet();
                    String expected =
                            "Basic "
                                    + Base64.getEncoder()
                                            .encodeToString(
                                                    "fixture-user:fixture-password"
                                                            .getBytes(StandardCharsets.UTF_8));
                    if (!"GET".equals(exchange.getRequestMethod())
                            || !expected.equals(
                                    exchange.getRequestHeaders().getFirst("Authorization"))
                            || !"project_id=42&from_date=2026-01-01&to_date=2026-01-02"
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
                    exchange.getResponseHeaders().set("Content-Type", "text/plain");
                    exchange.sendResponseHeaders(200, 0);
                    try (OutputStream output = exchange.getResponseBody()) {
                        for (int i = 1; i <= 3; i++) {
                            String event =
                                    "{\"event\":\"purchase\",\"properties\":{\"time\":1767225600,"
                                            + "\"distinct_id\":\"fixture-user\",\"$insert_id\":\""
                                            + i
                                            + "\"}}\n";
                            output.write(event.getBytes(StandardCharsets.UTF_8));
                            output.flush();
                            if (i == 1
                                    && activeContainer.identifier() == TestContainerId.SEATUNNEL) {
                                // Require a new checkpoint while this export body remains open.
                                long completed =
                                        activeContainer.getCompletedCheckpointCount(activeJobId);
                                Awaitility.await()
                                        .atMost(30, TimeUnit.SECONDS)
                                        .until(
                                                () ->
                                                        activeContainer.getCompletedCheckpointCount(
                                                                        activeJobId)
                                                                > completed);
                                checkpointObserved = true;
                            }
                        }
                    } finally {
                        exchange.close();
                    }
                });
        fixture.start();
    }

    @AfterAll
    @Override
    public void tearDown() {
        if (fixture != null) {
            fixture.stop(0);
        }
    }

    @TestTemplate
    public void readsCompleteExportAfterRateLimitAndServerFailure(TestContainer container)
            throws IOException, InterruptedException {
        requests.set(0);
        String jobId = Long.toString(System.currentTimeMillis());
        boolean zeta = container.identifier() == TestContainerId.SEATUNNEL;
        activeContainer = container;
        activeJobId = jobId;
        checkpointObserved = false;
        Container.ExecResult result =
                zeta
                        ? container.executeJob(
                                "/mixpanel_source_to_assert.conf",
                                jobId,
                                "mixpanel.test.port=" + port)
                        : container.executeJob(
                                "/mixpanel_source_to_assert.conf",
                                Collections.singletonList("mixpanel.test.port=" + port));
        Assertions.assertEquals(0, result.getExitCode(), result.getStderr());
        if (zeta) {
            Assertions.assertTrue(
                    checkpointObserved,
                    "A checkpoint must complete while the export body is still open");
            Assertions.assertEquals(
                    "FINISHED",
                    container.getJobStatus(jobId),
                    "The engine job must finish, not merely return a successful CLI exit");
            Assertions.assertFalse(
                    container.getServerLogs().contains("state RUNNING to FAILED"),
                    "The source must handle HTTP retries without engine task recovery");
        }
        Assertions.assertEquals(
                3, requests.get(), "two retries, then one full export without a limit");
    }
}
