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

package org.apache.seatunnel.e2e.connector.http;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.container.seatunnel.SeaTunnelContainer;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.Timeout;
import org.testcontainers.Testcontainers;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import com.sun.net.httpserver.HttpServer;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Checks installed connector output against fixed HTTP fixtures, including exact typed values. A
 * malformed row, lost nested field or changed numeric/text mapping must fail the Assert sink.
 */
class CbsStatLineIT extends TestSuiteBase implements TestResource {
    private GenericContainer<?> fixture;

    @BeforeAll
    @Override
    public void startUp() {
        fixture =
                new GenericContainer<>(DockerImageName.parse("nginx:1.27.5-alpine"))
                        .withNetwork(NETWORK)
                        .withNetworkAliases("statline-fixture")
                        .withCopyFileToContainer(
                                MountableFile.forClasspathResource("cbs_statline_nginx.conf"),
                                "/etc/nginx/conf.d/default.conf")
                        .withExposedPorts(80)
                        .waitingFor(Wait.forHttp("/health").forStatusCode(200));
        fixture.start();
    }

    @AfterAll
    @Override
    public void tearDown() {
        if (fixture != null) {
            fixture.close();
        }
    }

    /*
     * The job retains the two-row bound and checks every fixture value through contract_ok.
     */
    @TestTemplate
    void readsTwoObservationPages(TestContainer container) throws Exception {
        Container.ExecResult result = container.executeJob("/cbs_statline_to_assert.conf");
        Assertions.assertEquals(0, result.getExitCode(), result.getStderr());
    }
    /**
     * Keeps checkpoints progressing while one streamed page is throttled, then demonstrates the
     * documented full-scan replay of a null-state savepoint. Console rows are not commit receipts.
     */
    @Test
    @Timeout(value = 4, unit = TimeUnit.MINUTES)
    void checkpointsThrottledPageAndRestoresTheWholeScan() throws Exception {
        List<String> observations = new ArrayList<>();
        List<String> expected = new ArrayList<>();
        for (int i = 0; i < 100; i++) {
            String value = "{\"Id\":" + i + ",\"Measure\":\"M\",\"Value\":1}";
            observations.add(value);
            expected.add("83765NED, " + i + ", M, 1, , , " + value);
        }
        byte[] body =
                ("{\"value\":[" + String.join(",", observations) + "]}")
                        .getBytes(StandardCharsets.UTF_8);
        AtomicInteger requests = new AtomicInteger();
        HttpServer server = HttpServer.create(new InetSocketAddress(0), 0);
        server.createContext(
                "/odata/v1/CBS/83765NED/Observations",
                exchange -> {
                    requests.incrementAndGet();
                    exchange.sendResponseHeaders(200, body.length);
                    try (OutputStream output = exchange.getResponseBody()) {
                        output.write(body);
                    }
                });
        SeaTunnelContainer engine = new SeaTunnelContainer();
        ExecutorService command = Executors.newSingleThreadExecutor();
        String config = "/cbs_statline_throttled_to_console.conf";
        String job = Long.toString(System.currentTimeMillis());
        String[] arguments = {
            "cbs.origin=http://host.testcontainers.internal:" + server.getAddress().getPort()
        };
        Future<Container.ExecResult> original = null;
        try {
            server.start();
            Testcontainers.exposeHostPorts(server.getAddress().getPort());
            // Partial startup and job submission failures still execute the ownership cleanup.
            engine.startUp();
            original = command.submit(() -> engine.executeJob(config, job, arguments));
            Awaitility.await()
                    .atMost(45, TimeUnit.SECONDS)
                    .until(() -> !cbsRows(engine.getServerLogs()).isEmpty());
            long baseline = engine.getCompletedCheckpointCount(job);
            Awaitility.await()
                    .atMost(10, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> {
                                Assertions.assertTrue(
                                        engine.getCompletedCheckpointCount(job) >= baseline + 2);
                                Assertions.assertTrue(cbsRows(engine.getServerLogs()).size() < 40);
                                Assertions.assertEquals(1, requests.get());
                            });
            Assertions.assertEquals(0, engine.savepointJob(job).getExitCode());
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    Assertions.assertEquals(
                                            "SAVEPOINT_DONE", engine.getJobStatus(job)));
            Assertions.assertEquals(0, original.get(30, TimeUnit.SECONDS).getExitCode());
            List<String> prefix = cbsRows(engine.getServerLogs());
            Assertions.assertTrue(!prefix.isEmpty() && prefix.size() < expected.size());
            Assertions.assertEquals(expected.subList(0, prefix.size()), prefix);
            int offset = engine.getServerLogs().length();
            Assertions.assertEquals(0, engine.restoreJob(config, job, arguments).getExitCode());
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> Assertions.assertEquals("FINISHED", engine.getJobStatus(job)));
            Assertions.assertEquals(
                    expected,
                    cbsRows(engine.getServerLogs().substring(offset)),
                    "null split state replays the entire original scan, including its earlier prefix");
            Assertions.assertEquals(2, requests.get());
        } finally {
            try {
                if (original != null && !original.isDone()) {
                    try {
                        engine.cancelJob(job);
                    } finally {
                        original.cancel(true);
                    }
                }
            } finally {
                try {
                    engine.tearDown();
                } finally {
                    server.stop(0);
                    command.shutdownNow();
                    Assertions.assertTrue(command.awaitTermination(10, TimeUnit.SECONDS));
                }
            }
        }
    }

    private static List<String> cbsRows(String logs) {
        List<String> rows = new ArrayList<>();
        String marker = "SeaTunnelRow#kind=INSERT : ";
        for (String line : logs.split("\\R")) {
            int start = line.indexOf(marker);
            if (start >= 0) {
                rows.add(line.substring(start + marker.length()));
            }
        }
        return rows;
    }
}
