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

package org.apache.seatunnel.engine.e2e.console;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;

import org.apache.seatunnel.api.event.EventType;
import org.apache.seatunnel.engine.e2e.SeaTunnelEngineContainer;
import org.apache.seatunnel.engine.server.event.JobEventHttpReportHandler;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.testcontainers.Testcontainers;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.utility.MountableFile;

import lombok.extern.slf4j.Slf4j;
import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import okio.Buffer;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.seatunnel.e2e.common.util.ContainerUtil.PROJECT_ROOT_PATH;
import static org.awaitility.Awaitility.given;

@Slf4j
public class FakeSourceToConsoleWithEventReportIT extends SeaTunnelEngineContainer {
    private static final String MOCK_SERVER_PORT_PLACEHOLDER = "${MOCK_SERVER_PORT}";

    private MockWebServer mockWebServer;
    private Path eventReportConfig;

    @Override
    @BeforeAll
    public void startUp() throws Exception {
        mockWebServer = new MockWebServer();
        mockWebServer.setDispatcher(
                new Dispatcher() {
                    @Override
                    public MockResponse dispatch(RecordedRequest request) {
                        return new MockResponse().setResponseCode(200);
                    }
                });
        mockWebServer.start();
        Testcontainers.exposeHostPorts(mockWebServer.getPort());

        super.startUp();
        log.info("The TestContainer[{}] is running.", identifier());
    }

    @Override
    @AfterAll
    public void tearDown() throws Exception {
        super.tearDown();

        mockWebServer.shutdown();
        if (eventReportConfig != null) {
            Files.deleteIfExists(eventReportConfig);
        }
        log.info("The TestContainer[{}] is closed.", identifier());
    }

    @Override
    protected void executeExtraCommands(GenericContainer<?> container)
            throws IOException, InterruptedException {
        Path configTemplate =
                Paths.get(
                        PROJECT_ROOT_PATH
                                + "/seatunnel-e2e/seatunnel-engine-e2e/connector-console-seatunnel-e2e/src/test/resources/seatunnel_config_with_event_report.yaml");
        String config =
                new String(Files.readAllBytes(configTemplate), StandardCharsets.UTF_8)
                        .replace(
                                MOCK_SERVER_PORT_PLACEHOLDER,
                                String.valueOf(mockWebServer.getPort()));
        eventReportConfig = Files.createTempFile("seatunnel-event-report-", ".yaml");
        Files.write(eventReportConfig, config.getBytes(StandardCharsets.UTF_8));
        container.withCopyFileToContainer(
                MountableFile.forHostPath(eventReportConfig),
                Paths.get(SEATUNNEL_HOME, "config", "seatunnel.yaml").toString());
        // This test uses startUp() -> createSeaTunnelServer(), which calls this hook before
        // start().
        // Keep the base fixture's readiness condition and timeout; only add failure diagnostics.
        container.waitingFor(
                new LogMessageWaitStrategy() {
                    @Override
                    protected void waitUntilReady() {
                        try {
                            super.waitUntilReady();
                        } catch (RuntimeException startupFailure) {
                            logStartupThreads(container);
                            throw startupFailure;
                        }
                    }
                }.withRegEx(".*received new worker register:.*"));
    }

    /** Capture the blocked startup before Testcontainers stops the failed container. */
    private void logStartupThreads(GenericContainer<?> container) {
        try {
            Container.ExecResult processes = container.execInContainer("timeout", "10s", "jps");
            if (processes.getExitCode() != 0) {
                log.warn("Could not list startup JVMs: {}", processes.getStderr());
                return;
            }
            for (String process : processes.getStdout().split("\\n")) {
                if (process.contains("SeaTunnelServer")) {
                    String pid = process.trim().split("\\s+")[0];
                    Container.ExecResult dump =
                            container.execInContainer("timeout", "10s", "jstack", pid);
                    log.error(
                            "Event-report startup thread dump (exit {}):\n{}\n{}",
                            dump.getExitCode(),
                            dump.getStdout(),
                            dump.getStderr());
                    return;
                }
            }
            log.warn("No SeaTunnelServer JVM found during startup failure");
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            log.warn("Interrupted while capturing event-report startup threads", interrupted);
        } catch (Exception diagnosticFailure) {
            log.warn("Could not capture event-report startup threads", diagnosticFailure);
        }
    }

    @Test
    public void testEventReport() throws IOException, InterruptedException {
        Container.ExecResult execResult = executeSeaTunnelJob("/fakesource_to_console.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        Map<String, Integer> expectedEvents = new HashMap<>();
        expectedEvents.put(EventType.LIFECYCLE_READER_OPEN.name(), 2);
        expectedEvents.put(EventType.LIFECYCLE_ENUMERATOR_OPEN.name(), 1);
        expectedEvents.put(EventType.LIFECYCLE_ENUMERATOR_CLOSE.name(), 1);
        expectedEvents.put(EventType.LIFECYCLE_READER_CLOSE.name(), 2);
        expectedEvents.put(EventType.LIFECYCLE_WRITER_CLOSE.name(), 2);
        Map<String, Integer> eventCounts = new HashMap<>();
        AtomicLong lastRequestTime = new AtomicLong(System.nanoTime());
        // Require a full quiet report interval after the last request, counting duplicates too.
        given().await()
                .atMost(60, TimeUnit.SECONDS)
                .until(
                        () -> {
                            if (collectEventReports(eventCounts)) {
                                lastRequestTime.set(System.nanoTime());
                            }
                            return System.nanoTime() - lastRequestTime.get()
                                            >= JobEventHttpReportHandler.REPORT_INTERVAL.toNanos()
                                    && expectedEvents.entrySet().stream()
                                            .allMatch(
                                                    expected ->
                                                            eventCounts.getOrDefault(
                                                                            expected.getKey(), 0)
                                                                    >= expected.getValue());
                        });
        collectEventReports(eventCounts);
        // Count every received event, including duplicates, before checking exact totals.
        expectedEvents.forEach(
                (eventType, count) ->
                        Assertions.assertEquals(count, eventCounts.get(eventType), eventType));
    }

    private boolean collectEventReports(Map<String, Integer> eventCounts)
            throws IOException, InterruptedException {
        boolean received = false;
        RecordedRequest request;
        while ((request = mockWebServer.takeRequest(0, TimeUnit.SECONDS)) != null) {
            received = true;
            try (Buffer buffer = request.getBody()) {
                ArrayNode events =
                        (ArrayNode)
                                JobEventHttpReportHandler.JSON_MAPPER.readTree(buffer.readUtf8());
                for (JsonNode event : events) {
                    eventCounts.merge(event.get("eventType").asText(), 1, Integer::sum);
                }
            }
        }
        return received;
    }
}
