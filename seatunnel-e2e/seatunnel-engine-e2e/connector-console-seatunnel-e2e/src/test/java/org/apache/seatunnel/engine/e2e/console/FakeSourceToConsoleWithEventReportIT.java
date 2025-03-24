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
import org.testcontainers.utility.MountableFile;

import com.squareup.okhttp.mockwebserver.MockResponse;
import com.squareup.okhttp.mockwebserver.MockWebServer;
import com.squareup.okhttp.mockwebserver.RecordedRequest;
import lombok.extern.slf4j.Slf4j;
import okio.Buffer;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static org.apache.seatunnel.e2e.common.util.ContainerUtil.PROJECT_ROOT_PATH;
import static org.awaitility.Awaitility.given;

@Slf4j
public class FakeSourceToConsoleWithEventReportIT extends SeaTunnelEngineContainer {
    private static final int MOCK_SERVER_PORT = 1024;

    private MockWebServer mockWebServer;

    @Override
    @BeforeAll
    public void startUp() throws Exception {
        mockWebServer = new MockWebServer();
        mockWebServer.start(MOCK_SERVER_PORT);
        mockWebServer.enqueue(new MockResponse().setResponseCode(200));
        Testcontainers.exposeHostPorts(MOCK_SERVER_PORT);

        super.startUp();
        log.info("The TestContainer[{}] is running.", identifier());
    }

    @Override
    @AfterAll
    public void tearDown() throws Exception {
        super.tearDown();

        mockWebServer.shutdown();
        log.info("The TestContainer[{}] is closed.", identifier());
    }

    @Override
    protected void executeExtraCommands(GenericContainer<?> container)
            throws IOException, InterruptedException {
        container.withCopyFileToContainer(
                MountableFile.forHostPath(
                        PROJECT_ROOT_PATH
                                + "/seatunnel-e2e/seatunnel-engine-e2e/connector-console-seatunnel-e2e/src/test/resources/seatunnel_config_with_event_report.yaml"),
                Paths.get(SEATUNNEL_HOME, "config", "seatunnel.yaml").toString());
    }

    @Test
    public void testEventReport() throws IOException, InterruptedException {
        Container.ExecResult execResult = executeSeaTunnelJob("/fakesource_to_console.conf");
        Assertions.assertEquals(0, execResult.getExitCode());

        Thread.sleep(JobEventHttpReportHandler.REPORT_INTERVAL.toMillis());
        given().ignoreExceptions()
                .await()
                .atMost(60, TimeUnit.SECONDS)
                .until(() -> mockWebServer.getRequestCount(), count -> count > 0);

        List<JsonNode> events = new ArrayList<>();
        for (int i = 0; i < mockWebServer.getRequestCount(); i++) {
            RecordedRequest request = mockWebServer.takeRequest();
            try (Buffer buffer = request.getBody()) {
                String body = buffer.readUtf8();
                ArrayNode arrayNode =
                        (ArrayNode) JobEventHttpReportHandler.JSON_MAPPER.readTree(body);
                arrayNode.elements().forEachRemaining(jsonNode -> events.add(jsonNode));
            }
        }
        Map<String, Integer> eventMap =
                events.stream()
                        .map(e -> e.get("eventType").asText())
                        .collect(Collectors.groupingBy(e -> e, Collectors.summingInt(e -> 1)));
        Assertions.assertTrue(
                eventMap.keySet()
                        .containsAll(
                                Arrays.asList(
                                        EventType.LIFECYCLE_ENUMERATOR_OPEN.name(),
                                        EventType.LIFECYCLE_ENUMERATOR_CLOSE.name(),
                                        EventType.LIFECYCLE_READER_OPEN.name(),
                                        EventType.LIFECYCLE_READER_CLOSE.name(),
                                        EventType.LIFECYCLE_WRITER_CLOSE.name())));
        Assertions.assertEquals(2, eventMap.get(EventType.LIFECYCLE_READER_OPEN.name()));
        Assertions.assertEquals(1, eventMap.get(EventType.LIFECYCLE_ENUMERATOR_OPEN.name()));
        Assertions.assertEquals(1, eventMap.get(EventType.LIFECYCLE_ENUMERATOR_CLOSE.name()));
        Assertions.assertEquals(2, eventMap.get(EventType.LIFECYCLE_READER_CLOSE.name()));
        Assertions.assertEquals(2, eventMap.get(EventType.LIFECYCLE_WRITER_CLOSE.name()));
    }
}
