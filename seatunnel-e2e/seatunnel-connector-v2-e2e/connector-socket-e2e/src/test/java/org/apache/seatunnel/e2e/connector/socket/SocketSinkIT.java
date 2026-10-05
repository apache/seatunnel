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
package org.apache.seatunnel.e2e.connector.socket;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.MappingIterator;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.DockerLoggerFactory;

import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayInputStream;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Stream;

@Slf4j
public class SocketSinkIT extends TestSuiteBase implements TestResource {

    private static final DockerImageName SOCKET_SERVER_IMAGE =
            DockerImageName.parse("python:3.11.9-alpine3.20");
    private static final String SOCKET_SERVER_HOST = "socket-server";
    private static final int SOCKET_SERVER_PORT = 9999;
    private static final String SOCKET_OUTPUT_FILE = "/tmp/socket-output.txt";
    private static final int EXPECTED_RECORD_COUNT = 10;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private GenericContainer<?> socketServer;

    @Override
    @BeforeAll
    public void startUp() {
        socketServer =
                new GenericContainer<>(SOCKET_SERVER_IMAGE)
                        .withNetwork(NETWORK)
                        .withNetworkAliases(SOCKET_SERVER_HOST)
                        .withExposedPorts(SOCKET_SERVER_PORT)
                        .withCommand(
                                "python",
                                "-u",
                                "-c",
                                String.join(
                                        "\n",
                                        "import socket",
                                        "from pathlib import Path",
                                        "HOST='0.0.0.0'",
                                        "PORT=9999",
                                        "OUTPUT=Path('/tmp/socket-output.txt')",
                                        "OUTPUT.touch()",
                                        "server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)",
                                        "server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)",
                                        "server.bind((HOST, PORT))",
                                        "server.listen()",
                                        "print('socket-server-ready', flush=True)",
                                        "while True:",
                                        "    conn, _ = server.accept()",
                                        "    with conn, OUTPUT.open('ab') as output:",
                                        "        while True:",
                                        "            data = conn.recv(4096)",
                                        "            if not data:",
                                        "                break",
                                        "            output.write(data)",
                                        "            output.flush()"))
                        .withLogConsumer(
                                new Slf4jLogConsumer(
                                        DockerLoggerFactory.getLogger(
                                                SOCKET_SERVER_IMAGE.asCanonicalNameString())));
        Startables.deepStart(Stream.of(socketServer)).join();
    }

    @Override
    public void tearDown() {
        if (socketServer != null) {
            socketServer.close();
        }
    }

    @TestTemplate
    public void testFakeSourceWritesJsonLinesToSocket(TestContainer container) throws Exception {
        truncateSocketOutput();

        Container.ExecResult execResult = container.executeJob("/fake_to_socket.conf");
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());

        List<JsonNode> records = awaitSocketOutputRecords();
        Assertions.assertEquals(EXPECTED_RECORD_COUNT, records.size(), "Unexpected socket output");

        JsonNode firstRecord = records.get(0);
        Assertions.assertTrue(firstRecord.has("id"));
        Assertions.assertTrue(firstRecord.has("name"));
        Assertions.assertTrue(firstRecord.has("active"));
    }

    private void truncateSocketOutput() throws Exception {
        Container.ExecResult execResult =
                socketServer.execInContainer("sh", "-c", ": > " + SOCKET_OUTPUT_FILE);
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
    }

    private List<JsonNode> awaitSocketOutputRecords() {
        return Awaitility.await()
                .atMost(Duration.ofSeconds(60))
                .pollInterval(Duration.ofSeconds(1))
                .until(
                        this::readSocketOutputRecords,
                        records -> records.size() == EXPECTED_RECORD_COUNT);
    }

    private List<JsonNode> readSocketOutputRecords() {
        try {
            Container.ExecResult execResult =
                    socketServer.execInContainer("cat", SOCKET_OUTPUT_FILE);
            Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
            String stdout = execResult.getStdout().trim();
            if (stdout.isEmpty()) {
                return Collections.emptyList();
            }

            MappingIterator<JsonNode> iterator =
                    objectMapper
                            .readerFor(JsonNode.class)
                            .readValues(
                                    new ByteArrayInputStream(
                                            stdout.getBytes(StandardCharsets.UTF_8)));
            List<JsonNode> records = new ArrayList<>();
            while (iterator.hasNextValue()) {
                records.add(iterator.nextValue());
            }
            return records;
        } catch (Exception e) {
            throw new IllegalStateException("Failed to read socket output", e);
        }
    }
}
