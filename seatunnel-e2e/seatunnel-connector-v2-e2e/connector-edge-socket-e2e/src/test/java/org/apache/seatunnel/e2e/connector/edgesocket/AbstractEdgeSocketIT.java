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

package org.apache.seatunnel.e2e.connector.edgesocket;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;

import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.engine.client.SeaTunnelClient;
import org.apache.seatunnel.engine.common.config.ConfigProvider;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import com.hazelcast.client.config.ClientConfig;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@Slf4j
public abstract class AbstractEdgeSocketIT extends TestSuiteBase implements TestResource {

    protected static final String EDGE_INGRESS_HOST = "server";
    protected static final int EDGE_INGRESS_PORT = 10091;
    protected static final int EDGE_FORWARDER_PORT = 19091;
    protected static final String AUTH_TOKEN = "edge-e2e-token";
    protected static final String TRANSFORM_SUFFIX = "_transformed";

    protected GenericContainer<?> edgeSocketForwarderContainer;
    private String edgeSocketForwarderTargetHost;

    protected void startSinkDependencies() throws Exception {}

    protected void stopSinkDependencies() throws Exception {}

    protected abstract List<String> querySinkValues() throws Exception;

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        startSinkDependencies();
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        if (edgeSocketForwarderContainer != null) {
            edgeSocketForwarderContainer.close();
        }
        stopSinkDependencies();
    }

    protected GenericContainer<?> startEdgeSocketForwarderContainer() {
        return startEdgeSocketForwarderContainer(EDGE_INGRESS_HOST);
    }

    protected GenericContainer<?> startEdgeSocketForwarderContainer(String edgeIngressHost) {
        GenericContainer<?> container =
                new GenericContainer<>(DockerImageName.parse("alpine/socat:1.8.0.3"))
                        .withNetwork(NETWORK)
                        .withExposedPorts(EDGE_FORWARDER_PORT)
                        .withCommand(
                                String.format("TCP-LISTEN:%d,fork,reuseaddr", EDGE_FORWARDER_PORT),
                                String.format("TCP:%s:%d", edgeIngressHost, EDGE_INGRESS_PORT));
        container.start();
        log.info("Edge socket forwarder container started, target host: {}", edgeIngressHost);
        return container;
    }

    protected synchronized void restartEdgeSocketForwarderContainer(String edgeIngressHost) {
        if (edgeSocketForwarderContainer != null) {
            edgeSocketForwarderContainer.close();
            edgeSocketForwarderContainer = null;
        }
        edgeSocketForwarderContainer = startEdgeSocketForwarderContainer(edgeIngressHost);
        edgeSocketForwarderTargetHost = edgeIngressHost;
    }

    protected synchronized void ensureEdgeSocketForwarderContainer(String edgeIngressHost) {
        if (edgeSocketForwarderContainer == null
                || !edgeIngressHost.equals(edgeSocketForwarderTargetHost)) {
            restartEdgeSocketForwarderContainer(edgeIngressHost);
        }
    }

    protected void ensureEdgeSocketForwarderByJobId(String jobId) throws Exception {
        String ingressHost = resolveEdgeIngressHostByJobClient(jobId);
        ensureEdgeSocketForwarderContainer(ingressHost);
    }

    protected void sendRecordsThroughCollector(List<String> messages) throws Exception {
        if (messages == null || messages.isEmpty()) {
            throw new IllegalArgumentException("Messages should not be empty");
        }
        if (edgeSocketForwarderContainer == null) {
            throw new IllegalStateException("Edge socket forwarder container is not initialized");
        }
        String forwarderHost = edgeSocketForwarderContainer.getHost();
        int forwarderPort = edgeSocketForwarderContainer.getMappedPort(EDGE_FORWARDER_PORT);
        long deadlineMillis = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(60);
        while (System.currentTimeMillis() < deadlineMillis) {
            try (Socket socket = new Socket(forwarderHost, forwarderPort);
                    BufferedWriter writer =
                            new BufferedWriter(
                                    new OutputStreamWriter(
                                            socket.getOutputStream(), StandardCharsets.UTF_8));
                    BufferedReader reader =
                            new BufferedReader(
                                    new InputStreamReader(
                                            socket.getInputStream(), StandardCharsets.UTF_8))) {
                socket.setSoTimeout(3000);
                writeLine(writer, "__AUTH__:" + AUTH_TOKEN);
                String authReply = readLine(reader);
                Assertions.assertEquals("ACK", authReply, "Auth response should be ACK");

                for (String message : messages) {
                    sendMessageWithRetry(writer, reader, message);
                }
                return;
            } catch (SocketTimeoutException timeoutException) {
                if (System.currentTimeMillis() >= deadlineMillis) {
                    throw timeoutException;
                }
                TimeUnit.MILLISECONDS.sleep(200);
            } catch (IOException ioException) {
                if (System.currentTimeMillis() >= deadlineMillis) {
                    throw ioException;
                }
                TimeUnit.MILLISECONDS.sleep(500);
            }
        }
        throw new IllegalStateException("Send records to edge socket timed out");
    }

    protected void awaitSinkContainsExpectedMessages(List<String> expectedMessages) {
        if (expectedMessages == null || expectedMessages.isEmpty()) {
            throw new IllegalArgumentException("Expected messages should not be empty");
        }
        Awaitility.await()
                .atMost(90, TimeUnit.SECONDS)
                .pollInterval(1, TimeUnit.SECONDS)
                .untilAsserted(
                        () -> {
                            List<String> rows = querySinkValues();
                            for (String message : expectedMessages) {
                                Assertions.assertTrue(
                                        rows.contains(message),
                                        "Missing expected message in sink table: " + message);
                            }
                        });
    }

    protected List<String> buildExpectedTransformedMessages(List<String> sourceMessages) {
        List<String> expected = new ArrayList<>();
        for (String message : sourceMessages) {
            expected.add(message + TRANSFORM_SUFFIX);
        }
        return expected;
    }

    // ---------- Data generation helpers ----------
    // Count is always caller-controlled for flexible e2e workloads.
    protected List<String> buildPlainTextMessages(int count, String prefix) {
        List<String> messages = new ArrayList<>();
        for (int i = 1; i <= count; i++) {
            messages.add(prefix + "-" + i);
        }
        return messages;
    }

    protected List<String> buildFlatJsonMessages(int count) {
        List<String> messages = new ArrayList<>();
        for (int i = 1; i <= count; i++) {
            messages.add(
                    "{"
                            + "\"id\":"
                            + i
                            + ",\"name\":\"user-"
                            + i
                            + "\",\"active\":"
                            + (i % 2 == 0)
                            + ",\"score\":"
                            + (100 + i)
                            + "}");
        }
        return messages;
    }

    protected List<String> buildNestedJsonMessages(int count) {
        List<String> messages = new ArrayList<>();
        for (int i = 1; i <= count; i++) {
            messages.add(
                    "{"
                            + "\"event_id\":"
                            + i
                            + ",\"meta\":{\"source\":\"edge\",\"batch\":"
                            + ((i - 1) / 10 + 1)
                            + "},\"payload\":{\"name\":\"user-"
                            + i
                            + "\",\"age\":"
                            + (20 + i)
                            + "}}");
        }
        return messages;
    }

    /**
     * Generate JSON payload rows from a schema declaration.
     *
     * <p>Supported field types: string, int, long, double, boolean.
     */
    protected List<String> buildSchemaPayloadJsonMessages(
            int count, LinkedHashMap<String, String> schemaDefinition) {
        List<String> messages = new ArrayList<>();
        for (int i = 1; i <= count; i++) {
            StringBuilder builder = new StringBuilder();
            builder.append("{");
            int fieldIndex = 0;
            for (Map.Entry<String, String> field : schemaDefinition.entrySet()) {
                if (fieldIndex++ > 0) {
                    builder.append(",");
                }
                builder.append("\"").append(escapeJson(field.getKey())).append("\":");
                builder.append(buildTypedJsonValue(field.getValue(), i));
            }
            builder.append("}");
            messages.add(builder.toString());
        }
        return messages;
    }

    protected List<String> buildSchemaEnvelopeJsonMessages(
            int count, LinkedHashMap<String, String> schemaDefinition) {
        List<String> payloads = buildSchemaPayloadJsonMessages(count, schemaDefinition);
        List<String> envelopes = new ArrayList<>();
        StringBuilder schemaBuilder = new StringBuilder();
        schemaBuilder.append("{");
        int fieldIndex = 0;
        for (Map.Entry<String, String> field : schemaDefinition.entrySet()) {
            if (fieldIndex++ > 0) {
                schemaBuilder.append(",");
            }
            schemaBuilder
                    .append("\"")
                    .append(escapeJson(field.getKey()))
                    .append("\":\"")
                    .append(escapeJson(field.getValue()))
                    .append("\"");
        }
        schemaBuilder.append("}");
        String schemaPart = schemaBuilder.toString();
        for (String payload : payloads) {
            envelopes.add("{\"schema\":" + schemaPart + ",\"payload\":" + payload + "}");
        }
        return envelopes;
    }

    private String buildTypedJsonValue(String fieldType, int index) {
        String normalizedType = fieldType == null ? "" : fieldType.trim().toLowerCase();
        if ("string".equals(normalizedType)) {
            return "\"value-" + index + "\"";
        }
        if ("int".equals(normalizedType)) {
            return String.valueOf(index);
        }
        if ("long".equals(normalizedType)) {
            return String.valueOf(index * 1000L);
        }
        if ("double".equals(normalizedType)) {
            return String.valueOf(index + 0.5D);
        }
        if ("boolean".equals(normalizedType)) {
            return String.valueOf(index % 2 == 0);
        }
        throw new IllegalArgumentException("Unsupported schema type: " + fieldType);
    }

    private String escapeJson(String value) {
        return value.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    private String resolveEdgeIngressHostByJobClient(String jobId) {
        try {
            long id = Long.parseLong(jobId);
            String addressJson;
            ClientConfig clientConfig = ConfigProvider.locateAndGetClientConfig();
            try (SeaTunnelClient seaTunnelClient = new SeaTunnelClient(clientConfig)) {
                addressJson = seaTunnelClient.getJobTaskGroupAddresses(id);
            }
            ArrayNode addresses = JsonUtils.parseArray(addressJson);
            if (addresses == null || addresses.isEmpty()) {
                throw new IllegalStateException("Task group addresses are empty");
            }

            for (JsonNode address : addresses) {
                String host = address.path("host").asText();
                if (host == null || host.isEmpty()) {
                    continue;
                }
                if ("localhost".equalsIgnoreCase(host) || "127.0.0.1".equals(host)) {
                    return EDGE_INGRESS_HOST;
                }
                return host;
            }
            throw new IllegalStateException("No valid host found in task group addresses");
        } catch (Exception e) {
            log.warn(
                    "Resolve ingress host by getJobTaskGroupAddresses failed, fallback to '{}'. jobId={}",
                    EDGE_INGRESS_HOST,
                    jobId,
                    e);
            return EDGE_INGRESS_HOST;
        }
    }

    private void sendMessageWithRetry(BufferedWriter writer, BufferedReader reader, String message)
            throws Exception {
        while (true) {
            writeLine(writer, message);
            String reply = readLine(reader);
            if ("ACK".equals(reply)) {
                return;
            }
            if ("RETRY".equals(reply)) {
                TimeUnit.MILLISECONDS.sleep(100);
                continue;
            }
            throw new IllegalStateException("Unexpected collector response: " + reply);
        }
    }

    private void writeLine(BufferedWriter writer, String value) throws IOException {
        writer.write(value);
        writer.newLine();
        writer.flush();
    }

    private String readLine(BufferedReader reader) throws IOException {
        String line = reader.readLine();
        if (line == null) {
            throw new IOException("Read EOF from edge socket source");
        }
        return line.trim();
    }
}
