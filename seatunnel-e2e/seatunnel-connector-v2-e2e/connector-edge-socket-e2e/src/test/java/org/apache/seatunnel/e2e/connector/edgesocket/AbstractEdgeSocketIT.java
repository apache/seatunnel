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

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.DockerImageName;

import lombok.extern.slf4j.Slf4j;

import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Slf4j
public abstract class AbstractEdgeSocketIT extends TestSuiteBase implements TestResource {

    protected static final String EDGE_INGRESS_HOST = "server";
    protected static final int EDGE_INGRESS_PORT = 10091;
    protected static final int EDGE_FORWARDER_PORT = 19091;
    protected static final String AUTH_TOKEN = "edge-e2e-token";
    protected static final String E2E_SECRET_KEY = "dGVzdC1zZWNyZXQta2V5LTMyLWJ5dGVzLWFlczI1NiE=";
    protected static final String TRANSFORM_SUFFIX = "_transformed";
    protected static final String EDGE_BATCH_PREFIX = "__BATCH__:";
    protected static final String EDGE_COMMIT_PREFIX = "__COMMIT__:";

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

    protected synchronized void ensureEdgeSocketForwarder() {
        if (edgeSocketForwarderContainer == null
                || !EDGE_INGRESS_HOST.equals(edgeSocketForwarderTargetHost)) {
            restartEdgeSocketForwarderContainer(EDGE_INGRESS_HOST);
        }
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

                long batchId = 1L;
                for (String message : messages) {
                    sendMessageWithRetry(writer, reader, batchId, message);
                    awaitBatchCheckpointAck(writer, reader, batchId);
                    batchId++;
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

    protected Container.ExecResult waitForJobResult(
            CompletableFuture<Container.ExecResult> jobFuture) throws Exception {
        try {
            return jobFuture.get(180, TimeUnit.SECONDS);
        } catch (ExecutionException e) {
            Throwable cause = e.getCause();
            if (cause instanceof Exception) {
                throw (Exception) cause;
            }
            if (cause instanceof Error) {
                throw (Error) cause;
            }
            throw new AssertionError("Job command failed", cause);
        } catch (TimeoutException e) {
            throw new AssertionError("Timed out waiting for job command to finish", e);
        }
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

    private void sendMessageWithRetry(
            BufferedWriter writer, BufferedReader reader, long batchId, String message)
            throws Exception {
        while (true) {
            writeLine(writer, EDGE_BATCH_PREFIX + batchId + ":" + message);
            String reply = readLine(reader);
            if ("RECEIVED".equals(reply)) {
                return;
            }
            if ("RETRY".equals(reply)) {
                TimeUnit.MILLISECONDS.sleep(100);
                continue;
            }
            throw new IllegalStateException("Unexpected collector response: " + reply);
        }
    }

    private void awaitBatchCheckpointAck(
            BufferedWriter writer, BufferedReader reader, long expectedBatchId) throws Exception {
        long deadlineMillis = System.currentTimeMillis() + TimeUnit.SECONDS.toMillis(30);
        while (System.currentTimeMillis() < deadlineMillis) {
            writeLine(writer, EDGE_COMMIT_PREFIX + expectedBatchId);
            String reply = readLine(reader);
            if ("PENDING".equals(reply)) {
                TimeUnit.MILLISECONDS.sleep(200);
                continue;
            }
            if (reply.startsWith("ACK:")) {
                long ackedBatchId = Long.parseLong(reply.substring("ACK:".length()));
                if (ackedBatchId >= expectedBatchId) {
                    return;
                }
                TimeUnit.MILLISECONDS.sleep(200);
                continue;
            }
            if ("RETRY".equals(reply)) {
                TimeUnit.MILLISECONDS.sleep(200);
                continue;
            }
            throw new IllegalStateException("Unexpected commit response: " + reply);
        }
        throw new IllegalStateException(
                "Timeout waiting checkpoint ACK for batch: " + expectedBatchId);
    }

    /**
     * Send messages using PACKET mode framing (base64 + NONE compression, no encryption). Each
     * plain-text message is base64-encoded and wrapped in a JSON envelope before sending.
     */
    protected void sendPacketRecords(List<String> messages) throws Exception {
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

                long batchId = 1L;
                for (String message : messages) {
                    String packetJson = buildPacketJson(message);
                    sendMessageWithRetry(writer, reader, batchId, packetJson);
                    awaitBatchCheckpointAck(writer, reader, batchId);
                    batchId++;
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
        throw new IllegalStateException("Send packet records to edge socket timed out");
    }

    private String buildPacketJson(String plainText) {
        String base64Payload =
                Base64.getEncoder().encodeToString(plainText.getBytes(StandardCharsets.UTF_8));
        return "{\"version\":1,\"payload\":\""
                + base64Payload
                + "\",\"compression\":\"NONE\",\"encryption\":\"NONE\"}";
    }

    /**
     * Send messages using PACKET mode with AES-GCM encryption. Each plain-text message is
     * encrypted, base64-encoded, and wrapped in a JSON envelope before sending.
     */
    protected void sendEncryptedPacketRecords(List<String> messages, String secretKeyBase64)
            throws Exception {
        if (messages == null || messages.isEmpty()) {
            throw new IllegalArgumentException("Messages should not be empty");
        }
        if (edgeSocketForwarderContainer == null) {
            throw new IllegalStateException("Edge socket forwarder container is not initialized");
        }
        byte[] keyBytes = Base64.getDecoder().decode(secretKeyBase64);
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

                long batchId = 1L;
                for (String message : messages) {
                    String packetJson = buildEncryptedPacketJson(message, keyBytes);
                    sendMessageWithRetry(writer, reader, batchId, packetJson);
                    awaitBatchCheckpointAck(writer, reader, batchId);
                    batchId++;
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
        throw new IllegalStateException("Send encrypted packet records to edge socket timed out");
    }

    private String buildEncryptedPacketJson(String plainText, byte[] keyBytes) throws Exception {
        byte[] payload = plainText.getBytes(StandardCharsets.UTF_8);
        byte[] iv = new byte[12];
        new SecureRandom().nextBytes(iv);
        Cipher cipher = Cipher.getInstance("AES/GCM/NoPadding");
        cipher.init(
                Cipher.ENCRYPT_MODE,
                new SecretKeySpec(keyBytes, "AES"),
                new GCMParameterSpec(128, iv));
        byte[] ciphertext = cipher.doFinal(payload);
        return "{\"version\":1,\"payload\":\""
                + Base64.getEncoder().encodeToString(ciphertext)
                + "\",\"compression\":\"NONE\",\"encryption\":\"AES_GCM\",\"iv\":\""
                + Base64.getEncoder().encodeToString(iv)
                + "\"}";
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
