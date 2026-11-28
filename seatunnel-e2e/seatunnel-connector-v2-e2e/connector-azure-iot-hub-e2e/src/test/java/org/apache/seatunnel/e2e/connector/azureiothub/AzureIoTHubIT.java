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

package org.apache.seatunnel.e2e.connector.azureiothub;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.seatunnel.SeaTunnelContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.MountableFile;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

import static org.apache.seatunnel.e2e.connector.azureiothub.AzureIoTHubFixture.HOST;
import static org.apache.seatunnel.e2e.connector.azureiothub.AzureIoTHubFixture.PUBLIC_TEST_KEY;
import static org.apache.seatunnel.e2e.connector.azureiothub.AzureIoTHubFixture.STORE_PASSWORD;
import static org.apache.seatunnel.e2e.connector.azureiothub.AzureIoTHubFixture.reply;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Runs the installed sink in Zeta against independently authenticated HTTPS peers. Exact receipts
 * and bounded fault responses cover payload fidelity, retries, shutdown and checkpoint recovery.
 */
@DisabledOnContainer(
        value = {},
        type = {EngineType.SPARK, EngineType.FLINK})
@Execution(ExecutionMode.SAME_THREAD)
@Timeout(value = 8, unit = TimeUnit.MINUTES)
class AzureIoTHubIT extends TestSuiteBase {
    private static final String JSON = "/fake_to_azure_iot_hub_json.conf";
    private static final String VALUE = "/fake_to_azure_iot_hub_value.conf";
    private static final String CHECKPOINT = "/fake_to_azure_iot_hub_checkpoint.conf";
    private static final String PROPERTIES = "/fake_to_azure_iot_hub_properties.conf";
    private static final String INVALID_DEVICE = "/fake_to_azure_iot_hub_invalid_device.conf";
    private static final String RESPONSE_PRIVATE_TEXT = "fixture-do-not-log-response-body";
    private static final String CONTAINER_TRUST_STORE = "/tmp/azure-iot-hub-test-trust.jks";
    private static final byte[] TEXT = "雪 🦊".getBytes(StandardCharsets.UTF_8);
    private static final ObjectMapper JSON_MAPPER =
            new ObjectMapper().enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);

    @TempDir private Path temp;
    private AzureIoTHubFixture hub;
    private AzureIoTHubFixture untrustedHub;
    private SeaTunnelContainer engine;
    private final ExecutorService jobs = Executors.newSingleThreadExecutor();

    @BeforeAll
    void startZetaAndIoTHub() throws Exception {
        Path trustedStore = temp.resolve("trusted.jks");
        hub = new AzureIoTHubFixture(createCertificate(trustedStore));
        untrustedHub = new AzureIoTHubFixture(createCertificate(temp.resolve("untrusted.jks")));
        engine =
                new SeaTunnelContainer() {
                    @Override
                    protected String getJavaToolOptions() {
                        return "-Djavax.net.ssl.trustStore="
                                + CONTAINER_TRUST_STORE
                                + " -Djavax.net.ssl.trustStorePassword="
                                + STORE_PASSWORD
                                + " -Djavax.net.ssl.trustStoreType=JKS";
                    }

                    @Override
                    protected void executeExtraCommands(GenericContainer<?> container) {
                        container.withCopyFileToContainer(
                                MountableFile.forHostPath(trustedStore), CONTAINER_TRUST_STORE);
                    }
                };
        engine.startUp();
    }

    @AfterAll
    void stopZetaAndIoTHub() throws Exception {
        jobs.shutdownNow();
        try (AzureIoTHubFixture trusted = hub;
                AzureIoTHubFixture untrusted = untrustedHub) {
            if (engine != null) {
                engine.tearDown();
            }
        }
    }

    @Test
    void sendsExactJsonWithAuthenticatedDeviceScopeAndRoutingMetadata() throws Exception {
        AzureIoTHubFixture.Scenario scenario = accepting(hub);
        runSuccessfully(JSON, args(hub, 8000, 3, 262144));
        List<byte[]> accepted = scenario.receipts();
        assertEquals(2, accepted.size());
        assertEquals(
                JSON_MAPPER.readTree(
                        "{\"id\":9007199254740993,\"label\":\"雪 🦊\","
                                + "\"amount\":12345678901234567890.12345678,\"note\":null,"
                                + "\"details\":{\"samples\":[1,2],\"description\":\"nested\"}}"),
                JSON_MAPPER.readTree(accepted.get(0)));
        assertEquals(
                JSON_MAPPER.readTree(
                        "{\"id\":9007199254740994,\"label\":\"quoted \\\"data\\\"\","
                                + "\"amount\":10.125,\"note\":\"\","
                                + "\"details\":{\"samples\":[],\"description\":null}}"),
                JSON_MAPPER.readTree(accepted.get(1)));
        assertEquals(2, scenario.attempts().size());
        for (AzureIoTHubFixture.Attempt attempt : scenario.attempts()) {
            assertEquals("application/json", attempt.type);
            assertEquals("utf-8", attempt.encoding);
            assertEquals("application/json;charset=utf-8", attempt.contentType);
        }
    }

    @Test
    void sendsExactUtf8BinaryAndEmptyPayloads() throws Exception {
        AzureIoTHubFixture.Scenario scenario = accepting(hub);
        runSuccessfully(VALUE, valueArgs(hub, 8000, 3, 8, "TEXT", "label"));
        runSuccessfully(VALUE, valueArgs(hub, 8000, 3, 3, "BINARY", "data"));
        runSuccessfully(VALUE, valueArgs(hub, 8000, 3, 1, "TEXT", "empty"));
        List<byte[]> accepted = scenario.receipts();
        assertEquals(3, accepted.size());
        assertArrayEquals(TEXT, accepted.get(0));
        assertArrayEquals(new byte[] {0, (byte) 255, (byte) 128}, accepted.get(1));
        assertArrayEquals(new byte[0], accepted.get(2));
        List<AzureIoTHubFixture.Attempt> attempts = scenario.attempts();
        assertEquals(3, attempts.size());
        assertEquals("text/plain", attempts.get(0).type);
        assertEquals("utf-8", attempts.get(0).encoding);
        assertEquals("text/plain;charset=utf-8", attempts.get(0).contentType);
        assertEquals("application/octet-stream", attempts.get(1).type);
        assertNull(attempts.get(1).encoding);
        assertEquals("application/octet-stream", attempts.get(1).contentType);
    }

    @Test
    void rejectsOversizeNullAndInvalidDeviceOrFieldBeforeNetwork() throws Exception {
        AzureIoTHubFixture.Scenario scenario = accepting(hub);
        expectFailure(VALUE, valueArgs(hub, 8000, 3, 7, "TEXT", "label"), "payload exceeds");
        expectFailure(
                VALUE, valueArgs(hub, 8000, 3, 8, "TEXT", "missing"), "selected field is null");
        expectFailure(VALUE, valueArgs(hub, 8000, 3, 8, "BINARY", "label"), "field must select");
        expectFailure(INVALID_DEVICE, args(hub, 8000, 3, 8), "device_id contains unsupported");
        assertEquals(1, scenario.started.getCount());
        assertTrue(scenario.attempts().isEmpty());
        assertTrue(scenario.receipts().isEmpty());
    }

    @Test
    void retriesExplicitThrottlesWithFreshAuthenticationAndSingleAcceptance() throws Exception {
        AzureIoTHubFixture.Scenario scenario =
                scenario(
                        hub,
                        (state, index, request, exchange) -> {
                            if (index == 0) {
                                exchange.getResponseHeaders().set("Retry-After", "1");
                                reply(exchange, 429);
                            } else if (index == 1) {
                                String date =
                                        DateTimeFormatter.RFC_1123_DATE_TIME.format(
                                                Instant.now()
                                                        .plusSeconds(3)
                                                        .atZone(ZoneOffset.UTC));
                                exchange.getResponseHeaders().set("Retry-After", date);
                                reply(exchange, 429);
                            } else {
                                state.persist(request);
                                reply(exchange, 204);
                            }
                        });
        runSuccessfully(VALUE, valueArgs(hub, 15000, 3, 8, "TEXT", "label"));
        List<AzureIoTHubFixture.Attempt> attempts = scenario.attempts();
        assertEquals(3, attempts.size());
        for (AzureIoTHubFixture.Attempt attempt : attempts) {
            assertArrayEquals(TEXT, attempt.body);
        }
        assertTrue(
                attempts.get(1).arrival - attempts.get(0).arrival >= TimeUnit.SECONDS.toNanos(1));
        assertTrue(
                attempts.get(2).arrival - attempts.get(1).arrival >= TimeUnit.SECONDS.toNanos(2));
        assertTrue(attempts.get(1).expiry > attempts.get(0).expiry);
        assertTrue(attempts.get(2).expiry > attempts.get(1).expiry);
        assertEquals(1, scenario.receipts().size());
        assertArrayEquals(TEXT, scenario.receipts().get(0));
    }

    @Test
    void stopsAtThrottleAttemptLimitOrWhenDelayCannotFitRow() throws Exception {
        AzureIoTHubFixture.Scenario exhausted =
                scenario(
                        hub,
                        (state, index, request, exchange) -> {
                            exchange.getResponseHeaders().set("Retry-After", "0");
                            reply(exchange, 429);
                        });
        expectFailure(
                VALUE, valueArgs(hub, 8000, 2, 8, "TEXT", "label"), "attempt limit exhausted");
        assertEquals(2, exhausted.attempts().size());
        assertTrue(exhausted.receipts().isEmpty());

        AzureIoTHubFixture.Scenario late =
                scenario(
                        hub,
                        (state, index, request, exchange) -> {
                            exchange.getResponseHeaders().set("Retry-After", "1800");
                            reply(exchange, 429);
                        });
        expectFailure(
                VALUE, valueArgs(hub, 8000, 3, 8, "TEXT", "label"), "remaining row I/O budget");
        assertEquals(1, late.attempts().size());
        assertTrue(late.receipts().isEmpty());
    }

    @Test
    void rejectsUnexpectedStatusesRedirectsAndUntrustedCertificates() throws Exception {
        AzureIoTHubFixture.Scenario other = accepting(untrustedHub);
        for (int status : new int[] {200, 307, 401, 403, 413, 503}) {
            AzureIoTHubFixture.Scenario rejected =
                    scenario(
                            hub,
                            (state, index, request, exchange) -> {
                                if (status == 307) {
                                    exchange.getResponseHeaders()
                                            .set("Location", untrustedHub.origin() + "/redirected");
                                }
                                byte[] details =
                                        RESPONSE_PRIVATE_TEXT.getBytes(StandardCharsets.US_ASCII);
                                exchange.sendResponseHeaders(status, details.length);
                                exchange.getResponseBody().write(details);
                            });
            expectFailure(VALUE, valueArgs(hub, 8000, 3, 8, "TEXT", "label"), "HTTP " + status);
            assertEquals(1, rejected.attempts().size());
            assertTrue(rejected.receipts().isEmpty());
        }
        expectFailure(
                VALUE,
                valueArgs(untrustedHub, 8000, 3, 8, "TEXT", "label"),
                "TLS transport failed");
        assertEquals(1, other.started.getCount());
        assertTrue(other.attempts().isEmpty());
        assertTrue(other.receipts().isEmpty());
    }

    @Test
    void rejectsConflictingResponseHeaders() throws Exception {
        AzureIoTHubFixture.Scenario contradictory =
                scenario(
                        hub,
                        (state, index, request, exchange) -> {
                            exchange.getResponseHeaders().set("Transfer-Encoding", "chunked");
                            reply(exchange, 204);
                        });
        List<String> variables = valueArgs(hub, 8000, 3, 8, "TEXT", "label");
        expectFailure(VALUE, variables, "HTTP 204 has contradictory body framing");
        assertEquals(1, contradictory.attempts().size());
        assertTrue(contradictory.receipts().isEmpty());

        AzureIoTHubFixture.Scenario conflicting =
                scenario(
                        hub,
                        (state, index, request, exchange) -> {
                            exchange.getResponseHeaders().add("Retry-After", "1");
                            exchange.getResponseHeaders().add("Retry-After", "2");
                            reply(exchange, 429);
                        });
        expectFailure(VALUE, variables, "HTTP 429 has ambiguous retry timing");
        assertEquals(1, conflicting.attempts().size());
        assertTrue(conflicting.receipts().isEmpty());
    }

    @Test
    void doesNotReplayUnacknowledgedDeliveryWithinJob() throws Exception {
        AzureIoTHubFixture.Scenario scenario =
                scenario(
                        hub,
                        (state, index, request, exchange) -> {
                            state.persist(request);
                            if (index > 0) {
                                reply(exchange, 204);
                            }
                        });
        List<String> variables = valueArgs(hub, 8000, 3, 8, "TEXT", "label");
        expectFailure(VALUE, variables, "without HTTP 204", "delivery outcome can be unknown");
        assertEquals(1, scenario.attempts().size());
        assertEquals(1, scenario.receipts().size());
        assertArrayEquals(TEXT, scenario.receipts().get(0));

        runSuccessfully(VALUE, variables);
        assertEquals(2, scenario.attempts().size());
        assertEquals(2, scenario.receipts().size());
        assertArrayEquals(scenario.receipts().get(0), scenario.receipts().get(1));
    }

    @Test
    void endsBlockedResponseWithinRowBudgetWithoutReplay() throws Exception {
        CountDownLatch releasePeer = new CountDownLatch(1);
        AzureIoTHubFixture.Scenario scenario =
                scenario(
                        hub,
                        (state, index, request, exchange) -> {
                            if (releasePeer.await(30, TimeUnit.SECONDS)) {
                                reply(exchange, 503);
                            }
                        });
        String id = jobId();
        int logOffset = engine.getServerLogs().length();
        Future<Container.ExecResult> result =
                jobs.submit(() -> run(VALUE, id, valueArgs(hub, 3500, 3, 8, "TEXT", "label")));
        try {
            assertTrue(scenario.started.await(60, TimeUnit.SECONDS), "Expected a real TLS POST");
            assertFailed(
                    id,
                    logOffset,
                    result.get(20, TimeUnit.SECONDS),
                    "row I/O deadline exceeded",
                    "without HTTP 204");
            assertEquals(1, scenario.attempts().size());
            assertTrue(scenario.receipts().isEmpty());
        } finally {
            releasePeer.countDown();
        }
    }

    @Test
    void stoppingJobWithAnUnansweredPostDoesNotResend() throws Exception {
        CountDownLatch releasePeer = new CountDownLatch(1);
        AzureIoTHubFixture.Scenario scenario =
                scenario(
                        hub,
                        (state, index, request, exchange) -> {
                            if (releasePeer.await(30, TimeUnit.SECONDS)) {
                                reply(exchange, 503);
                            }
                        });
        String id = jobId();
        Future<Container.ExecResult> result =
                jobs.submit(() -> run(VALUE, id, valueArgs(hub, 6500, 3, 8, "TEXT", "label")));
        try {
            Awaitility.await()
                    .atMost(60, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertEquals(1, scenario.attempts().size()));
            engine.stopJob(id);
            Awaitility.await()
                    .atMost(20, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertEquals("CANCELED", engine.getJobStatus(id)));
            result.get(30, TimeUnit.SECONDS);
            assertEquals(1, scenario.attempts().size());
            assertTrue(scenario.receipts().isEmpty());
        } finally {
            releasePeer.countDown();
        }
    }

    @Test
    void restoresRemainingSourceSplitAfterAcknowledgedCheckpoint() throws Exception {
        AzureIoTHubFixture.Scenario scenario = accepting(hub);
        List<String> variables = args(hub, 8000, 3, 1024);
        variables.add("row.count=2");
        variables.add("split.interval=45000");
        String stopped = jobId();
        Future<Container.ExecResult> original =
                jobs.submit(() -> run(CHECKPOINT, stopped, variables));
        try {
            Awaitility.await()
                    .atMost(60, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertEquals(1, scenario.receipts().size()));
            long checkpointAtFirstReceipt = engine.getCompletedCheckpointCount(stopped);
            Awaitility.await()
                    .atMost(25, TimeUnit.SECONDS)
                    .untilAsserted(
                            () ->
                                    assertTrue(
                                            engine.getCompletedCheckpointCount(stopped)
                                                    > checkpointAtFirstReceipt));
            engine.stopJob(stopped);
            Awaitility.await()
                    .atMost(30, TimeUnit.SECONDS)
                    .untilAsserted(() -> assertEquals("CANCELED", engine.getJobStatus(stopped)));
            original.get(30, TimeUnit.SECONDS);
            assertEquals(1, scenario.receipts().size());
        } finally {
            if (!original.isDone()) {
                engine.stopJob(stopped);
                original.cancel(true);
            }
        }

        String restored = jobId();
        variables.set(variables.size() - 1, "split.interval=1");
        Container.ExecResult result =
                engine.restoreJobWithCheckpoint(CHECKPOINT, stopped, restored, variables);
        assertEquals(0, result.getExitCode(), result.getStderr());
        assertEquals("FINISHED", engine.getJobStatus(restored));
        List<byte[]> accepted = scenario.receipts();
        assertEquals(2, accepted.size());
        assertEquals(2, scenario.attempts().size());
        for (byte[] payload : accepted) {
            assertEquals(JSON_MAPPER.readTree("{\"id\":7}"), JSON_MAPPER.readTree(payload));
        }
    }

    @Test
    void rejectsJsonConversionBudgetBeforeOpeningTheEndpoint() throws Exception {
        AzureIoTHubFixture.Scenario scenario = accepting(hub);
        expectFailure(JSON, args(hub, 8000, 3, 64), "JSON within the payload budget");
        assertEquals(1, scenario.started.getCount());
        assertTrue(scenario.attempts().isEmpty());
        assertTrue(scenario.receipts().isEmpty());
    }

    @Test
    void validatesPropertyAlphabetBeforeNetworkAndPreservesAllowedPunctuation() throws Exception {
        String punctuation = "value!#$%&'*+-.^_`|~";
        AzureIoTHubFixture.Scenario scenario =
                scenario(
                        hub,
                        (state, index, request, exchange) -> {
                            assertEquals(
                                    punctuation,
                                    exchange.getRequestHeaders().getFirst("iothub-app-detail"));
                            state.persist(request);
                            reply(exchange, 204);
                        });
        for (String value : Arrays.asList("room 1", "a/b", "east:1", " east ")) {
            List<String> invalid = valueArgs(hub, 8000, 3, 8, "TEXT", "label");
            invalid.add(shellArgument("property.value=" + value));
            expectFailure(PROPERTIES, invalid, "invalid application property");
        }
        assertEquals(1, scenario.started.getCount());
        assertTrue(scenario.attempts().isEmpty());
        assertTrue(scenario.receipts().isEmpty());
        List<String> valid = valueArgs(hub, 8000, 3, 8, "TEXT", "label");
        valid.add(shellArgument("property.value=" + punctuation));
        runSuccessfully(PROPERTIES, valid);
        assertEquals(1, scenario.attempts().size());
        assertEquals(1, scenario.receipts().size());
        assertArrayEquals(TEXT, scenario.receipts().get(0));
    }

    private AzureIoTHubFixture.Scenario accepting(AzureIoTHubFixture peer) throws Exception {
        return scenario(
                peer,
                (state, index, request, exchange) -> {
                    state.persist(request);
                    reply(exchange, 204);
                });
    }

    private AzureIoTHubFixture.Scenario scenario(
            AzureIoTHubFixture peer, AzureIoTHubFixture.Script script) throws Exception {
        Path path = Files.createTempFile(temp, "receipts-", ".log");
        AzureIoTHubFixture.Scenario active = new AzureIoTHubFixture.Scenario(path, script);
        peer.serve(active);
        return active;
    }

    private void runSuccessfully(String config, List<String> variables) throws Exception {
        String id = jobId();
        Container.ExecResult result = run(config, id, variables);
        assertEquals(0, result.getExitCode(), result.getStderr());
        assertEquals("FINISHED", engine.getJobStatus(id));
    }

    private void expectFailure(String config, List<String> variables, String... diagnostics)
            throws Exception {
        String id = jobId();
        int logOffset = engine.getServerLogs().length();
        assertFailed(id, logOffset, run(config, id, variables), diagnostics);
    }

    private void assertFailed(
            String id, int logOffset, Container.ExecResult result, String... diagnostics) {
        assertTrue(result.getExitCode() != 0 || "FAILED".equals(engine.getJobStatus(id)));
        String logs = engine.getServerLogs();
        String output =
                result.getStdout()
                        + result.getStderr()
                        + logs.substring(Math.min(logOffset, logs.length()));
        assertTrue(
                Arrays.stream(diagnostics).anyMatch(output::contains),
                () -> "Expected one of " + Arrays.toString(diagnostics));
        assertFalse(output.contains(PUBLIC_TEST_KEY));
        assertFalse(output.contains("SharedAccessSignature sr="));
        assertFalse(output.contains(RESPONSE_PRIVATE_TEXT));
    }

    private Container.ExecResult run(String config, String id, List<String> variables)
            throws Exception {
        return engine.executeJob(config, id, variables.toArray(new String[0]));
    }

    private static List<String> valueArgs(
            AzureIoTHubFixture peer,
            int budget,
            int attempts,
            int payload,
            String format,
            String field) {
        List<String> variables = args(peer, budget, attempts, payload);
        variables.add("sink.format=" + format);
        variables.add("sink.field=" + field);
        return variables;
    }

    private static List<String> args(
            AzureIoTHubFixture peer, int budget, int attempts, int payload) {
        return new ArrayList<>(
                Arrays.asList(
                        "hub.url=" + peer.origin(),
                        "row.timeout=" + budget,
                        "max.attempts=" + attempts,
                        "max.payload=" + payload));
    }

    // The harness joins arguments into bash -c; preserve all property bytes as one CLI argument.
    private static String shellArgument(String value) {
        return "'" + value.replace("'", "'\\''") + "'";
    }

    private static String jobId() {
        return Long.toString(ThreadLocalRandom.current().nextLong(1L, Long.MAX_VALUE));
    }

    private static KeyStore createCertificate(Path path) throws Exception {
        Process keytool =
                new ProcessBuilder(
                                Paths.get(System.getProperty("java.home"), "bin", "keytool")
                                        .toString(),
                                "-genkeypair",
                                "-alias",
                                "fixture",
                                "-keyalg",
                                "RSA",
                                "-keysize",
                                "2048",
                                "-dname",
                                "CN=" + HOST,
                                "-ext",
                                "SAN=dns:" + HOST,
                                "-validity",
                                "2",
                                "-storetype",
                                "JKS",
                                "-keystore",
                                path.toString(),
                                "-storepass",
                                STORE_PASSWORD,
                                "-keypass",
                                STORE_PASSWORD,
                                "-noprompt")
                        .redirectErrorStream(true)
                        .redirectOutput(path.resolveSibling(path.getFileName() + ".log").toFile())
                        .start();
        if (!keytool.waitFor(30, TimeUnit.SECONDS)) {
            keytool.destroyForcibly();
            throw new IllegalStateException("Fixture keytool did not finish");
        }
        assertEquals(0, keytool.exitValue(), "Could not create fixture certificate");
        KeyStore store = KeyStore.getInstance("JKS");
        try (InputStream input = Files.newInputStream(path)) {
            store.load(input, STORE_PASSWORD.toCharArray());
        }
        return store;
    }
}
