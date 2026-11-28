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

package org.apache.seatunnel.e2e.connector.websocket;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.api.common.metrics.MetricsContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.event.EventListener;
import org.apache.seatunnel.api.sink.SeaTunnelSink;
import org.apache.seatunnel.api.sink.SinkWriter;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.factory.TableSinkFactoryContext;
import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.LocalTimeType;
import org.apache.seatunnel.api.table.type.MapType;
import org.apache.seatunnel.api.table.type.PrimitiveByteArrayType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.websocket.sink.WebSocketSinkFactory;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.seatunnel.SeaTunnelContainer;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.testcontainers.Testcontainers;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.utility.MountableFile;

import okhttp3.Response;
import okhttp3.WebSocket;
import okhttp3.WebSocketListener;
import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import okhttp3.mockwebserver.SocketPolicy;
import okio.ByteString;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.time.LocalTime;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;

/**
 * Checks exact wire output, finite transport failure, checkpoint replay and pre-aggregation receive
 * rejection through a private Zeta engine with independent HTTPS/WebSocket/TCP fixtures. Direct
 * factory/writer cases additionally observe early typed conversion and exact admitted wire bytes.
 */
@Timeout(value = 4, unit = TimeUnit.MINUTES)
@Execution(ExecutionMode.SAME_THREAD)
class WebSocketSinkIT extends TestSuiteBase {
    private static final String HOST = "host.testcontainers.internal";
    private static final String TOKEN = "fixture-ws-bearer-token";
    private static final String PASSWORD = "fixture-trust-password";
    private static final String JSON_CONF = "/fake_to_websocket_json.conf";
    private static final String PAYLOAD_CONF = "/fake_to_websocket_payload.conf";
    private static final String QUEUE_CONF = "/fake_to_websocket_queue.conf";
    private static final String CHECKPOINT_CONF = "/websocket_sink_checkpoint.conf";
    private static final String TEXT = "雪 🦊";
    private static final AtomicLong JOBS = new AtomicLong(System.currentTimeMillis());
    private static final ObjectMapper JSON =
            new ObjectMapper().enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);

    private volatile Function<RecordedRequest, MockResponse> plainResponder;
    private volatile Function<RecordedRequest, MockResponse> tlsResponder;
    private MockWebServer plain;
    private MockWebServer tls;
    private MockWebServer untrusted;
    private WebSocketFaultPeer raw;
    private WebSocketProxyFixture proxy;
    private Path directory;
    private SeaTunnelContainer engine;
    private ExecutorService commands;

    @BeforeAll
    void startFixtureAndZeta() throws Exception {
        directory = Files.createTempDirectory("websocket-sink-fixture-");
        KeyStore trustedKey = keys("server");
        KeyStore untrustedKey = keys("untrusted");
        Path trustPath = directory.resolve("trust.jks");
        KeyStore trust = KeyStore.getInstance("JKS");
        trust.load(null, null);
        trust.setCertificateEntry("fixture", trustedKey.getCertificate("fixture"));
        try (OutputStream output = Files.newOutputStream(trustPath)) {
            trust.store(output, PASSWORD.toCharArray());
        }
        plain = server(null, request -> plainResponder.apply(request));
        tls = server(trustedKey, request -> tlsResponder.apply(request));
        untrusted = server(untrustedKey, request -> new MockResponse().setResponseCode(403));
        raw = new WebSocketFaultPeer();
        proxy = new WebSocketProxyFixture(plain.getPort(), tls.getPort());
        commands = Executors.newCachedThreadPool();
        engine =
                new SeaTunnelContainer() {
                    @Override
                    protected String getJavaToolOptions() {
                        return "-Djavax.net.ssl.trustStore=/tmp/websocket-sink-test-trust.jks "
                                + "-Djavax.net.ssl.trustStorePassword="
                                + PASSWORD
                                + " -Dhttp.proxyHost="
                                + HOST
                                + " -Dhttp.proxyPort="
                                + proxy.port()
                                + " -Dhttps.proxyHost="
                                + HOST
                                + " -Dhttps.proxyPort="
                                + proxy.port()
                                + " -Dhttp.nonProxyHosts="
                                + HOST
                                + "|localhost|127.*|[::1]";
                    }

                    @Override
                    protected void executeExtraCommands(GenericContainer<?> container) {
                        container.withCopyFileToContainer(
                                MountableFile.forHostPath(trustPath),
                                "/tmp/websocket-sink-test-trust.jks");
                    }
                };
        engine.startUp();
    }

    @AfterAll
    void stopFixtureAndZeta() throws Exception {
        try {
            if (engine != null) {
                engine.tearDown();
            }
        } finally {
            if (commands != null) {
                commands.shutdownNow();
                Assertions.assertTrue(commands.awaitTermination(30, TimeUnit.SECONDS));
            }
            if (proxy != null) {
                proxy.close();
            }
            if (raw != null) {
                raw.close();
            }
            for (MockWebServer server : Arrays.asList(plain, tls, untrusted)) {
                if (server != null) {
                    server.shutdown();
                }
            }
            if (directory != null) {
                for (String file :
                        Arrays.asList("server.jks", "untrusted.jks", "trust.jks", "keytool.log")) {
                    Files.deleteIfExists(directory.resolve(file));
                }
                Files.deleteIfExists(directory);
            }
        }
    }

    @Test
    void sendsOrderedUtf8JsonWithExactPrecisionAndFinalDrainOverAuthenticatedTls()
            throws Exception {
        Capture capture = new Capture(true);
        List<String> authorization = new CopyOnWriteArrayList<>();
        tlsResponder =
                request -> {
                    authorization.add(request.getHeader("Authorization"));
                    return "Bearer ".concat(TOKEN).equals(request.getHeader("Authorization"))
                            ? capture.upgrade()
                            : new MockResponse().setResponseCode(401);
                };
        int logOffset = engine.getServerLogs().length();
        Container.ExecResult result =
                execute(JSON_CONF, arguments(url(tls, true, "/sink"), 10000, 10000));
        succeeded(result);
        capture.assertClosed();
        Assertions.assertEquals(Collections.singletonList("Bearer " + TOKEN), authorization);
        List<JsonNode> received = new ArrayList<>();
        for (String message : capture.text) {
            received.add(JSON.readTree(message));
        }
        Assertions.assertEquals(
                Arrays.asList(
                        JSON.readTree(
                                "{\"id\":9007199254740993,\"name\":\"雪 🦊\",\"amount\":12345678901234567890.12345678,\"note\":null}"),
                        JSON.readTree(
                                "{\"id\":9007199254740994,\"name\":\"quotes: \\\"x\\\"\",\"amount\":10.125,\"note\":\"\"}")),
                received);
        Assertions.assertTrue(capture.binary.isEmpty());
        assertRedacted(engine.getServerLogs().substring(logOffset) + diagnostic(result));
    }

    @Test
    void sendsRawTextAndBinaryMessages() throws Exception {
        Capture text = new Capture(true);
        plainResponder = request -> text.upgrade();
        succeeded(
                execute(PAYLOAD_CONF, typed(url(plain, false, "/sink"), "TEXT", "text", 8, 10000)));
        text.assertClosed();
        Assertions.assertEquals(Collections.singletonList(TEXT), text.text);
        Assertions.assertTrue(text.binary.isEmpty());

        Capture binary = new Capture(true);
        plainResponder = request -> binary.upgrade();
        succeeded(
                execute(
                        PAYLOAD_CONF,
                        typed(url(plain, false, "/sink"), "BINARY", "data", 3, 10000)));
        binary.assertClosed();
        Assertions.assertTrue(binary.text.isEmpty());
        Assertions.assertEquals(
                Collections.singletonList(ByteString.of((byte) 0, (byte) 255, (byte) 128)),
                binary.binary);
    }

    @Test
    void emptyInputDoesNotConnectAndInvalidFramesNeverOpenTheSocket() throws Exception {
        int before = plain.getRequestCount();
        plainResponder = request -> new MockResponse().setResponseCode(500);
        String target = url(plain, false, "/sink");
        succeeded(execute(QUEUE_CONF, queue(target, 0, 16, 1000, 1000)));
        rejected(PAYLOAD_CONF, typed(target, "TEXT", "text", 7, 10000), "max_frame_bytes");
        rejected(PAYLOAD_CONF, typed(target, "TEXT", "empty", 10, 10000), "empty TEXT or BINARY");
        rejected(PAYLOAD_CONF, typed(target, "TEXT", "missing", 10, 10000), "non-null");
        rejected(PAYLOAD_CONF, typed(target, "BINARY", "text", 10, 10000), "BYTES column");
        rejected(PAYLOAD_CONF, typed(target + "?unusable=1", "TEXT", "text", 10, 10000), "query");
        Assertions.assertEquals(before, plain.getRequestCount());
    }

    @Test
    void rejectsAuthenticationRedirectAndUntrustedTlsBeforeSending() throws Exception {
        String target = url(tls, true, "/sink");
        int before = tls.getRequestCount();
        List<String> requests = new CopyOnWriteArrayList<>();
        tlsResponder =
                request -> {
                    requests.add(request.getPath());
                    if (request.getPath().equals("/redirect")) {
                        return new MockResponse()
                                .setResponseCode(302)
                                .addHeader("Location", target);
                    }
                    return new MockResponse()
                            .setResponseCode(401)
                            .setBody("private-server-response");
                };
        rejected(JSON_CONF, arguments(target, 10000, 10000), "HTTP 401");
        rejected(JSON_CONF, arguments(url(tls, true, "/redirect"), 10000, 10000), "HTTP 302");
        Assertions.assertEquals(Arrays.asList("/sink", "/redirect"), requests);
        Assertions.assertEquals(before + 2, tls.getRequestCount());
        rejected(
                JSON_CONF,
                arguments(url(untrusted, true, "/sink"), 10000, 10000),
                "TLS handshake failed");
        Assertions.assertEquals(0, untrusted.getRequestCount(), "TLS failed before any request");
    }

    @Test
    void failsWhenThePeerWithholdsOpeningOrClosingHandshake() throws Exception {
        int before = plain.getRequestCount();
        plainResponder = request -> new MockResponse().setSocketPolicy(SocketPolicy.NO_RESPONSE);
        String[] opening = typed(url(plain, false, "/sink"), "TEXT", "text", 8, 2000);
        opening[1] = "connect.timeout=2000";
        rejected(PAYLOAD_CONF, opening, "opening handshake");
        Assertions.assertEquals(before + 1, plain.getRequestCount());

        Capture noReply = new Capture(false);
        plainResponder = request -> noReply.upgrade();
        rejected(
                PAYLOAD_CONF,
                typed(url(plain, false, "/sink"), "TEXT", "text", 8, 2000),
                "close handshake timed out");
        Assertions.assertTrue(noReply.closeReceived.await(10, TimeUnit.SECONDS));
        Assertions.assertEquals(Collections.singletonList(TEXT), noReply.text);
        Assertions.assertTrue(
                noReply.ended.await(10, TimeUnit.SECONDS), "timed-out writer canceled its socket");
    }

    @Test
    void reportsConnectionResetBeforeAndAfterAnObservedFrameWithoutResending() throws Exception {
        for (WebSocketFaultPeer.Mode mode :
                Arrays.asList(
                        WebSocketFaultPeer.Mode.RESET_AFTER_UPGRADE,
                        WebSocketFaultPeer.Mode.RESET_AFTER_MESSAGE)) {
            WebSocketFaultPeer.Scenario scenario = new WebSocketFaultPeer.Scenario(mode);
            raw.use(scenario);
            rejected(PAYLOAD_CONF, typed(raw.url(), "TEXT", "text", 8, 5000), "WebSocket sink:");
            Assertions.assertTrue(scenario.upgraded.await(10, TimeUnit.SECONDS));
            Assertions.assertNull(scenario.failure);
            Assertions.assertEquals(
                    1, scenario.connections.get(), "writer does not retry or reconnect");
            if (mode == WebSocketFaultPeer.Mode.RESET_AFTER_MESSAGE) {
                Assertions.assertEquals(TEXT, scenario.message);
            }
        }
    }

    @Test
    void boundsQueueAdmissionWhilePeerWithholdsReads() throws Exception {
        WebSocketFaultPeer.Scenario scenario =
                new WebSocketFaultPeer.Scenario(WebSocketFaultPeer.Mode.STALL);
        raw.use(scenario);
        try {
            rejected(
                    QUEUE_CONF,
                    queue(raw.url(), 2, 8388608, 3000, 8000),
                    "outbound queue admission timed out",
                    "connection failed during transmission");
            Assertions.assertTrue(scenario.upgraded.await(10, TimeUnit.SECONDS));
            Assertions.assertEquals(1, scenario.connections.get());
            Assertions.assertEquals(
                    1, scenario.release.getCount(), "failed before peer permitted reads");
            Assertions.assertTrue(
                    scenario.applicationFramesSent.get() > 1,
                    "peer sent repeated valid application frames while withholding reads");
            Assertions.assertTrue(
                    System.nanoTime() - scenario.upgradedAt < TimeUnit.SECONDS.toNanos(30),
                    "failure must precede the peer's own cleanup deadline");
            Assertions.assertNull(scenario.failure);
        } finally {
            scenario.release.countDown();
        }
    }

    @Test
    void boundsCheckpointOrFinalDrainWhileThePeerWithholdsReads() throws Exception {
        WebSocketFaultPeer.Scenario scenario =
                new WebSocketFaultPeer.Scenario(WebSocketFaultPeer.Mode.STALL);
        raw.use(scenario);
        try {
            rejected(
                    QUEUE_CONF,
                    queue(raw.url(), 1, 8388608, 15000, 3000),
                    "outbound queue drain timed out");
            Assertions.assertTrue(scenario.upgraded.await(10, TimeUnit.SECONDS));
            Assertions.assertEquals(1, scenario.connections.get());
            Assertions.assertEquals(
                    1, scenario.release.getCount(), "failed before peer permitted reads");
            Assertions.assertTrue(
                    scenario.applicationFramesSent.get() > 1,
                    "peer sent repeated valid application frames while withholding reads");
            Assertions.assertTrue(
                    System.nanoTime() - scenario.upgradedAt < TimeUnit.SECONDS.toNanos(30),
                    "failure must precede the peer's own cleanup deadline");
        } finally {
            scenario.release.countDown();
        }
    }

    @Test
    void checkpointDrainsWithoutAnApplicationAckAndSourceReplayCanRepeatAFrame() throws Exception {
        AtomicInteger sources = new AtomicInteger();
        Capture initial = new Capture(true);
        Capture restored = new Capture(true);
        AtomicReference<Capture> output = new AtomicReference<>(initial);
        plainResponder =
                request -> {
                    if ("/source".equals(request.getPath())) {
                        int sourceAttempt = sources.getAndIncrement();
                        return new MockResponse()
                                .withWebSocketUpgrade(
                                        new WebSocketListener() {
                                            @Override
                                            public void onOpen(
                                                    WebSocket socket, Response response) {
                                                socket.send("{\"id\":7}");
                                                if (sourceAttempt > 0) {
                                                    socket.send("{\"id\":8}");
                                                }
                                            }

                                            @Override
                                            public void onClosing(
                                                    WebSocket socket, int code, String reason) {
                                                socket.close(code, null);
                                            }
                                        });
                    }
                    return output.get().upgrade();
                };
        String[] args =
                merge(
                        arguments(url(plain, false, "/sink"), 10000, 10000),
                        "source.url=" + url(plain, false, "/source"));
        String firstJob = nextJob();
        Future<Container.ExecResult> original =
                commands.submit(() -> engine.executeJob(CHECKPOINT_CONF, firstJob, args));
        try {
            Awaitility.await().atMost(45, TimeUnit.SECONDS).until(() -> initial.text.size() == 1);
            long afterReceipt = engine.getCompletedCheckpointCount(firstJob);
            Awaitility.await()
                    .atMost(20, TimeUnit.SECONDS)
                    .until(() -> engine.getCompletedCheckpointCount(firstJob) > afterReceipt);
            engine.stopJob(firstJob);
            assertStatus(firstJob, "CANCELED");
            original.get(30, TimeUnit.SECONDS);
            Assertions.assertTrue(
                    initial.ended.await(10, TimeUnit.SECONDS), "canceled job released writer");
            Assertions.assertEquals(Collections.singletonList("{\"id\":7}"), initial.text);
            Assertions.assertEquals(1, initial.connections.get());

            output.set(restored);
            String resumedJob = nextJob();
            succeeded(
                    engine.restoreJobWithCheckpoint(
                            CHECKPOINT_CONF, firstJob, resumedJob, Arrays.asList(args)));
            assertStatus(resumedJob, "FINISHED");
            restored.assertClosed();
            Assertions.assertEquals(Arrays.asList("{\"id\":7}", "{\"id\":8}"), restored.text);
            Assertions.assertEquals(2, sources.get());
        } finally {
            if (!original.isDone()) {
                engine.cancelJob(firstJob);
                original.cancel(true);
            }
        }
    }

    @Test
    void rejectsInboundLengthsContinuationsAndExtensionsBeforeMessageAggregation()
            throws Exception {
        for (WebSocketFaultPeer.Mode mode :
                Arrays.asList(
                        WebSocketFaultPeer.Mode.OVERSIZED_INBOUND,
                        WebSocketFaultPeer.Mode.FRAGMENTED_INBOUND,
                        WebSocketFaultPeer.Mode.EXTENSION_INBOUND)) {
            WebSocketFaultPeer.Scenario scenario = new WebSocketFaultPeer.Scenario(mode);
            raw.use(scenario);
            String[] args = typed(raw.url(), "TEXT", "text", 8, 2000);
            args[3] = "inbound.bytes=128";
            rejected(
                    PAYLOAD_CONF,
                    args,
                    mode == WebSocketFaultPeer.Mode.EXTENSION_INBOUND
                            ? "unsupported or malformed incoming WebSocket frame"
                            : "max_inbound_message_bytes");
            Assertions.assertTrue(scenario.challengeSent.await(10, TimeUnit.SECONDS));
            Assertions.assertTrue(scenario.disconnected.await(10, TimeUnit.SECONDS));
            Assertions.assertEquals(1, scenario.connections.get());
            Assertions.assertFalse(scenario.extensionOffered);
            Assertions.assertNull(scenario.failure);
        }
    }

    @Test
    void preservesJvmHttpForwardingAndConnectWithVerifiedTls() throws Exception {
        int before = proxy.routes.size();
        Capture text = new Capture(true);
        plainResponder = request -> text.upgrade();
        succeeded(execute(PAYLOAD_CONF, typed(proxy.url(false), "TEXT", "text", 8, 10000)));
        text.assertClosed();
        Assertions.assertEquals(Collections.singletonList(TEXT), text.text);
        Capture json = new Capture(true);
        List<String> authorization = new CopyOnWriteArrayList<>();
        tlsResponder =
                request -> {
                    authorization.add(request.getHeader("Authorization"));
                    return json.upgrade();
                };
        succeeded(execute(JSON_CONF, arguments(proxy.url(true), 10000, 10000)));
        json.assertClosed();
        Assertions.assertEquals(2, json.text.size());
        Assertions.assertEquals(Collections.singletonList("Bearer " + TOKEN), authorization);
        Assertions.assertEquals(
                Arrays.asList(
                        "GET http://"
                                + WebSocketProxyFixture.ORIGIN
                                + ":"
                                + plain.getPort()
                                + "/sink HTTP/1.1",
                        "CONNECT "
                                + WebSocketProxyFixture.ORIGIN
                                + ":"
                                + tls.getPort()
                                + " HTTP/1.1"),
                proxy.routes.subList(before, proxy.routes.size()));
        Assertions.assertTrue(proxy.failures.isEmpty(), proxy.failures.toString());
    }

    @Test
    void preservesJvmSocksRemoteDnsAndLazyUsernameFallback() throws Exception {
        Capture capture = new Capture(true);
        plainResponder = request -> capture.upgrade();
        int before = proxy.routes.size();
        int users = proxy.users.size();
        SeaTunnelContainer socks =
                new SeaTunnelContainer() {
                    @Override
                    protected String getJavaToolOptions() {
                        return "-DsocksProxyHost="
                                + HOST
                                + " -DsocksProxyPort="
                                + proxy.port()
                                + " -Dhttp.nonProxyHosts="
                                + HOST
                                + "|localhost|127.*|[::1]"
                                + " -Duser.name=websocket-proxy-user";
                    }
                };
        socks.startUp();
        try {
            String job = nextJob();
            succeeded(
                    socks.executeJob(
                            PAYLOAD_CONF, job, typed(proxy.url(false), "TEXT", "text", 8, 10000)));
            Awaitility.await()
                    .atMost(20, TimeUnit.SECONDS)
                    .untilAsserted(
                            () -> Assertions.assertEquals("FINISHED", socks.getJobStatus(job)));
            capture.assertClosed();
            Assertions.assertEquals(Collections.singletonList(TEXT), capture.text);
            Assertions.assertEquals(
                    Collections.singletonList(
                            "SOCKS5 " + WebSocketProxyFixture.ORIGIN + ":" + plain.getPort()),
                    proxy.routes.subList(before, proxy.routes.size()));
            Assertions.assertEquals(
                    Collections.singletonList("websocket-proxy-user"),
                    proxy.users.subList(users, proxy.users.size()));
            Assertions.assertTrue(proxy.failures.isEmpty(), proxy.failures.toString());
        } finally {
            socks.tearDown();
        }
    }

    @Test
    void rejectsSharedRowExpansionBeforeTraversingEveryMapOccurrence() throws Exception {
        int before = plain.getRequestCount();
        MapType<String, String> strings =
                new MapType<>(BasicType.STRING_TYPE, BasicType.STRING_TYPE);
        String[] names = new String[16];
        SeaTunnelDataType<?>[] types = new SeaTunnelDataType<?>[16];
        Object[] fields = new Object[16];
        TrackingMap<String> inner = new TrackingMap<>();
        char[] characters = new char[128];
        Arrays.fill(characters, 'a');
        inner.values.put("s", new String(characters));
        for (int index = 0; index < fields.length; index++) {
            names[index] = "f" + index;
            types[index] = strings;
            fields[index] = inner;
        }
        SeaTunnelRowType nested = new SeaTunnelRowType(names, types);
        TrackingMap<SeaTunnelRow> outer = new TrackingMap<>();
        SeaTunnelRow shared = new SeaTunnelRow(fields);
        for (int index = 0; index < 16; index++) {
            outer.values.put("k" + index, shared);
        }
        SinkWriter<SeaTunnelRow, Void, Void> writer =
                payloadWriter(payloadType(new MapType<>(BasicType.STRING_TYPE, nested)), 512);
        IOException failure =
                Assertions.assertThrows(
                        IOException.class,
                        () -> writer.write(new SeaTunnelRow(new Object[] {outer})));
        Assertions.assertEquals(
                "WebSocket sink: message exceeds max_frame_bytes", failure.getMessage());
        // Native whole-row conversion visits all 16 outer entries and 256 inner occurrences.
        Assertions.assertTrue(outer.visited > 0 && outer.visited < 16);
        Assertions.assertTrue(inner.visited > 0 && inner.visited < 16);
        Assertions.assertEquals(
                failure.getMessage(),
                Assertions.assertThrows(IOException.class, writer::close).getMessage());

        SinkWriter<SeaTunnelRow, Void, Void> decimal =
                payloadWriter(payloadType(new DecimalType(30, 8)), 512);
        IOException expanded =
                Assertions.assertThrows(
                        IOException.class,
                        () ->
                                decimal.write(
                                        new SeaTunnelRow(
                                                new Object[] {new BigDecimal("1E+1000")})));
        Assertions.assertEquals(
                "WebSocket sink: message exceeds max_frame_bytes", expanded.getMessage());
        Assertions.assertThrows(IOException.class, decimal::close);

        SinkWriter<SeaTunnelRow, Void, Void> escaped =
                payloadWriter(payloadType(BasicType.STRING_TYPE), 32);
        IOException escaping =
                Assertions.assertThrows(
                        IOException.class,
                        () ->
                                escaped.write(
                                        new SeaTunnelRow(
                                                new Object[] {
                                                    "\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\"\""
                                                })));
        Assertions.assertEquals(
                "WebSocket sink: message exceeds max_frame_bytes", escaping.getMessage());
        Assertions.assertThrows(IOException.class, escaped::close);
        Assertions.assertEquals(before, plain.getRequestCount());
    }

    @Test
    void preservesNativeTypedJsonAndDeepNarrowValuesThroughTheRealWriter() throws Exception {
        Map<Object, String> collisions = new LinkedHashMap<>();
        collisions.put(1, "first");
        collisions.put(2, "雪");
        collisions.put("1", "last");
        assertPayload(
                new SeaTunnelRowType(
                        new String[] {
                            "id", "amount", "nested", "array", "binary", "time", "escaped"
                        },
                        new SeaTunnelDataType<?>[] {
                            BasicType.LONG_TYPE,
                            new DecimalType(30, 8),
                            new MapType<>(BasicType.INT_TYPE, BasicType.STRING_TYPE),
                            ArrayType.STRING_ARRAY_TYPE,
                            PrimitiveByteArrayType.INSTANCE,
                            LocalTimeType.LOCAL_TIME_TYPE,
                            BasicType.STRING_TYPE
                        }),
                new SeaTunnelRow(
                        new Object[] {
                            9007199254740993L,
                            new BigDecimal("1.20"),
                            collisions,
                            new String[] {"same", null, "same"},
                            new byte[] {0, (byte) 255},
                            LocalTime.of(1, 2, 3, 123000000),
                            "\"\\\n"
                        }),
                512,
                "{\"id\":9007199254740993,\"amount\":1.20,\"nested\":{\"1\":\"last\",\"2\":\"雪\"},"
                        + "\"array\":[\"same\",null,\"same\"],\"binary\":\"AP8=\",\"time\":\"01:02:03.123\","
                        + "\"escaped\":\"\\\"\\\\\\n\"}");
        Map<BigDecimal, String> compact = new LinkedHashMap<>();
        compact.put(new BigDecimal("1E+100"), "compact");
        assertPayload(
                payloadType(new MapType<>(new DecimalType(30, 8), BasicType.STRING_TYPE)),
                new SeaTunnelRow(new Object[] {compact}),
                64,
                "{\"value\":{\"1E+100\":\"compact\"}}");

        SeaTunnelDataType<?> type = BasicType.STRING_TYPE;
        Object value = "leaf";
        StringBuilder expected = new StringBuilder();
        for (int depth = 0; depth < 2000; depth++) {
            type = new SeaTunnelRowType(new String[] {"v"}, new SeaTunnelDataType<?>[] {type});
            value = new SeaTunnelRow(new Object[] {value});
            expected.append("{\"v\":");
        }
        expected.append("\"leaf\"");
        for (int depth = 0; depth < 2000; depth++) {
            expected.append('}');
        }
        assertPayload((SeaTunnelRowType) type, (SeaTunnelRow) value, 32768, expected.toString());
        SeaTunnelRowType shared = payloadType(BasicType.STRING_TYPE);
        for (int level = 0; level < 8; level++) {
            shared =
                    new SeaTunnelRowType(
                            new String[] {"left", "right"},
                            new SeaTunnelDataType<?>[] {shared, shared});
        }
        assertPayload(
                shared,
                new SeaTunnelRow(new Object[] {null, null}),
                64,
                "{\"left\":null,\"right\":null}");
    }

    private Container.ExecResult execute(String conf, String[] args) throws Exception {
        String job = nextJob();
        Container.ExecResult result = engine.executeJob(conf, job, args);
        assertStatus(job, "FINISHED");
        return result;
    }

    private SinkWriter<SeaTunnelRow, Void, Void> payloadWriter(SeaTunnelRowType type, int cap)
            throws IOException {
        Map<String, Object> values = new HashMap<>();
        values.put("url", "ws://localhost:" + plain.getPort() + "/sink");
        values.put("max_frame_bytes", cap);
        CatalogTable table = CatalogTableUtil.getCatalogTable("payload", type);
        SeaTunnelSink<SeaTunnelRow, Void, Void, Void> sink =
                new WebSocketSinkFactory()
                        .createSink(
                                new TableSinkFactoryContext(
                                        table,
                                        ReadonlyConfig.fromMap(values),
                                        getClass().getClassLoader()))
                        .createSink();
        Assertions.assertSame(table, sink.getWriteCatalogTable().get());
        return sink.createWriter(new PayloadContext());
    }

    private void assertPayload(SeaTunnelRowType type, SeaTunnelRow row, int cap, String expected)
            throws Exception {
        Capture capture = new Capture(true);
        plainResponder = request -> capture.upgrade();
        SinkWriter<SeaTunnelRow, Void, Void> writer = payloadWriter(type, cap);
        try {
            writer.write(row);
            writer.prepareCommit();
        } finally {
            writer.close();
        }
        capture.assertClosed();
        Assertions.assertEquals(Collections.singletonList(expected), capture.text);
        Assertions.assertTrue(capture.binary.isEmpty());
    }

    private static SeaTunnelRowType payloadType(SeaTunnelDataType<?> value) {
        return new SeaTunnelRowType(new String[] {"value"}, new SeaTunnelDataType<?>[] {value});
    }

    private void rejected(String conf, String[] args, String... errors) throws Exception {
        String job = nextJob();
        int previous = engine.getServerLogs().length();
        Container.ExecResult result = engine.executeJob(conf, job, args);
        String details = diagnostic(result) + engine.getServerLogs().substring(previous);
        Assertions.assertTrue(
                Arrays.stream(errors).anyMatch(details::contains),
                () -> "Expected " + Arrays.toString(errors) + " for job " + job);
        Assertions.assertTrue(
                result.getExitCode() != 0 || "FAILED".equals(engine.getJobStatus(job)),
                "connector failure must not be reported as success");
        assertRedacted(details);
    }

    private void assertStatus(String job, String expected) {
        Awaitility.await()
                .atMost(20, TimeUnit.SECONDS)
                .untilAsserted(() -> Assertions.assertEquals(expected, engine.getJobStatus(job)));
    }

    private static String[] arguments(String target, int connection, int close) {
        return new String[] {
            "sink.url=" + target,
            "connect.timeout=" + connection,
            "close.timeout=" + close,
            "inbound.bytes=1048576"
        };
    }

    private static String[] typed(String target, String format, String field, int cap, int close) {
        return merge(
                arguments(target, 10000, close),
                "sink.format=" + format,
                "sink.field=" + field,
                "frame.bytes=" + cap);
    }

    private static String[] queue(String target, int count, int bytes, int write, int flush) {
        return merge(
                arguments(target, 10000, 10000),
                "row.count=" + count,
                "payload.bytes=" + bytes,
                "write.timeout=" + write,
                "flush.timeout=" + flush);
    }

    private static String[] merge(String[] values, String... more) {
        String[] combined = Arrays.copyOf(values, values.length + more.length);
        System.arraycopy(more, 0, combined, values.length, more.length);
        return combined;
    }

    private static String nextJob() {
        return Long.toString(JOBS.incrementAndGet());
    }

    private static String url(MockWebServer server, boolean secure, String path) {
        return (secure ? "wss://" : "ws://") + HOST + ":" + server.getPort() + path;
    }

    private static void assertRedacted(String details) {
        Assertions.assertFalse(details.contains(TOKEN), "token must not appear in diagnostics");
        Assertions.assertFalse(
                details.contains("private-server-response"),
                "peer body must not appear in diagnostics");
    }

    private static String diagnostic(Container.ExecResult result) {
        return result.getStdout() + result.getStderr();
    }

    private static void succeeded(Container.ExecResult result) {
        Assertions.assertEquals(0, result.getExitCode(), result.getStderr());
    }

    private KeyStore keys(String name) throws Exception {
        Path path = directory.resolve(name + ".jks");
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
                                "SAN=dns:" + HOST + ",dns:" + WebSocketProxyFixture.ORIGIN,
                                "-validity",
                                "2",
                                "-storetype",
                                "JKS",
                                "-keystore",
                                path.toString(),
                                "-storepass",
                                PASSWORD,
                                "-keypass",
                                PASSWORD,
                                "-noprompt")
                        .redirectErrorStream(true)
                        .redirectOutput(directory.resolve("keytool.log").toFile())
                        .start();
        Assertions.assertTrue(
                keytool.waitFor(30, TimeUnit.SECONDS), "fixture certificate deadline");
        Assertions.assertEquals(0, keytool.exitValue(), "fixture certificate generation");
        KeyStore keys = KeyStore.getInstance("JKS");
        try (InputStream input = Files.newInputStream(path)) {
            keys.load(input, PASSWORD.toCharArray());
        }
        return keys;
    }

    private static MockWebServer server(
            KeyStore keys, Function<RecordedRequest, MockResponse> response) throws Exception {
        MockWebServer server = new MockWebServer();
        if (keys != null) {
            KeyManagerFactory manager =
                    KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            manager.init(keys, PASSWORD.toCharArray());
            SSLContext ssl = SSLContext.getInstance("TLS");
            ssl.init(manager.getKeyManagers(), null, null);
            server.useHttps(ssl.getSocketFactory(), false);
        }
        server.setDispatcher(
                new Dispatcher() {
                    @Override
                    public MockResponse dispatch(RecordedRequest request) {
                        return response.apply(request);
                    }
                });
        server.start();
        Testcontainers.exposeHostPorts(server.getPort());
        return server;
    }

    /**
     * Tracks real map traversal through nested ROW values so early admission is observed before
     * native conversion could expand every shared occurrence; it never fabricates serializer
     * output.
     */
    private static final class TrackingMap<T> extends AbstractMap<String, T> {
        private final Map<String, T> values = new LinkedHashMap<>();
        private int visited;

        @Override
        public Set<Entry<String, T>> entrySet() {
            return new AbstractSet<Entry<String, T>>() {
                @Override
                public Iterator<Entry<String, T>> iterator() {
                    Iterator<Entry<String, T>> delegate = values.entrySet().iterator();
                    return new Iterator<Entry<String, T>>() {
                        @Override
                        public boolean hasNext() {
                            return delegate.hasNext();
                        }

                        @Override
                        public Entry<String, T> next() {
                            visited++;
                            return delegate.next();
                        }
                    };
                }

                @Override
                public int size() {
                    return values.size();
                }
            };
        }
    }

    /**
     * Supplies the minimal real-writer context for component-level transport assertions inside this
     * existing Sink suite; periodic engine flushing remains covered by the original engine cases.
     */
    private static final class PayloadContext implements SinkWriter.Context {
        private static final long serialVersionUID = 1L;

        @Override
        public int getIndexOfSubtask() {
            return 0;
        }

        @Override
        public MetricsContext getMetricsContext() {
            return null;
        }

        @Override
        public EventListener getEventListener() {
            return null;
        }
    }

    /**
     * Captures only client-emitted messages and verifies one completed normal close handshake.
     *
     * @see WebSocketFaultPeer
     */
    private static final class Capture extends WebSocketListener {
        final List<String> text = new CopyOnWriteArrayList<>();
        final List<ByteString> binary = new CopyOnWriteArrayList<>();
        final AtomicInteger connections = new AtomicInteger();
        final CountDownLatch closeReceived = new CountDownLatch(1);
        final CountDownLatch ended = new CountDownLatch(1);
        final boolean reply;
        volatile int closeCode;
        volatile boolean cleanClose;

        Capture(boolean reply) {
            this.reply = reply;
        }

        MockResponse upgrade() {
            connections.incrementAndGet();
            return new MockResponse().withWebSocketUpgrade(this);
        }

        @Override
        public void onOpen(WebSocket socket, Response response) {
            // Valid small inbound messages are ignored, without acknowledging exported rows.
            socket.send("{\"status\":\"ready\"}");
            socket.send(ByteString.of((byte) 1, (byte) 2));
        }

        @Override
        public void onMessage(WebSocket socket, String message) {
            text.add(message);
        }

        @Override
        public void onMessage(WebSocket socket, ByteString message) {
            binary.add(message);
        }

        @Override
        public void onClosing(WebSocket socket, int code, String reason) {
            closeCode = code;
            closeReceived.countDown();
            if (reply) {
                socket.close(code, null);
            }
        }

        @Override
        public void onClosed(WebSocket socket, int code, String reason) {
            cleanClose = true;
            ended.countDown();
        }

        @Override
        public void onFailure(WebSocket socket, Throwable failure, Response response) {
            ended.countDown();
        }

        void assertClosed() throws InterruptedException {
            Assertions.assertTrue(
                    ended.await(10, TimeUnit.SECONDS), "fixture socket reached terminal state");
            Assertions.assertTrue(cleanClose, "fixture completed the closing handshake");
            Assertions.assertEquals(1000, closeCode);
            Assertions.assertEquals(1, connections.get());
        }
    }
}
