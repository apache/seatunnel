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

package org.apache.seatunnel.connectors.seatunnel.google.analytics4;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.Signature;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.seatunnel.connectors.seatunnel.google.analytics4.GoogleAnalytics4ReportTest.JSON;
import static org.apache.seatunnel.connectors.seatunnel.google.analytics4.GoogleAnalytics4ReportTest.bytes;
import static org.apache.seatunnel.connectors.seatunnel.google.analytics4.GoogleAnalytics4ReportTest.options;
import static org.apache.seatunnel.connectors.seatunnel.google.analytics4.GoogleAnalytics4ReportTest.page;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class GoogleAnalytics4SourceReaderTest {
    private HttpServer server;
    private GoogleAnalytics4SourceReader reader;
    private final SourceReader.Context context = mock(SourceReader.Context.class);
    private final List<SeaTunnelRow> rows = new ArrayList<>();
    private final AtomicReference<Throwable> serverFailure = new AtomicReference<>();
    private ExecutorService executor;
    private String origin;
    @TempDir Path temp;

    @AfterEach
    void close() throws Exception {
        if (reader != null) {
            reader.close();
        }
        if (server != null) {
            server.stop(0);
        }
        if (executor != null) {
            executor.shutdownNow();
            Assertions.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
        if (serverFailure.get() != null) {
            throw new AssertionError("Mock HTTP contract failed", serverFailure.get());
        }
    }

    private void start(Handler handler) throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext(
                "/",
                exchange -> {
                    try {
                        handler.handle(exchange);
                    } catch (Throwable e) {
                        serverFailure.set(e);
                    } finally {
                        exchange.close();
                    }
                });
        server.start();
        origin = "http://127.0.0.1:" + server.getAddress().getPort();
    }

    private interface Handler {
        void handle(HttpExchange exchange) throws Exception;
    }

    private GoogleAnalytics4Config configure(Map<String, Object> values) throws IOException {
        values.put("emulator_url", origin);
        GoogleAnalytics4Config config = new GoogleAnalytics4Config(ReadonlyConfig.fromMap(values));
        reader = new GoogleAnalytics4SourceReader(config, new SingleSplitReaderContext(context));
        reader.open();
        return config;
    }

    @SuppressWarnings("unchecked")
    private Collector<SeaTunnelRow> collector() {
        Collector<SeaTunnelRow> collector = mock(Collector.class);
        when(collector.getCheckpointLock()).thenReturn(new Object());
        doAnswer(
                        invocation -> {
                            rows.add(invocation.getArgument(0));
                            return null;
                        })
                .when(collector)
                .collect(any(SeaTunnelRow.class));
        return collector;
    }

    private void reply(HttpExchange exchange, int status, byte[] body) throws IOException {
        exchange.getResponseHeaders().set("Content-Type", "application/json");
        exchange.sendResponseHeaders(status, body.length);
        exchange.getResponseBody().write(body);
    }

    @Test
    void followsActualOffsetsAndSignalsCompletionOnlyOnce() throws Exception {
        List<Integer> offsets = new ArrayList<>();
        start(
                exchange -> {
                    Assertions.assertEquals(
                            "/v1beta/properties/123:runReport", exchange.getRequestURI().getPath());
                    Assertions.assertEquals("POST", exchange.getRequestMethod());
                    Assertions.assertNull(exchange.getRequestHeaders().getFirst("Authorization"));
                    JsonNode request = JSON.readTree(exchange.getRequestBody());
                    int offset = request.path("offset").asInt();
                    offsets.add(offset);
                    reply(exchange, 200, bytes(offset == 0 ? page(3, "CA") : page(3, "DE", "US")));
                });
        configure(options());
        Collector<SeaTunnelRow> collector = collector();
        reader.pollNext(collector);
        reader.pollNext(collector);
        Assertions.assertEquals(java.util.Arrays.asList(0, 1), offsets);
        Assertions.assertEquals(3, rows.size());
        verify(context, times(1)).signalNoMoreElement();
        // The common single-split snapshot has no page offset. A new reader replays.
        Assertions.assertNull(reader.snapshotState(1).get(0).getState());
        reader.close();
        configure(options());
        reader.pollNext(collector());
        Assertions.assertEquals(java.util.Arrays.asList(0, 1, 0, 1), offsets);
    }

    @Test
    void retriesTransientStatusAtSameOffset() throws Exception {
        AtomicInteger requests = new AtomicInteger();
        start(
                exchange -> {
                    JsonNode request = JSON.readTree(exchange.getRequestBody());
                    Assertions.assertEquals("0", request.path("offset").asText());
                    int call = requests.incrementAndGet();
                    reply(
                            exchange,
                            call == 1 ? 503 : 200,
                            call == 1
                                    ? "{}".getBytes(StandardCharsets.UTF_8)
                                    : bytes(page(1, "CA")));
                });
        Map<String, Object> options = options();
        options.put("max_retries", 1);
        configure(options);
        reader.pollNext(collector());
        Assertions.assertEquals(2, requests.get());
        Assertions.assertEquals(1, rows.size());
    }

    @Test
    void oversizedBodyIsNotRetriedAndDoesNotComplete() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        start(
                exchange -> {
                    calls.incrementAndGet();
                    reply(exchange, 200, new byte[2048]);
                });
        Map<String, Object> options = options();
        options.put("max_response_bytes", 1024);
        options.put("max_retries", 5);
        configure(options);
        IOException error =
                Assertions.assertThrows(IOException.class, () -> reader.pollNext(collector()));
        Assertions.assertTrue(error.getMessage().contains("size limit"));
        Assertions.assertEquals(1, calls.get());
        verify(context, never()).signalNoMoreElement();
    }

    @Test
    void permissionFailureDoesNotRetryOrLeakErrorBody() throws Exception {
        AtomicInteger calls = new AtomicInteger();
        start(
                exchange -> {
                    calls.incrementAndGet();
                    reply(
                            exchange,
                            403,
                            "{\"error\":\"private-token\"}".getBytes(StandardCharsets.UTF_8));
                });
        Map<String, Object> options = options();
        options.put("max_retries", 5);
        configure(options);
        IOException error =
                Assertions.assertThrows(IOException.class, () -> reader.pollNext(collector()));
        Assertions.assertTrue(error.getMessage().contains("403"));
        Assertions.assertFalse(error.getMessage().contains("private-token"));
        Assertions.assertNull(error.getCause());
        Assertions.assertEquals(1, calls.get());
        verify(context, never()).signalNoMoreElement();
    }

    @Test
    void laterPageFailureDoesNotSignalSuccessfulPartialReport() throws Exception {
        start(
                exchange -> {
                    int offset = JSON.readTree(exchange.getRequestBody()).path("offset").asInt();
                    reply(exchange, 200, bytes(offset == 0 ? page(2, "CA") : page(3, "US")));
                });
        configure(options());
        Assertions.assertThrows(IOException.class, () -> reader.pollNext(collector()));
        Assertions.assertEquals(1, rows.size());
        verify(context, never()).signalNoMoreElement();
    }

    @Test
    void cancellationWakesRetryWait() throws Exception {
        CountDownLatch arrived = new CountDownLatch(1);
        start(
                exchange -> {
                    exchange.getResponseHeaders().set("Retry-After", "30");
                    reply(exchange, 429, "{}".getBytes(StandardCharsets.UTF_8));
                    arrived.countDown();
                });
        Map<String, Object> options = options();
        options.put("max_retries", 3);
        configure(options);
        executor = Executors.newSingleThreadExecutor();
        AtomicReference<Thread> pollingThread = new AtomicReference<>();
        Future<?> poll =
                executor.submit(
                        () -> {
                            pollingThread.set(Thread.currentThread());
                            return Assertions.assertThrows(
                                    IOException.class, () -> reader.pollNext(collector()));
                        });
        Assertions.assertTrue(arrived.await(5, TimeUnit.SECONDS));
        org.awaitility.Awaitility.await()
                .atMost(5, TimeUnit.SECONDS)
                .until(
                        () ->
                                pollingThread.get() != null
                                        && pollingThread.get().getState()
                                                == Thread.State.TIMED_WAITING
                                        && java.util.Arrays.stream(
                                                        pollingThread.get().getStackTrace())
                                                .anyMatch(
                                                        frame ->
                                                                frame.getMethodName()
                                                                        .equals("awaitRetry")));
        reader.close();
        poll.get(5, TimeUnit.SECONDS);
        verify(context, never()).signalNoMoreElement();
    }

    @Test
    void cancellationAbortsActiveResponse() throws Exception {
        CountDownLatch arrived = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        start(
                exchange -> {
                    exchange.sendResponseHeaders(200, 0);
                    exchange.getResponseBody().write('{');
                    exchange.getResponseBody().flush();
                    arrived.countDown();
                    try {
                        release.await(5, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                });
        configure(options());
        executor = Executors.newSingleThreadExecutor();
        Future<?> poll =
                executor.submit(
                        () ->
                                Assertions.assertThrows(
                                        IOException.class, () -> reader.pollNext(collector())));
        try {
            Assertions.assertTrue(arrived.await(5, TimeUnit.SECONDS));
            reader.close();
            poll.get(5, TimeUnit.SECONDS);
        } finally {
            release.countDown();
        }
        verify(context, never()).signalNoMoreElement();
    }

    @Test
    void retryAfterIsBoundedAndHttpDateIsHonored() throws Exception {
        Assertions.assertEquals(3000, GoogleAnalytics4SourceReader.retryDelay("3", 1000, 5000, 0));
        Assertions.assertEquals(
                3000,
                GoogleAnalytics4SourceReader.retryDelay(
                        "Thu, 01 Jan 1970 00:00:03 GMT", 1000, 5000, 0));
        for (String value :
                java.util.Arrays.asList("3600", "999999999999999999999", "private-invalid")) {
            IOException error =
                    Assertions.assertThrows(
                            IOException.class,
                            () -> GoogleAnalytics4SourceReader.retryDelay(value, 1000, 30000, 0));
            Assertions.assertFalse(error.getMessage().contains(value));
        }
    }

    @Test
    void invalidCredentialFileFailsLocallyAndRedacts() throws Exception {
        Path file = temp.resolve("key.json");
        Files.write(file, "{\"private_key\":\"secret-key\"}".getBytes(StandardCharsets.UTF_8));
        Map<String, Object> options = options();
        options.remove("emulator_url");
        options.put("service_account_key_file", file.toString());
        reader =
                new GoogleAnalytics4SourceReader(
                        new GoogleAnalytics4Config(ReadonlyConfig.fromMap(options)),
                        new SingleSplitReaderContext(context));
        IOException error = Assertions.assertThrows(IOException.class, reader::open);
        Assertions.assertNull(error.getCause());
        Assertions.assertFalse(error.getMessage().contains(file.toString()));
        Assertions.assertFalse(error.getMessage().contains("secret-key"));
        Assertions.assertThrows(IOException.class, reader::open);
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void realServiceAccountRefreshesAfter401OrExpiryAndRedactsOAuthFailure(boolean unauthorized)
            throws Exception {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2048);
        KeyPair key = generator.generateKeyPair();
        AtomicInteger tokens = new AtomicInteger();
        AtomicInteger reports = new AtomicInteger();
        start(
                exchange -> {
                    if ("/token".equals(exchange.getRequestURI().getPath())) {
                        String form =
                                new String(
                                        GoogleAnalytics4HttpTransport.readBounded(
                                                exchange.getRequestBody(), 65536),
                                        StandardCharsets.UTF_8);
                        String assertion = null;
                        for (String pair : form.split("&")) {
                            if (pair.startsWith("assertion=")) {
                                assertion =
                                        URLDecoder.decode(
                                                pair.substring("assertion=".length()), "UTF-8");
                            }
                        }
                        Assertions.assertNotNull(assertion);
                        String[] parts = assertion.split("\\.");
                        Signature verifier = Signature.getInstance("SHA256withRSA");
                        verifier.initVerify(key.getPublic());
                        verifier.update(
                                (parts[0] + "." + parts[1]).getBytes(StandardCharsets.US_ASCII));
                        Assertions.assertTrue(
                                verifier.verify(Base64.getUrlDecoder().decode(parts[2])));
                        JsonNode claims = JSON.readTree(Base64.getUrlDecoder().decode(parts[1]));
                        Assertions.assertEquals(
                                "https://www.googleapis.com/auth/analytics.readonly",
                                claims.path("scope").asText());
                        Assertions.assertEquals(
                                "https://oauth2.googleapis.com/token", claims.path("aud").asText());
                        int count = tokens.incrementAndGet();
                        reply(
                                exchange,
                                count < 3 ? 200 : 400,
                                (count < 3
                                                ? "{\"access_token\":\"token-"
                                                        + count
                                                        + "\",\"expires_in\":"
                                                        + (unauthorized ? 3600 : 1)
                                                        + ",\"token_type\":\"Bearer\"}"
                                                : "{\"error\":\"invalid_grant\",\"error_description\":\"private-secret\"}")
                                        .getBytes(StandardCharsets.UTF_8));
                    } else {
                        int call = reports.incrementAndGet();
                        Assertions.assertEquals(
                                "Bearer token-" + (call == 1 ? 1 : 2),
                                exchange.getRequestHeaders().getFirst("Authorization"));
                        reply(
                                exchange,
                                call == 1 && unauthorized ? 401 : 200,
                                call == 1 && unauthorized
                                        ? "{}".getBytes(StandardCharsets.UTF_8)
                                        : bytes(
                                                unauthorized
                                                        ? page(1, "CA")
                                                        : page(2, call == 1 ? "CA" : "US")));
                    }
                });
        Path file = temp.resolve("service-account.json");
        org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode json =
                JSON.createObjectNode()
                        .put("type", "service_account")
                        .put("project_id", "fixture")
                        .put("client_id", "123")
                        .put("client_email", "fixture@example.iam.gserviceaccount.com")
                        .put("private_key_id", "fixture-key")
                        .put(
                                "private_key",
                                "-----BEGIN PRIVATE KEY-----\n"
                                        + Base64.getEncoder()
                                                .encodeToString(key.getPrivate().getEncoded())
                                        + "\n-----END PRIVATE KEY-----\n")
                        .put("token_uri", "https://oauth2.googleapis.com/token");
        Files.write(file, bytes(json));
        Map<String, Object> values = options();
        values.remove("emulator_url");
        values.put("service_account_key_file", file.toString());
        GoogleAnalytics4Config config = new GoogleAnalytics4Config(ReadonlyConfig.fromMap(values));
        // Only reroute the HTTP destination. Production key parsing, endpoint validation,
        // JWT signing, LowLevelHttpRequest/Response adapter and token refresh all execute.
        reader =
                new GoogleAnalytics4SourceReader(
                        config, new SingleSplitReaderContext(context), fixtureTransport(config));
        reader.open();
        reader.pollNext(collector());
        Assertions.assertEquals(2, tokens.get());
        Assertions.assertEquals(2, reports.get());
        Assertions.assertEquals(unauthorized ? 1 : 2, rows.size());
        reader.close();
        reader =
                new GoogleAnalytics4SourceReader(
                        config, new SingleSplitReaderContext(context), fixtureTransport(config));
        reader.open();
        IOException redacted =
                Assertions.assertThrows(IOException.class, () -> reader.pollNext(collector()));
        Assertions.assertFalse(redacted.getMessage().contains("private-secret"));
        Assertions.assertNull(redacted.getCause());
        Assertions.assertEquals(3, tokens.get());
        reader.close();
        json.put("token_uri", "http://private-secret.invalid/token");
        Files.write(file, bytes(json));
        reader = new GoogleAnalytics4SourceReader(config, new SingleSplitReaderContext(context));
        IOException invalidUri = Assertions.assertThrows(IOException.class, reader::open);
        Assertions.assertFalse(invalidUri.getMessage().contains("private-secret"));
        Assertions.assertNull(invalidUri.getCause());
    }

    private GoogleAnalytics4HttpTransport fixtureTransport(GoogleAnalytics4Config config) {
        return new GoogleAnalytics4HttpTransport(config) {
            @Override
            Response post(String url, byte[] body, String token, int limit) throws IOException {
                Assertions.assertTrue(
                        url.equals("https://oauth2.googleapis.com/token")
                                || url.equals(
                                        "https://analyticsdata.googleapis.com/v1beta/properties/123:runReport"));
                return super.post(origin + java.net.URI.create(url).getPath(), body, token, limit);
            }
        };
    }
}
