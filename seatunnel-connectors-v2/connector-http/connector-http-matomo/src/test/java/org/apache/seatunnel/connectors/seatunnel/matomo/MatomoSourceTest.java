/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.matomo;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplit;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.http.Header;
import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.AuthenticationException;
import org.apache.http.auth.Credentials;
import org.apache.http.auth.InvalidCredentialsException;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.protocol.HttpClientContext;
import org.apache.http.conn.routing.HttpRoute;
import org.apache.http.impl.auth.AuthSchemeBase;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicHttpRequest;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.AbstractAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.apache.logging.log4j.core.config.Property;

import org.ietf.jgss.GSSException;
import org.junit.jupiter.api.Test;

import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import okhttp3.mockwebserver.SocketPolicy;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Exercises the real reporting reader's typed range, lifecycle and HTTP boundaries. Controlled
 * responses and completed poll calls distinguish early validation, cancellation and handoff
 * behavior without requiring a public Matomo account.
 */
class MatomoSourceTest {
    private static final LocalDate FIRST = LocalDate.of(2026, 6, 1);
    private static final LocalDate SECOND = FIRST.plusDays(1);
    private static final String METRICS = "{\"nb_visits\":25,\"nb_actions\":40}";

    @Test
    void parsesRangeInOrderAndIgnoresExplicitEmptyDays() throws IOException {
        MatomoSourceReader r = reader(URI.create("http://127.0.0.1"), SECOND, null);
        List<SeaTunnelRow> rows =
                r.decode("{\"" + SECOND + "\":[] ,\"" + FIRST + "\":" + METRICS + "}");
        assertEquals(1, rows.size());
        assertArrayEquals(
                new Object[] {1, FIRST.toString(), 25L, 40L, null}, rows.get(0).getFields());
        assertEquals("matomo", rows.get(0).getTableId());
    }

    @Test
    void rejectsMissingDaysAndApplicationErrorsWithoutRepeatingBody() {
        MatomoSourceReader r = reader(URI.create("http://127.0.0.1"), SECOND, null);
        assertThrows(IOException.class, () -> r.decode("{\"" + FIRST + "\":" + METRICS + "}"));
        IOException e =
                assertThrows(
                        IOException.class,
                        () ->
                                r.decode(
                                        "{\"result\":\"error\",\"message\":\"synthetic-sensitive\"}"));
        assertFalse(e.getMessage().contains("synthetic-sensitive"));
        assertThrows(
                IOException.class,
                () ->
                        reader(URI.create("http://127.0.0.1"), FIRST, null)
                                .decode("{\"nb_visits\":-1,\"nb_actions\":40}"));
        assertThrows(
                IOException.class,
                () ->
                        reader(URI.create("http://127.0.0.1"), FIRST, null)
                                .decode("{\"nb_visits\":9223372036854775808,\"nb_actions\":40}"));
    }

    @Test
    void requiresOneCompleteJsonDocumentAndRetainsValidMetricShapes() throws IOException {
        MatomoSourceReader reader = reader(URI.create("http://127.0.0.1"), FIRST, null);
        for (String suffix : new String[] {"{}", "{\"result\":\"error\"}", "trailing"}) {
            assertEquals(
                    "Matomo returned invalid JSON",
                    assertThrows(IOException.class, () -> reader.decode(METRICS + suffix))
                            .getMessage());
        }
        assertArrayEquals(
                new Object[] {1, FIRST.toString(), 25L, 40L, null},
                reader.decode("  " + METRICS + " \n ").get(0).getFields());
        assertArrayEquals(
                new Object[] {1, FIRST.toString(), 25L, 40L, 7L},
                reader.decode(
                                "{\"nb_visits\":\"25\",\"nb_actions\":40,\"nb_uniq_visitors\":7,\"雪🦊\":{\"unknown\":true}}")
                        .get(0)
                        .getFields());
        assertTrue(reader.decode("[]").isEmpty());
    }

    @Test
    void usesAnonymousGetAndPrivatePostWithoutTokenInUri() throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            server.enqueue(new MockResponse().setBody(METRICS));
            server.enqueue(new MockResponse().setBody(METRICS));
            server.start();
            MatomoSourceReader publicReader = reader(server.url("/index.php").uri(), FIRST, null);
            MatomoSourceReader formReader =
                    reader(server.url("/index.php").uri(), FIRST, "fake +&token");
            publicReader.open();
            formReader.open();
            try {
                assertEquals(1, publicReader.readRows().size());
                assertEquals(1, formReader.readRows().size());
                RecordedRequest get = server.takeRequest(1, TimeUnit.SECONDS);
                RecordedRequest post = server.takeRequest(1, TimeUnit.SECONDS);
                assertEquals("GET", get.getMethod());
                assertTrue(get.getPath().contains("method=VisitsSummary.get"));
                assertEquals("POST", post.getMethod());
                assertEquals("/index.php", post.getPath());
                assertTrue(post.getBody().readUtf8().contains("token_auth=fake+%2B%26token"));
            } finally {
                publicReader.close();
                formReader.close();
            }
        }
    }

    @Test
    void rejectsRedirectsAndHttpErrors() throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            server.enqueue(
                    new MockResponse().setResponseCode(302).setHeader("Location", "/another"));
            server.enqueue(new MockResponse().setResponseCode(429));
            server.start();
            MatomoSourceReader r = reader(server.url("/index.php").uri(), FIRST, null);
            r.open();
            try {
                assertTrue(
                        assertThrows(IOException.class, r::readRows).getMessage().contains("302"));
                assertTrue(
                        assertThrows(IOException.class, r::readRows).getMessage().contains("429"));
                assertEquals(2, server.getRequestCount());
            } finally {
                r.close();
            }
        }
    }

    @Test
    void cancellationBeforeOpenIsStickyAndDoesNotSendAnyRequest() throws Exception {
        try (MockWebServer server = new MockWebServer();
                MatomoSourceReader reader = reader(server.url("/index.php").uri(), FIRST, null)) {
            server.enqueue(new MockResponse().setBody(METRICS));
            reader.cancel();
            reader.cancel();
            reader.open();
            assertCancelled(assertThrows(IOException.class, reader::readRows));
            reader.close();
            reader.open();
            assertCancelled(assertThrows(IOException.class, reader::readRows));
            assertEquals(0, server.getRequestCount());
        }
    }

    @Test
    void cancellationAbortsAnInFlightRequest() throws Exception {
        assertStopsInFlight(false);
    }

    @Test
    void closingAbortsAnInFlightRequestAndPreventsReopening() throws Exception {
        assertStopsInFlight(true);
    }

    @Test
    void pollDoesNotHoldCheckpointLockDuringHttpAndEmitsNothingAfterCancellation()
            throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            server.enqueue(new MockResponse().setSocketPolicy(SocketPolicy.NO_RESPONSE));
            URI uri = server.url("/index.php").uri();
            MatomoSourceReader reader =
                    new MatomoSourceReader(uri, 1, FIRST, FIRST, null, 60_000, null, "matomo");
            ExecutorService worker = Executors.newFixedThreadPool(2);
            Collector<SeaTunnelRow> collector = rejectingCollector();
            try {
                reader.open();
                Future<?> poll =
                        worker.submit(
                                () -> {
                                    reader.pollNext(collector);
                                    return null;
                                });
                assertNotNull(server.takeRequest(10, TimeUnit.SECONDS));
                Future<?> checkpoint =
                        worker.submit(
                                () -> {
                                    synchronized (collector.getCheckpointLock()) {
                                        return null;
                                    }
                                });
                checkpoint.get(5, TimeUnit.SECONDS);
                reader.cancel();
                poll.get(5, TimeUnit.SECONDS);
            } finally {
                reader.close();
                worker.shutdownNow();
                assertTrue(worker.awaitTermination(5, TimeUnit.SECONDS));
            }
        }
    }

    @Test
    void pollPropagatesNonCancellationHttpErrors() throws Exception {
        try (MockWebServer server = new MockWebServer();
                MatomoSourceReader reader = reader(server.url("/index.php").uri(), FIRST, null)) {
            server.enqueue(new MockResponse().setResponseCode(429));
            reader.open();
            assertTrue(
                    assertThrows(IOException.class, () -> reader.pollNext(rejectingCollector()))
                            .getMessage()
                            .contains("429"));
        }
    }

    @Test
    void validatesHttpsForPrivateTokensAndDateBounds() {
        Map<String, Object> c = new HashMap<>();
        c.put("url", "http://127.0.0.1/index.php");
        c.put("site_id", 1);
        c.put("start_date", FIRST.toString());
        c.put("end_date", SECOND.toString());
        assertEquals("Matomo", new MatomoSource(ReadonlyConfig.fromMap(c)).getPluginName());
        c.put("token", "fake");
        assertThrows(
                IllegalArgumentException.class, () -> new MatomoSource(ReadonlyConfig.fromMap(c)));
        c.put("url", "https://example.org/index.php");
        c.put("end_date", "2029-12-31");
        assertThrows(
                IllegalArgumentException.class, () -> new MatomoSource(ReadonlyConfig.fromMap(c)));
    }

    @Test
    void progressingResponseExpiresWithoutPartialRowsAndDoesNotPoisonTheNextFetch()
            throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            String body =
                    METRICS.substring(0, METRICS.length() - 1)
                            + ",\"unused\":\""
                            + "x".repeat(128)
                            + "\"}";
            server.enqueue(
                    new MockResponse().setBody(body).throttleBody(1, 50, TimeUnit.MILLISECONDS));
            try (MatomoSourceReader reader =
                    new MatomoSourceReader(
                            server.url("/index.php").uri(),
                            1,
                            FIRST,
                            FIRST,
                            null,
                            3000,
                            null,
                            "matomo",
                            2000)) {
                reader.open();
                ExecutorService worker = Executors.newSingleThreadExecutor();
                try {
                    Future<?> poll =
                            worker.submit(
                                    () -> {
                                        reader.pollNext(rejectingCollector());
                                        return null;
                                    });
                    assertNotNull(server.takeRequest(1, TimeUnit.SECONDS));
                    ExecutionException failure =
                            assertThrows(
                                    ExecutionException.class, () -> poll.get(8, TimeUnit.SECONDS));
                    assertEquals(
                            "Matomo request exceeded the elapsed fetch timeout",
                            failure.getCause().getMessage());
                    assertEquals(1, server.getRequestCount());
                    server.enqueue(new MockResponse().setBody(METRICS));
                    assertEquals(1, reader.readRows().size());
                    assertEquals(2, server.getRequestCount());
                } finally {
                    worker.shutdownNow();
                    assertTrue(worker.awaitTermination(5, TimeUnit.SECONDS));
                }
            }
        }
    }

    @Test
    void preparedRowsYieldOncePerPollAndNullStateRestoresTheWholeInterval() throws Exception {
        for (boolean cancelAfterFirst : new boolean[] {false, true}) {
            try (MockWebServer server = new MockWebServer()) {
                LocalDate third = SECOND.plusDays(1);
                String body =
                        "{\"" + FIRST + "\":" + METRICS + ",\"" + SECOND + "\":" + METRICS + ",\""
                                + third + "\":" + METRICS + "}";
                server.enqueue(new MockResponse().setBody(body));
                SourceReader.Context context = mock(SourceReader.Context.class);
                List<SingleSplit> checkpoint;
                try (MatomoSourceReader reader =
                        new MatomoSourceReader(
                                server.url("/index.php").uri(),
                                1,
                                FIRST,
                                third,
                                null,
                                3000,
                                new SingleSplitReaderContext(context),
                                "matomo")) {
                    reader.open();
                    List<SeaTunnelRow> rows = new ArrayList<>();
                    Collector<SeaTunnelRow> output = collecting(rows);
                    reader.pollNext(output);
                    assertEquals(1, rows.size());
                    assertEquals(FIRST.toString(), rows.get(0).getField(1));
                    verify(context, never()).signalNoMoreElement();
                    checkpoint = reader.snapshotState(7);
                    assertEquals(1, checkpoint.size());
                    assertNull(checkpoint.get(0).getState());
                    assertEquals(1, server.getRequestCount());
                    if (cancelAfterFirst) {
                        reader.cancel();
                        reader.pollNext(output);
                        assertEquals(1, rows.size());
                        verify(context, never()).signalNoMoreElement();
                    } else {
                        reader.pollNext(output);
                        assertEquals(2, rows.size());
                        assertEquals(SECOND.toString(), rows.get(1).getField(1));
                        verify(context, never()).signalNoMoreElement();
                        reader.pollNext(output);
                        assertEquals(3, rows.size());
                        assertEquals(third.toString(), rows.get(2).getField(1));
                        verify(context, times(1)).signalNoMoreElement();
                        reader.pollNext(output);
                        assertEquals(3, rows.size());
                        verify(context, times(1)).signalNoMoreElement();
                    }
                    assertEquals(1, server.getRequestCount());
                }
                server.enqueue(new MockResponse().setBody(body));
                SourceReader.Context restored = mock(SourceReader.Context.class);
                try (MatomoSourceReader reader =
                        new MatomoSourceReader(
                                server.url("/index.php").uri(),
                                1,
                                FIRST,
                                third,
                                null,
                                3000,
                                new SingleSplitReaderContext(restored),
                                "matomo")) {
                    reader.addSplits(checkpoint);
                    reader.open();
                    List<SeaTunnelRow> replay = new ArrayList<>();
                    Collector<SeaTunnelRow> output = collecting(replay);
                    reader.pollNext(output);
                    reader.pollNext(output);
                    reader.pollNext(output);
                    assertEquals(3, replay.size());
                    assertEquals(FIRST.toString(), replay.get(0).getField(1));
                    assertEquals(SECOND.toString(), replay.get(1).getField(1));
                    assertEquals(third.toString(), replay.get(2).getField(1));
                    verify(restored, times(1)).signalNoMoreElement();
                    assertEquals(2, server.getRequestCount());
                }
            }
        }
    }

    @Test
    void diagnosticLoggingWithActualPrivateFormAndFramingNeverExposesUpstreamContent()
            throws Exception {
        String token = "matomo-private-token-marker";
        String report = "314159265358";
        String marker = "matomo-private-framing-marker";
        String cookie = "matomo-private-cookie-marker";
        String badCookie = "matomo-private-malformed-cookie-marker";
        String authUser = "matomo-private-auth-user";
        String authPassword = "matomo-private-auth-password";
        String realm = "matomo-private-auth-realm";
        String gssChallenge = "matomo-private-gss-challenge";
        String gssToken = "matomo-private-gss-token";
        LoggerContext logging = (LoggerContext) LogManager.getContext(false);
        Configuration configuration = logging.getConfiguration();
        List<String> categories =
                Arrays.asList("org.apache.http", "org.apache.http.headers", "org.apache.http.wire");
        Map<String, LoggerConfig> previous = new HashMap<>();
        List<String> messages = new CopyOnWriteArrayList<>();
        AbstractAppender capture =
                new AbstractAppender(
                        "matomo-http-log-capture", null, null, true, Property.EMPTY_ARRAY) {
                    @Override
                    public void append(LogEvent event) {
                        messages.add(event.getMessage().getFormattedMessage());
                    }
                };
        capture.start();
        Level previousRoot = configuration.getRootLogger().getLevel();
        configuration.getRootLogger().setLevel(Level.DEBUG);
        configuration.getRootLogger().addAppender(capture, Level.DEBUG, null);
        for (String category : categories) {
            previous.put(category, configuration.getLoggers().get(category));
            LoggerConfig diagnostic = new LoggerConfig(category, Level.DEBUG, false);
            diagnostic.addAppender(capture, Level.DEBUG, null);
            configuration.removeLogger(category);
            configuration.addLogger(category, diagnostic);
        }
        logging.updateLoggers();
        try (MockWebServer server = new MockWebServer()) {
            Log wire = LogFactory.getLog("org.apache.http.wire");
            assertTrue(wire.isDebugEnabled());
            assertTrue(LogFactory.getLog("org.apache.http.headers").isDebugEnabled());
            wire.debug("matomo-debug-positive-control");
            LogManager.getLogger(MatomoHttpClient.class)
                    .debug("matomo-root-debug-positive-control");
            server.enqueue(
                    new MockResponse()
                            .setHeader("Set-Cookie", "session=" + cookie + "; Path=/")
                            .setBody("{\"nb_visits\":" + report + ",\"nb_actions\":40}"));
            // Only this loopback transport fixture bypasses the unchanged Source HTTPS guard.
            try (MatomoSourceReader reader = reader(server.url("/index.php").uri(), FIRST, token)) {
                reader.open();
                assertEquals(Long.valueOf(report), reader.readRows().get(0).getField(2));
                RecordedRequest request = server.takeRequest(1, TimeUnit.SECONDS);
                assertNotNull(request);
                assertEquals("POST", request.getMethod());
                assertEquals("/index.php", request.getPath());
                assertTrue(request.getBody().readUtf8().contains("token_auth=" + token));
                server.enqueue(
                        new MockResponse()
                                .setHeader("Set-Cookie", "=" + badCookie)
                                .setBody(METRICS));
                assertEquals(1, reader.readRows().size());
                RecordedRequest withCookie = server.takeRequest(1, TimeUnit.SECONDS);
                assertNotNull(withCookie);
                assertEquals("session=" + cookie, withCookie.getHeader("Cookie"));
                server.enqueue(new MockResponse().setStatus("HTTP/1.1 invalid " + marker));
                server.enqueue(
                        new MockResponse()
                                .setHeader("Transfer-Encoding", "chunked")
                                .setBody(marker + "\r\n"));
                server.enqueue(
                        new MockResponse()
                                .setHeader("X-Private", marker.repeat(1024))
                                .setBody(METRICS));
                for (String expected :
                        Arrays.asList(
                                "Matomo HTTP exchange failed",
                                "Matomo response could not be read",
                                "Matomo HTTP exchange failed")) {
                    IOException failure = assertThrows(IOException.class, reader::readRows);
                    assertEquals(expected, failure.getMessage());
                    assertNull(failure.getCause());
                    assertEquals(0, failure.getSuppressed().length);
                }
                assertEquals(5, server.getRequestCount());
            }
            for (boolean proxy : new boolean[] {false, true}) {
                assertNativeAuthentication(proxy, authUser, authPassword, realm);
                assertGssEnvelope(proxy, false, gssChallenge, gssToken);
                assertGssEnvelope(proxy, true, gssChallenge, gssToken);
            }
            String text = String.join("\n", messages);
            assertTrue(text.contains("matomo-debug-positive-control"));
            assertTrue(
                    text.contains("Connection request:"), "Capture actual connection diagnostics");
            assertFalse(text.contains(token));
            assertFalse(text.contains(report));
            assertFalse(text.contains(marker));
            assertTrue(text.contains("matomo-root-debug-positive-control"));
            for (String secret :
                    Arrays.asList(
                            cookie,
                            badCookie,
                            authUser,
                            authPassword,
                            realm,
                            gssChallenge,
                            gssToken,
                            Base64.encodeBase64String(
                                    (authUser + ":" + authPassword)
                                            .getBytes(StandardCharsets.ISO_8859_1)),
                            Base64.encodeBase64String(
                                    gssChallenge.getBytes(StandardCharsets.UTF_8)),
                            Base64.encodeBase64String(gssToken.getBytes(StandardCharsets.UTF_8)))) {
                assertFalse(text.contains(secret));
            }
        } finally {
            for (String category : categories) {
                configuration.removeLogger(category);
                if (previous.get(category) != null) {
                    configuration.addLogger(category, previous.get(category));
                }
            }
            configuration.getRootLogger().removeAppender(capture.getName());
            configuration.getRootLogger().setLevel(previousRoot);
            logging.updateLoggers();
            capture.stop();
        }
    }

    @Test
    void proxyRefusalAdmissionPrecedesNativeBufferingAndAuthenticationDrain() throws Exception {
        // Missing terminal chunks make the old buffer/drain wait for its 30-second idle timeout.
        // The modest over-limit body tests progress, not an actual OOM allocation.
        for (int mode = 0; mode < 3; mode++) {
            try (MockWebServer proxy = new MockWebServer()) {
                MockResponse refusal = new MockResponse().setResponseCode(mode == 1 ? 407 : 403);
                if (mode == 2) {
                    refusal.setBody("").setHeader("Content-Length", 3 * 1024 * 1024);
                } else {
                    String body = "x".repeat(2 * 1024 * 1024 + 1);
                    refusal.setBody(Integer.toHexString(body.length()) + "\r\n" + body + "\r\n")
                            .setHeader("Transfer-Encoding", "chunked");
                }
                if (mode == 1) {
                    refusal.setHeader("Proxy-Authenticate", "Basic realm=\"proxy-budget-fixture\"");
                }
                proxy.enqueue(refusal);
                proxy.start();
                BasicCredentialsProvider credentials = new BasicCredentialsProvider();
                credentials.setCredentials(
                        AuthScope.ANY,
                        new UsernamePasswordCredentials(
                                "proxy-fixture-user", "proxy-fixture-password"));
                HttpHost route = new HttpHost(proxy.getHostName(), proxy.getPort());
                CloseableHttpClient client =
                        MatomoHttpClient.create(
                                30000,
                                builder ->
                                        builder.setProxy(route)
                                                .setDefaultCredentialsProvider(credentials));
                ExecutorService worker = Executors.newSingleThreadExecutor();
                try {
                    Future<?> response =
                            worker.submit(
                                    () -> {
                                        try (CloseableHttpResponse ignored =
                                                client.execute(
                                                        new HttpGet(
                                                                "https://matomo-refusal.invalid/index.php"))) {
                                            return null;
                                        }
                                    });
                    RecordedRequest connect = proxy.takeRequest(2, TimeUnit.SECONDS);
                    assertNotNull(connect);
                    assertEquals("CONNECT", connect.getMethod());
                    ExecutionException failure =
                            assertThrows(
                                    ExecutionException.class,
                                    () -> response.get(5, TimeUnit.SECONDS));
                    assertTrue(
                            failure.getCause() instanceof MatomoHttpClient.ProxyBodyLimitException);
                    assertEquals(
                            "Matomo proxy response exceeds the 2 MiB limit",
                            failure.getCause().getMessage());
                    assertEquals(1, proxy.getRequestCount());
                } finally {
                    // Close the real connection before joining an old implementation held in
                    // native buffering/consume; interruption alone does not unblock socket reads.
                    client.close();
                    worker.shutdownNow();
                    assertTrue(worker.awaitTermination(5, TimeUnit.SECONDS));
                }
            }
        }
    }

    private static MatomoSourceReader reader(URI base, LocalDate end, String token) {
        return new MatomoSourceReader(base, 1, FIRST, end, token, 3000, null, "matomo");
    }

    private static void assertStopsInFlight(boolean close) throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            server.enqueue(new MockResponse().setSocketPolicy(SocketPolicy.NO_RESPONSE));
            URI uri = server.url("/index.php").uri();
            MatomoSourceReader reader =
                    new MatomoSourceReader(uri, 1, FIRST, FIRST, null, 60_000, null, "matomo");
            ExecutorService worker = Executors.newSingleThreadExecutor();
            try {
                reader.open();
                Future<List<SeaTunnelRow>> rows = worker.submit(reader::readRows);
                assertNotNull(server.takeRequest(10, TimeUnit.SECONDS));
                if (close) {
                    reader.close();
                } else {
                    reader.cancel();
                }
                assertCancelled(
                        assertThrows(ExecutionException.class, () -> rows.get(5, TimeUnit.SECONDS))
                                .getCause());
                reader.open();
                assertCancelled(assertThrows(IOException.class, reader::readRows));
                assertEquals(1, server.getRequestCount());
            } finally {
                reader.close();
                worker.shutdownNow();
                assertTrue(worker.awaitTermination(5, TimeUnit.SECONDS));
            }
        }
    }

    private static Collector<SeaTunnelRow> rejectingCollector() {
        return new Collector<SeaTunnelRow>() {
            private final Object checkpointLock = new Object();

            @Override
            public void collect(SeaTunnelRow record) {
                throw new AssertionError("Cancelled or failed reads must not emit rows");
            }

            @Override
            public Object getCheckpointLock() {
                return checkpointLock;
            }
        };
    }

    private static void assertCancelled(Throwable error) {
        assertTrue(error instanceof IOException);
        assertEquals("Matomo source was cancelled", error.getMessage());
    }

    /*
     * Records actual accepted rows under a stable checkpoint monitor used by the reader.
     */
    private static Collector<SeaTunnelRow> collecting(List<SeaTunnelRow> rows) {
        return new Collector<SeaTunnelRow>() {
            @Override
            public void collect(SeaTunnelRow row) {
                rows.add(row);
            }

            @Override
            public Object getCheckpointLock() {
                return this;
            }
        };
    }
    /*
     * Exercises real native 401/407 authentication, not a mock response from the client. The
     * private builder seam supplies only fixture credentials/routing, without changing JVM state.
     */
    private static void assertNativeAuthentication(
            boolean proxy, String user, String password, String realm) throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            String challengeName = proxy ? "Proxy-Authenticate" : "WWW-Authenticate";
            String responseName = proxy ? "Proxy-Authorization" : "Authorization";
            server.enqueue(
                    new MockResponse()
                            .setResponseCode(proxy ? 407 : 401)
                            .setHeader(challengeName, "Basic realm=\"" + realm + "\"")
                            .setBody("challenge"));
            server.enqueue(new MockResponse().setBody(METRICS));
            server.start();
            HttpHost route = new HttpHost(server.getHostName(), server.getPort());
            BasicCredentialsProvider credentials = new BasicCredentialsProvider();
            credentials.setCredentials(
                    AuthScope.ANY, new UsernamePasswordCredentials(user, password));
            try (CloseableHttpClient client =
                    MatomoHttpClient.create(
                            3000,
                            builder -> {
                                builder.setDefaultCredentialsProvider(credentials);
                                if (proxy) {
                                    builder.setProxy(route);
                                }
                            })) {
                URI target =
                        proxy
                                ? URI.create("http://matomo-auth-target.invalid/index.php")
                                : server.url("/index.php").uri();
                try (CloseableHttpResponse response = client.execute(new HttpGet(target))) {
                    assertEquals(200, response.getStatusLine().getStatusCode());
                    assertEquals(
                            METRICS,
                            EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8));
                }
                RecordedRequest first = server.takeRequest(1, TimeUnit.SECONDS);
                RecordedRequest second = server.takeRequest(1, TimeUnit.SECONDS);
                assertNotNull(first);
                assertNotNull(second);
                assertNull(first.getHeader(responseName));
                assertEquals(
                        "Basic "
                                + Base64.encodeBase64String(
                                        (user + ":" + password)
                                                .getBytes(StandardCharsets.ISO_8859_1)),
                        second.getHeader(responseName));
                assertEquals(2, server.getRequestCount());
            }
        }
    }

    /*
     * Calls the actual quiet GSS envelope through native public challenge/authentication methods.
     * Only the protected cryptographic generator is controlled; this is not Kerberos acceptance.
     */
    private static void assertGssEnvelope(
            boolean proxy, boolean kerberos, String challengeText, String tokenText)
            throws Exception {
        byte[] challenge = challengeText.getBytes(StandardCharsets.UTF_8);
        byte[] token = tokenText.getBytes(StandardCharsets.UTF_8);
        Credentials credentials =
                new UsernamePasswordCredentials("gss-fixture-user", "gss-fixture-password");
        AtomicInteger generated = new AtomicInteger();
        AuthSchemeBase scheme;
        if (kerberos) {
            scheme =
                    new MatomoHttpClient.QuietKerberos() {
                        @Override
                        protected byte[] generateToken(
                                byte[] input, String server, Credentials supplied) {
                            assertArrayEquals(challenge, input);
                            assertSame(credentials, supplied);
                            assertFalse(server.isEmpty());
                            assertFalse(server.contains(":"), "Native default strips the port");
                            generated.incrementAndGet();
                            return token;
                        }
                    };
        } else {
            scheme =
                    new MatomoHttpClient.QuietSpnego() {
                        @Override
                        protected byte[] generateToken(
                                byte[] input, String server, Credentials supplied) {
                            assertArrayEquals(challenge, input);
                            assertSame(credentials, supplied);
                            assertFalse(server.isEmpty());
                            assertFalse(server.contains(":"), "Native default strips the port");
                            generated.incrementAndGet();
                            return token;
                        }
                    };
        }
        BasicHttpRequest request = new BasicHttpRequest("GET", "/index.php");
        assertFalse(scheme.isComplete());
        assertThrows(
                AuthenticationException.class, () -> scheme.authenticate(credentials, request));
        Header header =
                new BasicHeader(
                        proxy ? "Proxy-Authenticate" : "WWW-Authenticate",
                        scheme.getSchemeName() + " " + Base64.encodeBase64String(challenge));
        scheme.processChallenge(header);
        assertFalse(scheme.isComplete());
        HttpClientContext context = HttpClientContext.create();
        context.setAttribute(
                HttpClientContext.HTTP_ROUTE,
                new HttpRoute(
                        new HttpHost("localhost", 9876),
                        null,
                        new HttpHost("localhost", 8765),
                        false));
        Header response = scheme.authenticate(credentials, request, context);
        assertEquals(proxy ? "Proxy-Authorization" : "Authorization", response.getName());
        assertEquals("Negotiate " + Base64.encodeBase64String(token), response.getValue());
        assertTrue(scheme.isComplete());
        assertEquals(response.getValue(), scheme.authenticate(credentials, request).getValue());
        assertEquals(1, generated.get());
        scheme.processChallenge(header);
        assertTrue(scheme.isComplete());
        AuthenticationException rejected =
                assertThrows(
                        AuthenticationException.class,
                        () -> scheme.authenticate(credentials, request, context));
        assertNull(rejected.getCause());
        assertFalse(rejected.getMessage().contains(challengeText));
        assertFalse(rejected.getMessage().contains(tokenText));
        for (int major : new int[] {GSSException.NO_CRED, GSSException.DEFECTIVE_TOKEN}) {
            MatomoHttpClient.QuietSpnego failing =
                    new MatomoHttpClient.QuietSpnego() {
                        @Override
                        protected byte[] generateToken(
                                byte[] input, String server, Credentials supplied)
                                throws GSSException {
                            throw new GSSException(major, -1, tokenText);
                        }
                    };
            failing.processChallenge(
                    new BasicHeader(
                            "WWW-Authenticate",
                            "Negotiate " + Base64.encodeBase64String(challenge)));
            AuthenticationException failure =
                    assertThrows(
                            AuthenticationException.class,
                            () -> failing.authenticate(credentials, request, context));
            assertEquals(
                    major == GSSException.NO_CRED, failure instanceof InvalidCredentialsException);
            assertTrue(failing.isComplete());
            assertNull(failure.getCause());
            assertFalse(failure.getMessage().contains(tokenText));
        }
    }
}
