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

package org.apache.seatunnel.connectors.seatunnel.paypal.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class PayPalClientTest {
    @Test
    void statusPolicyWorksWithBodyless503429And401() throws Exception {
        Reply unavailable = new Reply(503, "");
        unavailable.bodyless = true;
        replies.add(unavailable);
        token(3600, "first-token");
        Reply unauthorized = new Reply(401, "");
        unauthorized.bodyless = true;
        replies.add(unauthorized);
        token(3600, "next-token");
        page(1, 0, "");
        assertNotNull(client().page(1));
        assertEquals(5, requests.size());
    }

    @Test
    void bodylessRateLimitUsesBoundedRetry() throws Exception {
        token(3600, "token");
        Reply limit = new Reply(429, "");
        limit.bodyless = true;
        replies.add(limit);
        page(1, 0, "");
        assertNotNull(client().page(1));
        assertEquals(3, requests.size());
    }

    @Test
    void repeatedOpenIsRejectedAndCloseIsIdempotent() throws Exception {
        PayPalSourceReader reader =
                new PayPalSourceReader(
                        new PayPalConfig(ReadonlyConfig.fromMap(options)),
                        new SingleSplitReaderContext(mock(SourceReader.Context.class)));
        reader.open();
        try {
            assertThrows(IllegalStateException.class, reader::open);
        } finally {
            reader.close();
            reader.close();
        }
        assertThrows(IllegalStateException.class, reader::open);
    }

    private HttpServer server;
    private ExecutorService executor;
    private final Queue<Reply> replies = new ConcurrentLinkedQueue<>();
    private final List<String> requests = Collections.synchronizedList(new ArrayList<>());
    private final CountDownLatch arrived = new CountDownLatch(1);
    private final CountDownLatch release = new CountDownLatch(1);
    private Map<String, Object> options;
    private PayPalClient client;

    @BeforeEach
    void start() throws Exception {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        executor = Executors.newCachedThreadPool();
        server.setExecutor(executor);
        server.createContext("/", this::serve);
        server.start();
        options = PayPalResponseTest.options();
        options.put("mock_mode", true);
        options.put("api_base_url", "http://127.0.0.1:" + server.getAddress().getPort());
        options.put("retry_delay_ms", 10);
        options.put("max_retries", 1);
    }

    private void serve(HttpExchange exchange) throws IOException {
        Reply reply = replies.poll();
        String body = new String(read(exchange.getRequestBody()), StandardCharsets.UTF_8);
        requests.add(
                exchange.getRequestMethod()
                        + " "
                        + exchange.getRequestURI()
                        + " "
                        + exchange.getRequestHeaders().getFirst("Authorization")
                        + " "
                        + body
                        + " "
                        + exchange.getRequestHeaders().getFirst("PayPal-Enforce-ISO8601-Format"));
        if (reply == null) {
            exchange.sendResponseHeaders(500, -1);
            exchange.close();
            return;
        }
        try {
            if (reply.delay > 0) {
                Thread.sleep(reply.delay);
            }
            if (reply.gzip) {
                exchange.getResponseHeaders().set("Content-Encoding", "gzip");
            }
            if (reply.retryAfter != null) {
                exchange.getResponseHeaders().set("Retry-After", reply.retryAfter);
            }
            if (reply.status == 302) {
                exchange.getResponseHeaders()
                        .set("Location", options.get("api_base_url") + "/credential-theft");
            }
            exchange.sendResponseHeaders(
                    reply.status, reply.bodyless ? -1 : reply.chunked ? 0 : reply.body.length);
            if (reply.bodyless) {
                arrived.countDown();
                return;
            }
            if (reply.block) {
                exchange.getResponseBody().write(' ');
                exchange.getResponseBody().flush();
                arrived.countDown();
                release.await(5, TimeUnit.SECONDS);
            } else {
                exchange.getResponseBody().write(reply.body);
                arrived.countDown();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (IOException ignored) {
            /* Cancellation intentionally closes the peer socket. */
        } finally {
            exchange.close();
        }
    }

    private static byte[] read(InputStream input) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        byte[] buffer = new byte[1024];
        int count;
        while ((count = input.read(buffer)) != -1) {
            bytes.write(buffer, 0, count);
        }
        return bytes.toByteArray();
    }

    private PayPalClient client() {
        client = new PayPalClient(new PayPalConfig(ReadonlyConfig.fromMap(options)));
        return client;
    }

    private void token(int lifetime, String value) {
        replies.add(
                new Reply(
                        200,
                        "{\"access_token\":\""
                                + value
                                + "\",\"token_type\":\"Bearer\",\"expires_in\":"
                                + lifetime
                                + "}"));
    }

    private Reply page(int number, int total, String records) {
        Reply reply = new Reply(200, PayPalResponseTest.page(number, total, records));
        replies.add(reply);
        return reply;
    }

    @AfterEach
    void stop() throws Exception {
        release.countDown();
        if (client != null) {
            client.close();
        }
        server.stop(0);
        executor.shutdownNow();
        assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
    }

    @Test
    void exchangesOAuthAndRefreshesExpiredTokenAcrossPages() throws Exception {
        token(1, "first-token");
        page(
                                1,
                                3,
                                PayPalResponseTest.record("123", "JPY")
                                        + ","
                                        + PayPalResponseTest.record("0.123", "TND"))
                        .delay =
                1200;
        token(3600, "next-token");
        page(2, 3, PayPalResponseTest.record("-1.23", "USD"));
        PayPalClient transport = client();
        assertEquals(1, transport.page(1).path("page").intValue());
        assertEquals(2, transport.page(2).path("page").intValue());
        assertEquals(4, requests.size());
        String basic =
                Base64.getEncoder()
                        .encodeToString("mock-client:mock-secret".getBytes(StandardCharsets.UTF_8));
        assertTrue(
                requests.get(0)
                        .startsWith(
                                "POST /v1/oauth2/token Basic "
                                        + basic
                                        + " grant_type=client_credentials"));
        assertTrue(requests.get(1).contains("balance_affecting_records_only=N"));
        assertTrue(requests.get(1).contains("fields=all"));
        assertTrue(requests.get(1).contains("page_size=2"));
        assertTrue(requests.get(1).contains("Bearer first-token"));
        assertTrue(requests.get(3).contains("Bearer next-token"));
        assertTrue(requests.get(3).endsWith("true"));
    }

    @Test
    void unauthorizedRefreshIsBoundedAndSanitized() throws Exception {
        token(3600, "first-token");
        replies.add(new Reply(401, "secret-error-body"));
        token(3600, "second-token");
        replies.add(new Reply(401, "secret-error-body"));
        Exception error = assertThrows(Exception.class, () -> client().page(1));
        assertTrue(error.getMessage().contains("401"));
        assertFalse(error.toString().contains("secret-error-body"));
        assertNull(error.getCause());
        assertEquals(4, requests.size());
    }

    @ParameterizedTest
    @ValueSource(ints = {400, 401, 403})
    void invalidOAuthCredentialsFailWithoutRetries(int status) {
        replies.add(
                new Reply(
                        status,
                        "{\"error\":\"invalid_client\",\"error_description\":\"secret-body\"}"));
        Exception error = assertThrows(Exception.class, () -> client().page(1));
        assertFalse(error.toString().contains("secret-body"));
        assertNull(error.getCause());
        assertEquals(1, requests.size());
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "{\"access_token\":\"secret-value\",\"expires_in\":-1,\"token_type\":\"Bearer\"}",
                "{\"access_token\":\"secret-value\",\"expires_in\":1.5,\"token_type\":\"Bearer\"}",
                "{\"access_token\":\"secret-value\",\"expires_in\":3600,\"token_type\":\"Basic\"}",
                "{\"access_token\":\"secret-value\",\"expires_in\":3600}"
            })
    void rejectsMalformedTokensWithoutCauses(String body) {
        replies.add(new Reply(200, body));
        Exception error = assertThrows(Exception.class, () -> client().page(1));
        assertFalse(error.toString().contains("secret-value"));
        assertNull(error.getCause());
    }

    @Test
    void retriesOAuthAndReportTransientResponses() throws Exception {
        replies.add(new Reply(503, "unavailable"));
        token(3600, "token");
        replies.add(new Reply(429, "rate-limited"));
        page(1, 0, "");
        assertEquals(0, client().page(1).path("total_items").intValue());
        assertEquals(4, requests.size());
    }

    @Test
    void rejectsLongRetryAfterAndRedirect() {
        token(3600, "token");
        Reply busy = new Reply(429, "secret-body");
        busy.retryAfter = "61";
        replies.add(busy);
        assertThrows(Exception.class, () -> client().page(1));
        assertEquals(2, requests.size());
    }

    @Test
    void neverFollowsRedirectWithCredentials() {
        replies.add(new Reply(302, "{}"));
        assertThrows(Exception.class, () -> client().page(1));
        assertEquals(1, requests.size());
        assertFalse(requests.get(0).contains("credential-theft"));
    }

    @Test
    void failsOnResultsetTooLargeAndPermissionErrors() throws Exception {
        token(3600, "token");
        replies.add(
                new Reply(400, "{\"name\":\"RESULTSET_TOO_LARGE\",\"message\":\"secret-body\"}"));
        Exception error = assertThrows(Exception.class, () -> client().page(1));
        assertTrue(error.getMessage().contains("narrow"));
        assertFalse(error.toString().contains("secret-body"));
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void boundsPlainAndGzipResponses(boolean gzip) throws Exception {
        options.put("max_response_bytes", 1024);
        Reply huge = new Reply(200, String.join("", Collections.nCopies(2048, "x")));
        huge.chunked = true;
        if (gzip) {
            ByteArrayOutputStream compressed = new ByteArrayOutputStream();
            try (GZIPOutputStream output = new GZIPOutputStream(compressed)) {
                output.write(huge.body);
            }
            huge.body = compressed.toByteArray();
            huge.gzip = true;
        }
        replies.add(huge);
        assertThrows(Exception.class, () -> client().page(1));
        assertEquals(1, requests.size());
    }

    @Test
    void deadlineAbortsStalledBodyAndRetryBudgetTerminates() {
        options.put("request_timeout_ms", 150);
        options.put("max_retries", 0);
        Reply stalled = new Reply(200, "{}");
        stalled.block = true;
        stalled.chunked = true;
        replies.add(stalled);
        long before = System.nanoTime();
        assertThrows(Exception.class, () -> client().page(1));
        assertTrue(TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - before) < 3000);
    }

    @Test
    void closeAbortsActiveBody() throws Exception {
        Reply stalled = new Reply(200, "{}");
        stalled.block = true;
        stalled.chunked = true;
        replies.add(stalled);
        PayPalClient transport = client();
        Future<?> result =
                executor.submit(() -> assertThrows(Exception.class, () -> transport.page(1)));
        assertTrue(arrived.await(3, TimeUnit.SECONDS));
        transport.close();
        result.get(3, TimeUnit.SECONDS);
    }

    @Test
    void closeWakesRetryWait() throws Exception {
        options.put("retry_delay_ms", 60000);
        replies.add(new Reply(503, "{}"));
        PayPalClient transport = client();
        CountDownLatch complete = new CountDownLatch(1);
        Thread worker =
                new Thread(
                        () -> {
                            try {
                                assertThrows(Exception.class, () -> transport.page(1));
                            } finally {
                                complete.countDown();
                            }
                        });
        worker.start();
        assertTrue(arrived.await(3, TimeUnit.SECONDS));
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
        while (worker.getState() != Thread.State.TIMED_WAITING && System.nanoTime() < deadline) {
            Thread.sleep(10);
        }
        assertEquals(Thread.State.TIMED_WAITING, worker.getState());
        transport.close();
        assertTrue(complete.await(3, TimeUnit.SECONDS));
        worker.join();
        assertEquals(1, requests.size());
    }

    @Test
    void readerPreservesRecordsAndSignalsCompletionOnlyAfterValidation() throws Exception {
        token(3600, "token");
        page(
                1,
                3,
                PayPalResponseTest.record("1", "USD")
                        + ","
                        + PayPalResponseTest.record("2", "USD"));
        page(2, 3, "{\"transaction_info\":{}}");
        SourceReader.Context context = mock(SourceReader.Context.class);
        Collector<SeaTunnelRow> collector = mock(Collector.class);
        when(collector.getCheckpointLock()).thenReturn(new Object());
        try (PayPalSourceReader reader =
                new PayPalSourceReader(
                        new PayPalConfig(ReadonlyConfig.fromMap(options)),
                        new SingleSplitReaderContext(context))) {
            reader.open();
            reader.pollNext(collector);
            reader.pollNext(collector);
            verify(collector, times(3)).collect(any(SeaTunnelRow.class));
            verify(context).signalNoMoreElement();
        }
    }

    @Test
    void changedTotalsFailWithoutCompletionSignal() throws Exception {
        token(3600, "token");
        page(
                1,
                3,
                PayPalResponseTest.record("1", "USD")
                        + ","
                        + PayPalResponseTest.record("2", "USD"));
        page(
                2,
                4,
                PayPalResponseTest.record("3", "USD")
                        + ","
                        + PayPalResponseTest.record("4", "USD"));
        SourceReader.Context context = mock(SourceReader.Context.class);
        Collector<SeaTunnelRow> collector = mock(Collector.class);
        try (PayPalSourceReader reader =
                new PayPalSourceReader(
                        new PayPalConfig(ReadonlyConfig.fromMap(options)),
                        new SingleSplitReaderContext(context))) {
            reader.open();
            assertThrows(Exception.class, () -> reader.internalPollNext(collector));
            verify(context, never()).signalNoMoreElement();
            verify(collector, times(2)).collect(any(SeaTunnelRow.class));
        }
    }

    private static class Reply {
        private final int status;
        private byte[] body;
        private int delay;
        private boolean gzip;
        private boolean chunked;
        private boolean block;
        private boolean bodyless;
        private String retryAfter;

        private Reply(int status, String body) {
            this.status = status;
            this.body = body.getBytes(StandardCharsets.UTF_8);
        }
    }
}
