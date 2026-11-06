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

package org.apache.seatunnel.connectors.seatunnel.mailchimp;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.catalog.CatalogTableUtil;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplit;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.sun.net.httpserver.HttpServer;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import okhttp3.mockwebserver.SocketPolicy;

import java.io.IOException;
import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class MailchimpSourceReaderTest {
    private MockWebServer server;
    private MailchimpSourceReader reader;
    private SourceReader.Context context;
    private Collector<SeaTunnelRow> output;
    private final List<SeaTunnelRow> rows = new ArrayList<>();

    static String member(String id) {
        return "{\"id\":\""
                + id
                + "\",\"email_address\":\""
                + id
                + "@example.invalid\",\"status\":\"subscribed\",\"last_changed\":\"2026-01-01T00:00:00Z\","
                + "\"merge_fields\":{\"FNAME\":\"Example\"}}";
    }

    static String page(int total, String... members) {
        return "{\"total_items\":" + total + ",\"members\":[" + String.join(",", members) + "]}";
    }

    @BeforeEach
    @SuppressWarnings("unchecked")
    void setUp() throws Exception {
        server = new MockWebServer();
        server.start();
        context = mock(SourceReader.Context.class);
        output = mock(Collector.class);
        when(output.getCheckpointLock()).thenReturn(new Object());
        doAnswer(
                        call -> {
                            rows.add(call.getArgument(0));
                            return null;
                        })
                .when(output)
                .collect(any(SeaTunnelRow.class));
        reader = newReader(2, 1);
        reader.open();
    }

    private MailchimpSourceReader newReader(int retry, int backoff) {
        return newReader(retry, backoff, "map<string,string>");
    }

    @SuppressWarnings("unchecked")
    private MailchimpSourceReader newReader(int retry, int backoff, String mergeType) {
        Map<String, Object> opts = MailchimpConfigTest.options();
        Map<String, Object> schema = new LinkedHashMap<>((Map<String, Object>) opts.get("schema"));
        Map<String, Object> fields =
                new LinkedHashMap<>((Map<String, Object>) schema.get("fields"));
        fields.put("merge_fields", mergeType);
        schema.put("fields", fields);
        opts.put("schema", schema);
        opts.put("base_url", server.url("/3.0").toString());
        opts.put("allow_insecure_http", true);
        opts.put("page_size", 2);
        opts.put("retry", retry);
        opts.put("retry_backoff_ms", backoff);
        opts.put("request_timeout_ms", 30000);
        return new MailchimpSourceReader(
                new MailchimpConfig(MailchimpConfigTest.config(opts)),
                CatalogTableUtil.buildWithConfig(MailchimpConfigTest.config(opts)),
                new SingleSplitReaderContext(context));
    }

    @AfterEach
    void tearDown() throws Exception {
        reader.close();
        server.shutdown();
    }

    @Test
    void readsMultiplePagesAndSchemaWithoutExtraRequest() throws Exception {
        server.enqueue(new MockResponse().setBody(page(3, member("one"), member("two"))));
        server.enqueue(new MockResponse().setBody(page(3, member("three"))));
        reader.pollNext(output);
        reader.pollNext(output);
        Assertions.assertEquals(3, rows.size());
        Assertions.assertEquals("one", rows.get(0).getField(0));
        Assertions.assertEquals("three@example.invalid", rows.get(2).getField(1));
        Assertions.assertEquals(
                Collections.singletonMap("FNAME", "Example"), rows.get(0).getField(4));
        Assertions.assertEquals("default.default.default", rows.get(0).getTableId());
        Assertions.assertEquals(2, server.getRequestCount());
        RecordedRequest first = server.takeRequest();
        Assertions.assertEquals("GET", first.getMethod());
        Assertions.assertEquals("/3.0/lists/audience1/members?count=2&offset=0", first.getPath());
        Assertions.assertEquals(
                "Basic "
                        + Base64.getEncoder()
                                .encodeToString(
                                        "seatunnel:testkey-us1".getBytes(StandardCharsets.UTF_8)),
                first.getHeader("Authorization"));
        Assertions.assertEquals(
                "/3.0/lists/audience1/members?count=1&offset=2", server.takeRequest().getPath());
        verify(context).signalNoMoreElement();
    }

    @Test
    void emptyAudienceCompletes() throws Exception {
        server.enqueue(new MockResponse().setBody(page(0)));
        reader.pollNext(output);
        Assertions.assertTrue(rows.isEmpty());
        verify(context).signalNoMoreElement();
    }

    @ParameterizedTest
    @ValueSource(strings = {"map<string,decimal(24,18)>", "{VALUE=\"decimal(24,18)\"}"})
    void preservesNestedDecimalPrecision(String mergeType) throws Exception {
        reader.close();
        reader = newReader(0, 1, mergeType);
        reader.open();
        server.enqueue(
                new MockResponse()
                        .setBody(
                                page(
                                        1,
                                        "{\"id\":\"one\",\"merge_fields\":{\"VALUE\":0.123456789012345678}}")));
        reader.pollNext(output);
        Object merge = rows.get(0).getField(4);
        Object value =
                merge instanceof Map
                        ? ((Map<?, ?>) merge).get("VALUE")
                        : ((SeaTunnelRow) merge).getField(0);
        Assertions.assertEquals(new BigDecimal("0.123456789012345678"), value);
        verify(context).signalNoMoreElement();
    }

    @Test
    void exactFullPageCompletesAndMissingOptionalFieldsAreNull() throws Exception {
        server.enqueue(new MockResponse().setBody(page(2, "{\"id\":\"one\"}", "{\"id\":\"two\"}")));
        reader.pollNext(output);
        Assertions.assertEquals(2, rows.size());
        Assertions.assertNull(rows.get(0).getField(1));
        Assertions.assertEquals(1, server.getRequestCount());
        verify(context).signalNoMoreElement();
    }

    @Test
    void growthCannotExtendInitialBound() throws Exception {
        server.enqueue(new MockResponse().setBody(page(3, member("one"), member("two"))));
        server.enqueue(new MockResponse().setBody(page(100, member("three"))));
        reader.pollNext(output);
        Assertions.assertEquals(3, rows.size());
        Assertions.assertEquals(2, server.getRequestCount());
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                "{}",
                "[]",
                "null",
                "{\"members\":{},\"total_items\":0}",
                "{\"members\":[],\"total_items\":\"0\"}",
                "{\"members\":[],\"total_items\":-1}",
                "{\"members\":[],\"total_items\":2147483648}",
                "{\"members\":[],\"total_items\":1}",
                "{\"members\":[{}],\"total_items\":2}",
                "{\"members\":[null],\"total_items\":1}",
                "{\"members\":[1],\"total_items\":1}",
                "{\"members\":[],\"total_items\":0} {}",
                "{\"members\":[{\"merge_fields\":42}],\"total_items\":1}",
                "PII-email@example.invalid"
            })
    void rejectsMalformedOrTruncatedPages(String body) {
        server.enqueue(new MockResponse().setBody(body));
        IOException error =
                Assertions.assertThrows(IOException.class, () -> reader.pollNext(output));
        Assertions.assertNull(error.getCause());
        Assertions.assertFalse(error.getMessage().contains("PII"));
        Assertions.assertTrue(rows.isEmpty());
        verify(context, never()).signalNoMoreElement();
    }

    @Test
    void retries429And5xxAtSameOffset() throws Exception {
        server.enqueue(
                new MockResponse()
                        .setResponseCode(429)
                        .setHeader("Retry-After", "0")
                        .setBody("private"));
        server.enqueue(
                new MockResponse()
                        .setResponseCode(503)
                        .setHeader("Retry-After", "invalid")
                        .setBody("private"));
        server.enqueue(new MockResponse().setBody(page(1, member("one"))));
        reader.pollNext(output);
        Assertions.assertEquals(1, rows.size());
        for (int i = 0; i < 3; i++) {
            Assertions.assertEquals(
                    "/3.0/lists/audience1/members?count=2&offset=0",
                    server.takeRequest().getPath());
        }
    }

    @Test
    void retriesBodylessErrorsAndReusesSuccessfulPageConnection() throws Exception {
        HttpServer fixture = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        AtomicInteger requests = new AtomicInteger();
        List<String> paths = Collections.synchronizedList(new ArrayList<>());
        List<Integer> ports = Collections.synchronizedList(new ArrayList<>());
        fixture.createContext(
                "/3.0/lists/audience1/members",
                exchange -> {
                    int request = requests.incrementAndGet();
                    paths.add(exchange.getRequestURI().toString());
                    ports.add(exchange.getRemoteAddress().getPort());
                    try {
                        if (request <= 2) {
                            exchange.getResponseHeaders().set("Retry-After", "0");
                            exchange.sendResponseHeaders(request == 1 ? 429 : 503, -1);
                        } else {
                            byte[] body =
                                    (request == 3
                                                    ? page(3, member("one"), member("two"))
                                                    : page(3, member("three")))
                                            .getBytes(StandardCharsets.UTF_8);
                            exchange.sendResponseHeaders(200, body.length);
                            exchange.getResponseBody().write(body);
                        }
                    } finally {
                        exchange.close();
                    }
                });
        fixture.start();
        try {
            reader.close();
            Map<String, Object> opts = MailchimpConfigTest.options();
            opts.put("base_url", "http://127.0.0.1:" + fixture.getAddress().getPort() + "/3.0");
            opts.put("allow_insecure_http", true);
            opts.put("page_size", 2);
            opts.put("retry", 2);
            opts.put("retry_backoff_ms", 1);
            opts.put("request_timeout_ms", 3000);
            reader =
                    new MailchimpSourceReader(
                            new MailchimpConfig(MailchimpConfigTest.config(opts)),
                            CatalogTableUtil.buildWithConfig(MailchimpConfigTest.config(opts)),
                            new SingleSplitReaderContext(context));
            reader.open();
            Assertions.assertDoesNotThrow(
                    () -> reader.pollNext(output),
                    () -> "Bodyless retry sequence failed after " + requests.get() + " requests");
            Assertions.assertEquals(3, rows.size());
            Assertions.assertEquals(4, requests.get());
            for (int i = 0; i < 3; i++) {
                Assertions.assertEquals(
                        "/3.0/lists/audience1/members?count=2&offset=0", paths.get(i));
            }
            Assertions.assertEquals("/3.0/lists/audience1/members?count=1&offset=2", paths.get(3));
            Assertions.assertEquals(
                    ports.get(2), ports.get(3), "Successful pages reuse a connection");
            verify(context).signalNoMoreElement();
        } finally {
            fixture.stop(0);
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {400, 401, 403, 404, 204, 301, 302, 307, 308})
    void permanentErrorsAndRedirectsNeverRetryOrExposeBodies(int status) throws Exception {
        server.enqueue(
                new MockResponse()
                        .setResponseCode(status)
                        .setHeader("Location", server.url("/stolen"))
                        .setBody("testkey-us1 private@example.invalid"));
        IOException error =
                Assertions.assertThrows(IOException.class, () -> reader.pollNext(output));
        Assertions.assertTrue(error.getMessage().contains("HTTP " + status));
        Assertions.assertNull(error.getCause());
        Assertions.assertFalse(error.getMessage().contains("testkey"));
        Assertions.assertFalse(error.getMessage().contains("private"));
        Assertions.assertEquals(1, server.getRequestCount());
        verify(context, never()).signalNoMoreElement();
    }

    @ParameterizedTest
    @ValueSource(ints = {429, 500, 503})
    void retryBudgetIsFinite(int status) {
        for (int i = 0; i < 3; i++) {
            server.enqueue(new MockResponse().setResponseCode(status).setBody("private"));
        }
        Assertions.assertThrows(IOException.class, () -> reader.pollNext(output));
        Assertions.assertEquals(3, server.getRequestCount());
        verify(context, never()).signalNoMoreElement();
    }

    @Test
    void redirectCannotForwardCredentialsToAnotherOrigin() throws Exception {
        try (MockWebServer destination = new MockWebServer()) {
            destination.start();
            server.enqueue(
                    new MockResponse()
                            .setResponseCode(302)
                            .setHeader("Location", destination.url("/credential-target")));
            Assertions.assertThrows(IOException.class, () -> reader.pollNext(output));
            Assertions.assertNull(destination.takeRequest(100, TimeUnit.MILLISECONDS));
            Assertions.assertEquals(1, server.getRequestCount());
            verify(context, never()).signalNoMoreElement();
        }
    }

    @Test
    void httpDateRetryAfterIsAccepted() throws Exception {
        server.enqueue(
                new MockResponse()
                        .setResponseCode(429)
                        .setHeader("Retry-After", "Thu, 01 Jan 1970 00:00:00 GMT"));
        server.enqueue(new MockResponse().setBody(page(0)));
        reader.pollNext(output);
        Assertions.assertEquals(2, server.getRequestCount());
        verify(context).signalNoMoreElement();
    }

    @Test
    void brokenTransportDoesNotComplete() {
        server.enqueue(
                new MockResponse()
                        .setBody(page(1, member("one")))
                        .setSocketPolicy(SocketPolicy.DISCONNECT_DURING_RESPONSE_BODY));
        Assertions.assertThrows(IOException.class, () -> reader.pollNext(output));
        Assertions.assertTrue(rows.isEmpty());
        verify(context, never()).signalNoMoreElement();
    }

    @Test
    void oversizedUnfinishedBodyFailsPromptlyWithoutCancellation() throws Exception {
        server.enqueue(
                new MockResponse()
                        .setBody("x".repeat(32 * 1024 * 1024 + 16384))
                        .setHeader("Content-Length", 64 * 1024 * 1024));
        CompletableFuture<Exception> task = pollInBackground();
        Assertions.assertInstanceOf(IOException.class, task.get(5, TimeUnit.SECONDS));
        Assertions.assertTrue(rows.isEmpty());
        verify(context, never()).signalNoMoreElement();
    }

    @Test
    void failureAfterAcceptedPageReplaysWholeBatchOnRestore() throws Exception {
        server.enqueue(new MockResponse().setBody(page(3, member("one"), member("two"))));
        server.enqueue(new MockResponse().setResponseCode(401).setBody("private"));
        Assertions.assertThrows(IOException.class, () -> reader.pollNext(output));
        Assertions.assertEquals(2, rows.size());
        List<SingleSplit> state = reader.snapshotState(1);
        Assertions.assertNull(
                state.get(0).getState(), "No offset, credentials or PII in split state");
        verify(context, never()).signalNoMoreElement();
        reader.close();
        reader = newReader(0, 1);
        reader.addSplits(state);
        reader.open();
        server.enqueue(new MockResponse().setBody(page(3, member("one"), member("two"))));
        server.enqueue(new MockResponse().setBody(page(3, member("three"))));
        reader.pollNext(output);
        Assertions.assertEquals(5, rows.size());
        server.takeRequest();
        server.takeRequest();
        Assertions.assertTrue(server.takeRequest().getPath().endsWith("offset=0"));
        verify(context).signalNoMoreElement();
    }

    @Test
    void audienceShrinkIsFailureNotSuccess() {
        server.enqueue(new MockResponse().setBody(page(3, member("one"), member("two"))));
        server.enqueue(new MockResponse().setBody(page(2)));
        Assertions.assertThrows(IOException.class, () -> reader.pollNext(output));
        Assertions.assertEquals(2, rows.size());
        verify(context, never()).signalNoMoreElement();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void closeOrCancelWakesRetryAndIsIdempotent(boolean cancel) throws Exception {
        reader.close();
        reader = newReader(10, 60000);
        reader.open();
        server.enqueue(
                new MockResponse().setResponseCode(429).setHeader("Retry-After", "9999999999999"));
        CompletableFuture<Exception> task = pollInBackground();
        Assertions.assertNotNull(server.takeRequest(2, TimeUnit.SECONDS));
        if (cancel) {
            reader.cancel();
        } else {
            reader.close();
        }
        Assertions.assertInstanceOf(IOException.class, task.get(2, TimeUnit.SECONDS));
        reader.close();
        Assertions.assertThrows(IOException.class, () -> reader.open());
        Assertions.assertEquals(1, server.getRequestCount());
        verify(context, never()).signalNoMoreElement();
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    void closeOrCancelAbortsActiveIo(boolean cancel) throws Exception {
        server.enqueue(new MockResponse().setSocketPolicy(SocketPolicy.NO_RESPONSE));
        CompletableFuture<Exception> task = pollInBackground();
        Assertions.assertNotNull(server.takeRequest(2, TimeUnit.SECONDS));
        Assertions.assertTrue(
                CompletableFuture.supplyAsync(
                                () -> {
                                    synchronized (output.getCheckpointLock()) {
                                        return true;
                                    }
                                })
                        .get(1, TimeUnit.SECONDS),
                "Network I/O must not hold the checkpoint lock");
        if (cancel) {
            reader.cancel();
        } else {
            reader.close();
        }
        Assertions.assertInstanceOf(IOException.class, task.get(2, TimeUnit.SECONDS));
        verify(context, never()).signalNoMoreElement();
    }

    @Test
    void cancellationBeforeOpenIsSticky() throws Exception {
        reader.close();
        reader = newReader(0, 1);
        reader.cancel();
        reader.cancel();
        Assertions.assertThrows(IOException.class, () -> reader.open());
        reader.pollNext(output);
        Assertions.assertEquals(0, server.getRequestCount());
        verify(context, never()).signalNoMoreElement();
    }

    @Test
    void cancelDoesNotWaitForCompletionAcknowledgement() throws Exception {
        server.enqueue(new MockResponse().setBody(page(0)));
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        doAnswer(
                        call -> {
                            entered.countDown();
                            release.await();
                            return null;
                        })
                .when(context)
                .signalNoMoreElement();
        CompletableFuture<Exception> poll = pollInBackground();
        Assertions.assertTrue(entered.await(2, TimeUnit.SECONDS));
        CompletableFuture<Void> cancel = CompletableFuture.runAsync(reader::cancel);
        try {
            cancel.get(1, TimeUnit.SECONDS);
        } finally {
            release.countDown();
            poll.get(2, TimeUnit.SECONDS);
            cancel.get(2, TimeUnit.SECONDS);
        }
    }

    @Test
    void closeDuringCollectionDoesNotSignalCompletion() throws Exception {
        server.enqueue(new MockResponse().setBody(page(2, member("one"), member("two"))));
        doAnswer(
                        call -> {
                            rows.add(call.getArgument(0));
                            reader.close();
                            return null;
                        })
                .when(output)
                .collect(any(SeaTunnelRow.class));
        reader.pollNext(output);
        Assertions.assertEquals(1, rows.size());
        verify(context, never()).signalNoMoreElement();
    }

    private CompletableFuture<Exception> pollInBackground() {
        return CompletableFuture.supplyAsync(
                () -> {
                    try {
                        reader.pollNext(output);
                        return null;
                    } catch (Exception e) {
                        return e;
                    }
                });
    }
}
