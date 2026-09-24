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

package org.apache.seatunnel.connectors.seatunnel.sentry.source;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.common.multitable.MultiTableFailureHelper;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.factory.TableSourceFactoryContext;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.common.exception.CommonErrorCodeDeprecated;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplit;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitEnumeratorState;
import org.apache.seatunnel.connectors.seatunnel.sentry.exception.SentryConnectorException;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class SentrySourceTest {

    @Test
    void distinguishesConfigurationErrorsFromReaderFailures() throws Exception {
        Map<String, Object> invalid = options();
        invalid.put("page_size", 0);
        SentryConnectorException configError =
                Assertions.assertThrows(SentryConnectorException.class, () -> source(invalid));
        Assertions.assertEquals(
                CommonErrorCodeDeprecated.ILLEGAL_ARGUMENT, configError.getSeaTunnelErrorCode());
        AbstractSingleSplitReader<SeaTunnelRow> reader =
                reader(options(), mock(SourceReader.Context.class));
        reader.close();
        SentryConnectorException readerError =
                Assertions.assertThrows(
                        SentryConnectorException.class,
                        () -> reader.pollNext(collector(new ArrayList<>())));
        Assertions.assertEquals(
                CommonErrorCodeDeprecated.READER_OPERATION_FAILED,
                readerError.getSeaTunnelErrorCode());
    }

    @Test
    void acceptsEngineInjectedFailFastButRejectsUnsupportedFailurePolicy() {
        ReadonlyConfig injected =
                MultiTableFailureHelper.withMultiTableFailurePolicy(
                        ReadonlyConfig.fromMap(options()),
                        ReadonlyConfig.fromMap(Collections.emptyMap()));
        Assertions.assertNotNull(
                new SentrySourceFactory()
                        .createSource(
                                new TableSourceFactoryContext(
                                        injected, getClass().getClassLoader()))
                        .createSource());
        Map<String, Object> config = options();
        config.put("multi_table.failure_policy", "CONTINUE_OTHER_TABLES");
        assertFailure(() -> source(config), "FAIL_FAST");
        config.put("multi_table.failure_policy", "invalid");
        assertFailure(() -> source(config), "Invalid option multi_table.failure_policy");
    }

    private HttpServer server;
    private ExecutorService executor;
    private final List<AbstractSingleSplitReader<SeaTunnelRow>> readers = new ArrayList<>();
    private static final String PATH = "/api/0/projects/acme/demo/events/";

    @AfterEach
    void close() throws Exception {
        for (AbstractSingleSplitReader<SeaTunnelRow> reader : readers) {
            reader.close();
        }
        if (server != null) {
            server.stop(0);
        }
        if (executor != null) {
            executor.shutdownNow();
            Assertions.assertTrue(executor.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    @Test
    void factoryCreatesSourceWithFixedSchemaAndSupportedClass() throws Exception {
        SentrySource source = source(options());
        Assertions.assertEquals(SentrySource.class, new SentrySourceFactory().getSourceClass());
        Assertions.assertEquals(
                8, source.getProducedCatalogTables().get(0).getTableSchema().getColumns().size());
        Assertions.assertEquals(
                "event_id",
                source.getProducedCatalogTables()
                        .get(0)
                        .getTableSchema()
                        .getColumns()
                        .get(0)
                        .getName());
        JobContext job = mock(JobContext.class);
        when(job.getJobMode()).thenReturn(JobMode.STREAMING);
        assertFailure(() -> source.setJobContext(job), "BATCH only");
    }

    @Test
    void rejectsIgnoredAndInvalidOptionsWithoutLeakingValues() {
        for (String key :
                Arrays.asList("schema", "query", "multi_table_failure_policy", "dsn", "full")) {
            Map<String, Object> options = options();
            options.put(key, "unsupported");
            Assertions.assertThrows(RuntimeException.class, () -> source(options));
        }
        Map<String, Object> options = options();
        options.put("token", "secret\r\nvalue");
        SentryConnectorException error =
                Assertions.assertThrows(SentryConnectorException.class, () -> source(options));
        Assertions.assertFalse(error.toString().contains("secret"));
        options.put("token", "real-token");
        options.put("mock_mode", true);
        assertFailure(() -> source(options), "mock_mode");
    }

    @Test
    void rejectsInvalidBoundsOriginsAndParallelism() {
        Map<String, Object> invalid = new HashMap<>();
        invalid.put("page_size", 101);
        invalid.put("max_pages", 0);
        invalid.put("max_retries", 6);
        invalid.put("retry_delay_ms", 0);
        invalid.put("request_timeout_ms", 0);
        invalid.put("max_response_bytes", Integer.MAX_VALUE);
        invalid.put("parallelism", 2);
        invalid.put("organization", "../other");
        invalid.put("project", "a/b");
        invalid.put("start_time", "2026-01-03T00:00:00Z");
        invalid.put("end_time", "bad");
        invalid.put("api_base_url", "https://user:pass@example.com/private");
        for (Map.Entry<String, Object> entry : invalid.entrySet()) {
            Map<String, Object> options = options();
            options.put(entry.getKey(), entry.getValue());
            Assertions.assertThrows(
                    SentryConnectorException.class, () -> source(options), entry.getKey());
        }
        for (String url :
                Arrays.asList(
                        "http://example.com",
                        "https://example.com/",
                        "https://example.com?secret=1",
                        "https://example.com#x")) {
            Map<String, Object> options = options();
            options.put("api_base_url", url);
            assertFailure(() -> source(options), "HTTPS origin");
        }
    }

    @Test
    void readsTwoPagesThroughFactoryAndReaderAndStopsOnFalseResults() throws Exception {
        AtomicInteger requests = new AtomicInteger();
        start(
                exchange -> {
                    int page = requests.incrementAndGet();
                    Assertions.assertEquals(
                            "Bearer mock-token",
                            exchange.getRequestHeaders().getFirst("Authorization"));
                    String query = exchange.getRequestURI().getRawQuery();
                    Assertions.assertTrue(query.contains("start=2026-01-01T00%3A00%3A00Z"));
                    Assertions.assertTrue(query.contains("end=2026-01-02T00%3A00%3A00Z"));
                    Assertions.assertTrue(query.contains("per_page=2"));
                    Assertions.assertTrue(query.contains("full=false"));
                    if (page == 1) {
                        Assertions.assertFalse(query.contains("cursor="));
                    } else {
                        Assertions.assertTrue(query.contains("cursor=0%3A2%3A0"));
                    }
                    reply(
                            exchange,
                            200,
                            page == 1
                                    ? "[" + event("a") + "," + event("b") + "]"
                                    : "[" + event("c") + "]",
                            link(page == 1, "0:2:0"));
                });
        SourceReader.Context context = mock(SourceReader.Context.class);
        AbstractSingleSplitReader<SeaTunnelRow> reader = reader(localOptions(), context);
        List<SeaTunnelRow> rows = new ArrayList<>();
        Collector<SeaTunnelRow> collector = collector(rows);
        reader.pollNext(collector);
        reader.pollNext(collector);
        Assertions.assertEquals(2, requests.get());
        Assertions.assertEquals(
                Arrays.asList("a", "b", "c"),
                Arrays.asList(
                        rows.get(0).getField(0), rows.get(1).getField(0), rows.get(2).getField(0)));
        Assertions.assertEquals("Résumé ✓", rows.get(0).getField(4));
        Assertions.assertNull(rows.get(0).getField(5));
        Assertions.assertTrue(
                rows.get(0).getField(7).toString().contains("0.12345678901234567890123456789"));
        verify(context).signalNoMoreElement();
    }

    @Test
    void emptyFinalPageCompletesWithoutExtraRequest() throws Exception {
        AtomicInteger count = new AtomicInteger();
        start(
                exchange -> {
                    count.incrementAndGet();
                    reply(exchange, 200, "[]", link(false, "0:0:0"));
                });
        SourceReader.Context context = mock(SourceReader.Context.class);
        List<SeaTunnelRow> rows = new ArrayList<>();
        reader(localOptions(), context).pollNext(collector(rows));
        Assertions.assertTrue(rows.isEmpty());
        Assertions.assertEquals(1, count.get());
        verify(context).signalNoMoreElement();
    }

    @Test
    void retriesTransientFailureWithoutEmittingDuplicates() throws Exception {
        AtomicInteger count = new AtomicInteger();
        start(
                exchange -> {
                    if (count.incrementAndGet() == 1) {
                        exchange.getResponseHeaders().add("Retry-After", "0");
                        reply(exchange, 429, "private", null);
                    } else {
                        reply(exchange, 200, "[" + event("a") + "]", link(false, "0:0:0"));
                    }
                });
        List<SeaTunnelRow> rows = new ArrayList<>();
        reader(localOptions(), mock(SourceReader.Context.class)).pollNext(collector(rows));
        Assertions.assertEquals(2, count.get());
        Assertions.assertEquals(1, rows.size());
    }

    @Test
    void doesNotRetryPermissionFailuresOrRedirectCredentials() throws Exception {
        AtomicInteger count = new AtomicInteger();
        start(
                exchange -> {
                    count.incrementAndGet();
                    exchange.getResponseHeaders().add("Location", origin() + "/unexpected");
                    reply(exchange, 302, "private-event-and-token", null);
                });
        assertFailure(
                () ->
                        reader(localOptions(), mock(SourceReader.Context.class))
                                .pollNext(collector(new ArrayList<>())),
                "HTTP 302");
        Assertions.assertEquals(1, count.get());
    }

    @Test
    void boundsRetryBudgetAndWithholdsRemoteBody() throws Exception {
        AtomicInteger count = new AtomicInteger();
        start(
                exchange -> {
                    count.incrementAndGet();
                    reply(exchange, 503, "private-event-and-token", null);
                });
        Map<String, Object> options = localOptions();
        options.put("max_retries", 2);
        SentryConnectorException error =
                Assertions.assertThrows(
                        SentryConnectorException.class,
                        () ->
                                reader(options, mock(SourceReader.Context.class))
                                        .pollNext(collector(new ArrayList<>())));
        Assertions.assertTrue(error.getMessage().contains("retry budget"));
        Assertions.assertEquals(
                CommonErrorCodeDeprecated.HTTP_OPERATION_FAILED, error.getSeaTunnelErrorCode());
        Assertions.assertFalse(error.toString().contains("private-event"));
        Assertions.assertNull(error.getCause());
        Assertions.assertEquals(3, count.get());
    }

    @Test
    void permissionFailureIsNotRetried() throws Exception {
        AtomicInteger count = new AtomicInteger();
        start(
                exchange -> {
                    count.incrementAndGet();
                    reply(exchange, 403, "private", null);
                });
        assertFailure(
                () ->
                        reader(localOptions(), mock(SourceReader.Context.class))
                                .pollNext(collector(new ArrayList<>())),
                "HTTP 403");
        Assertions.assertEquals(1, count.get());
    }

    @Test
    void rejectsMalformedPayloadBeforeEmittingRows() {
        String endpoint = "https://sentry.io" + PATH;
        String link = "<" + endpoint + "?cursor=0:0:0>; rel=\"next\"; results=\"false\"";
        for (String body :
                Arrays.asList(
                        "{}",
                        "null",
                        "[null]",
                        "[{}]",
                        "[" + event("a") + ",{}]",
                        "[{\"eventID\":3}]",
                        "[{\"eventID\":\"a\",\"eventID\":\"b\"}]",
                        "[" + event("a") + "] {}")) {
            SentryConnectorException error =
                    Assertions.assertThrows(
                            SentryConnectorException.class,
                            () ->
                                    new SentryPage(
                                            body.getBytes(StandardCharsets.UTF_8), link, endpoint),
                            body);
            Assertions.assertEquals(
                    CommonErrorCodeDeprecated.HTTP_OPERATION_FAILED, error.getSeaTunnelErrorCode());
        }
    }

    @Test
    void validatesLinkContractAndNeverTrustsForeignUrls() {
        String endpoint = "https://sentry.io" + PATH;
        for (String header :
                Arrays.asList(
                        null,
                        "",
                        "<" + endpoint + ">; rel=\"next\"",
                        "<" + endpoint + ">; rel=\"previous\"; results=\"false\"",
                        "<https://evil.example"
                                + PATH
                                + "?cursor=a>; rel=\"next\"; results=\"true\"",
                        "<" + endpoint + "?cursor=a>; rel=\"next\"; results=\"true\"; cursor=\"b\"",
                        "<" + endpoint + "?cursor=a&cursor=b>; rel=\"next\"; results=\"true\"",
                        "<" + endpoint + "?cursor&cursor=b>; rel=\"next\"; results=\"true\"",
                        "<" + endpoint + "?cursor=a>; rel=\"next\"; results=\"maybe\"",
                        "<"
                                + endpoint
                                + "?cursor=a>; rel=\"next\"; results=\"true\", <"
                                + endpoint
                                + "?cursor=b>; rel=\"next\"; results=\"true\"")) {
            Assertions.assertThrows(
                    SentryConnectorException.class, () -> SentryPage.nextCursor(header, endpoint));
        }
        Assertions.assertEquals(
                "0:100:0",
                SentryPage.nextCursor(
                        "<"
                                + endpoint
                                + "?cursor=0%3A100%3A0>; rel=\"next\"; results=\"true\"; cursor=\"0:100:0\"",
                        endpoint));
    }

    @Test
    void repeatedCursorFailsInsteadOfCompleting() throws Exception {
        start(exchange -> reply(exchange, 200, "[" + event("a") + "]", link(true, "0:2:0")));
        SourceReader.Context context = mock(SourceReader.Context.class);
        assertFailure(
                () -> reader(localOptions(), context).pollNext(collector(new ArrayList<>())),
                "repeated pagination cursor");
        verify(context, never()).signalNoMoreElement();
    }

    @Test
    void maxPagesFailsRatherThanTruncating() throws Exception {
        start(exchange -> reply(exchange, 200, "[]", link(true, "0:2:0")));
        Map<String, Object> options = localOptions();
        options.put("max_pages", 1);
        SourceReader.Context context = mock(SourceReader.Context.class);
        assertFailure(
                () -> reader(options, context).pollNext(collector(new ArrayList<>())), "max_pages");
        verify(context, never()).signalNoMoreElement();
    }

    @Test
    void limitsChunkedBodyAndDoesNotCompleteOnMissingPagination() throws Exception {
        start(
                exchange -> {
                    exchange.getResponseHeaders().add("Link", link(false, "0:0:0"));
                    exchange.sendResponseHeaders(200, 0);
                    exchange.getResponseBody().write(new byte[2048]);
                    exchange.close();
                });
        Map<String, Object> options = localOptions();
        options.put("max_response_bytes", 1024);
        assertFailure(
                () ->
                        reader(options, mock(SourceReader.Context.class))
                                .pollNext(collector(new ArrayList<>())),
                "max_response_bytes");
    }

    @Test
    void respectsRetryAfterBoundsAndHttpDate() {
        Instant now = Instant.parse("2026-01-01T00:00:00Z");
        Assertions.assertEquals(1000, SentryClient.retryDelay("0", 1000, now));
        Assertions.assertEquals(
                2000, SentryClient.retryDelay("Thu, 1 Jan 2026 00:00:02 GMT", 1, now));
        assertFailure(() -> SentryClient.retryDelay("61", 1, now), "Retry-After");
        assertFailure(() -> SentryClient.retryDelay("9223372036854775807", 1, now), "Retry-After");
        assertFailure(() -> SentryClient.retryDelay("private-token", 1, now), "Retry-After");
    }

    @Test
    void rejectsMissingLinkBeforeAnyRowsOrCompletion() throws Exception {
        start(exchange -> reply(exchange, 200, "[" + event("a") + "]", null));
        SourceReader.Context context = mock(SourceReader.Context.class);
        List<SeaTunnelRow> rows = new ArrayList<>();
        assertFailure(
                () -> reader(localOptions(), context).pollNext(collector(rows)), "Link header");
        Assertions.assertTrue(rows.isEmpty());
        verify(context, never()).signalNoMoreElement();
    }

    @Test
    void rejectsOversizedHttpHeadersWithBoundedRedactedFailure() throws Exception {
        start(
                exchange -> {
                    char[] padding = new char[20000];
                    Arrays.fill(padding, 'x');
                    exchange.getResponseHeaders().add("X-Example", new String(padding));
                    reply(exchange, 200, "[]", link(false, "0:0:0"));
                });
        Map<String, Object> options = localOptions();
        options.put("max_retries", 0);
        assertFailure(
                () ->
                        reader(options, mock(SourceReader.Context.class))
                                .pollNext(collector(new ArrayList<>())),
                "retry budget");
    }

    @Test
    void closeWakesLongRetryDelayWithoutCompleting() throws Exception {
        CountDownLatch responseSent = new CountDownLatch(1);
        start(
                exchange -> {
                    exchange.sendResponseHeaders(503, -1);
                    exchange.close();
                    responseSent.countDown();
                });
        Map<String, Object> options = localOptions();
        options.put("retry_delay_ms", 60000);
        SourceReader.Context context = mock(SourceReader.Context.class);
        AbstractSingleSplitReader<SeaTunnelRow> reader = reader(options, context);
        executor = Executors.newSingleThreadExecutor();
        AtomicReference<Thread> pollingThread = new AtomicReference<>();
        Future<?> future =
                executor.submit(
                        () -> {
                            pollingThread.set(Thread.currentThread());
                            return Assertions.assertThrows(
                                    Exception.class,
                                    () -> reader.pollNext(collector(new ArrayList<>())));
                        });
        Assertions.assertTrue(responseSent.await(5, TimeUnit.SECONDS));
        org.awaitility.Awaitility.await()
                .atMost(5, TimeUnit.SECONDS)
                .until(
                        () ->
                                pollingThread.get() != null
                                        && pollingThread.get().getState()
                                                == Thread.State.TIMED_WAITING
                                        && Arrays.stream(pollingThread.get().getStackTrace())
                                                .anyMatch(
                                                        frame ->
                                                                frame.getMethodName()
                                                                        .equals("retry")));
        reader.close();
        future.get(5, TimeUnit.SECONDS);
        verify(context, never()).signalNoMoreElement();
    }

    @Test
    void closingCancelsInFlightRequestAndPreventsCompletion() throws Exception {
        CountDownLatch received = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        start(
                exchange -> {
                    received.countDown();
                    try {
                        release.await(5, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    exchange.close();
                });
        SourceReader.Context context = mock(SourceReader.Context.class);
        AbstractSingleSplitReader<SeaTunnelRow> reader = reader(localOptions(), context);
        executor = Executors.newSingleThreadExecutor();
        Future<?> future =
                executor.submit(
                        () -> {
                            Assertions.assertThrows(
                                    Exception.class,
                                    () -> reader.pollNext(collector(new ArrayList<>())));
                        });
        Assertions.assertTrue(received.await(5, TimeUnit.SECONDS));
        reader.close();
        future.get(5, TimeUnit.SECONDS);
        release.countDown();
        verify(context, never()).signalNoMoreElement();
    }

    @Test
    void requestDeadlineBoundsStalledResponse() throws Exception {
        CountDownLatch release = new CountDownLatch(1);
        start(
                exchange -> {
                    try {
                        release.await(5, TimeUnit.SECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    exchange.close();
                });
        Map<String, Object> options = localOptions();
        options.put("request_timeout_ms", 50);
        options.put("max_retries", 0);
        try {
            assertFailure(
                    () ->
                            reader(options, mock(SourceReader.Context.class))
                                    .pollNext(collector(new ArrayList<>())),
                    "retry budget");
        } finally {
            release.countDown();
        }
    }

    @Test
    void checkpointContainsNoCredentialsAndRestorationReplaysWindow() throws Exception {
        AtomicInteger count = new AtomicInteger();
        start(
                exchange -> {
                    count.incrementAndGet();
                    reply(exchange, 200, "[" + event("a") + "]", link(false, "0:0:0"));
                });
        AbstractSingleSplitReader<SeaTunnelRow> first =
                reader(localOptions(), mock(SourceReader.Context.class));
        List<SingleSplit> state = first.snapshotState(1);
        Assertions.assertNull(state.get(0).getState());
        first.pollNext(collector(new ArrayList<>()));
        AbstractSingleSplitReader<SeaTunnelRow> restored =
                reader(localOptions(), mock(SourceReader.Context.class));
        restored.addSplits(state);
        List<SeaTunnelRow> rows = new ArrayList<>();
        restored.pollNext(collector(rows));
        Assertions.assertEquals(2, count.get());
        Assertions.assertEquals(1, rows.size());
    }

    private SentrySource source(Map<String, Object> options) {
        return (SentrySource)
                new SentrySourceFactory()
                        .<SeaTunnelRow, SingleSplit, SingleSplitEnumeratorState>createSource(
                                new TableSourceFactoryContext(
                                        ReadonlyConfig.fromMap(options),
                                        getClass().getClassLoader()))
                        .createSource();
    }

    private AbstractSingleSplitReader<SeaTunnelRow> reader(
            Map<String, Object> options, SourceReader.Context context) throws Exception {
        AbstractSingleSplitReader<SeaTunnelRow> reader = source(options).createReader(context);
        readers.add(reader);
        reader.open();
        return reader;
    }

    private Collector<SeaTunnelRow> collector(List<SeaTunnelRow> rows) throws Exception {
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

    private Map<String, Object> options() {
        Map<String, Object> options = new HashMap<>();
        options.put("token", "mock-token");
        options.put("organization", "acme");
        options.put("project", "demo");
        options.put("start_time", "2026-01-01T00:00:00Z");
        options.put("end_time", "2026-01-02T00:00:00Z");
        options.put("parallelism", 1);
        options.put("retry_delay_ms", 1);
        options.put("page_size", 2);
        return options;
    }

    private Map<String, Object> localOptions() {
        Map<String, Object> options = options();
        options.put("mock_mode", true);
        options.put("api_base_url", origin());
        return options;
    }

    private void start(com.sun.net.httpserver.HttpHandler handler) throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext(PATH, handler);
        server.start();
    }

    private String origin() {
        return "http://127.0.0.1:" + server.getAddress().getPort();
    }

    private String link(boolean more, String cursor) {
        return "<"
                + origin()
                + PATH
                + "?cursor="
                + cursor
                + ">; rel=\"next\"; results=\""
                + more
                + "\"";
    }

    private static String event(String id) {
        return "{\"eventID\":\""
                + id
                + "\",\"groupID\":\"42\",\"projectID\":\"7\",\"dateCreated\":\"2026-01-01T12:00:00Z\",\"title\":\"Résumé ✓\",\"message\":null,\"platform\":\"java\",\"value\":0.12345678901234567890123456789}";
    }

    private static void reply(HttpExchange exchange, int status, String body, String link)
            throws IOException {
        if (link != null) {
            exchange.getResponseHeaders().add("Link", link);
        }
        byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
        exchange.sendResponseHeaders(status, bytes.length);
        exchange.getResponseBody().write(bytes);
        exchange.close();
    }

    private static void assertFailure(
            org.junit.jupiter.api.function.Executable action, String message) {
        SentryConnectorException error =
                Assertions.assertThrows(SentryConnectorException.class, action);
        Assertions.assertTrue(error.getMessage().contains(message), error.getMessage());
    }
}
