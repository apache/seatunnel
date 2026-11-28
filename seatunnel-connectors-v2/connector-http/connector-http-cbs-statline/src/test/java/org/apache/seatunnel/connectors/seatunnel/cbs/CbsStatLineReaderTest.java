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

package org.apache.seatunnel.connectors.seatunnel.cbs;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;

import org.apache.http.HttpHost;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import com.sun.net.httpserver.HttpServer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Exercises the actual polling entry, streamed output and finite failure paths through local HTTP.
 * Existing observation and prefix-cap assertions also protect whole-scan null-state semantics.
 */
@Timeout(120)
class CbsStatLineReaderTest {
    private static final String PATH = "/odata/v1/CBS/83765NED/Observations";
    private static final String FIRST =
            "{\"Id\":0,\"Measure\":\"GM000C\",\"Value\":null,\"StringValue\":\"Nederland "
                    + " \",\"WijkenEnBuurten\":\"NL00\"}";
    private static final String SECOND =
            "{\"Id\":1,\"Measure\":\"M2\",\"Value\":0.123456789012345678901,\"ValueAttribute\":\"None\"}";
    private HttpServer server;
    private String origin;
    private final AtomicInteger requests = new AtomicInteger();
    private final AtomicBoolean complete = new AtomicBoolean();
    private final List<SeaTunnelRow> rows = new ArrayList<>();

    @BeforeEach
    void startServer() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        origin = "http://127.0.0.1:" + server.getAddress().getPort();
        server.start();
    }

    @AfterEach
    void stopServer() {
        server.stop(0);
    }

    @Test
    void readsOpaqueContinuationAndPreservesPublicDatasetFields() throws IOException {
        respond(
                200,
                "{\"value\":[" + FIRST + "],\"@odata.nextLink\":\"?$skiptoken=a%2Bb\"}",
                "{\"@odata.nextLink\":null,\"value\":[" + SECOND + "]}");
        read(0);
        assertTrue(complete.get());
        assertEquals(2, requests.get());
        assertEquals("83765NED", rows.get(0).getField(0));
        assertEquals(0L, rows.get(0).getField(1));
        assertNull(rows.get(0).getField(3));
        assertEquals("Nederland  ", rows.get(0).getField(4));
        assertTrue(((String) rows.get(0).getField(6)).contains("\"WijkenEnBuurten\":\"NL00\""));
        assertEquals("0.123456789012345678901", rows.get(1).getField(3));
        assertEquals("None", rows.get(1).getField(5));
    }

    @Test
    void limitsRowsWithoutRequestingAnotherPage() throws IOException {
        respond(
                200,
                "{\"value\":["
                        + FIRST
                        + ","
                        + SECOND
                        + "],\"@odata.nextLink\":\"?$skiptoken=a%2Bb\"}",
                null);
        read(1);
        assertTrue(complete.get());
        assertEquals(1, rows.size());
        assertEquals(1, requests.get());
    }

    @Test
    void rejectsWrongDatasetAndOrigins() {
        URI current = URI.create(origin + PATH);
        assertThrows(
                IOException.class,
                () -> CbsStatLineReader.continuation(current, "https://datasets.cbs.nl" + PATH));
        assertThrows(
                IOException.class,
                () -> CbsStatLineReader.continuation(current, "/odata/v1/CBS/other/Observations"));
        assertThrows(
                IOException.class, () -> CbsStatLineReader.continuation(current, PATH + "#part"));
        assertThrows(IOException.class, () -> CbsStatLineReader.continuation(current, ""));
    }

    @Test
    void failsWhenContinuationRepeats() {
        respond(200, "{\"value\":[],\"@odata.nextLink\":\"" + PATH + "\"}", null);
        assertThrows(IOException.class, () -> read(0));
        assertEquals(1, requests.get());
        assertFalse(complete.get());
    }

    @Test
    void nonSuccessIsNeverReportedAsEmptyDataset() {
        respond(429, "{\"error\":\"rate limited\"}", null);
        IOException error = assertThrows(IOException.class, () -> read(0));
        assertTrue(error.getMessage().contains("429"));
        assertFalse(complete.get());
    }

    @Test
    void failsOnAnInvalidEnvelope() {
        respond(200, "{\"value\":{}}", null);
        assertThrows(IOException.class, () -> read(0));
        assertFalse(complete.get());
    }

    @Test
    void validatesConfiguration() {
        assertThrows(
                IllegalArgumentException.class,
                () -> CbsStatLineSource.observationsUri(origin, "../id", 0));
        assertThrows(
                IllegalArgumentException.class,
                () -> CbsStatLineSource.observationsUri(origin, "valid", -1));
        assertThrows(
                IllegalArgumentException.class,
                () -> CbsStatLineSource.observationsUri("ftp://datasets.cbs.nl", "valid", 0));
    }

    /**
     * Guards byte-parser compatibility across its existing BOM/autodetection paths. Long names,
     * Unicode and exact numeric text remain payload tests, not a cache/heap retention probe.
     */
    @Test
    void preservesNativeEncodingsAndRejectsBoundedInputs() throws IOException {
        AtomicReference<byte[]> body = new AtomicReference<>();
        AtomicBoolean extraHeaders = new AtomicBoolean();
        server.createContext(
                PATH,
                exchange -> {
                    requests.incrementAndGet();
                    if (extraHeaders.get()) {
                        for (int i = 0; i < 65; i++) {
                            exchange.getResponseHeaders().add("X-Extra-" + i, "value");
                        }
                    }
                    byte[] value = body.get();
                    exchange.sendResponseHeaders(200, value.length);
                    try (OutputStream output = exchange.getResponseBody()) {
                        output.write(value);
                    }
                });
        String name = String.join("", Collections.nCopies(1024, "k"));
        String observation =
                SECOND.substring(0, SECOND.length() - 1) + ",\"" + name + "\":\"雪🦊\"}";
        String json = "{\"value\":[" + observation + "]}";
        for (String encoding :
                new String[] {"UTF-8", "UTF-16BE", "UTF-16LE", "UTF-32BE", "UTF-32LE"}) {
            for (boolean bom : new boolean[] {false, true}) {
                rows.clear();
                complete.set(false);
                body.set(((bom ? "\uFEFF" : "") + json).getBytes(Charset.forName(encoding)));
                read(1);
                assertEquals(1, rows.size());
                assertEquals("0.123456789012345678901", rows.get(0).getField(3));
                assertEquals(observation, rows.get(0).getField(6));
            }
        }
        rows.clear();
        complete.set(false);
        body.set(json.getBytes(StandardCharsets.UTF_8));
        CbsStatLineCursor.Limits small =
                new CbsStatLineCursor.Limits(16384, 128, 4096, 1000, 10000);
        assertThrows(IOException.class, () -> read(0, small));
        assertTrue(rows.isEmpty());
        assertFalse(complete.get());
        byte[] malformed = ("{\"value\":[" + FIRST + "]}").getBytes(StandardCharsets.UTF_8);
        malformed[new String(malformed, StandardCharsets.UTF_8).indexOf("Nederland")] = (byte) 0xFF;
        body.set(malformed);
        assertThrows(IOException.class, () -> read(0));
        assertTrue(rows.isEmpty());
        assertFalse(complete.get());
        extraHeaders.set(true);
        body.set(("{\"value\":[" + FIRST + "]}").getBytes(StandardCharsets.UTF_8));
        assertThrows(IOException.class, () -> read(0));
        assertTrue(rows.isEmpty());
        assertFalse(complete.get());
        extraHeaders.set(false);
        String link = "?$skiptoken=" + String.join("", Collections.nCopies(8193, "a"));
        body.set(
                ("{\"value\":[" + FIRST + "],\"@odata.nextLink\":\"" + link + "\"}")
                        .getBytes(StandardCharsets.UTF_8));
        int before = requests.get();
        IOException continuation = assertThrows(IOException.class, () -> read(0));
        assertTrue(continuation.getMessage().contains("URL limit"));
        assertEquals(before + 1, requests.get());
        assertEquals(1, rows.size(), "a late envelope failure retains accepted streaming output");
    }

    /**
     * An oversized observation must fail while its unread suffix is still held remotely, before the
     * native tree builder could finish it. The existing row cap still accepts that same prefix.
     */
    @Test
    void rejectsWideObservationBeforeItsHeldSuffixAndPreservesPrefixStop() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        server.createContext(
                PATH,
                exchange -> {
                    requests.incrementAndGet();
                    exchange.sendResponseHeaders(200, 0);
                    try (OutputStream output = exchange.getResponseBody()) {
                        if (release.getCount() == 0) {
                            output.write(
                                    ("{\"value\":[" + FIRST + ",invalid unread suffix")
                                            .getBytes(StandardCharsets.UTF_8));
                            return;
                        }
                        StringBuilder prefix =
                                new StringBuilder(
                                        "{\"value\":["
                                                + FIRST
                                                + ",{\"Id\":1,\"Measure\":\"wide\",");
                        for (int i = 0; i < 16; i++) {
                            prefix.append('"')
                                    .append("field-")
                                    .append(i)
                                    .append("\":\"")
                                    .append(String.join("", Collections.nCopies(128, "a")))
                                    .append("\",");
                        }
                        prefix.append("\"unfinished\":\"");
                        output.write(prefix.toString().getBytes(StandardCharsets.UTF_8));
                        output.flush();
                        entered.countDown();
                        release.await(30, TimeUnit.SECONDS);
                        output.write("tail\"}]}".getBytes(StandardCharsets.UTF_8));
                    } catch (IOException ignored) {
                        // The bounded reader intentionally aborts this unread response.
                    } catch (InterruptedException error) {
                        Thread.currentThread().interrupt();
                    }
                });
        ExecutorService operation = Executors.newSingleThreadExecutor();
        try {
            CbsStatLineCursor.Limits small =
                    new CbsStatLineCursor.Limits(16384, 4096, 256, 1000, 10000);
            Future<IOException> failure =
                    operation.submit(() -> assertThrows(IOException.class, () -> read(0, small)));
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            assertTrue(
                    failure.get(5, TimeUnit.SECONDS)
                            .getMessage()
                            .contains("observation exceeds the byte limit"));
            assertEquals(1, rows.size());
            assertEquals(FIRST, rows.get(0).getField(6));
            assertEquals(1, release.getCount(), "rejection precedes remote observation completion");
            assertFalse(complete.get());
        } finally {
            release.countDown();
            operation.shutdownNow();
            assertTrue(operation.awaitTermination(5, TimeUnit.SECONDS));
        }
        rows.clear();
        complete.set(false);
        read(1);
        assertEquals(1, rows.size());
        assertTrue(complete.get());
        assertEquals(FIRST, rows.get(0).getField(6));
    }

    /**
     * Active I/O expires despite trickling bytes, while intentionally parked rows and collector
     * backpressure consume no fetch/decode budget. Null checkpoint state deliberately restarts.
     */
    @Test
    void boundsActiveIoButExcludesCollectorAndCheckpointWaiting() throws Exception {
        AtomicBoolean trickle = new AtomicBoolean(true);
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        server.createContext(
                PATH,
                exchange -> {
                    exchange.sendResponseHeaders(200, 0);
                    try (OutputStream output = exchange.getResponseBody()) {
                        if (trickle.get()) {
                            for (byte b :
                                    ("{\"value\":[" + FIRST + "]}")
                                            .getBytes(StandardCharsets.UTF_8)) {
                                output.write(b);
                                output.flush();
                                Thread.sleep(20);
                            }
                        } else {
                            output.write(
                                    ("{\"value\":[" + FIRST + ",")
                                            .getBytes(StandardCharsets.UTF_8));
                            output.flush();
                            entered.countDown();
                            release.await(15, TimeUnit.SECONDS);
                            output.write((SECOND + "]}").getBytes(StandardCharsets.UTF_8));
                        }
                    } catch (IOException ignored) {
                        // Deadline/cancel closes the active response without requiring remote
                        // release.
                    } catch (InterruptedException error) {
                        Thread.currentThread().interrupt();
                    }
                });
        CbsStatLineCursor.Limits fast = new CbsStatLineCursor.Limits(16384, 4096, 4096, 1000, 200);
        IOException deadline = assertThrows(IOException.class, () -> read(0, fast));
        assertTrue(deadline.getMessage().contains("active fetch/decode deadline"));
        assertTrue(rows.isEmpty());
        assertFalse(complete.get());
        trickle.set(false);
        CbsStatLineCursor.Limits parked =
                new CbsStatLineCursor.Limits(16384, 4096, 4096, 1000, 2000);
        SingleSplitReaderContext context =
                new SingleSplitReaderContext(null) {
                    @Override
                    public void signalNoMoreElement() {
                        complete.set(true);
                    }
                };
        Object lock = new Object();
        Collector<SeaTunnelRow> output =
                new Collector<SeaTunnelRow>() {
                    @Override
                    public void collect(SeaTunnelRow row) {
                        rows.add(row);
                        if (rows.size() == 1) {
                            try {
                                Thread.sleep(2500);
                            } catch (InterruptedException error) {
                                Thread.currentThread().interrupt();
                                throw new IllegalStateException(error);
                            } finally {
                                release.countDown();
                            }
                        }
                    }

                    @Override
                    public Object getCheckpointLock() {
                        return lock;
                    }
                };
        try (CbsStatLineReader reader =
                new CbsStatLineReader(URI.create(origin + PATH), "83765NED", 0, context, parked)) {
            reader.open();
            reader.pollNext(output);
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            synchronized (lock) {
                assertNull(reader.snapshotState(1).get(0).getState());
            }
            pollUntilComplete(reader, output);
            assertEquals(2, rows.size());
            assertEquals(FIRST, rows.get(0).getField(6));
            assertEquals(SECOND, rows.get(1).getField(6));
        } finally {
            release.countDown();
        }
    }

    /**
     * Cancellation aborts the published socket while poll has already returned. A late provider
     * release cannot collect a row or signal successful completion after cancellation.
     */
    @Test
    void cancelsHeldResponseWithoutLateRows() throws Exception {
        CountDownLatch entered = new CountDownLatch(1);
        CountDownLatch disconnected = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        server.createContext(
                PATH,
                exchange -> {
                    exchange.sendResponseHeaders(200, 0);
                    try (OutputStream output = exchange.getResponseBody()) {
                        output.write("{\"value\":[".getBytes(StandardCharsets.UTF_8));
                        output.flush();
                        entered.countDown();
                        while (!release.await(20, TimeUnit.MILLISECONDS)) {
                            output.write(' ');
                            output.flush();
                        }
                        output.write((FIRST + "]}").getBytes(StandardCharsets.UTF_8));
                    } catch (IOException expected) {
                        disconnected.countDown();
                    } catch (InterruptedException error) {
                        Thread.currentThread().interrupt();
                    }
                });
        SingleSplitReaderContext context =
                new SingleSplitReaderContext(null) {
                    @Override
                    public void signalNoMoreElement() {
                        complete.set(true);
                    }
                };
        Collector<SeaTunnelRow> output =
                new Collector<SeaTunnelRow>() {
                    @Override
                    public void collect(SeaTunnelRow row) {
                        rows.add(row);
                    }

                    @Override
                    public Object getCheckpointLock() {
                        return this;
                    }
                };
        ExecutorService polling = Executors.newSingleThreadExecutor();
        try (CbsStatLineReader reader =
                new CbsStatLineReader(URI.create(origin + PATH), "83765NED", 0, context)) {
            reader.open();
            polling.submit(
                            () -> {
                                reader.pollNext(output);
                                return null;
                            })
                    .get(5, TimeUnit.SECONDS);
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            synchronized (output.getCheckpointLock()) {
                assertNull(reader.snapshotState(1).get(0).getState());
            }
            reader.cancel();
            assertTrue(disconnected.await(5, TimeUnit.SECONDS));
            release.countDown();
            reader.pollNext(output);
            assertTrue(rows.isEmpty());
            assertFalse(complete.get());
        } finally {
            release.countDown();
            polling.shutdownNow();
            assertTrue(polling.awaitTermination(5, TimeUnit.SECONDS));
        }
    }

    /**
     * Real proxy CONNECT failures reach native HttpClient buffering before the observation parser.
     * Both declared and streaming excess must fail before a held refusal body can finish.
     */
    @Test
    void boundsProxyRefusalBeforeNativeTunnelBuffering() throws Exception {
        for (boolean declared : new boolean[] {true, false}) {
            CountDownLatch headers = new CountDownLatch(1);
            CountDownLatch release = new CountDownLatch(1);
            ExecutorService operations = Executors.newFixedThreadPool(2);
            try (ServerSocket proxy = new ServerSocket(0, 1);
                    CloseableHttpClient client =
                            HttpClients.custom()
                                    .setProxy(new HttpHost("127.0.0.1", proxy.getLocalPort()))
                                    .setRequestExecutor(new CbsStatLineRequestExecutor())
                                    .disableAutomaticRetries()
                                    .disableRedirectHandling()
                                    .setDefaultRequestConfig(
                                            RequestConfig.custom()
                                                    .setConnectTimeout(2000)
                                                    .setSocketTimeout(15000)
                                                    .build())
                                    .build()) {
                Future<?> peer =
                        operations.submit(
                                () -> {
                                    try (Socket connection = proxy.accept()) {
                                        BufferedReader input =
                                                new BufferedReader(
                                                        new InputStreamReader(
                                                                connection.getInputStream(),
                                                                StandardCharsets.US_ASCII));
                                        assertTrue(
                                                input.readLine()
                                                        .startsWith(
                                                                "CONNECT statline.invalid:443 "));
                                        String line;
                                        while ((line = input.readLine()) != null
                                                && !line.isEmpty()) {
                                            // Consume only the finite request headers of this owned
                                            // client.
                                        }
                                        OutputStream output = connection.getOutputStream();
                                        output.write(
                                                ("HTTP/1.1 503 Unavailable\r\n"
                                                                + (declared
                                                                        ? "Content-Length: 1000000000\r\n"
                                                                        : "Transfer-Encoding: chunked\r\n")
                                                                + "Connection: close\r\n\r\n")
                                                        .getBytes(StandardCharsets.US_ASCII));
                                        output.flush();
                                        headers.countDown();
                                        if (!declared) {
                                            output.write(
                                                    "10001\r\n"
                                                            .getBytes(StandardCharsets.US_ASCII));
                                            byte[] body = new byte[65537];
                                            Arrays.fill(body, (byte) 'a');
                                            output.write(body);
                                        }
                                        output.flush();
                                        release.await(10, TimeUnit.SECONDS);
                                        if (!declared) {
                                            output.write(
                                                    "\r\n0\r\n\r\n"
                                                            .getBytes(StandardCharsets.US_ASCII));
                                        }
                                    } catch (IOException expected) {
                                        // The connector-owned executor shuts down the refusal
                                        // before draining.
                                    } catch (InterruptedException error) {
                                        Thread.currentThread().interrupt();
                                    }
                                });
                Future<IOException> rejected =
                        operations.submit(
                                () ->
                                        assertThrows(
                                                IOException.class,
                                                () -> {
                                                    try (CloseableHttpResponse ignored =
                                                            client.execute(
                                                                    new HttpGet(
                                                                            "https://statline.invalid/"))) {
                                                        // Refusal admission must fail before
                                                        // execute can return a buffered response.
                                                    }
                                                }));
                assertTrue(headers.await(5, TimeUnit.SECONDS));
                assertTrue(
                        rejected.get(5, TimeUnit.SECONDS)
                                .getMessage()
                                .contains("proxy refusal exceeds the byte limit"));
                assertEquals(1, release.getCount());
                release.countDown();
                peer.get(5, TimeUnit.SECONDS);
            } finally {
                release.countDown();
                operations.shutdownNow();
                assertTrue(operations.awaitTermination(5, TimeUnit.SECONDS));
            }
        }
    }

    private void respond(int status, String first, String second) {
        server.createContext(
                PATH,
                exchange -> {
                    int index = requests.getAndIncrement();
                    String payload = index == 0 ? first : second;
                    int code = status;
                    if (index != 0
                            && !"$skiptoken=a%2Bb".equals(exchange.getRequestURI().getRawQuery())) {
                        code = 400;
                    }
                    byte[] body =
                            (payload == null ? "{}" : payload).getBytes(StandardCharsets.UTF_8);
                    exchange.sendResponseHeaders(code, body.length);
                    try (OutputStream output = exchange.getResponseBody()) {
                        output.write(body);
                    }
                });
    }

    private void read(int maxRows) throws IOException {
        read(maxRows, CbsStatLineCursor.Limits.defaults());
    }

    private void read(int maxRows, CbsStatLineCursor.Limits limits) throws IOException {
        SingleSplitReaderContext context =
                new SingleSplitReaderContext(null) {
                    @Override
                    public void signalNoMoreElement() {
                        complete.set(true);
                    }
                };
        try (CbsStatLineReader reader =
                new CbsStatLineReader(
                        CbsStatLineSource.observationsUri(origin, "83765NED", maxRows),
                        "83765NED",
                        maxRows,
                        context,
                        limits)) {
            reader.open();
            Collector<SeaTunnelRow> output =
                    new Collector<SeaTunnelRow>() {
                        @Override
                        public void collect(SeaTunnelRow row) {
                            rows.add(row);
                        }

                        @Override
                        public Object getCheckpointLock() {
                            return this;
                        }
                    };
            pollUntilComplete(reader, output);
        }
    }

    private void pollUntilComplete(CbsStatLineReader reader, Collector<SeaTunnelRow> output)
            throws IOException {
        while (!complete.get()) {
            reader.pollNext(output);
            try {
                Thread.sleep(1);
            } catch (InterruptedException error) {
                Thread.currentThread().interrupt();
                throw new IOException(error);
            }
        }
    }
}
