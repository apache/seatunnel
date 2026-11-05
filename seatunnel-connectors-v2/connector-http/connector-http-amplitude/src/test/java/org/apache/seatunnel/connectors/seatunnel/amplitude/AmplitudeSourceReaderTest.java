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
package org.apache.seatunnel.connectors.seatunnel.amplitude;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplit;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.zip.GZIPOutputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class AmplitudeSourceReaderTest {
    private static final String EVENT =
            "{\"event_type\":\"purchase\",\"uuid\":\"one\",\"event_properties\":{\"amount\":42}}\n";
    @TempDir Path temporaryDirectory;
    private HttpServer server;
    private ExecutorService serverThreads;
    private ExecutorService worker;
    private SourceReader.Context context;
    private Map<String, Object> options;
    private final List<AmplitudeSourceReader> readers = new ArrayList<>();
    private final AtomicInteger requests = new AtomicInteger();

    @BeforeEach
    void setUp() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        serverThreads = Executors.newCachedThreadPool();
        server.setExecutor(serverThreads);
        worker = Executors.newSingleThreadExecutor();
        context = mock(SourceReader.Context.class);
        options = AmplitudeSourceFactoryTest.options();
        options.put("base_url", "http://127.0.0.1:" + server.getAddress().getPort());
        options.put("allow_insecure_http", true);
        options.put("retry_backoff_ms", 1);
        options.put("max_retry_backoff_ms", 5);
        options.put("read_timeout_ms", 2000);
        server.start();
    }

    @AfterEach
    void tearDown() throws Exception {
        for (AmplitudeSourceReader reader : readers) {
            reader.close();
        }
        server.stop(0);
        worker.shutdownNow();
        serverThreads.shutdownNow();
        assertTrue(worker.awaitTermination(5, TimeUnit.SECONDS));
        assertTrue(serverThreads.awaitTermination(5, TimeUnit.SECONDS));
        try (java.util.stream.Stream<Path> files = Files.list(temporaryDirectory)) {
            assertEquals(0, files.count(), "temporary exports must be deleted after all outcomes");
        }
    }

    private AmplitudeSourceReader reader() throws Exception {
        ReadonlyConfig config = ReadonlyConfig.fromMap(options);
        AmplitudeSource source = new AmplitudeSource(config);
        AmplitudeSourceReader reader =
                new AmplitudeSourceReader(
                        new AmplitudeSourceConfig(config),
                        source.getProducedCatalogTables().get(0),
                        new SingleSplitReaderContext(context),
                        temporaryDirectory);
        readers.add(reader);
        reader.open();
        return reader;
    }

    @SuppressWarnings("unchecked")
    private Collector<SeaTunnelRow> collector(List<SeaTunnelRow> rows) {
        Collector<SeaTunnelRow> collector = mock(Collector.class);
        when(collector.getCheckpointLock()).thenReturn(new Object());
        doAnswer(
                        call -> {
                            rows.add(call.getArgument(0));
                            return null;
                        })
                .when(collector)
                .collect(any(SeaTunnelRow.class));
        return collector;
    }

    private void pollToEnd(AmplitudeSourceReader reader, Collector<SeaTunnelRow> output)
            throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        do {
            reader.pollNext(output);
            if (org.mockito.Mockito.mockingDetails(context).getInvocations().stream()
                    .anyMatch(call -> call.getMethod().getName().equals("signalNoMoreElement"))) {
                return;
            }
            assertTrue(System.nanoTime() < deadline, "reader did not terminate");
            Thread.sleep(1);
        } while (true);
    }

    private void respond(HttpExchange exchange, int status, byte[] body) throws IOException {
        exchange.sendResponseHeaders(status, body.length == 0 ? -1 : body.length);
        try (OutputStream output = exchange.getResponseBody()) {
            if (body.length > 0) {
                output.write(body);
            }
        } finally {
            exchange.close();
        }
    }

    private void serve(byte[] zip) {
        server.createContext(
                "/api/2/export",
                exchange -> {
                    requests.incrementAndGet();
                    respond(exchange, 200, zip);
                });
    }

    static byte[] gzip(String events) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (GZIPOutputStream output = new GZIPOutputStream(bytes)) {
            output.write(events.getBytes(StandardCharsets.UTF_8));
        }
        return bytes.toByteArray();
    }

    static byte[] zip(byte[]... entries) throws IOException {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ZipOutputStream output = new ZipOutputStream(bytes)) {
            for (int i = 0; i < entries.length; i++) {
                output.putNextEntry(new ZipEntry(i + ".json.gz"));
                output.write(entries[i]);
                output.closeEntry();
            }
        }
        return bytes.toByteArray();
    }

    @Test
    void readsMultipleEntriesWithExactAuthWindowAndNestedMapping() throws Exception {
        byte[] zip = zip(gzip(EVENT), gzip(EVENT.replace("one", "two")));
        server.createContext(
                "/api/2/export",
                exchange -> {
                    requests.incrementAndGet();
                    assertEquals(
                            "start=20260101T00&end=20260101T01",
                            exchange.getRequestURI().getRawQuery());
                    assertEquals(
                            "Basic "
                                    + Base64.getEncoder()
                                            .encodeToString(
                                                    "fixture-api-key:fixture-secret"
                                                            .getBytes(StandardCharsets.UTF_8)),
                            exchange.getRequestHeaders().getFirst("Authorization"));
                    respond(exchange, 200, zip);
                });
        options.put(
                "field_mapping",
                Collections.singletonMap("event_properties", "/event_properties/amount"));
        List<SeaTunnelRow> rows = new ArrayList<>();
        AmplitudeSourceReader reader = reader();
        pollToEnd(reader, collector(rows));
        pollToEnd(reader, collector(rows));
        assertEquals(2, rows.size());
        assertEquals("one", rows.get(0).getField(1));
        assertEquals("two", rows.get(1).getField(1));
        assertEquals("42", rows.get(0).getField(2));
        assertEquals(1, requests.get());
        verify(context).signalNoMoreElement();
    }

    @Test
    void preservesDecimalMappingAndNullMissingPointers() throws Exception {
        serve(
                zip(
                        gzip(
                                "{\"event_type\":\"purchase\",\"event_properties\":{\"amount\":1234567890123456.123456}}\n")));
        Map<String, String> fields = new LinkedHashMap<>();
        fields.put("event_type", "string");
        fields.put("amount", "decimal(30,6)");
        fields.put("missing", "string");
        options.put("schema", Collections.singletonMap("fields", fields));
        Map<String, String> mapping = new LinkedHashMap<>();
        mapping.put("amount", "/event_properties/amount");
        mapping.put("missing", "/not_present");
        options.put("field_mapping", mapping);
        List<SeaTunnelRow> rows = new ArrayList<>();
        pollToEnd(reader(), collector(rows));
        assertEquals(new BigDecimal("1234567890123456.123456"), rows.get(0).getField(1));
        assertNull(rows.get(0).getField(2));
    }

    @Test
    void retriesEmpty429And503WithoutEngineRecovery() throws Exception {
        byte[] zip = zip(gzip(EVENT));
        server.createContext(
                "/api/2/export",
                exchange -> {
                    int attempt = requests.incrementAndGet();
                    if (attempt <= 2) {
                        exchange.getResponseHeaders().set("Retry-After", "0");
                    }
                    respond(
                            exchange,
                            attempt == 1 ? 429 : attempt == 2 ? 503 : 200,
                            attempt <= 2 ? new byte[0] : zip);
                });
        List<SeaTunnelRow> rows = new ArrayList<>();
        pollToEnd(reader(), collector(rows));
        assertEquals(3, requests.get());
        assertEquals(1, rows.size());
    }

    @Test
    void retriesAnInterruptedDownloadBeforeEmittingRows() throws Exception {
        byte[] zip = zip(gzip(EVENT));
        server.createContext(
                "/api/2/export",
                exchange -> {
                    if (requests.incrementAndGet() == 1) {
                        exchange.sendResponseHeaders(200, zip.length);
                        exchange.getResponseBody().write(zip, 0, zip.length / 2);
                        exchange.close();
                    } else {
                        respond(exchange, 200, zip);
                    }
                });
        List<SeaTunnelRow> rows = new ArrayList<>();
        pollToEnd(reader(), collector(rows));
        assertEquals(1, rows.size());
        assertEquals(2, requests.get());
    }

    @Test
    void accepts404AndEmptyZipAsEmptyWindows() throws Exception {
        server.createContext("/api/2/export", exchange -> respond(exchange, 404, new byte[0]));
        List<SeaTunnelRow> rows = new ArrayList<>();
        pollToEnd(reader(), collector(rows));
        assertTrue(rows.isEmpty());
        verify(context).signalNoMoreElement();
        server.removeContext("/api/2/export");
        serve(zip());
        context = mock(SourceReader.Context.class);
        pollToEnd(reader(), collector(rows));
        assertTrue(rows.isEmpty());
        verify(context).signalNoMoreElement();
    }

    @Test
    void rejectsTruncatedZipWithCompleteLocalEntry() throws Exception {
        byte[] zip = zip(gzip(EVENT));
        serve(Arrays.copyOf(zip, zip.length - 22));
        List<SeaTunnelRow> rows = new ArrayList<>();
        assertThrows(IOException.class, () -> pollToEnd(reader(), collector(rows)));
        assertTrue(rows.isEmpty());
        verify(context, never()).signalNoMoreElement();
        assertEquals(1, requests.get());
    }

    @Test
    void rejectsCorruptGzipBeforeEarlierEntriesEmit() throws Exception {
        byte[] corrupt = gzip(EVENT);
        corrupt[corrupt.length - 8] ^= 1;
        serve(zip(gzip(EVENT), corrupt));
        List<SeaTunnelRow> rows = new ArrayList<>();
        assertThrows(IOException.class, () -> pollToEnd(reader(), collector(rows)));
        assertTrue(rows.isEmpty());
        verify(context, never()).signalNoMoreElement();
    }

    @Test
    void rejectsZipCrcMismatchBeforeEmitting() throws Exception {
        byte[] bytes = zip(gzip(EVENT));
        for (int i = 0; i < bytes.length - 20; i++) {
            if (bytes[i] == 0x50
                    && bytes[i + 1] == 0x4b
                    && bytes[i + 2] == 1
                    && bytes[i + 3] == 2) {
                bytes[i + 16] ^= 1;
                break;
            }
        }
        serve(bytes);
        List<SeaTunnelRow> rows = new ArrayList<>();
        IOException error =
                assertThrows(IOException.class, () -> pollToEnd(reader(), collector(rows)));
        assertTrue(error.getMessage().contains("CRC"));
        assertTrue(rows.isEmpty());
        verify(context, never()).signalNoMoreElement();
    }

    @Test
    void rejectsNonGzipContent() throws Exception {
        serve(zip(EVENT.getBytes(StandardCharsets.UTF_8)));
        assertThrows(IOException.class, () -> pollToEnd(reader(), collector(new ArrayList<>())));
        verify(context, never()).signalNoMoreElement();
    }

    @Test
    void rejectsConcatenatedGzipMembersWithoutStackExhaustion() throws Exception {
        ByteArrayOutputStream members = new ByteArrayOutputStream();
        byte[] empty = gzip("");
        for (int i = 0; i < 20000; i++) {
            members.write(empty);
        }
        serve(zip(members.toByteArray()));
        assertThrows(IOException.class, () -> pollToEnd(reader(), collector(new ArrayList<>())));
        verify(context, never()).signalNoMoreElement();
    }

    @Test
    void boundsGzipHeaderMetadataBeforeDecoderAllocatesIt() throws Exception {
        byte[] original = gzip(EVENT);
        byte[] named = new byte[original.length + 1000001];
        System.arraycopy(original, 0, named, 0, 10);
        named[3] |= 8;
        Arrays.fill(named, 10, 1000010, (byte) 'x');
        System.arraycopy(original, 10, named, 1000011, original.length - 10);
        serve(zip(named));
        assertThrows(IOException.class, () -> pollToEnd(reader(), collector(new ArrayList<>())));
        verify(context, never()).signalNoMoreElement();
    }

    @Test
    void rejectsHttpContentEncodingBeforeAnyAutomaticDecoder() throws Exception {
        byte[] zip = zip(gzip(EVENT));
        ByteArrayOutputStream encoded = new ByteArrayOutputStream();
        try (GZIPOutputStream gzip = new GZIPOutputStream(encoded)) {
            gzip.write(zip);
        }
        server.createContext(
                "/api/2/export",
                exchange -> {
                    exchange.getResponseHeaders().set("Content-Encoding", "gzip");
                    respond(exchange, 200, encoded.toByteArray());
                });
        assertThrows(IOException.class, () -> pollToEnd(reader(), collector(new ArrayList<>())));
        verify(context, never()).signalNoMoreElement();
    }

    @Test
    void boundsCentralDirectoryBeforeOpeningZip() throws Exception {
        ByteArrayOutputStream bytes = new ByteArrayOutputStream();
        try (ZipOutputStream zip = new ZipOutputStream(bytes)) {
            for (int i = 0; i < 1000; i++) {
                ZipEntry entry = new ZipEntry(i + ".gz");
                entry.setComment(String.join("", Collections.nCopies(8192, "x")));
                zip.putNextEntry(entry);
                zip.write(gzip(""));
                zip.closeEntry();
            }
        }
        serve(bytes.toByteArray());
        assertThrows(IOException.class, () -> pollToEnd(reader(), collector(new ArrayList<>())));
        verify(context, never()).signalNoMoreElement();
    }

    @Test
    void readsZip64ThroughTheCompleteReader() throws Exception {
        serve(AmplitudeArchiveTest.zip64());
        List<SeaTunnelRow> rows = new ArrayList<>();
        pollToEnd(reader(), collector(rows));
        assertTrue(rows.isEmpty());
        verify(context).signalNoMoreElement();
    }

    @Test
    void rejectsForgedZipEntrySizeBeforeEmitting() throws Exception {
        byte[] bytes = zip(gzip(EVENT));
        for (int i = 0; i < bytes.length - 28; i++) {
            if (bytes[i] == 0x50
                    && bytes[i + 1] == 0x4b
                    && bytes[i + 2] == 1
                    && bytes[i + 3] == 2) {
                bytes[i + 24] = 1;
                bytes[i + 25] = 0;
                bytes[i + 26] = 0;
                bytes[i + 27] = 0;
                break;
            }
        }
        serve(bytes);
        List<SeaTunnelRow> rows = new ArrayList<>();
        assertThrows(IOException.class, () -> pollToEnd(reader(), collector(rows)));
        assertTrue(rows.isEmpty());
        verify(context, never()).signalNoMoreElement();
    }

    @Test
    void boundsArchiveDecodedBytesAndRecords() throws Exception {
        byte[] bytes = zip(gzip(EVENT));
        serve(bytes);
        options.put("max_archive_bytes", (long) bytes.length - 1);
        assertTrue(
                assertThrows(
                                IOException.class,
                                () -> pollToEnd(reader(), collector(new ArrayList<>())))
                        .getMessage()
                        .contains("max_archive_bytes"));
        options.remove("max_archive_bytes");
        options.put("max_uncompressed_bytes", 50L);
        assertTrue(
                assertThrows(
                                IOException.class,
                                () -> pollToEnd(reader(), collector(new ArrayList<>())))
                        .getMessage()
                        .contains("max_uncompressed_bytes"));
        options.remove("max_uncompressed_bytes");
        options.put("max_event_bytes", 10);
        assertTrue(
                assertThrows(
                                IOException.class,
                                () -> pollToEnd(reader(), collector(new ArrayList<>())))
                        .getMessage()
                        .contains("max_event_bytes"));
        verify(context, never()).signalNoMoreElement();
    }

    @Test
    void malformedEventsFailWithoutLeakingOrRetryingAfterEmission() throws Exception {
        serve(zip(gzip(EVENT + "{\"fixture-secret\":\"malformed")));
        List<SeaTunnelRow> rows = new ArrayList<>();
        IOException error =
                assertThrows(IOException.class, () -> pollToEnd(reader(), collector(rows)));
        assertEquals(1, rows.size());
        assertFalse(error.toString().contains("fixture-secret"));
        assertNull(error.getCause());
        assertEquals(0, error.getSuppressed().length);
        verify(context, never()).signalNoMoreElement();
        assertEquals(1, requests.get());
    }

    @Test
    void oversizedChunkedResponseAbortsBeforeClosingItsUnfinishedStream() throws Exception {
        CountDownLatch tail = new CountDownLatch(1);
        server.createContext(
                "/api/2/export",
                exchange -> {
                    exchange.sendResponseHeaders(200, 0);
                    try {
                        exchange.getResponseBody().write(new byte[256]);
                        exchange.getResponseBody().flush();
                        tail.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        exchange.close();
                    }
                });
        options.put("max_archive_bytes", 32L);
        options.put("read_timeout_ms", 30000);
        AmplitudeSourceReader reader = reader();
        Future<?> result =
                worker.submit(
                        () ->
                                assertThrows(
                                        IOException.class,
                                        () -> pollToEnd(reader, collector(new ArrayList<>()))));
        try {
            result.get(2, TimeUnit.SECONDS);
        } finally {
            tail.countDown();
            result.get(5, TimeUnit.SECONDS);
        }
        verify(context, never()).signalNoMoreElement();
    }

    @Test
    void decodedLimitCountsExactBytesWithoutRequiringFinalNewline() throws Exception {
        String event = EVENT.trim() + String.join("", Collections.nCopies(100, " "));
        serve(zip(gzip(event)));
        options.put("max_uncompressed_bytes", (long) event.getBytes(StandardCharsets.UTF_8).length);
        List<SeaTunnelRow> rows = new ArrayList<>();
        pollToEnd(reader(), collector(rows));
        assertEquals(1, rows.size());
    }

    @Test
    void boundsDecodedTotalAcrossEntriesAndReadsManySmallRecords() throws Exception {
        String events = String.join("", Collections.nCopies(10000, EVENT));
        byte[] zip = zip(gzip(events), gzip(events));
        serve(zip);
        AtomicInteger count = new AtomicInteger();
        Collector<SeaTunnelRow> output = collector(new ArrayList<>());
        doAnswer(
                        call -> {
                            count.incrementAndGet();
                            return null;
                        })
                .when(output)
                .collect(any(SeaTunnelRow.class));
        pollToEnd(reader(), output);
        assertEquals(20000, count.get());
        context = mock(SourceReader.Context.class);
        options.put("max_uncompressed_bytes", (long) events.length());
        count.set(0);
        assertThrows(IOException.class, () -> pollToEnd(reader(), output));
        assertEquals(0, count.get());
        verify(context, never()).signalNoMoreElement();
    }

    @Test
    void cancellationWakesLongRetryWait() throws Exception {
        CountDownLatch response = new CountDownLatch(1);
        server.createContext(
                "/api/2/export",
                exchange -> {
                    requests.incrementAndGet();
                    respond(exchange, 503, new byte[0]);
                    response.countDown();
                });
        options.put("retry_backoff_ms", 60000);
        options.put("max_retry_backoff_ms", 60000);
        AmplitudeSourceReader reader = reader();
        Future<?> result =
                worker.submit(
                        () ->
                                assertThrows(
                                        IOException.class,
                                        () -> pollToEnd(reader, collector(new ArrayList<>()))));
        assertTrue(response.await(5, TimeUnit.SECONDS));
        reader.cancel();
        result.get(5, TimeUnit.SECONDS);
        assertEquals(1, requests.get());
        verify(context, never()).signalNoMoreElement();
    }

    @Test
    void rejectsAuthAndRedirectsWithoutLeakingBodies() throws Exception {
        server.createContext(
                "/api/2/export",
                exchange -> {
                    requests.incrementAndGet();
                    exchange.getResponseHeaders().set("Location", "/leak");
                    respond(exchange, 302, "fixture-secret".getBytes(StandardCharsets.UTF_8));
                });
        server.createContext(
                "/leak",
                exchange -> {
                    throw new AssertionError("must not follow redirect");
                });
        IOException error =
                assertThrows(
                        IOException.class, () -> pollToEnd(reader(), collector(new ArrayList<>())));
        assertTrue(error.getMessage().contains("302"));
        assertFalse(error.toString().contains("fixture-secret"));
        assertEquals(1, requests.get());
        server.removeContext("/api/2/export");
        server.createContext(
                "/api/2/export",
                exchange ->
                        respond(exchange, 401, "fixture-secret".getBytes(StandardCharsets.UTF_8)));
        error =
                assertThrows(
                        IOException.class, () -> pollToEnd(reader(), collector(new ArrayList<>())));
        assertTrue(error.getMessage().contains("401"));
        assertFalse(error.toString().contains("fixture-secret"));
    }

    @Test
    void respectsRetryBudgetAndRetryAfter() throws Exception {
        server.createContext(
                "/api/2/export",
                exchange -> {
                    requests.incrementAndGet();
                    exchange.getResponseHeaders().set("Retry-After", "600");
                    respond(exchange, 429, new byte[0]);
                });
        IOException error =
                assertThrows(
                        IOException.class, () -> pollToEnd(reader(), collector(new ArrayList<>())));
        assertTrue(error.getMessage().contains("Retry-After"));
        assertEquals(1, requests.get());
        server.removeContext("/api/2/export");
        server.createContext(
                "/api/2/export",
                exchange -> {
                    requests.incrementAndGet();
                    respond(exchange, 503, new byte[0]);
                });
        options.put("max_retries", 1);
        assertThrows(IOException.class, () -> pollToEnd(reader(), collector(new ArrayList<>())));
        assertEquals(3, requests.get());
    }

    @Test
    void cancellationUnblocksHeadersAndIsSticky() throws Exception {
        cancelWhileBlocked(false);
    }

    @Test
    void blockedDownloadReturnsControlForEngineSavepointStop() throws Exception {
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        byte[] zip = zip(gzip(EVENT));
        server.createContext(
                "/api/2/export",
                exchange -> {
                    entered.countDown();
                    try {
                        release.await();
                        respond(exchange, 200, zip);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } finally {
                        exchange.close();
                    }
                });
        options.put("read_timeout_ms", 30000);
        AmplitudeSourceReader reader = reader();
        Future<?> poll =
                worker.submit(
                        () -> {
                            try {
                                reader.pollNext(collector(new ArrayList<>()));
                            } catch (Exception e) {
                                throw new RuntimeException(e);
                            }
                        });
        try {
            assertTrue(entered.await(5, TimeUnit.SECONDS));
            poll.get(2, TimeUnit.SECONDS);
            verify(context, never()).signalNoMoreElement();
        } finally {
            release.countDown();
            poll.get(5, TimeUnit.SECONDS);
        }
    }

    @Test
    void checkpointAndCancelRemainAvailableDuringDownload() throws Exception {
        cancelWhileBlocked(true);
    }

    private void cancelWhileBlocked(boolean body) throws Exception {
        byte[] bytes = zip(gzip(EVENT));
        CountDownLatch entered = new CountDownLatch(1), release = new CountDownLatch(1);
        server.createContext(
                "/api/2/export",
                exchange -> {
                    if (body) {
                        exchange.sendResponseHeaders(200, bytes.length);
                        exchange.getResponseBody().write(bytes, 0, bytes.length / 2);
                        exchange.getResponseBody().flush();
                    }
                    entered.countDown();
                    try {
                        release.await();
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    exchange.close();
                });
        AmplitudeSourceReader reader = reader();
        Collector<SeaTunnelRow> output = collector(new ArrayList<>());
        Object checkpointLock = output.getCheckpointLock();
        AtomicInteger checkpointLookups = new AtomicInteger();
        CountDownLatch acquiring = new CountDownLatch(1), allowAcquire = new CountDownLatch(1);
        doAnswer(
                        call -> {
                            if (checkpointLookups.incrementAndGet() > 1) {
                                acquiring.countDown();
                                assertTrue(allowAcquire.await(5, TimeUnit.SECONDS));
                            }
                            return checkpointLock;
                        })
                .when(output)
                .getCheckpointLock();
        Future<?> future =
                worker.submit(
                        () -> assertThrows(IOException.class, () -> pollToEnd(reader, output)));
        assertTrue(entered.await(5, TimeUnit.SECONDS));
        try {
            synchronized (checkpointLock) {
                assertEquals(1, reader.snapshotState(1).size());
                assertTrue(acquiring.await(5, TimeUnit.SECONDS));
                reader.cancel();
                allowAcquire.countDown();
            }
            future.get(5, TimeUnit.SECONDS);
        } finally {
            allowAcquire.countDown();
            release.countDown();
        }
        verify(context, never()).signalNoMoreElement();
        assertThrows(IOException.class, reader::open);
    }

    @Test
    void checkpointRestoreReplaysWindowAfterPartialEmission() throws Exception {
        serve(zip(gzip(EVENT + EVENT.replace("one", "two"))));
        AmplitudeSourceReader first = reader();
        List<SeaTunnelRow> before = new ArrayList<>();
        Collector<SeaTunnelRow> interrupted = collector(before);
        doAnswer(
                        call -> {
                            before.add(call.getArgument(0));
                            throw new IOException("injected downstream failure");
                        })
                .when(interrupted)
                .collect(any(SeaTunnelRow.class));
        List<SingleSplit> state = first.snapshotState(1);
        assertThrows(IOException.class, () -> pollToEnd(first, interrupted));
        first.close();
        context = mock(SourceReader.Context.class);
        AmplitudeSourceReader restored = reader();
        restored.addSplits(state);
        List<SeaTunnelRow> after = new ArrayList<>();
        pollToEnd(restored, collector(after));
        assertEquals(1, before.size());
        assertEquals(2, after.size());
        assertEquals(before.get(0).getField(1), after.get(0).getField(1));
        assertEquals(2, requests.get());
        verify(context).signalNoMoreElement();
    }

    @Test
    void emissionReturnsBetweenBoundedCheckpointAtomicBatches() throws Exception {
        serve(zip(gzip(String.join("", java.util.Collections.nCopies(300, EVENT)))));
        List<SeaTunnelRow> rows = new ArrayList<>();
        Collector<SeaTunnelRow> output = collector(rows);
        Thread pollingThread = Thread.currentThread();
        doAnswer(
                        call -> {
                            assertEquals(pollingThread, Thread.currentThread());
                            assertTrue(Thread.holdsLock(output.getCheckpointLock()));
                            rows.add(call.getArgument(0));
                            return null;
                        })
                .when(output)
                .collect(any(SeaTunnelRow.class));
        AmplitudeSourceReader reader = reader();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (rows.size() < 300) {
            int before = rows.size();
            reader.pollNext(output);
            assertTrue(rows.size() - before <= 128, "one poll must not monopolize the engine");
            assertTrue(System.nanoTime() < deadline);
            Thread.sleep(1);
        }
        pollToEnd(reader, output);
        assertEquals(300, rows.size());
        verify(context).signalNoMoreElement();
    }

    @Test
    void rejectsOversizedHttpHeaderLines() throws Exception {
        server.createContext(
                "/api/2/export",
                exchange -> {
                    exchange.getResponseHeaders()
                            .add(
                                    "X-Untrusted",
                                    String.join("", java.util.Collections.nCopies(10000, "a")));
                    respond(exchange, 200, zip(gzip(EVENT)));
                });
        List<SeaTunnelRow> rows = new ArrayList<>();
        assertThrows(IOException.class, () -> pollToEnd(reader(), collector(rows)));
        assertTrue(rows.isEmpty());
        verify(context, never()).signalNoMoreElement();
    }

    @Test
    void completionCallbackDoesNotHoldCheckpointLock() throws Exception {
        serve(zip(gzip(EVENT)));
        Collector<SeaTunnelRow> output = collector(new ArrayList<>());
        doAnswer(
                        call -> {
                            assertFalse(Thread.holdsLock(output.getCheckpointLock()));
                            return null;
                        })
                .when(context)
                .signalNoMoreElement();
        pollToEnd(reader(), output);
        verify(context).signalNoMoreElement();
    }
}
