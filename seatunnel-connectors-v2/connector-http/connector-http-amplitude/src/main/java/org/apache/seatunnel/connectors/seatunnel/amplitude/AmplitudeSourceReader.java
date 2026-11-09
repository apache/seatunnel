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

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonParser;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.format.json.JsonDeserializationSchema;

import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.http.Header;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.config.MessageConstraints;
import org.apache.http.impl.NoConnectionReuseStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Enumeration;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.zip.CRC32;
import java.util.zip.CheckedInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/** One bounded export; checkpoint restore deliberately replays the entire configured window. */
final class AmplitudeSourceReader extends AbstractSingleSplitReader<SeaTunnelRow> {
    private final AmplitudeSourceConfig config;
    private final SingleSplitReaderContext context;
    private final JsonDeserializationSchema deserializer;
    private final ObjectMapper mapper;
    private final String tableId;
    private final Path temporaryDirectory;
    private final Object lifecycle = new Object();
    private volatile boolean cancelled;
    private volatile HttpGet inFlight;
    private CloseableHttpClient client;
    private ExecutorService downloads;
    private Future<Boolean> prepared;
    private volatile Path temporaryArchive;
    private final BlockingQueue<SeaTunnelRow> readyRows = new ArrayBlockingQueue<>(1);

    AmplitudeSourceReader(
            AmplitudeSourceConfig config, CatalogTable table, SingleSplitReaderContext context) {
        this(config, table, context, null);
    }

    AmplitudeSourceReader(
            AmplitudeSourceConfig config,
            CatalogTable table,
            SingleSplitReaderContext context,
            Path temporaryDirectory) {
        this.config = config;
        this.context = context;
        this.temporaryDirectory = temporaryDirectory;
        deserializer = new JsonDeserializationSchema(table, false, false);
        mapper = deserializer.getObjectMapper();
        mapper.enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
        mapper.disable(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS);
        tableId = table.getTablePath().toString();
    }

    /** Creates a connector-owned client without redirects or hidden retries. */
    @Override
    public void open() throws IOException {
        synchronized (lifecycle) {
            checkCancelled();
            if (client == null) {
                client =
                        HttpClients.custom()
                                .setConnectionReuseStrategy(NoConnectionReuseStrategy.INSTANCE)
                                .setDefaultConnectionConfig(
                                        ConnectionConfig.custom()
                                                .setMessageConstraints(
                                                        MessageConstraints.custom()
                                                                .setMaxHeaderCount(100)
                                                                .setMaxLineLength(8192)
                                                                .build())
                                                .build())
                                .setDefaultRequestConfig(
                                        RequestConfig.custom()
                                                .setConnectTimeout(config.connectTimeout)
                                                .setConnectionRequestTimeout(config.connectTimeout)
                                                .setSocketTimeout(config.readTimeout)
                                                .build())
                                .disableRedirectHandling()
                                .disableAutomaticRetries()
                                .disableCookieManagement()
                                .disableAuthCaching()
                                .disableContentCompression()
                                .build();
                downloads =
                        Executors.newSingleThreadExecutor(
                                runnable -> {
                                    Thread thread =
                                            new Thread(runnable, "seatunnel-amplitude-export");
                                    thread.setDaemon(true);
                                    return thread;
                                });
            }
        }
    }

    /** Only bounded, nonblocking row batches run under the checkpoint lock. */
    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        if (noMoreSplits) {
            return;
        }
        checkCancelled();
        try {
            synchronized (lifecycle) {
                checkCancelled();
                if (downloads == null) {
                    throw new ExportFailure("Amplitude reader is not open");
                }
                if (prepared == null) {
                    prepared = downloads.submit(this::prepareArchive);
                }
            }
            // A transiently empty handoff must not trigger the engine's 100 ms idle sleep
            // between decoded rows. Keep this short wait outside both local monitors.
            SeaTunnelRow row = readyRows.poll(50, TimeUnit.MILLISECONDS);
            synchronized (output.getCheckpointLock()) {
                checkCancelled();
                for (int count = 0; count < 128 && row != null; count++) {
                    checkCancelled();
                    output.collect(row);
                    row = count < 127 ? readyRows.poll() : null;
                }
                if (!prepared.isDone() || !readyRows.isEmpty()) {
                    // Do not wait for I/O or parsing: the engine must regain control for
                    // savepoints.
                    return;
                }
                try {
                    prepared.get();
                } catch (CancellationException cancelled) {
                    throw new ExportFailure("Amplitude reader cancelled");
                } catch (ExecutionException failed) {
                    Throwable cause = failed.getCause();
                    if (cause instanceof Exception) {
                        throw (Exception) cause;
                    }
                    throw (Error) cause;
                }
                synchronized (lifecycle) {
                    checkCancelled();
                    noMoreSplits = true;
                }
            }
        } catch (IOException e) {
            // Libraries may include credentials, response bodies, entry names or worker paths.
            throw new ExportFailure(
                    e instanceof ExportFailure
                            ? e.getMessage()
                            : "Amplitude export failed ("
                                    + e.getClass().getSimpleName()
                                    + "); replay the complete upload-time window");
        }
        // This can wait for remote acknowledgement; never hold local monitors on it.
        context.signalNoMoreElement();
    }

    private boolean prepareArchive() throws Exception {
        Path archive =
                temporaryDirectory == null
                        ? Files.createTempFile("seatunnel-amplitude-", ".zip")
                        : Files.createTempFile(temporaryDirectory, "seatunnel-amplitude-", ".zip");
        try {
            temporaryArchive = archive;
            checkCancelled();
            boolean available = download(archive);
            if (available) {
                AmplitudeArchive.validateDirectory(archive);
                try (ZipFile zip = new ZipFile(archive.toFile())) {
                    readArchive(zip, false);
                    readArchive(zip, true);
                }
            }
            checkCancelled();
            return available;
        } finally {
            deleteArchive();
        }
    }

    private void deleteArchive() throws IOException {
        Path archive = temporaryArchive;
        if (archive != null) {
            Files.deleteIfExists(archive);
            temporaryArchive = null;
        }
    }

    private boolean download(Path archive) throws IOException, InterruptedException {
        for (int attempt = 0; ; attempt++) {
            checkCancelled();
            HttpGet request = request();
            inFlight = request;
            CloseableHttpClient active;
            synchronized (lifecycle) {
                checkCancelled();
                active = client;
            }
            if (active == null) {
                throw new ExportFailure("Amplitude reader is not open");
            }
            long delay = backoff(attempt);
            boolean bodyComplete = false;
            try (CloseableHttpResponse response = active.execute(request)) {
                int status = response.getStatusLine().getStatusCode();
                if (status == 404) {
                    return false;
                }
                if (status == 200) {
                    Header encoding = response.getFirstHeader("Content-Encoding");
                    if (encoding != null
                            && !"identity".equalsIgnoreCase(encoding.getValue().trim())) {
                        throw new ExportFailure(
                                "Amplitude export requires identity HTTP content encoding");
                    }
                    if (response.getEntity() == null) {
                        throw new ExportFailure("Amplitude ZIP response has no entity");
                    }
                    if (response.getEntity().getContentLength() > config.maxArchiveBytes) {
                        throw new ExportFailure(
                                "Amplitude export exceeds max_archive_bytes; reduce the window");
                    }
                    try (InputStream input = response.getEntity().getContent();
                            OutputStream file = Files.newOutputStream(archive)) {
                        try {
                            byte[] buffer = new byte[8192];
                            long downloaded = 0;
                            int count;
                            while ((count = input.read(buffer)) != -1) {
                                checkCancelled();
                                downloaded += count;
                                if (downloaded > config.maxArchiveBytes) {
                                    throw new ExportFailure(
                                            "Amplitude export exceeds max_archive_bytes; reduce the window");
                                }
                                try {
                                    file.write(buffer, 0, count);
                                } catch (IOException e) {
                                    throw new ExportFailure(
                                            "Amplitude export temporary file write failed");
                                }
                            }
                            bodyComplete = true;
                        } finally {
                            if (!bodyComplete) {
                                request.abort();
                            }
                        }
                    }
                    return true;
                }
                if ((status != 429 && (status < 500 || status > 599))
                        || attempt >= config.maxRetries) {
                    throw new ExportFailure(
                            "Amplitude export rejected with HTTP status "
                                    + status
                                    + (status == 400
                                            ? "; check the window and service export-size limit"
                                            : ""));
                }
                delay = retryDelay(attempt, response.getFirstHeader("Retry-After"));
            } catch (ExportFailure e) {
                throw e;
            } catch (IOException e) {
                checkCancelled();
                if (attempt >= config.maxRetries) {
                    throw new ExportFailure("Amplitude export transport retry budget exhausted");
                }
            } finally {
                inFlight = null;
            }
            awaitRetry(delay);
        }
    }

    private HttpGet request() throws IOException {
        try {
            HttpGet request =
                    new HttpGet(
                            new URIBuilder(config.endpoint)
                                    .addParameter("start", config.start)
                                    .addParameter("end", config.end)
                                    .build());
            request.setHeader("Authorization", config.authorization());
            request.setHeader("Accept", "application/zip");
            request.setHeader("Accept-Encoding", "identity");
            return request;
        } catch (URISyntaxException e) {
            throw new ExportFailure("Amplitude export request could not be constructed");
        }
    }

    private long backoff(int attempt) {
        long exponential = Math.min((long) config.backoff << attempt, config.maxBackoff);
        long jitter =
                ThreadLocalRandom.current()
                        .nextLong(Math.min(exponential / 4, config.maxBackoff - exponential) + 1);
        return exponential + jitter;
    }

    private long retryDelay(int attempt, Header retryAfter) throws IOException {
        long delay = backoff(attempt);
        if (retryAfter == null) {
            return delay;
        }
        long requested;
        try {
            requested = Math.multiplyExact(Long.parseLong(retryAfter.getValue().trim()), 1000L);
        } catch (NumberFormatException | ArithmeticException e) {
            try {
                requested =
                        ZonedDateTime.parse(
                                                retryAfter.getValue(),
                                                DateTimeFormatter.RFC_1123_DATE_TIME)
                                        .toInstant()
                                        .toEpochMilli()
                                - System.currentTimeMillis();
            } catch (DateTimeParseException ignored) {
                return delay;
            }
        }
        if (requested > config.maxBackoff) {
            throw new ExportFailure(
                    "Amplitude Retry-After exceeds max_retry_backoff_ms; retry the job later");
        }
        return Math.max(delay, requested);
    }

    private void awaitRetry(long delay) throws IOException, InterruptedException {
        synchronized (lifecycle) {
            checkCancelled();
            long deadline = System.nanoTime() + delay * 1000000L;
            long remaining = delay;
            while (remaining > 0) {
                lifecycle.wait(remaining);
                checkCancelled();
                remaining = (deadline - System.nanoTime() + 999999L) / 1000000L;
            }
        }
    }

    private void readArchive(ZipFile zip, boolean emit) throws Exception {
        if (zip.size() > 10000) {
            throw new ExportFailure("Amplitude ZIP exceeds 10000 entries; reduce the window");
        }
        long entryBytes = 0;
        long decodedBytes = 0;
        Enumeration<? extends ZipEntry> entries = zip.entries();
        while (entries.hasMoreElements()) {
            checkCancelled();
            ZipEntry entry = entries.nextElement();
            if (entry.isDirectory()) {
                if (entry.getSize() != 0) {
                    throw new ExportFailure("Amplitude ZIP has an invalid directory");
                }
                continue;
            }
            if (entry.getSize() < 0 || entry.getSize() > config.maxUncompressedBytes - entryBytes) {
                throw new ExportFailure("Amplitude ZIP exceeds max_uncompressed_bytes");
            }
            entryBytes += entry.getSize();
            CRC32 crc = new CRC32();
            try (BoundedInputStream entryInput =
                            new BoundedInputStream(zip.getInputStream(entry), entry.getSize());
                    CheckedInputStream raw =
                            new CheckedInputStream(new BufferedInputStream(entryInput), crc);
                    BufferedInputStream gzipSource = new BufferedInputStream(raw);
                    BoundedInputStream header = new BoundedInputStream(gzipSource, 65536);
                    InputStream gzip = openGzip(header);
                    BoundedInputStream decoded =
                            new BoundedInputStream(
                                    gzip, config.maxUncompressedBytes - decodedBytes);
                    InputStream events = new BufferedInputStream(decoded)) {
                byte[] line;
                while ((line = readLine(events)) != null) {
                    checkCancelled();
                    if (emit) {
                        readyRows.put(decode(line));
                    }
                }
                // One gzip member per file; reject trailing members/data without recursive reads.
                if (gzipSource.read() != -1) {
                    throw new ExportFailure(
                            "Amplitude gzip entry contains trailing data or multiple members");
                }
                decodedBytes += decoded.count;
                if (entryInput.count != entry.getSize() || crc.getValue() != entry.getCrc()) {
                    throw new ExportFailure("Amplitude ZIP entry CRC mismatch");
                }
            }
        }
    }

    private InputStream openGzip(BoundedInputStream header) throws IOException {
        InputStream gzip = new GzipCompressorInputStream(header, false);
        header.limit = Long.MAX_VALUE;
        return gzip;
    }

    private byte[] readLine(InputStream input) throws IOException {
        ByteArrayOutputStream line =
                new ByteArrayOutputStream(Math.min(8192, config.maxEventBytes));
        int next;
        while ((next = input.read()) != -1 && next != '\n') {
            if (line.size() >= config.maxEventBytes) {
                throw new ExportFailure("Amplitude event exceeds max_event_bytes");
            }
            line.write(next);
        }
        return next == -1 && line.size() == 0 ? null : line.toByteArray();
    }

    private SeaTunnelRow decode(byte[] line) throws IOException {
        try {
            JsonNode event = mapper.readTree(line);
            if (event == null || !event.isObject() || !event.path("event_type").isTextual()) {
                throw new ExportFailure("Amplitude export contains an invalid event");
            }
            ObjectNode mapped = ((ObjectNode) event).deepCopy();
            for (Map.Entry<String, String> field : config.fieldMapping.entrySet()) {
                JsonNode value = event.at(field.getValue());
                mapped.set(field.getKey(), value.isMissingNode() ? mapper.nullNode() : value);
            }
            SeaTunnelRow row = deserializer.convertToRowData(mapped);
            row.setTableId(tableId);
            return row;
        } catch (IOException | RuntimeException e) {
            throw new ExportFailure(
                    "Amplitude event cannot be decoded using the configured schema");
        }
    }

    private void checkCancelled() throws IOException {
        if (cancelled || Thread.currentThread().isInterrupted()) {
            throw new ExportFailure("Amplitude reader cancelled");
        }
    }

    /** Abort header/body waits and retry sleeps without acquiring the checkpoint lock. */
    @Override
    public void cancel() {
        CloseableHttpClient active;
        synchronized (lifecycle) {
            cancelled = true;
            lifecycle.notifyAll();
            active = client;
            client = null;
            if (prepared != null) {
                prepared.cancel(true);
            }
            if (downloads != null) {
                downloads.shutdownNow();
            }
        }
        HttpGet request = inFlight;
        if (request != null) {
            request.abort();
        }
        if (active != null) {
            try {
                active.close();
            } catch (IOException ignored) {
                // Do not retain transport errors containing credentials.
            }
        }
    }

    @Override
    public void close() throws IOException {
        cancel();
        try {
            if (downloads != null && !downloads.awaitTermination(5, TimeUnit.SECONDS)) {
                throw new ExportFailure("Amplitude download did not terminate after cancellation");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new ExportFailure("Amplitude close interrupted");
        }
        try {
            deleteArchive();
        } catch (IOException e) {
            throw new ExportFailure("Amplitude temporary export cleanup failed");
        }
    }

    private static final class ExportFailure extends IOException {
        private ExportFailure(String message) {
            super(message);
        }
    }

    private final class BoundedInputStream extends FilterInputStream {
        private long limit;
        private long count;

        private BoundedInputStream(InputStream input, long limit) {
            super(input);
            this.limit = limit;
        }

        @Override
        public int read() throws IOException {
            checkCancelled();
            int value = in.read();
            record(value < 0 ? 0 : 1);
            return value;
        }

        @Override
        public int read(byte[] buffer, int offset, int length) throws IOException {
            checkCancelled();
            int read = in.read(buffer, offset, length);
            record(Math.max(0, read));
            return read;
        }

        private void record(int bytes) throws IOException {
            count += bytes;
            if (count > limit) {
                throw new ExportFailure(
                        "Amplitude ZIP exceeds its declared size or max_uncompressed_bytes");
            }
        }
    }
}
