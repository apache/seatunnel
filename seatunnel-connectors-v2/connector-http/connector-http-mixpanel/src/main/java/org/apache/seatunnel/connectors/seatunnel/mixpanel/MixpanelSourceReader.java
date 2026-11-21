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

package org.apache.seatunnel.connectors.seatunnel.mixpanel;

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

import org.apache.http.Header;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.NoConnectionReuseStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URISyntaxException;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Map;

/** Streams one window. State deliberately contains no event offset: recovery replays the window. */
final class MixpanelSourceReader extends AbstractSingleSplitReader<SeaTunnelRow> {
    private final MixpanelSourceConfig config;
    private final SingleSplitReaderContext context;
    private final JsonDeserializationSchema deserializer;
    private final ObjectMapper mapper;
    private final String tableId;
    private final Object lifecycle = new Object();
    private volatile boolean cancelled;
    private volatile HttpGet inFlight;
    private CloseableHttpClient client;

    MixpanelSourceReader(
            MixpanelSourceConfig config, CatalogTable table, SingleSplitReaderContext context) {
        this.config = config;
        this.context = context;
        this.deserializer = new JsonDeserializationSchema(table, false, false);
        this.mapper = deserializer.getObjectMapper();
        mapper.enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
        mapper.disable(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS);
        this.tableId = table.getTablePath().toString();
    }

    @Override
    public void open() throws IOException {
        synchronized (lifecycle) {
            checkCancelled();
            if (client == null) {
                client =
                        HttpClients.custom()
                                // One long export per reader; retries must not reuse error sockets.
                                .setConnectionReuseStrategy(NoConnectionReuseStrategy.INSTANCE)
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
                                .build();
            }
        }
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        if (noMoreSplits) {
            return;
        }
        checkCancelled();
        try {
            export(output);
        } catch (IOException e) {
            if (e instanceof ExportFailure) {
                // Drop any library close errors attached by try-with-resources as suppressed.
                throw new ExportFailure(e.getMessage());
            }
            // HTTP and JSON libraries may include URIs, payloads or credentials in their causes.
            throw new IOException(
                    "Mixpanel export failed ("
                            + e.getClass().getSimpleName()
                            + "); the entire date window must be replayed");
        } finally {
            inFlight = null;
        }
        synchronized (output.getCheckpointLock()) {
            synchronized (lifecycle) {
                checkCancelled();
                noMoreSplits = true;
            }
        }
        // Completion may wait for a remote acknowledgement; never hold local monitors on it.
        context.signalNoMoreElement();
    }

    private void export(Collector<SeaTunnelRow> output) throws Exception {
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
                throw new ExportFailure("Mixpanel reader is not open");
            }
            long delay;
            try (CloseableHttpResponse response = active.execute(request)) {
                int status = response.getStatusLine().getStatusCode();
                if (status == 200) {
                    if (response.getEntity() == null) {
                        throw new ExportFailure("Mixpanel export response has no entity");
                    }
                    // No retry surrounds body consumption: partial exports must fail the task.
                    try (InputStream input =
                            new BufferedInputStream(response.getEntity().getContent())) {
                        boolean finished = false;
                        try {
                            byte[] line;
                            while ((line = readLine(input)) != null) {
                                checkCancelled();
                                SeaTunnelRow row = decode(line);
                                synchronized (output.getCheckpointLock()) {
                                    checkCancelled();
                                    output.collect(row);
                                }
                            }
                            finished = true;
                        } finally {
                            // Closing an entity stream can drain unread data for connection reuse.
                            if (!finished) {
                                request.abort();
                            }
                        }
                    }
                    return;
                }
                if ((status != 429 && (status < 500 || status > 599))
                        || attempt >= config.maxRetries) {
                    throw new ExportFailure("Mixpanel export rejected with HTTP status " + status);
                }
                delay = retryDelay(attempt, response.getFirstHeader("Retry-After"));
                // Close the response without consuming its error body. Aborting a completed,
                // empty response can invalidate an already-released pooled connection.
            } finally {
                inFlight = null;
            }
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
    }

    private HttpGet request() throws IOException {
        try {
            HttpGet request =
                    new HttpGet(
                            new URIBuilder(config.endpoint)
                                    .addParameter("project_id", Long.toString(config.projectId))
                                    .addParameter("from_date", config.fromDate)
                                    .addParameter("to_date", config.toDate)
                                    .build());
            request.setHeader("Authorization", config.authorization());
            request.setHeader("Accept", "text/plain");
            return request;
        } catch (URISyntaxException e) {
            throw new ExportFailure("Mixpanel export request could not be constructed");
        }
    }

    private long retryDelay(int attempt, Header retryAfter) {
        long exponential = Math.min((long) config.backoff << attempt, config.maxBackoff);
        if (retryAfter == null) {
            return exponential;
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
                return exponential;
            }
        }
        return Math.min(config.maxBackoff, Math.max(exponential, requested));
    }

    private byte[] readLine(InputStream input) throws IOException {
        ByteArrayOutputStream line =
                new ByteArrayOutputStream(Math.min(8192, config.maxEventBytes));
        int next;
        while ((next = input.read()) != -1 && next != '\n') {
            if (line.size() >= config.maxEventBytes) {
                throw new ExportFailure("Mixpanel event exceeds max_event_bytes");
            }
            line.write(next);
        }
        return next == -1 && line.size() == 0 ? null : line.toByteArray();
    }

    private SeaTunnelRow decode(byte[] line) throws IOException {
        try {
            JsonNode event = mapper.readTree(line);
            if (event == null
                    || !event.isObject()
                    || !event.path("event").isTextual()
                    || !event.path("properties").isObject()) {
                throw new IOException("Invalid Mixpanel event");
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
            throw new ExportFailure("Mixpanel event cannot be decoded using the configured schema");
        }
    }

    private void checkCancelled() throws IOException {
        if (cancelled) {
            throw new ExportFailure("Mixpanel reader cancelled");
        }
    }

    /**
     * Abort without acquiring the checkpoint lock, including a request waiting on response headers.
     */
    @Override
    public void cancel() {
        CloseableHttpClient active;
        synchronized (lifecycle) {
            cancelled = true;
            lifecycle.notifyAll();
            active = client;
            client = null;
        }
        HttpGet request = inFlight;
        if (request != null) {
            request.abort();
        }
        if (active != null) {
            try {
                active.close();
            } catch (IOException ignored) {
                // The request was already aborted. Never retain client errors containing secrets.
            }
        }
    }

    @Override
    public void close() {
        cancel();
    }

    private static final class ExportFailure extends IOException {
        private ExportFailure(String message) {
            super(message);
        }
    }
}
