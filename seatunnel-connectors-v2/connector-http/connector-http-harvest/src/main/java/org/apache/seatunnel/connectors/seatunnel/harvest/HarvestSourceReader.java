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

package org.apache.seatunnel.connectors.seatunnel.harvest;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

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
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.time.Clock;
import java.time.Duration;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** Reads one page at a time; network waits never hold the checkpoint lock. */
final class HarvestSourceReader extends AbstractSingleSplitReader<SeaTunnelRow> {
    private final HarvestSourceConfig config;
    private final SingleSplitReaderContext context;
    private final JsonDeserializationSchema deserializer;
    private final Clock clock;
    private final Sleeper sleeper;
    private volatile CloseableHttpClient client;
    private long page = 1;
    private final CatalogTable table;
    private boolean restoredFinished;

    HarvestSourceReader(
            HarvestSourceConfig config, CatalogTable table, SingleSplitReaderContext context) {
        this(config, table, context, Clock.systemUTC(), Thread::sleep);
    }

    HarvestSourceReader(
            HarvestSourceConfig config,
            CatalogTable table,
            SingleSplitReaderContext context,
            Clock clock,
            Sleeper sleeper) {
        this.config = config;
        this.table = table;
        this.context = context;
        this.deserializer = new JsonDeserializationSchema(table, false, false);
        this.clock = clock;
        this.sleeper = sleeper;
    }

    @Override
    public void open() {
        RequestConfig request =
                RequestConfig.custom()
                        .setConnectTimeout(config.connectTimeout)
                        .setConnectionRequestTimeout(config.connectTimeout)
                        .setSocketTimeout(config.socketTimeout)
                        .build();
        // Never forward credentials to a redirect or retry an ambiguous transport failure.
        client =
                HttpClients.custom()
                        .setDefaultRequestConfig(request)
                        .disableRedirectHandling()
                        .disableAutomaticRetries()
                        .build();
    }

    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        if (noMoreSplits) {
            return;
        }
        if (restoredFinished) {
            synchronized (output.getCheckpointLock()) {
                noMoreSplits = true;
                context.signalNoMoreElement();
            }
            return;
        }
        JsonNode response = requestPage();
        JsonNode entries = response.get("time_entries");
        JsonNode current = response.get("page");
        JsonNode next = response.get("next_page");
        if (entries == null
                || !entries.isArray()
                || current == null
                || !current.isIntegralNumber()
                || !current.canConvertToLong()
                || current.longValue() != page
                || next == null) {
            throw failure("invalid time_entries, page or next_page in response");
        }
        if (!next.isNull()
                && (!next.isIntegralNumber()
                        || !next.canConvertToLong()
                        || page == Long.MAX_VALUE
                        || next.longValue() != page + 1)) {
            throw failure("next_page must advance by one");
        }
        List<SeaTunnelRow> rows = new ArrayList<>(entries.size());
        for (JsonNode entry : entries) {
            if (!entry.isObject()) {
                throw failure("time_entries must contain JSON objects");
            }
            try {
                SeaTunnelRow row = deserializer.convertToRowData(entry);
                deserializer.setCollectorTablePath(row, table);
                rows.add(row);
            } catch (RuntimeException e) {
                // Conversion exceptions can include record contents or echoed credentials.
                throw failure("time entry does not match the configured schema");
            }
        }
        synchronized (output.getCheckpointLock()) {
            for (SeaTunnelRow row : rows) {
                output.collect(row);
            }
            if (next.isNull()) {
                noMoreSplits = true;
                context.signalNoMoreElement();
            } else {
                page = next.longValue();
            }
        }
    }

    @Override
    protected byte[] snapshotStateToBytes(long checkpointId) {
        return new byte[] {1, (byte) ((noMoreSplits || restoredFinished) ? 1 : 0)};
    }

    @Override
    protected void restoreState(byte[] state) {
        if (state.length != 2 || state[0] != 1 || (state[1] != 0 && state[1] != 1)) {
            throw new IllegalArgumentException("Invalid Harvest reader checkpoint state");
        }
        restoredFinished = state[1] == 1;
    }

    private JsonNode requestPage() throws Exception {
        URIBuilder uri = new URIBuilder(config.endpoint);
        uri.addParameter("page", Long.toString(page));
        uri.addParameter("per_page", Integer.toString(config.pageSize));
        for (Map.Entry<String, String> filter : config.filters.entrySet()) {
            uri.addParameter(filter.getKey(), filter.getValue());
        }
        long localDelay = config.backoff;
        for (int attempt = 0; ; attempt++) {
            if (Thread.currentThread().isInterrupted()) {
                throw new InterruptedException("Harvest page read interrupted");
            }
            HttpGet request = new HttpGet(uri.build());
            request.setHeader("Authorization", "Bearer " + config.accessToken);
            request.setHeader("Harvest-Account-Id", config.accountId);
            request.setHeader("User-Agent", "Apache-SeaTunnel/Harvest");
            request.setHeader("Accept", "application/json");
            long delay;
            try (CloseableHttpResponse response = client.execute(request)) {
                int status = response.getStatusLine().getStatusCode();
                if (status == 200) {
                    if (response.getEntity() == null) {
                        throw failure("empty HTTP response");
                    }
                    byte[] body = EntityUtils.toByteArray(response.getEntity());
                    try {
                        JsonNode json = deserializer.deserializeToJsonNode(body);
                        if (json == null || !json.isObject()) {
                            throw failure("expected a JSON object");
                        }
                        return json;
                    } catch (IOException | RuntimeException e) {
                        throw failure("invalid JSON response");
                    }
                }
                if (!retryable(status) || attempt >= config.retries) {
                    throw failure("HTTP " + status + " after " + (attempt + 1) + " attempt(s)");
                }
                Header retryAfter = response.getFirstHeader("Retry-After");
                delay = Math.max(localDelay, retryAfterMillis(retryAfter));
                if (delay > config.maxDelay) {
                    throw failure("Retry-After exceeds max_retry_delay_ms");
                }
            }
            // Close each response before waiting so the connection is never held during backoff.
            try {
                sleeper.sleep(delay);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw e;
            }
            localDelay = Math.min((long) config.maxDelay, localDelay * 2);
        }
    }

    private long retryAfterMillis(Header header) {
        if (header == null) {
            return 0;
        }
        String value = header.getValue().trim();
        if (value.matches("[0-9]+")) {
            try {
                return Math.multiplyExact(Long.parseLong(value), 1000L);
            } catch (ArithmeticException | NumberFormatException e) {
                return Long.MAX_VALUE;
            }
        }
        try {
            return Math.max(
                    0,
                    Duration.between(
                                    clock.instant(),
                                    ZonedDateTime.parse(value, DateTimeFormatter.RFC_1123_DATE_TIME)
                                            .toInstant())
                            .toMillis());
        } catch (DateTimeParseException | ArithmeticException e) {
            // Harvest documents seconds; malformed headers explicitly use local backoff.
            return 0;
        }
    }

    private static boolean retryable(int status) {
        return status == 429 || status == 500 || status == 502 || status == 503 || status == 504;
    }

    private IOException failure(String reason) {
        return new IOException("Harvest time_entries page " + page + ": " + reason);
    }

    @Override
    public void close() throws IOException {
        CloseableHttpClient current = client;
        if (current != null) {
            current.close();
        }
    }

    @FunctionalInterface
    interface Sleeper {
        void sleep(long millis) throws InterruptedException;
    }
}
