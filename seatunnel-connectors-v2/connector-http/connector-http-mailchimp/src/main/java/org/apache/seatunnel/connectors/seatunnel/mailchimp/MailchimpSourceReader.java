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

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.format.json.JsonDeserializationSchema;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.DefaultConnectionReuseStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.Base64;

final class MailchimpSourceReader extends AbstractSingleSplitReader<SeaTunnelRow> {
    private static final int MAX_PAGE_BYTES = 32 * 1024 * 1024;
    private final MailchimpConfig config;
    private final SingleSplitReaderContext context;
    private final JsonDeserializationSchema deserializer;
    private final String tableId;
    private final ObjectMapper mapper =
            new ObjectMapper()
                    .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                    .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
    private final Object lifecycle = new Object();
    private volatile boolean closed;
    private CloseableHttpClient client;
    private HttpGet activeRequest;

    MailchimpSourceReader(
            MailchimpConfig config, CatalogTable table, SingleSplitReaderContext context) {
        this.config = config;
        this.context = context;
        deserializer = new JsonDeserializationSchema(table, false, false);
        tableId = table.getTablePath().toString();
    }

    @Override
    public void open() throws IOException {
        synchronized (lifecycle) {
            if (closed) {
                throw new IOException("Mailchimp reader is closed");
            }
            if (client == null) {
                RequestConfig timeouts =
                        RequestConfig.custom()
                                .setConnectTimeout(config.timeoutMs)
                                .setSocketTimeout(config.timeoutMs)
                                .setConnectionRequestTimeout(config.timeoutMs)
                                .setRedirectsEnabled(false)
                                .build();
                client =
                        HttpClients.custom()
                                .setDefaultRequestConfig(timeouts)
                                .setConnectionReuseStrategy(
                                        (response, context) ->
                                                response.getStatusLine().getStatusCode() < 400
                                                        && DefaultConnectionReuseStrategy.INSTANCE
                                                                .keepAlive(response, context))
                                .disableRedirectHandling()
                                .disableAutomaticRetries()
                                .disableCookieManagement()
                                .build();
            }
        }
    }

    /**
     * One bounded batch. Checkpoints deliberately retain an empty split, so a recovered reader
     * replays from offset zero instead of treating offsets in a mutable audience as durable
     * cursors.
     */
    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        if (closed || noMoreSplits) {
            return;
        }
        long offset = 0;
        long target = -1;
        while (!closed) {
            int count =
                    target < 0 ? config.pageSize : (int) Math.min(config.pageSize, target - offset);
            JsonNode page = requestPage(offset, count);
            JsonNode members = page.get("members");
            JsonNode total = page.get("total_items");
            if (!page.isObject()
                    || members == null
                    || !members.isArray()
                    || total == null
                    || !total.isIntegralNumber()
                    || !total.canConvertToInt()
                    || total.intValue() < 0) {
                throw new IOException(
                        "Mailchimp page must contain members array and nonnegative integer total_items");
            }
            if (target < 0) {
                target = total.intValue();
            }
            if (total.intValue() < target || members.size() != Math.min(count, target - offset)) {
                throw new IOException(
                        "Mailchimp pagination is incomplete or audience changed; retry the batch");
            }
            // Validate and convert the entire page before accepting any of its rows.
            SeaTunnelRow[] rows = new SeaTunnelRow[members.size()];
            for (int i = 0; i < rows.length; i++) {
                if (!members.get(i).isObject()) {
                    throw new IOException("Mailchimp members must be JSON objects");
                }
                try {
                    rows[i] = deserializer.convertToRowData(members.get(i));
                    rows[i].setTableId(tableId);
                } catch (Exception e) {
                    // JSON conversion errors may include member data. Do not retain their causes.
                    throw new IOException("Mailchimp member does not match the configured schema");
                }
            }
            for (SeaTunnelRow row : rows) {
                synchronized (output.getCheckpointLock()) {
                    if (closed) {
                        return;
                    }
                    output.collect(row);
                }
            }
            offset += rows.length;
            if (offset >= target) {
                boolean finished;
                synchronized (lifecycle) {
                    finished = !closed;
                    if (finished) {
                        noMoreSplits = true;
                    }
                }
                // Completion may wait for a remote acknowledgement; never block cancel on it.
                if (finished) {
                    context.signalNoMoreElement();
                }
                return;
            }
        }
    }

    private JsonNode requestPage(long offset, int count) throws Exception {
        for (int attempt = 0; ; attempt++) {
            HttpGet request =
                    new HttpGet(
                            new URIBuilder(config.membersUri)
                                    .addParameter("count", Integer.toString(count))
                                    .addParameter("offset", Long.toString(offset))
                                    .build());
            String basic =
                    Base64.getEncoder()
                            .encodeToString(
                                    ("seatunnel:" + config.apiKey)
                                            .getBytes(StandardCharsets.UTF_8));
            request.setHeader("Authorization", "Basic " + basic);
            request.setHeader("Accept", "application/json");
            CloseableHttpClient httpClient;
            synchronized (lifecycle) {
                ensureOpen();
                activeRequest = request;
                httpClient = client;
            }
            int status;
            long delay = Math.min(60000L, (long) config.backoffMs << attempt);
            byte[] body = null;
            try (CloseableHttpResponse response = httpClient.execute(request)) {
                status = response.getStatusLine().getStatusCode();
                if (status == 200) {
                    if (response.getEntity() == null) {
                        throw new IOException("Missing response");
                    }
                    try (InputStream in = response.getEntity().getContent()) {
                        boolean consumed = false;
                        try {
                            body = readPage(in);
                            consumed = true;
                        } finally {
                            if (!consumed) {
                                // Entity close can drain an unfinished tail. Abort before close.
                                request.abort();
                            }
                        }
                    }
                } else if (response.getFirstHeader("Retry-After") != null) {
                    delay = retryDelay(response.getFirstHeader("Retry-After").getValue(), delay);
                }
            } catch (IOException | RuntimeException e) {
                // Transport errors can expose URI values. No response/error content is surfaced.
                throw new IOException("Mailchimp request failed or reader was closed");
            } finally {
                synchronized (lifecycle) {
                    activeRequest = null;
                }
                request.releaseConnection();
            }
            if (status == 200) {
                try {
                    JsonNode page = mapper.readTree(body);
                    if (page == null) {
                        throw new IOException("Empty response");
                    }
                    return page;
                } catch (IOException e) {
                    throw new IOException("Mailchimp returned invalid JSON");
                }
            }
            if ((status != 429 && (status < 500 || status > 599)) || attempt >= config.retry) {
                throw new IOException("Mailchimp request failed with HTTP " + status);
            }
            synchronized (lifecycle) {
                ensureOpen();
                long deadline = System.nanoTime() + delay * 1_000_000L;
                long remaining;
                while ((remaining = deadline - System.nanoTime()) > 0) {
                    try {
                        lifecycle.wait(Math.max(1, remaining / 1_000_000L));
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new IOException("Mailchimp retry interrupted");
                    }
                    ensureOpen();
                }
            }
        }
    }

    private byte[] readPage(InputStream in) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        byte[] buffer = new byte[8192];
        int n;
        while ((n = in.read(buffer)) != -1) {
            if (out.size() > MAX_PAGE_BYTES - n) {
                throw new IOException("Mailchimp page exceeds size limit");
            }
            out.write(buffer, 0, n);
        }
        return out.toByteArray();
    }

    private static long retryDelay(String value, long fallback) {
        try {
            long seconds = Long.parseLong(value);
            return seconds < 0 ? fallback : Math.min(seconds, 60) * 1000L;
        } catch (NumberFormatException e) {
            try {
                long millis =
                        ZonedDateTime.parse(value, DateTimeFormatter.RFC_1123_DATE_TIME)
                                        .toInstant()
                                        .toEpochMilli()
                                - System.currentTimeMillis();
                return Math.max(1, Math.min(60000, millis));
            } catch (DateTimeParseException ignored) {
                return fallback;
            }
        }
    }

    private void ensureOpen() throws IOException {
        if (closed || client == null) {
            throw new IOException("Mailchimp reader is not open");
        }
    }

    @Override
    public void cancel() {
        try {
            close();
        } catch (IOException e) {
            throw new IllegalStateException("Mailchimp reader could not close its HTTP client");
        }
    }

    /** Abort in-flight I/O and wake retry waits without waiting for the checkpoint lock. */
    @Override
    public void close() throws IOException {
        synchronized (lifecycle) {
            closed = true;
            if (activeRequest != null) {
                activeRequest.abort();
            }
            lifecycle.notifyAll();
            if (client != null) {
                client.close();
            }
        }
    }
}
