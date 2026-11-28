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

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;

import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Reads and validates one bounded daily report before collecting rows. No remote cursor is
 * checkpointed; restoration replays the configured report interval.
 */
final class MatomoSourceReader extends AbstractSingleSplitReader<SeaTunnelRow> {
    private static final int MAX_RESPONSE_BYTES = 2 * 1024 * 1024;
    private final URI endpoint;
    private final int siteId;
    private final LocalDate start;
    private final LocalDate end;
    private final String token;
    private final int timeoutMs;
    private final SingleSplitReaderContext context;
    private final String tableId;
    /**
     * Sticky cancellation prevents dispatch and terminal signaling. open() closes a client
     * published after cancellation rather than retaining it.
     */
    private final AtomicBoolean cancelled = new AtomicBoolean();
    /*
     * CAS publishes one client; getAndSet detaches it for exactly one close owner.
     */
    private final AtomicReference<CloseableHttpClient> client = new AtomicReference<>();
    /*
     * Publishes the request before a cancellation recheck so cancel cannot miss dispatch.
     */
    private final AtomicReference<HttpRequestBase> activeRequest = new AtomicReference<>();

    MatomoSourceReader(
            URI endpoint,
            int siteId,
            LocalDate start,
            LocalDate end,
            String token,
            int timeoutMs,
            SingleSplitReaderContext context,
            String tableId) {
        this.endpoint = endpoint;
        this.siteId = siteId;
        this.start = start;
        this.end = end;
        this.token = token;
        this.timeoutMs = timeoutMs;
        this.context = context;
        this.tableId = tableId;
    }

    /*
     * Publishes at most one client and closes losing or post-cancellation clients.
     */
    @Override
    public void open() throws IOException {
        if (cancelled.get() || client.get() != null) {
            return;
        }
        CloseableHttpClient created =
                HttpClients.custom()
                        .useSystemProperties()
                        .disableRedirectHandling()
                        .disableAutomaticRetries()
                        .setDefaultRequestConfig(
                                RequestConfig.custom()
                                        .setConnectTimeout(timeoutMs)
                                        .setConnectionRequestTimeout(timeoutMs)
                                        .setSocketTimeout(timeoutMs)
                                        .build())
                        .build();
        if (!client.compareAndSet(null, created)) {
            created.close();
        } else if (cancelled.get()) {
            // close() may have run before this client was published.
            closeClient();
        }
    }

    /*
     * Permanently marks cancellation and aborts the request currently owned by this reader.
     */
    @Override
    public void cancel() {
        cancelled.set(true);
        HttpRequestBase request = activeRequest.get();
        if (request != null) {
            request.abort();
        }
    }

    /*
     * Cancels I/O, then atomically detaches and closes the client.
     */
    @Override
    public void close() throws IOException {
        cancel();
        closeClient();
    }

    /*
     * Transfers close ownership atomically so concurrent lifecycle calls do not close twice.
     */
    private void closeClient() throws IOException {
        CloseableHttpClient current = client.getAndSet(null);
        if (current != null) {
            current.close();
        }
    }

    /**
     * Fetches and validates outside the checkpoint lock, then collects under that lock. Marks
     * completion with collection and signals end afterwards unless cancellation wins.
     */
    @Override
    public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
        if (noMoreSplits || cancelled.get()) {
            return;
        }
        List<SeaTunnelRow> rows;
        try {
            // Keep the HTTP exchange outside the collector checkpoint lock.
            rows = readRows();
        } catch (IOException e) {
            if (cancelled.get()) {
                return;
            }
            throw e;
        }
        synchronized (output.getCheckpointLock()) {
            if (noMoreSplits || cancelled.get()) {
                return;
            }
            for (SeaTunnelRow row : rows) {
                if (cancelled.get()) {
                    return;
                }
                output.collect(row);
            }
            noMoreSplits = true;
        }
        if (!cancelled.get()) {
            context.signalNoMoreElement();
        }
    }

    /**
     * Uses anonymous GET or an HTTPS form POST containing token_auth. Claims the single
     * active-request slot and clears it even on cancellation or HTTP failure.
     */
    List<SeaTunnelRow> readRows() throws Exception {
        checkCancelled();
        CloseableHttpClient current = client.get();
        if (current == null) {
            checkCancelled();
            throw new IOException("Matomo source reader is not open");
        }
        List<NameValuePair> params = new ArrayList<>();
        params.add(new BasicNameValuePair("module", "API"));
        params.add(new BasicNameValuePair("method", "VisitsSummary.get"));
        params.add(new BasicNameValuePair("idSite", String.valueOf(siteId)));
        params.add(new BasicNameValuePair("period", "day"));
        params.add(
                new BasicNameValuePair(
                        "date", start.equals(end) ? start.toString() : start + "," + end));
        params.add(new BasicNameValuePair("format", "JSON"));
        final HttpRequestBase request;
        if (token == null) {
            request = new HttpGet(new URIBuilder(endpoint).addParameters(params).build());
        } else {
            params.add(new BasicNameValuePair("token_auth", token));
            HttpPost post = new HttpPost(endpoint);
            post.setEntity(new UrlEncodedFormEntity(params, StandardCharsets.UTF_8));
            request = post;
        }
        request.setHeader("Accept", "application/json");
        if (!activeRequest.compareAndSet(null, request)) {
            throw new IOException("Matomo source reader already has an active request");
        }
        try {
            // A cancel before publication may have missed the newly tracked request.
            checkCancelled();
            return execute(current, request);
        } catch (IOException | IllegalStateException e) {
            // A concurrent close can shut down the connection pool before execute() acquires it.
            checkCancelled();
            throw e;
        } finally {
            activeRequest.compareAndSet(request, null);
        }
    }

    /*
     * Rejects failed, missing or decoded responses over 2 MiB before converting any rows.
     */
    private List<SeaTunnelRow> execute(CloseableHttpClient current, HttpRequestBase request)
            throws IOException {
        try (CloseableHttpResponse response = current.execute(request)) {
            int code = response.getStatusLine().getStatusCode();
            if (code < 200 || code >= 300) {
                request.abort();
                throw new IOException("Matomo Reporting API returned HTTP " + code);
            }
            HttpEntity entity = response.getEntity();
            if (entity == null || entity.getContentLength() > MAX_RESPONSE_BYTES) {
                request.abort();
                throw new IOException("Missing or oversized Matomo response");
            }
            try (InputStream input = entity.getContent()) {
                byte[] bytes = input.readNBytes(MAX_RESPONSE_BYTES + 1);
                if (bytes.length > MAX_RESPONSE_BYTES) {
                    request.abort();
                    throw new IOException("Oversized Matomo response");
                }
                checkCancelled();
                return decode(new String(bytes, StandardCharsets.UTF_8));
            }
        }
    }

    private void checkCancelled() throws IOException {
        if (cancelled.get()) {
            throw new IOException("Matomo source was cancelled");
        }
    }

    /**
     * Validates every requested day before returning rows. Only an explicit empty array means no
     * report; a missing day or API error fails the entire response.
     */
    List<SeaTunnelRow> decode(String json) throws IOException {
        final JsonNode root;
        try {
            root = JsonUtils.stringToJsonNode(json);
        } catch (Exception ignored) {
            throw new IOException("Matomo returned invalid JSON");
        }
        if (root == null || root.has("result")) {
            throw new IOException("Matomo Reporting API returned an error or no JSON document");
        }
        List<SeaTunnelRow> rows = new ArrayList<>();
        if (start.equals(end)) {
            addRow(rows, start, root);
            return rows;
        }
        if (!root.isObject()) {
            throw new IOException("Matomo date range response is not a date-keyed object");
        }
        for (LocalDate date = start; !date.isAfter(end); date = date.plusDays(1)) {
            addRow(rows, date, root.get(date.toString()));
        }
        return rows;
    }

    private void addRow(List<SeaTunnelRow> rows, LocalDate date, JsonNode data) throws IOException {
        if (data != null && data.isArray() && data.isEmpty()) {
            // A valid Matomo day without a report is represented exactly as an empty array.
            return;
        }
        if (data == null || !data.isObject()) {
            throw new IOException(
                    "Matomo did not return a report or an explicit empty value for " + date);
        }
        SeaTunnelRow row =
                new SeaTunnelRow(
                        new Object[] {
                            siteId,
                            date.toString(),
                            readCount(data, "nb_visits", false),
                            readCount(data, "nb_actions", false),
                            readCount(data, "nb_uniq_visitors", true)
                        });
        row.setTableId(tableId);
        rows.add(row);
    }

    /**
     * Accepts nonnegative integral JSON numbers or digit strings that fit BIGINT. Only optional
     * unique visitors may be missing or null; visits and actions are required.
     */
    private static Long readCount(JsonNode data, String name, boolean nullable) throws IOException {
        JsonNode field = data.get(name);
        if (field == null || field.isNull()) {
            if (nullable) {
                return null;
            }
        } else if (field.isIntegralNumber() && field.canConvertToLong() && field.longValue() >= 0) {
            return field.longValue();
        } else if (field.isTextual() && field.textValue().matches("[0-9]+")) {
            try {
                return Long.parseLong(field.textValue());
            } catch (NumberFormatException ignored) {
                // The count will be treated as invalid if it does not fit BIGINT.
            }
        }
        throw new IOException("Invalid or missing Matomo metric: " + name);
    }
}
