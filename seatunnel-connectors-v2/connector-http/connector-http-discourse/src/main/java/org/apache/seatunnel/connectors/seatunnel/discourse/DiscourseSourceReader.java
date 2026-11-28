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
package org.apache.seatunnel.connectors.seatunnel.discourse;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.SystemDefaultRoutePlanner;

import java.io.IOException;
import java.io.InputStream;
import java.net.ProxySelector;
import java.net.URI;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Fetches and validates one bounded metadata response before emitting any rows. No remote cursor is
 * checkpointed; a restored split requests the current response again.
 */
final class DiscourseSourceReader extends AbstractSingleSplitReader<SeaTunnelRow> {
    static final int MAX_RESPONSE_BYTES = 16 * 1024 * 1024;
    private static final int CONNECT_TIMEOUT_MS = 10_000;
    private static final int SOCKET_TIMEOUT_MS = 30_000;

    private final URI endpoint;
    private final String apiKey;
    private final String apiUsername;
    private final DiscourseSourceOptions.Resource resource;
    private final boolean includeSubcategories;
    private final CatalogTable table;
    private final SingleSplitReaderContext context;
    private final ObjectMapper mapper =
            new ObjectMapper().enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
    /*
     * Sticky cancellation is checked before dispatch and while decoding category/topic rows.
     */
    private final AtomicBoolean cancelled = new AtomicBoolean();
    /*
     * Published before checking cancellation so cancel either aborts or prevents dispatch.
     */
    private final AtomicReference<HttpGet> activeRequest = new AtomicReference<>();
    /*
     * Created by open and closed by close under the normal engine lifecycle.
     * Client publication is not atomically coordinated with a concurrent open/close call.
     */
    private CloseableHttpClient client;

    DiscourseSourceReader(
            URI endpoint,
            String apiKey,
            String apiUsername,
            DiscourseSourceOptions.Resource resource,
            boolean includeSubcategories,
            CatalogTable table,
            SingleSplitReaderContext context) {
        this.endpoint = endpoint;
        this.apiKey = apiKey;
        this.apiUsername = apiUsername;
        this.resource = resource;
        this.includeSubcategories = includeSubcategories;
        this.table = table;
        this.context = context;
    }

    /*
     * Creates the reader-owned HTTP client with finite inactivity timeouts and no redirects or retries.
     * This method relies on the engine calling open before polling and close after use.
     */
    @Override
    public void open() {
        client =
                HttpClients.custom()
                        .setDefaultRequestConfig(
                                RequestConfig.custom()
                                        .setConnectTimeout(CONNECT_TIMEOUT_MS)
                                        .setConnectionRequestTimeout(CONNECT_TIMEOUT_MS)
                                        .setSocketTimeout(SOCKET_TIMEOUT_MS)
                                        .build())
                        .disableRedirectHandling()
                        .disableAutomaticRetries()
                        .disableCookieManagement()
                        .disableAuthCaching()
                        .setRoutePlanner(new SystemDefaultRoutePlanner(ProxySelector.getDefault()))
                        .build();
    }

    /*
     * Permanently marks cancellation and aborts any currently published request.
     */
    @Override
    public void cancel() {
        cancelled.set(true);
        HttpGet request = activeRequest.get();
        if (request != null) {
            request.abort();
        }
    }

    /*
     * Cancels active I/O before closing the client created by open.
     */
    @Override
    public void close() throws IOException {
        cancel();
        if (client != null) {
            client.close();
        }
    }

    /*
     * Runs under the inherited checkpoint lock, including HTTP, validation and emission.
     * fetchRows validates the entire response first; only then are rows collected and end signaled.
     */
    @Override
    public void internalPollNext(Collector<SeaTunnelRow> output) throws IOException {
        for (SeaTunnelRow row : fetchRows()) {
            output.collect(row);
        }
        context.signalNoMoreElement();
    }

    /*
     * Publishes the request before checking the sticky cancellation flag.
     * Always detaches and releases the request, including validation and cancellation failures.
     */
    List<SeaTunnelRow> fetchRows() throws IOException {
        HttpGet request = new HttpGet(endpoint);
        activeRequest.set(request);
        try {
            if (cancelled.get()) {
                throw new IOException("Discourse source was cancelled");
            }
            return fetchRows(request);
        } finally {
            activeRequest.compareAndSet(request, null);
            request.releaseConnection();
        }
    }

    /*
     * Sends optional credentials only as headers and accepts exactly HTTP 200.
     * Bounds the decoded response to 16 MiB before conversion; no continuation link is followed.
     */
    private List<SeaTunnelRow> fetchRows(HttpGet request) throws IOException {
        request.setHeader("Accept", "application/json");
        if (apiKey != null) {
            request.setHeader("Api-Key", apiKey);
            request.setHeader("Api-Username", apiUsername);
        }
        byte[] bytes;
        try (CloseableHttpResponse response = client.execute(request)) {
            int status = response.getStatusLine().getStatusCode();
            if (status != 200) {
                request.abort();
                throw new IOException("Discourse " + resource + " request returned HTTP " + status);
            }
            if (response.getEntity() == null) {
                throw new IOException("Discourse response has no body");
            }
            try (InputStream stream = response.getEntity().getContent()) {
                bytes = stream.readNBytes(MAX_RESPONSE_BYTES + 1);
                if (bytes.length > MAX_RESPONSE_BYTES) {
                    request.abort();
                    throw new IOException("Discourse response exceeds the 16 MiB limit");
                }
            } catch (IOException e) {
                request.abort();
                throw e;
            }
        }
        return decodeRows(bytes);
    }

    /*
     * Validates all field types, required values and unique IDs before returning rows.
     * An explicit iterator stack flattens requested nested categories without Java recursion;
     * latest topics remain the single returned array. Diagnostics never echo parser payloads.
     */
    private List<SeaTunnelRow> decodeRows(byte[] bytes) throws IOException {
        final JsonNode body;
        try {
            body = mapper.readTree(bytes);
        } catch (IOException e) {
            // Do not attach parser diagnostics, which may quote the upstream response.
            throw new IOException("Discourse response is not valid JSON");
        }
        JsonNode rows = body == null ? null : body.path(resource.envelope).path(resource.rows);
        if (rows == null || !rows.isArray()) {
            throw new IOException(
                    "Discourse response lacks the "
                            + resource.envelope
                            + "."
                            + resource.rows
                            + " array");
        }
        List<SeaTunnelRow> result = new ArrayList<>(rows.size());
        List<Column> columns = table.getTableSchema().getColumns();
        Set<Long> ids = new HashSet<>();
        Deque<Iterator<JsonNode>> pending = new ArrayDeque<>();
        pending.push(rows.elements());
        while (!pending.isEmpty()) {
            if (cancelled.get()) {
                throw new IOException("Discourse source was cancelled");
            }
            Iterator<JsonNode> iterator = pending.peek();
            if (!iterator.hasNext()) {
                pending.pop();
                continue;
            }
            JsonNode entry = iterator.next();
            Object[] values = new Object[columns.size()];
            for (int i = 0; i < values.length; i++) {
                Column column = columns.get(i);
                JsonNode value = entry.get(column.getName());
                if (value == null || value.isNull()) {
                    if (!column.isNullable()) {
                        throw new IOException(
                                "Discourse record has no required field " + column.getName());
                    }
                    continue;
                }
                if (column.getDataType().equals(BasicType.STRING_TYPE) && value.isTextual()) {
                    values[i] = value.textValue();
                } else if (column.getDataType().equals(BasicType.LONG_TYPE)
                        && value.isIntegralNumber()
                        && value.canConvertToLong()) {
                    values[i] = value.longValue();
                } else if (column.getDataType().equals(BasicType.BOOLEAN_TYPE)
                        && value.isBoolean()) {
                    values[i] = value.booleanValue();
                } else {
                    throw new IOException(
                            "Discourse record has an invalid type for field " + column.getName());
                }
            }
            if (!ids.add((Long) values[0])) {
                throw new IOException("Discourse response contains a duplicate id");
            }
            SeaTunnelRow row = new SeaTunnelRow(values);
            row.setTableId(table.getTablePath().toString());
            result.add(row);
            if (includeSubcategories) {
                JsonNode children = entry.get("subcategory_list");
                if (children != null && !children.isNull()) {
                    if (!children.isArray()) {
                        throw new IOException(
                                "Discourse category subcategory_list is not an array");
                    }
                    pending.push(children.elements());
                }
            }
        }
        return result;
    }
}
