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
    private CloseableHttpClient client;

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

    @Override
    public void open() {
        client =
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
    }

    @Override
    public void close() throws IOException {
        if (client != null) {
            client.close();
        }
    }

    @Override
    public void internalPollNext(Collector<SeaTunnelRow> output) throws Exception {
        List<SeaTunnelRow> rows = readRows();
        rows.forEach(output::collect);
        context.signalNoMoreElement();
    }

    List<SeaTunnelRow> readRows() throws Exception {
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
        try (CloseableHttpResponse response = client.execute(request)) {
            int code = response.getStatusLine().getStatusCode();
            if (code < 200 || code >= 300) {
                throw new IOException("Matomo Reporting API returned HTTP " + code);
            }
            HttpEntity entity = response.getEntity();
            if (entity == null || entity.getContentLength() > MAX_RESPONSE_BYTES) {
                throw new IOException("Missing or oversized Matomo response");
            }
            try (InputStream input = entity.getContent()) {
                byte[] bytes = input.readNBytes(MAX_RESPONSE_BYTES + 1);
                if (bytes.length > MAX_RESPONSE_BYTES) {
                    request.abort();
                    throw new IOException("Oversized Matomo response");
                }
                return decode(new String(bytes, StandardCharsets.UTF_8));
            }
        }
    }

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
