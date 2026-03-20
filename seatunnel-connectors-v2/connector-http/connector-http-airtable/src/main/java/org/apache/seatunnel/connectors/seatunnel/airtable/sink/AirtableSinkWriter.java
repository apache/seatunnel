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

package org.apache.seatunnel.connectors.seatunnel.airtable.sink;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.api.sink.SupportMultiTableSinkWriter;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.http.client.HttpClientProvider;
import org.apache.seatunnel.connectors.seatunnel.http.client.HttpResponse;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpParameter;
import org.apache.seatunnel.format.json.JsonSerializationSchema;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

@Slf4j
public class AirtableSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void>
        implements SupportMultiTableSinkWriter<Void> {

    private static final int STATUS_TOO_MANY_REQUESTS = 429;
    private static final long MAX_BACKOFF_MILLIS = 300000L;

    private final HttpClientProvider httpClient;
    private final String url;
    private final Map<String, String> headers;
    private final JsonSerializationSchema serializationSchema;
    private final ObjectMapper objectMapper;
    private final int batchSize;
    private final boolean typecast;
    private final int requestIntervalMs;
    private final int rateLimitBackoffMs;
    private final int rateLimitMaxRetries;
    private final List<SeaTunnelRow> batchBuffer;
    private long lastRequestTimeMillis;

    public AirtableSinkWriter(
            SeaTunnelRowType seaTunnelRowType,
            HttpParameter httpParameter,
            int batchSize,
            boolean typecast,
            int requestIntervalMs,
            int rateLimitBackoffMs,
            int rateLimitMaxRetries) {
        this.url = httpParameter.getUrl();
        this.headers = httpParameter.getHeaders();
        this.httpClient = new HttpClientProvider(httpParameter);
        this.serializationSchema = new JsonSerializationSchema(seaTunnelRowType);
        this.objectMapper = serializationSchema.getMapper();
        this.batchSize = Math.min(Math.max(batchSize, 1), 10);
        this.typecast = typecast;
        this.requestIntervalMs = Math.max(0, requestIntervalMs);
        this.rateLimitBackoffMs = Math.max(0, rateLimitBackoffMs);
        this.rateLimitMaxRetries = Math.max(0, rateLimitMaxRetries);
        this.batchBuffer = new ArrayList<>(this.batchSize);
        this.lastRequestTimeMillis = 0L;
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        batchBuffer.add(element);
        if (batchBuffer.size() >= batchSize) {
            flush();
        }
    }

    private void flush() throws IOException {
        if (batchBuffer.isEmpty()) {
            return;
        }

        String body = buildRequestBody();
        sendWithRateLimitRetry(body);
        batchBuffer.clear();
    }

    private String buildRequestBody() throws IOException {
        ObjectNode root = objectMapper.createObjectNode();
        ArrayNode records = objectMapper.createArrayNode();

        for (SeaTunnelRow row : batchBuffer) {
            byte[] serialized = serializationSchema.serialize(row);
            JsonNode fieldsNode = objectMapper.readTree(serialized);
            ObjectNode record = objectMapper.createObjectNode();
            record.set("fields", fieldsNode);
            records.add(record);
        }

        root.set("records", records);
        if (typecast) {
            root.put("typecast", true);
        }

        return objectMapper.writeValueAsString(root);
    }

    private void sendWithRateLimitRetry(String body) throws IOException {
        int retryCount = 0;
        while (true) {
            waitForRequestSlot();
            try {
                HttpResponse response = httpClient.doPost(url, headers, body);
                if (HttpResponse.STATUS_OK == response.getCode()) {
                    return;
                }
                if (response.getCode() == STATUS_TOO_MANY_REQUESTS
                        && retryCount < rateLimitMaxRetries) {
                    retryCount++;
                    long backoffMillis = calculateBackoffMillis(retryCount);
                    log.warn(
                            "Airtable API rate limit reached, retry {}/{} after {} ms",
                            retryCount,
                            rateLimitMaxRetries,
                            backoffMillis);
                    try {
                        Thread.sleep(backoffMillis);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        throw new RuntimeException(e);
                    }
                    continue;
                }
                throw new IOException(
                        String.format(
                                "Airtable API request failed, status code:[%s], content:[%s]",
                                response.getCode(), response.getContent()));
            } catch (IOException e) {
                throw e;
            } catch (Exception e) {
                throw new IOException("Failed to send Airtable API request", e);
            }
        }
    }

    private void waitForRequestSlot() {
        if (requestIntervalMs <= 0) {
            return;
        }
        long now = System.currentTimeMillis();
        long elapsed = now - lastRequestTimeMillis;
        if (elapsed < requestIntervalMs) {
            try {
                Thread.sleep(requestIntervalMs - elapsed);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new RuntimeException(e);
            }
        }
        lastRequestTimeMillis = System.currentTimeMillis();
    }

    private long calculateBackoffMillis(int retryCount) {
        if (rateLimitBackoffMs <= 0) {
            return 0L;
        }
        long exponential = 1L << Math.min(20, Math.max(0, retryCount - 1));
        long waitMillis = rateLimitBackoffMs * exponential;
        return Math.min(waitMillis, MAX_BACKOFF_MILLIS);
    }

    @Override
    public Optional<Void> prepareCommit() {
        try {
            flush();
        } catch (IOException e) {
            throw new RuntimeException("Failed to flush data in prepareCommit", e);
        }
        return Optional.empty();
    }

    @Override
    public void close() throws IOException {
        flush();
        if (Objects.nonNull(httpClient)) {
            httpClient.close();
        }
    }
}
