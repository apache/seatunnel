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
import org.apache.seatunnel.shade.com.google.common.annotations.VisibleForTesting;

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
import java.util.concurrent.ThreadLocalRandom;

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

    @VisibleForTesting
    long calculateBackoffMillis(int retryCount) {
        if (rateLimitBackoffMs <= 0) {
            return 0L;
        }
        long exponential = 1L << Math.min(20, Math.max(0, retryCount - 1));
        long waitMillis = Math.min(rateLimitBackoffMs * exponential, MAX_BACKOFF_MILLIS);

        // Spread the delay by adding a random amount on top of it. Without this
        // the delay is a pure function of the retry count, so every reader and
        // writer that hits the rate limit at the same moment retries at the same
        // instants and the burst that caused the 429 reforms on each attempt.
        //
        // The jitter is added rather than centred so the wait is never shorter
        // than rateLimitBackoffMs asked for: this fires on 429, so retrying
        // sooner than configured would work against the setting's purpose. The
        // result stays capped at MAX_BACKOFF_MILLIS.
        long extra = Math.min(waitMillis, MAX_BACKOFF_MILLIS - waitMillis);
        if (extra > 0) {
            return waitMillis + ThreadLocalRandom.current().nextLong(extra + 1);
        }

        // Once the wait reaches MAX_BACKOFF_MILLIS there is no headroom left to
        // add into, so every retry past that point would come back unjittered
        // and the callers would be back in lockstep exactly when the rate limit
        // is at its most persistent. Spread the wait downwards instead. The cap
        // is an upper bound rather than a target, so drawing below it breaks
        // nothing.
        //
        // The floor is the last scheduled wait that still fitted under the cap,
        // or half the wait when the very first retry is already capped. Flooring
        // there keeps the minimum from dropping as the schedule crosses the cap:
        // half of MAX can be less than the previous retry's wait, which would let
        // a later retry sleep for less than an earlier one. It also keeps the
        // wait at or above rateLimitBackoffMs for free, since the last uncapped
        // wait is never smaller than the configured backoff.
        long floor = waitMillis / 2;
        for (long scheduled = rateLimitBackoffMs; scheduled < MAX_BACKOFF_MILLIS; scheduled <<= 1) {
            if (scheduled > floor) {
                floor = scheduled;
            }
        }
        return waitMillis - ThreadLocalRandom.current().nextLong(waitMillis - floor + 1);
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
