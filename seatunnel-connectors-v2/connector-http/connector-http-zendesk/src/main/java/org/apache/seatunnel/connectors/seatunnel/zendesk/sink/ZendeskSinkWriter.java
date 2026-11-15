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

package org.apache.seatunnel.connectors.seatunnel.zendesk.sink;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
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
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

@Slf4j
public class ZendeskSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void>
        implements SupportMultiTableSinkWriter<Void> {

    private static final int STATUS_OK = 200;
    private static final int STATUS_CREATED = 201;
    private static final int STATUS_TOO_MANY_REQUESTS = 429;
    private static final long MAX_BACKOFF_MILLIS = 300000L;

    private final HttpClientProvider httpClient;
    private final String url;
    private final Map<String, String> headers;
    private final JsonSerializationSchema serializationSchema;
    private final ObjectMapper objectMapper;
    private final String resourceKey;
    private final int requestIntervalMs;
    private final int rateLimitBackoffMs;
    private final int rateLimitMaxRetries;
    private long lastRequestTimeMillis;

    public ZendeskSinkWriter(
            SeaTunnelRowType seaTunnelRowType,
            HttpParameter httpParameter,
            String resourceKeyOverride,
            int requestIntervalMs,
            int rateLimitBackoffMs,
            int rateLimitMaxRetries,
            int numberOfParallelSubtasks) {
        this.url = httpParameter.getUrl();
        this.headers = httpParameter.getHeaders();
        this.httpClient = new HttpClientProvider(httpParameter);
        this.serializationSchema = new JsonSerializationSchema(seaTunnelRowType);
        this.objectMapper = serializationSchema.getMapper();
        if (resourceKeyOverride != null && resourceKeyOverride.trim().isEmpty()) {
            throw new IllegalArgumentException("resource_key must not be blank");
        }
        this.resourceKey =
                resourceKeyOverride != null ? resourceKeyOverride : inferResourceKey(this.url);
        int parallelism = Math.max(1, numberOfParallelSubtasks);
        this.requestIntervalMs = Math.max(0, requestIntervalMs) * parallelism;
        this.rateLimitBackoffMs = Math.max(0, rateLimitBackoffMs);
        this.rateLimitMaxRetries = Math.max(0, rateLimitMaxRetries);
        this.lastRequestTimeMillis = 0L;
    }

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        String body = buildRequestBody(element);
        sendWithRateLimitRetry(body);
    }

    /** Wraps the serialized row in a Zendesk resource key, e.g. {"ticket": {...}}. */
    @VisibleForTesting
    String buildRequestBody(SeaTunnelRow row) throws IOException {
        byte[] serialized = serializationSchema.serialize(row);
        JsonNode fieldsNode = objectMapper.readTree(serialized);

        if (resourceKey == null) {
            return objectMapper.writeValueAsString(fieldsNode);
        }

        ObjectNode root = objectMapper.createObjectNode();
        root.set(resourceKey, fieldsNode);
        return objectMapper.writeValueAsString(root);
    }

    /** Extracts the first path segment after /api/v2/ and singularizes it (tickets → ticket). */
    @VisibleForTesting
    static String inferResourceKey(String url) {
        if (url == null) {
            return null;
        }

        String path = url;
        int queryIdx = path.indexOf('?');
        if (queryIdx >= 0) {
            path = path.substring(0, queryIdx);
        }
        if (path.endsWith("/")) {
            path = path.substring(0, path.length() - 1);
        }
        if (path.endsWith(".json")) {
            path = path.substring(0, path.length() - 5);
        }

        int v2Idx = path.indexOf("/api/v2/");
        if (v2Idx < 0) {
            return null;
        }

        String afterV2 = path.substring(v2Idx + "/api/v2/".length());
        String[] segments = afterV2.split("/");
        if (segments.length == 0 || segments[0].isEmpty()) {
            return null;
        }

        String resource = segments[0];
        if (resource.endsWith("sses")) {
            return resource.substring(0, resource.length() - 2);
        } else if (resource.endsWith("ies")) {
            return resource.substring(0, resource.length() - 3) + "y";
        } else if (resource.endsWith("ses")
                || resource.endsWith("xes")
                || resource.endsWith("zes")) {
            return resource.substring(0, resource.length() - 2);
        } else if (resource.endsWith("s")) {
            return resource.substring(0, resource.length() - 1);
        }
        return resource;
    }

    /** POSTs body to Zendesk, retrying with exponential backoff on HTTP 429. */
    private void sendWithRateLimitRetry(String body) throws IOException {
        int retryCount = 0;
        while (true) {
            waitForRequestSlot();
            try {
                HttpResponse response = httpClient.doPost(url, headers, body);
                if (STATUS_OK == response.getCode() || STATUS_CREATED == response.getCode()) {
                    return;
                }
                if (response.getCode() == STATUS_TOO_MANY_REQUESTS
                        && retryCount < rateLimitMaxRetries) {
                    retryCount++;
                    long backoffMillis = calculateBackoffMillis(retryCount);
                    log.warn(
                            "Zendesk API rate limit reached, retry {}/{} after {} ms",
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
                                "Zendesk API request failed, url:[%s], status code:[%s], content:[%s]",
                                url, response.getCode(), response.getContent()));
            } catch (IOException e) {
                throw e;
            } catch (Exception e) {
                throw new IOException("Failed to send Zendesk API request", e);
            }
        }
    }

    /** Throttles requests to respect requestIntervalMs between consecutive calls. */
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

    /** Equal jitter backoff: half the exponential ceiling plus random jitter in [0, half]. */
    @VisibleForTesting
    long calculateBackoffMillis(int retryCount) {
        if (rateLimitBackoffMs <= 0) {
            return 0L;
        }
        long exponential = 1L << Math.min(20, Math.max(0, retryCount - 1));
        long ceiling = Math.min(rateLimitBackoffMs * exponential, MAX_BACKOFF_MILLIS);
        long half = ceiling / 2;
        return half + ThreadLocalRandom.current().nextLong(half + 1);
    }

    @Override
    public Optional<Void> prepareCommit() {
        return Optional.empty();
    }

    @Override
    public void close() throws IOException {
        if (Objects.nonNull(httpClient)) {
            httpClient.close();
        }
    }
}
