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

package org.apache.seatunnel.connectors.seatunnel.facebook.ads.client;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.connectors.seatunnel.facebook.ads.config.FacebookAdsParameters;
import org.apache.seatunnel.connectors.seatunnel.facebook.ads.config.FacebookAdsTableConfig;
import org.apache.seatunnel.connectors.seatunnel.facebook.ads.exception.FacebookAdsConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.facebook.ads.exception.FacebookAdsConnectorException;

import org.apache.http.HttpHeaders;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

@Slf4j
public class FacebookAdsClient implements Closeable {

    /**
     * Graph API error codes that indicate rate limiting; Facebook reports them with HTTP 400/403,
     * so the status code alone is not enough to detect a transient throttle.
     */
    private static final Set<Integer> RATE_LIMIT_ERROR_CODES =
            new HashSet<>(Arrays.asList(4, 17, 32, 613));

    private final FacebookAdsParameters params;
    private final CloseableHttpClient httpClient;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public FacebookAdsClient(FacebookAdsParameters params) {
        this.params = params;
        RequestConfig requestConfig =
                RequestConfig.custom()
                        .setConnectTimeout(params.getRequestTimeoutMs())
                        .setSocketTimeout(params.getRequestTimeoutMs())
                        .build();
        this.httpClient = HttpClients.custom().setDefaultRequestConfig(requestConfig).build();
    }

    /**
     * Reads one ad account edge end-to-end, walking forward via the paging.cursors.after cursor.
     * Each element of the data array is flattened to an Object[] aligned with the configured field
     * order and pushed to rowConsumer immediately; only one page's JSON body is held in memory at a
     * time. Every value is emitted as a string (the Graph API serializes most metrics as strings
     * anyway); nested objects and arrays are emitted as JSON text, absent fields as null.
     */
    public void search(FacebookAdsTableConfig tableConfig, Consumer<Object[]> rowConsumer) {
        String baseUrl =
                params.getApiEndpoint()
                        + "/"
                        + params.getApiVersion()
                        + "/act_"
                        + tableConfig.getAdAccountId()
                        + "/"
                        + tableConfig.getResource();
        String afterCursor = null;

        do {
            String url = buildPageUrl(baseUrl, tableConfig, afterCursor);
            String body = executeGet(url);
            try {
                JsonNode json = objectMapper.readTree(body);
                JsonNode data = json.get("data");
                if (data != null) {
                    for (JsonNode result : data) {
                        Object[] row = new Object[tableConfig.getFieldNames().size()];
                        for (int i = 0; i < tableConfig.getFieldNames().size(); i++) {
                            JsonNode node = result.get(tableConfig.getFieldNames().get(i));
                            row[i] =
                                    (node == null || node.isNull())
                                            ? null
                                            : (node.isValueNode()
                                                    ? node.asText()
                                                    : node.toString());
                        }
                        rowConsumer.accept(row);
                    }
                }
                afterCursor = nextCursor(json);
            } catch (FacebookAdsConnectorException e) {
                throw e;
            } catch (Exception e) {
                throw new FacebookAdsConnectorException(
                        FacebookAdsConnectorErrorCode.REQUEST_FAILED, e);
            }
        } while (afterCursor != null);
    }

    private String buildPageUrl(
            String baseUrl, FacebookAdsTableConfig tableConfig, String afterCursor) {
        try {
            URIBuilder builder = new URIBuilder(baseUrl);
            builder.addParameter("fields", String.join(",", tableConfig.getFieldNames()));
            if (tableConfig.getFiltering() != null) {
                builder.addParameter("filtering", tableConfig.getFiltering());
            }
            for (Map.Entry<String, String> entry : tableConfig.getParams().entrySet()) {
                builder.addParameter(entry.getKey(), entry.getValue());
            }
            if (params.getPageSize() != null) {
                builder.addParameter("limit", String.valueOf(params.getPageSize()));
            }
            if (afterCursor != null) {
                builder.addParameter("after", afterCursor);
            }
            return builder.build().toString();
        } catch (Exception e) {
            throw new FacebookAdsConnectorException(
                    FacebookAdsConnectorErrorCode.REQUEST_FAILED, e);
        }
    }

    /**
     * The Graph API signals a further page by including paging.next; the after cursor is only
     * followed when next is present, since Facebook always echoes the cursors block even on the
     * last page.
     */
    private String nextCursor(JsonNode json) {
        JsonNode paging = json.get("paging");
        if (paging == null || paging.get("next") == null) {
            return null;
        }
        JsonNode cursors = paging.get("cursors");
        if (cursors == null || cursors.get("after") == null) {
            return null;
        }
        String after = cursors.get("after").asText();
        return after.isEmpty() ? null : after;
    }

    /**
     * GETs a Graph API URL with the access token as a Bearer header (kept out of the URL so it
     * cannot leak into logs). 401 fails fast as an auth error. Transient failures - HTTP 429/5xx,
     * Facebook rate-limit error codes carried in 400/403 bodies, and IO errors - are retried up to
     * max_retries with exponential backoff; other non-200 statuses fail fast with the API error
     * body surfaced.
     */
    private String executeGet(String url) {
        int attempt = 0;
        while (true) {
            try {
                HttpGet get = new HttpGet(url);
                get.setHeader(HttpHeaders.AUTHORIZATION, "Bearer " + params.getAccessToken());
                try (CloseableHttpResponse response = httpClient.execute(get)) {
                    int status = response.getStatusLine().getStatusCode();
                    String body =
                            EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
                    if (status == 200) {
                        return body;
                    }
                    if (isRateLimited(status, body) || isTransient(status)) {
                        if (attempt < params.getMaxRetries()) {
                            backoff(++attempt, "HTTP " + status);
                            continue;
                        }
                        throw new FacebookAdsConnectorException(
                                FacebookAdsConnectorErrorCode.REQUEST_FAILED,
                                "HTTP " + status + ": " + body);
                    }
                    if (status == 401) {
                        throw new FacebookAdsConnectorException(
                                FacebookAdsConnectorErrorCode.AUTH_FAILED,
                                "HTTP " + status + ": " + body);
                    }
                    throw new FacebookAdsConnectorException(
                            FacebookAdsConnectorErrorCode.REQUEST_FAILED,
                            "HTTP " + status + ": " + body);
                }
            } catch (FacebookAdsConnectorException e) {
                throw e;
            } catch (IOException e) {
                if (attempt < params.getMaxRetries()) {
                    backoff(++attempt, e.getMessage());
                    continue;
                }
                throw new FacebookAdsConnectorException(
                        FacebookAdsConnectorErrorCode.REQUEST_FAILED, e);
            }
        }
    }

    private boolean isTransient(int status) {
        return status == 429 || (status >= 500 && status <= 504);
    }

    private boolean isRateLimited(int status, String body) {
        if (status != 400 && status != 403) {
            return false;
        }
        try {
            JsonNode error = objectMapper.readTree(body).get("error");
            return error != null
                    && error.get("code") != null
                    && RATE_LIMIT_ERROR_CODES.contains(error.get("code").asInt());
        } catch (Exception e) {
            return false;
        }
    }

    private void backoff(int attempt, String reason) {
        long sleepMs = params.getRetryBackoffMs() * (1L << (attempt - 1));
        log.warn(
                "Transient Facebook Graph API failure ({}); retry {}/{} after {}ms",
                reason,
                attempt,
                params.getMaxRetries(),
                sleepMs);
        try {
            Thread.sleep(sleepMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new FacebookAdsConnectorException(
                    FacebookAdsConnectorErrorCode.REQUEST_FAILED,
                    "Interrupted during retry backoff");
        }
    }

    @Override
    public void close() throws IOException {
        httpClient.close();
    }
}
