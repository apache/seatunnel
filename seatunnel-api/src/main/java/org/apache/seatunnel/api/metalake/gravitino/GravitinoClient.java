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

package org.apache.seatunnel.api.metalake.gravitino;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.google.common.annotations.VisibleForTesting;

import org.apache.seatunnel.api.metalake.MetalakeClient;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.common.exception.SeaTunnelRuntimeException;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.common.utils.SeaTunnelException;

import org.apache.http.HttpEntity;
import org.apache.http.HttpStatus;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static org.apache.seatunnel.api.table.schema.exception.SchemaEvolutionErrorCode.ERROR_INVALID_TABLE_URL;

@Slf4j
public class GravitinoClient implements MetalakeClient {

    private static final String HEADER_ACCEPT = "Accept";
    private static final String MEDIA_TYPE_GRAVITINO_V1 = "application/vnd.gravitino.v1+json";
    private static final String JSON_FIELD_CATALOG = "catalog";
    private static final String JSON_FIELD_TABLE = "table";
    private static final String JSON_FIELD_PROPERTIES = "properties";
    private static final String ERROR_NO_RESPONSE_ENTITY = "No response entity";
    private static final String ERROR_MISSING_FIELD_TEMPLATE = "Response JSON has no '%s' field";
    private static final int MAX_RETRY_ATTEMPTS = 3;
    private static final long RETRY_DELAY_MS = 2000;
    private static final Pattern TABLE_URL_PATTERN =
            Pattern.compile("/catalogs/([^/]+)/schemas/([^/]+)/tables/([^/]+)");

    private final CloseableHttpClient httpClient;

    public GravitinoClient() {
        RequestConfig config =
                RequestConfig.custom()
                        .setConnectTimeout(5000)
                        .setConnectionRequestTimeout(5000)
                        .setSocketTimeout(30000)
                        .build();

        this.httpClient =
                HttpClients.custom()
                        .setDefaultRequestConfig(config)
                        .setMaxConnTotal(50)
                        .setMaxConnPerRoute(20)
                        .build();
    }

    @VisibleForTesting
    protected GravitinoClient(CloseableHttpClient httpClient) {
        this.httpClient = httpClient;
    }

    @Override
    public JsonNode getMetaInfo(String sourceId, String metalakeUrl) throws IOException {
        JsonNode rootNode = executeGetRequest(metalakeUrl + sourceId);
        JsonNode catalogNode = getRequiredNode(rootNode, JSON_FIELD_CATALOG);
        return getRequiredNode(catalogNode, JSON_FIELD_PROPERTIES);
    }

    @Override
    public JsonNode getTableSchema(String schemaHttpUrl) throws IOException {
        JsonNode rootNode = executeGetRequest(schemaHttpUrl);
        return getRequiredNode(rootNode, JSON_FIELD_TABLE);
    }

    @Override
    public TablePath getTableSchemaPath(String schemaHttpUrl) {
        if (schemaHttpUrl == null || schemaHttpUrl.isEmpty()) {
            throw new SeaTunnelRuntimeException(
                    ERROR_INVALID_TABLE_URL, "Table URL cannot be null or empty");
        }
        final Matcher matcher = getMatcher(schemaHttpUrl);
        String catalogName = matcher.group(1);
        String schemaName = matcher.group(2);
        String tableName = matcher.group(3);
        return TablePath.of(catalogName, schemaName, tableName);
    }

    private Matcher getMatcher(String schemaHttpUrl) {
        Matcher matcher = TABLE_URL_PATTERN.matcher(schemaHttpUrl);
        if (!matcher.find()) {
            throw new SeaTunnelRuntimeException(
                    ERROR_INVALID_TABLE_URL,
                    String.format(
                            "Invalid table URL format: '%s'. "
                                    + "Expected format: http://host/api/metalakes/{metalake}/catalogs/{catalog}/schemas/{schema}/tables/{table}",
                            schemaHttpUrl));
        }
        return matcher;
    }

    /**
     * Execute HTTP GET request and return parsed JSON response. Implements retry with exponential
     * backoff for transient failures.
     *
     * @param url the request URL
     * @return parsed JSON root node
     */
    private JsonNode executeGetRequest(String url) {
        for (int attempt = 1; attempt <= MAX_RETRY_ATTEMPTS; attempt++) {
            HttpGet request = new HttpGet(url);
            request.addHeader(HEADER_ACCEPT, MEDIA_TYPE_GRAVITINO_V1);
            try (CloseableHttpResponse response = httpClient.execute(request)) {
                final int statusCode = response.getStatusLine().getStatusCode();
                if (statusCode != HttpStatus.SC_OK) {
                    if (!isRetryableHttpStatus(statusCode)) {
                        throw new SeaTunnelException(
                                String.format(
                                        "Failed to execute HTTP request to %s , http status code is %s",
                                        url, statusCode));
                    } else {
                        sleepQuietly(RETRY_DELAY_MS);
                    }
                } else {
                    HttpEntity entity = response.getEntity();
                    if (entity == null) {
                        throw new RuntimeException(ERROR_NO_RESPONSE_ENTITY);
                    }
                    try {
                        return JsonUtils.readTree(entity.getContent());
                    } finally {
                        EntityUtils.consume(entity);
                    }
                }
            } catch (IOException e) {
                if (attempt >= MAX_RETRY_ATTEMPTS) {
                    break;
                }
                // Exponential backoff delay before retry
                long delayMs = RETRY_DELAY_MS;
                log.debug(
                        "HTTP request to {} failed on attempt {}/{}, retrying in {}ms: {}",
                        url,
                        attempt,
                        MAX_RETRY_ATTEMPTS,
                        delayMs,
                        e.getMessage());
                sleepQuietly(delayMs);
            }
        }
        throw new SeaTunnelException(
                String.format(
                        "Failed to execute HTTP request to %s after %d attempts",
                        url, MAX_RETRY_ATTEMPTS));
    }

    /** 5xx and 408 and 429 will be retried */
    private boolean isRetryableHttpStatus(int httpStatus) {
        return httpStatus == HttpStatus.SC_INTERNAL_SERVER_ERROR
                || httpStatus == HttpStatus.SC_NOT_IMPLEMENTED
                || httpStatus == HttpStatus.SC_BAD_GATEWAY
                || httpStatus == HttpStatus.SC_SERVICE_UNAVAILABLE
                || httpStatus == HttpStatus.SC_GATEWAY_TIMEOUT
                || httpStatus == HttpStatus.SC_HTTP_VERSION_NOT_SUPPORTED
                || httpStatus == HttpStatus.SC_INSUFFICIENT_STORAGE
                || httpStatus == HttpStatus.SC_REQUEST_TIMEOUT
                || httpStatus == HttpStatus.SC_TOO_MANY_REQUESTS;
    }

    /**
     * Sleep without throwing InterruptedException. If interrupted, the thread's interrupt status
     * will be restored.
     *
     * @param millis sleep duration in milliseconds
     */
    private void sleepQuietly(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.debug("Sleep interrupted during retry backoff", e);
        }
    }

    /**
     * Get a required child node from parent node, throw exception if not found.
     *
     * @param parentNode the parent JSON node
     * @param fieldName the field name to retrieve
     * @return the child node
     * @throws RuntimeException if the field is not present
     */
    private JsonNode getRequiredNode(JsonNode parentNode, String fieldName) {
        JsonNode node = parentNode.get(fieldName);
        if (node == null) {
            throw new RuntimeException(String.format(ERROR_MISSING_FIELD_TEMPLATE, fieldName));
        }
        return node;
    }

    /** Close the HTTP client and release resources. Safe to call multiple times. */
    @Override
    public void close() {
        if (httpClient != null) {
            try {
                httpClient.close();
            } catch (IOException e) {
                // Ignore close exception as HttpClient is being shut down anyway
                log.debug("Failed to close HTTP client, ignoring", e);
            }
        }
    }
}
