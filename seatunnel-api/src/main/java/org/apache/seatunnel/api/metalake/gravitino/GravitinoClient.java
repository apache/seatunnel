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

import org.apache.http.HttpEntity;
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
        this.httpClient = HttpClients.createDefault();
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
     * @throws IOException if network or parsing error occurs after all retries
     */
    private JsonNode executeGetRequest(String url) throws IOException {
        IOException lastException = null;
        for (int attempt = 1; attempt <= MAX_RETRY_ATTEMPTS; attempt++) {
            HttpGet request = new HttpGet(url);
            request.addHeader(HEADER_ACCEPT, MEDIA_TYPE_GRAVITINO_V1);
            try (CloseableHttpResponse response = httpClient.execute(request)) {
                HttpEntity entity = response.getEntity();
                if (entity == null) {
                    throw new RuntimeException(ERROR_NO_RESPONSE_ENTITY);
                }
                try {
                    return JsonUtils.readTree(entity.getContent());
                } finally {
                    EntityUtils.consume(entity);
                }
            } catch (IOException e) {
                lastException = e;
                // Check if exception is retryable
                if (!isRetryableException(e) || attempt >= MAX_RETRY_ATTEMPTS) {
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
        throw new IOException(
                String.format(
                        "Failed to execute HTTP request to %s after %d attempts",
                        url, MAX_RETRY_ATTEMPTS),
                lastException);
    }

    /**
     * Determine if an exception is retryable. Certain exceptions like DNS resolution failures, SSL
     * errors, or 4xx client errors should not be retried as they will likely fail again.
     *
     * @param e the exception to check
     * @return true if the exception is retryable, false otherwise
     */
    private boolean isRetryableException(IOException e) {
        String message = e.getMessage();
        if (message == null) {
            return true;
        }
        // Non-retryable error patterns
        String lowerMessage = message.toLowerCase();
        if (lowerMessage.contains("unknownhost")
                || lowerMessage.contains("dns")
                || lowerMessage.contains("hostname")
                || lowerMessage.contains("ssl")
                || lowerMessage.contains("certificate")
                || lowerMessage.contains("400")
                || lowerMessage.contains("401")
                || lowerMessage.contains("403")
                || lowerMessage.contains("404")) {
            log.debug("Non-retryable exception detected", e);
            return false;
        }
        // Retryable: network timeouts, connection resets, 5xx server errors, etc.
        return true;
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
