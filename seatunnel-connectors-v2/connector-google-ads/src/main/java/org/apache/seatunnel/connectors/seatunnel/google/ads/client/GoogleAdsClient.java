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

package org.apache.seatunnel.connectors.seatunnel.google.ads.client;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.google.ads.config.GoogleAdsParameters;
import org.apache.seatunnel.connectors.seatunnel.google.ads.exception.GoogleAdsConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.google.ads.exception.GoogleAdsConnectorException;

import org.apache.http.HttpHeaders;
import org.apache.http.NameValuePair;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;

import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

@Slf4j
public class GoogleAdsClient implements Closeable {

    private static final String TOKEN_PATH = "/token";
    private static final String FIELDS_SEARCH_PATH = "/%s/googleAdsFields:search";
    private static final String SEARCH_PATH = "/%s/customers/%s/googleAds:search";
    private static final String PLUGIN_NAME = "GoogleAds";
    private static final long TOKEN_EXPIRY_SAFETY_MARGIN_MS = 60_000L;

    private final GoogleAdsParameters params;
    private final CloseableHttpClient httpClient;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private String accessToken;
    private long tokenExpiresAtMs;

    public GoogleAdsClient(GoogleAdsParameters params) {
        this.params = params;
        RequestConfig requestConfig =
                RequestConfig.custom()
                        .setConnectTimeout(params.getRequestTimeoutMs())
                        .setSocketTimeout(params.getRequestTimeoutMs())
                        .build();
        this.httpClient = HttpClients.custom().setDefaultRequestConfig(requestConfig).build();
    }

    public void authenticate() {
        String tokenUrl = params.getOauthEndpoint() + TOKEN_PATH;
        HttpPost post = new HttpPost(tokenUrl);

        List<NameValuePair> form = new ArrayList<>();
        form.add(new BasicNameValuePair("grant_type", "refresh_token"));
        form.add(new BasicNameValuePair("client_id", params.getClientId()));
        form.add(new BasicNameValuePair("client_secret", params.getClientSecret()));
        form.add(new BasicNameValuePair("refresh_token", params.getRefreshToken()));

        try {
            post.setEntity(new UrlEncodedFormEntity(form, StandardCharsets.UTF_8));
            try (CloseableHttpResponse response = httpClient.execute(post)) {
                int status = response.getStatusLine().getStatusCode();
                String body = EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
                if (status != 200) {
                    throw new GoogleAdsConnectorException(
                            GoogleAdsConnectorErrorCode.AUTH_FAILED,
                            "HTTP " + status + ": " + body);
                }
                JsonNode json = objectMapper.readTree(body);
                this.accessToken = json.get("access_token").asText();
                long expiresInSec =
                        json.has("expires_in") ? json.get("expires_in").asLong() : 3600L;
                this.tokenExpiresAtMs =
                        System.currentTimeMillis()
                                + expiresInSec * 1000
                                - TOKEN_EXPIRY_SAFETY_MARGIN_MS;
                log.info("Obtained Google OAuth2 access token");
            }
        } catch (GoogleAdsConnectorException e) {
            throw e;
        } catch (Exception e) {
            throw new GoogleAdsConnectorException(GoogleAdsConnectorErrorCode.AUTH_FAILED, e);
        }
    }

    /**
     * Resolves the data type of every requested field via the GoogleAdsFieldService and builds a
     * CatalogTable whose column order follows fieldPaths (the GAQL SELECT order), never the
     * metadata response order, so schema position i always corresponds to selected field i.
     */
    public CatalogTable describeFields(String database, String resource, List<String> fieldPaths) {
        StringBuilder inList = new StringBuilder();
        for (int i = 0; i < fieldPaths.size(); i++) {
            if (i > 0) {
                inList.append(", ");
            }
            inList.append('\'').append(fieldPaths.get(i)).append('\'');
        }
        String metaQuery =
                "SELECT name, data_type FROM google_ads_field WHERE name IN (" + inList + ")";

        String url =
                params.getApiEndpoint() + String.format(FIELDS_SEARCH_PATH, params.getApiVersion());
        ObjectNode requestBody = objectMapper.createObjectNode();
        requestBody.put("query", metaQuery);

        String body =
                executeJsonPost(
                        url, requestBody, GoogleAdsConnectorErrorCode.DESCRIBE_FIELDS_FAILED);
        try {
            JsonNode json = objectMapper.readTree(body);
            Map<String, String> nameToType = new HashMap<>();
            JsonNode results = json.get("results");
            if (results != null) {
                for (JsonNode field : results) {
                    nameToType.put(field.get("name").asText(), field.get("dataType").asText());
                }
            }

            TableSchema.Builder schemaBuilder = TableSchema.builder();
            for (String fieldPath : fieldPaths) {
                String dataType = nameToType.get(fieldPath);
                if (dataType == null) {
                    throw new GoogleAdsConnectorException(
                            GoogleAdsConnectorErrorCode.DESCRIBE_FIELDS_FAILED,
                            "Unknown Google Ads field: "
                                    + fieldPath
                                    + ". Check the field name against the "
                                    + resource
                                    + " resource documentation.");
                }
                schemaBuilder.column(
                        PhysicalColumn.of(
                                fieldPath,
                                mapGoogleAdsType(dataType),
                                null,
                                null,
                                true,
                                null,
                                null));
            }
            return CatalogTable.of(
                    TableIdentifier.of(PLUGIN_NAME, database, resource),
                    schemaBuilder.build(),
                    Collections.emptyMap(),
                    Collections.emptyList(),
                    "");
        } catch (GoogleAdsConnectorException e) {
            throw e;
        } catch (Exception e) {
            throw new GoogleAdsConnectorException(
                    GoogleAdsConnectorErrorCode.DESCRIBE_FIELDS_FAILED, e);
        }
    }

    private SeaTunnelDataType<?> mapGoogleAdsType(String dataType) {
        switch (dataType) {
            case "INT64":
            case "UINT64":
                return BasicType.LONG_TYPE;
            case "INT32":
                return BasicType.INT_TYPE;
            case "DOUBLE":
            case "FLOAT":
                return BasicType.DOUBLE_TYPE;
            case "BOOLEAN":
                return BasicType.BOOLEAN_TYPE;
            default:
                // DATE (non-uniform formats like 2026-09 for segments.month), STRING, ENUM,
                // RESOURCE_NAME, MESSAGE (emitted as JSON text) and unknown future types
                return BasicType.STRING_TYPE;
        }
    }

    /**
     * Runs one GAQL query end-to-end, walking forward via nextPageToken. Each result row is
     * flattened to a typed Object[] aligned with fieldPaths and pushed to rowConsumer immediately;
     * only one page's JSON body is held in memory at a time.
     */
    public void search(
            String customerId,
            String gaql,
            List<String> fieldPaths,
            SeaTunnelRowType rowType,
            Consumer<Object[]> rowConsumer) {
        String url =
                params.getApiEndpoint()
                        + String.format(SEARCH_PATH, params.getApiVersion(), customerId);
        String pageToken = null;

        do {
            ObjectNode requestBody = objectMapper.createObjectNode();
            requestBody.put("query", gaql);
            if (pageToken != null) {
                requestBody.put("pageToken", pageToken);
            }
            if (params.getPageSize() != null) {
                requestBody.put("pageSize", params.getPageSize());
            }

            String body =
                    executeJsonPost(url, requestBody, GoogleAdsConnectorErrorCode.SEARCH_FAILED);
            try {
                JsonNode json = objectMapper.readTree(body);
                JsonNode results = json.get("results");
                if (results != null) {
                    for (JsonNode result : results) {
                        Object[] row = new Object[fieldPaths.size()];
                        for (int i = 0; i < fieldPaths.size(); i++) {
                            JsonNode node = resolvePath(result, fieldPaths.get(i));
                            row[i] =
                                    (node == null || node.isNull() || node.isMissingNode())
                                            ? null
                                            : extractValue(node, rowType.getFieldType(i));
                        }
                        rowConsumer.accept(row);
                    }
                }
                JsonNode next = json.get("nextPageToken");
                pageToken = (next != null && !next.asText().isEmpty()) ? next.asText() : null;
            } catch (GoogleAdsConnectorException e) {
                throw e;
            } catch (Exception e) {
                throw new GoogleAdsConnectorException(GoogleAdsConnectorErrorCode.SEARCH_FAILED, e);
            }
        } while (pageToken != null);
    }

    /**
     * GAQL field paths are snake_case (ad_group.cpc_bid_micros) but the REST response uses protobuf
     * JSON camelCase keys (adGroup.cpcBidMicros), so each segment is converted before lookup.
     * Absent fields (Google omits empty proto fields entirely) resolve to null.
     */
    private JsonNode resolvePath(JsonNode result, String fieldPath) {
        JsonNode node = result;
        for (String segment : fieldPath.split("\\.")) {
            if (node == null) {
                return null;
            }
            node = node.get(snakeToCamel(segment));
        }
        return node;
    }

    private String snakeToCamel(String segment) {
        if (segment.indexOf('_') < 0) {
            return segment;
        }
        StringBuilder sb = new StringBuilder(segment.length());
        boolean upperNext = false;
        for (int i = 0; i < segment.length(); i++) {
            char c = segment.charAt(i);
            if (c == '_') {
                upperNext = true;
            } else {
                sb.append(upperNext ? Character.toUpperCase(c) : c);
                upperNext = false;
            }
        }
        return sb.toString();
    }

    private Object extractValue(JsonNode node, SeaTunnelDataType<?> targetType) {
        switch (targetType.getSqlType()) {
            case BIGINT:
                // protobuf JSON serializes int64 as a string
                return Long.parseLong(node.asText());
            case INT:
                return node.asInt();
            case DOUBLE:
                return node.asDouble();
            case BOOLEAN:
                return node.asBoolean();
            default:
                return node.isValueNode() ? node.asText() : node.toString();
        }
    }

    /**
     * POSTs a JSON body with Google Ads headers. On 401 the access token is refreshed once and the
     * request replayed (not counted against max_retries). Transient failures (429/5xx/IO errors)
     * are retried up to max_retries with exponential backoff; other non-200 statuses fail fast with
     * the API error body surfaced.
     */
    private String executeJsonPost(
            String url, ObjectNode requestBody, GoogleAdsConnectorErrorCode errorCode) {
        boolean tokenRefreshed = false;
        int attempt = 0;
        while (true) {
            ensureToken();
            try {
                HttpPost post = new HttpPost(url);
                post.setHeader(HttpHeaders.AUTHORIZATION, "Bearer " + accessToken);
                post.setHeader("developer-token", params.getDeveloperToken());
                if (params.getLoginCustomerId() != null) {
                    post.setHeader("login-customer-id", params.getLoginCustomerId());
                }
                post.setEntity(
                        new StringEntity(
                                objectMapper.writeValueAsString(requestBody),
                                ContentType.APPLICATION_JSON));

                try (CloseableHttpResponse response = httpClient.execute(post)) {
                    int status = response.getStatusLine().getStatusCode();
                    String body =
                            EntityUtils.toString(response.getEntity(), StandardCharsets.UTF_8);
                    if (status == 200) {
                        return body;
                    }
                    if (status == 401 && !tokenRefreshed) {
                        log.info("Access token rejected (401); refreshing and replaying request");
                        tokenRefreshed = true;
                        authenticate();
                        continue;
                    }
                    if (isTransient(status) && attempt < params.getMaxRetries()) {
                        backoff(++attempt, "HTTP " + status);
                        continue;
                    }
                    throw new GoogleAdsConnectorException(
                            errorCode, "HTTP " + status + ": " + body);
                }
            } catch (GoogleAdsConnectorException e) {
                throw e;
            } catch (IOException e) {
                if (attempt < params.getMaxRetries()) {
                    backoff(++attempt, e.getMessage());
                    continue;
                }
                throw new GoogleAdsConnectorException(errorCode, e);
            }
        }
    }

    private boolean isTransient(int status) {
        return status == 429 || (status >= 500 && status <= 504);
    }

    private void backoff(int attempt, String reason) {
        long sleepMs = params.getRetryBackoffMs() * (1L << (attempt - 1));
        log.warn(
                "Transient Google Ads API failure ({}); retry {}/{} after {}ms",
                reason,
                attempt,
                params.getMaxRetries(),
                sleepMs);
        try {
            Thread.sleep(sleepMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new GoogleAdsConnectorException(
                    GoogleAdsConnectorErrorCode.SEARCH_FAILED, "Interrupted during retry backoff");
        }
    }

    private void ensureToken() {
        if (accessToken == null || System.currentTimeMillis() >= tokenExpiresAtMs) {
            authenticate();
        }
    }

    @Override
    public void close() throws IOException {
        httpClient.close();
    }
}
