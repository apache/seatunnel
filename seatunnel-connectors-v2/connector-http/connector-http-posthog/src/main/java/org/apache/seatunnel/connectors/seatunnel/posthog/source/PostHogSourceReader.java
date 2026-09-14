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

package org.apache.seatunnel.connectors.seatunnel.posthog.source;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.seatunnel.shade.com.google.common.annotations.VisibleForTesting;

import org.apache.seatunnel.api.serialization.DeserializationSchema;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.http.client.HttpClientProvider;
import org.apache.seatunnel.connectors.seatunnel.http.client.HttpResponse;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpParameter;
import org.apache.seatunnel.connectors.seatunnel.http.exception.HttpConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.http.exception.HttpConnectorException;
import org.apache.seatunnel.connectors.seatunnel.http.source.DeserializationCollector;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Set;

public class PostHogSourceReader extends AbstractSingleSplitReader<SeaTunnelRow> {

    private final HttpParameter httpParameter;
    private final SingleSplitReaderContext context;
    private final DeserializationCollector deserializationCollector;
    private final SeaTunnelRowType rowType;
    private HttpClientProvider httpClient;

    public PostHogSourceReader(
            HttpParameter httpParameter,
            SingleSplitReaderContext context,
            DeserializationSchema<SeaTunnelRow> deserializationSchema,
            SeaTunnelRowType rowType) {
        this(httpParameter, context, new DeserializationCollector(deserializationSchema), rowType);
    }

    @VisibleForTesting
    PostHogSourceReader(
            HttpParameter httpParameter,
            SingleSplitReaderContext context,
            DeserializationCollector deserializationCollector,
            SeaTunnelRowType rowType) {
        this.httpParameter = httpParameter;
        this.context = context;
        this.deserializationCollector = deserializationCollector;
        this.rowType = rowType;
    }

    @Override
    public void open() {
        httpClient = new HttpClientProvider(httpParameter);
    }

    @VisibleForTesting
    void setHttpClient(HttpClientProvider httpClient) {
        this.httpClient = httpClient;
    }

    @Override
    public void close() throws IOException {
        if (httpClient != null) {
            httpClient.close();
        }
    }

    @Override
    public void internalPollNext(Collector<SeaTunnelRow> output) throws Exception {
        HttpResponse response =
                httpClient.doPost(
                        httpParameter.getUrl(),
                        httpParameter.getHeaders(),
                        httpParameter.getBody());
        if (response.getCode() < 200 || response.getCode() >= 300) {
            throw requestFailed(
                    "PostHog query request failed with HTTP status " + response.getCode());
        }
        collectResponse(response.getContent(), output);
        context.signalNoMoreElement();
    }

    @VisibleForTesting
    void collectResponse(String content, Collector<SeaTunnelRow> output) throws IOException {
        if (content == null || content.trim().isEmpty()) {
            throw requestFailed("PostHog query returned an empty response");
        }

        JsonNode response;
        try {
            response = JsonUtils.stringToJsonNode(content);
        } catch (Exception e) {
            throw new HttpConnectorException(
                    HttpConnectorErrorCode.REQUEST_FAILED,
                    "PostHog query returned invalid JSON",
                    e);
        }
        validateQueryStatus(response);

        JsonNode columnsNode = response.get("columns");
        JsonNode resultsNode = response.get("results");
        if (columnsNode == null || !columnsNode.isArray()) {
            throw requestFailed("PostHog query response is missing the columns array");
        }
        if (resultsNode == null || !resultsNode.isArray()) {
            throw requestFailed("PostHog query response is missing the results array");
        }

        String[] columns = readColumns(columnsNode);
        validateSchemaColumns(columns);
        for (JsonNode result : resultsNode) {
            if (!result.isArray()) {
                throw requestFailed("PostHog query result rows must be arrays");
            }
            if (result.size() != columns.length) {
                throw requestFailed("PostHog query result width does not match the columns array");
            }
            ObjectNode row = JsonUtils.createObjectNode();
            for (int index = 0; index < columns.length; index++) {
                row.set(columns[index], result.get(index));
            }
            deserializationCollector.collect(
                    row.toString().getBytes(StandardCharsets.UTF_8), output);
        }
    }

    private void validateQueryStatus(JsonNode response) {
        JsonNode error = response.get("error");
        if (error != null && !error.isNull() && !error.asText().trim().isEmpty()) {
            throw requestFailed("PostHog query failed: " + error.asText());
        }

        JsonNode queryStatus = response.get("query_status");
        if (queryStatus == null || queryStatus.isNull()) {
            return;
        }
        if (queryStatus.path("error").asBoolean(false)) {
            String message = queryStatus.path("error_message").asText("unknown query error");
            throw requestFailed("PostHog query failed: " + message);
        }
        if (queryStatus.has("complete") && !queryStatus.path("complete").asBoolean()) {
            throw requestFailed("PostHog query did not complete in blocking mode");
        }
    }

    private String[] readColumns(JsonNode columnsNode) {
        String[] columns = new String[columnsNode.size()];
        Set<String> uniqueColumns = new HashSet<>();
        for (int index = 0; index < columnsNode.size(); index++) {
            JsonNode columnNode = columnsNode.get(index);
            if (!columnNode.isTextual() || columnNode.asText().trim().isEmpty()) {
                throw requestFailed("PostHog query response contains an invalid column name");
            }
            String column = columnNode.asText();
            if (!uniqueColumns.add(column)) {
                throw requestFailed(
                        "PostHog query returned duplicate column name '" + column + "'");
            }
            columns[index] = column;
        }
        return columns;
    }

    private void validateSchemaColumns(String[] columns) {
        Set<String> availableColumns = new LinkedHashSet<>();
        Collections.addAll(availableColumns, columns);
        Set<String> missingColumns = new LinkedHashSet<>();
        for (String fieldName : rowType.getFieldNames()) {
            if (!availableColumns.contains(fieldName)) {
                missingColumns.add(fieldName);
            }
        }
        if (!missingColumns.isEmpty()) {
            throw requestFailed(
                    "PostHog query does not return schema columns "
                            + missingColumns
                            + ". Alias the selected HogQL columns to match the SeaTunnel schema");
        }
    }

    private static HttpConnectorException requestFailed(String message) {
        return new HttpConnectorException(HttpConnectorErrorCode.REQUEST_FAILED, message);
    }
}
