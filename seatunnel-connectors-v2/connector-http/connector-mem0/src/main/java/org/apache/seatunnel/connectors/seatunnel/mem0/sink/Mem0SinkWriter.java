/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0.
 */
package org.apache.seatunnel.connectors.seatunnel.mem0.sink;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.http.client.HttpClientProvider;
import org.apache.seatunnel.connectors.seatunnel.http.client.HttpResponse;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpParameter;
import org.apache.seatunnel.format.json.JsonSerializationSchema;

import java.io.IOException;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/** Writes SeaTunnel rows to the hosted Mem0 Platform V3 asynchronous add API. */
public class Mem0SinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {
    private final HttpClientProvider httpClient;
    private final String url;
    private final JsonSerializationSchema serializationSchema;
    private final ObjectMapper objectMapper;
    private final String messagesField;
    private final String userIdField;
    private final String agentIdField;
    private final String appIdField;
    private final String runIdField;
    private final String metadataField;
    private final int maxRetries;
    private final int retryBackoffMultiplierMillis;
    private final int retryBackoffMaxMillis;

    public Mem0SinkWriter(
            SeaTunnelRowType rowType,
            HttpParameter parameter,
            String messagesField,
            String userIdField,
            String agentIdField,
            String appIdField,
            String runIdField,
            String metadataField) {
        this.httpClient = new HttpClientProvider(parameter);
        this.url = parameter.getUrl();
        this.serializationSchema = new JsonSerializationSchema(rowType);
        this.objectMapper = serializationSchema.getMapper();
        this.messagesField = messagesField;
        this.userIdField = userIdField;
        this.agentIdField = agentIdField;
        this.appIdField = appIdField;
        this.runIdField = runIdField;
        this.metadataField = metadataField;
        this.headers = parameter.getHeaders();
        this.maxRetries = Math.max(0, parameter.getRetry());
        this.retryBackoffMultiplierMillis =
                Math.max(0, parameter.getRetryBackoffMultiplierMillis());
        this.retryBackoffMaxMillis = Math.max(0, parameter.getRetryBackoffMaxMillis());
    }

    private final java.util.Map<String, String> headers;

    @Override
    public void write(SeaTunnelRow element) throws IOException {
        ObjectNode row = parseRow(element);
        ObjectNode request = objectMapper.createObjectNode();
        JsonNode messages = required(row, messagesField, "messages");
        if (!messages.isArray()) {
            throw new IOException("Mem0 messages field must contain a JSON array");
        }
        request.set("messages", messages);
        boolean hasScope = false;
        hasScope |= copyIfPresent(row, userIdField, "user_id", request);
        hasScope |= copyIfPresent(row, agentIdField, "agent_id", request);
        hasScope |= copyIfPresent(row, appIdField, "app_id", request);
        hasScope |= copyIfPresent(row, runIdField, "run_id", request);
        if (!hasScope) {
            throw new IOException("Mem0 requires at least one scope field");
        }
        if (metadataField != null) {
            JsonNode metadata = row.get(metadataField);
            if (metadata != null && !metadata.isNull()) {
                if (!metadata.isObject()) {
                    throw new IOException("Mem0 metadata field must contain a JSON object");
                }
                request.set("metadata", metadata);
            }
        }
        send(request);
    }

    private ObjectNode parseRow(SeaTunnelRow element) throws IOException {
        return (ObjectNode) objectMapper.readTree(serializationSchema.serialize(element));
    }

    private JsonNode required(ObjectNode row, String field, String logicalName) throws IOException {
        JsonNode value = row.get(field);
        if (value == null
                || value.isNull()
                || (value.isTextual() && value.textValue().trim().isEmpty())) {
            throw new IOException("Mem0 " + logicalName + " field is missing or empty");
        }
        return value;
    }

    private boolean copyIfPresent(ObjectNode row, String field, String name, ObjectNode target) {
        if (field == null) {
            return false;
        }
        JsonNode value = row.get(field);
        if (value == null
                || value.isNull()
                || (value.isTextual() && value.textValue().trim().isEmpty())) {
            return false;
        }
        target.set(name, value);
        return true;
    }

    private void send(ObjectNode request) throws IOException {
        final String body;
        try {
            body = objectMapper.writeValueAsString(request);
            for (int attempt = 0; ; attempt++) {
                HttpResponse response = httpClient.doPost(url, headers, body);
                if (response.getCode() >= 200 && response.getCode() < 300) {
                    validateAcceptedResponse(response);
                    return;
                }
                if (!isRetryableStatus(response.getCode()) || attempt >= maxRetries) {
                    throw new IOException(
                            "Mem0 add request failed with HTTP status " + response.getCode());
                }
                waitBeforeRetry(attempt);
            }
        } catch (IOException e) {
            throw e;
        } catch (Exception e) {
            throw new IOException("Failed to send Mem0 add request", e);
        }
    }

    private void validateAcceptedResponse(HttpResponse response) throws IOException {
        JsonNode responseBody = objectMapper.readTree(response.getContent());
        JsonNode eventId = responseBody == null ? null : responseBody.get("event_id");
        if (eventId == null || eventId.isNull() || eventId.asText().trim().isEmpty()) {
            throw new IOException("Mem0 add response did not contain a nonblank event_id");
        }
    }

    static boolean isRetryableStatus(int statusCode) {
        return statusCode == 408 || statusCode == 429 || statusCode >= 500;
    }

    private void waitBeforeRetry(int attempt) throws IOException {
        long delay =
                Math.min(
                        (long) retryBackoffMultiplierMillis * (attempt + 1),
                        (long) retryBackoffMaxMillis);
        if (delay <= 0) {
            return;
        }
        try {
            TimeUnit.MILLISECONDS.sleep(delay);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while retrying Mem0 add request", e);
        }
    }

    @Override
    public void close() throws IOException {
        if (Objects.nonNull(httpClient)) {
            httpClient.close();
        }
    }
}
