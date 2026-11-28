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

package org.apache.seatunnel.connectors.seatunnel.jira.sink;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonParser;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.StreamReadConstraints;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.sink.AbstractSinkWriter;
import org.apache.seatunnel.connectors.seatunnel.jira.sink.config.JiraSinkOptions.Operation;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.regex.Pattern;

/**
 * Converts INSERT command rows into one validated Jira mutation at a time, preserving field values.
 * A returned write acknowledges that command, without guaranteeing exactly-once engine replay.
 */
final class JiraSinkWriter extends AbstractSinkWriter<SeaTunnelRow, Void> {

    static final int MAX_JSON_BYTES = 1024 * 1024;
    static final Pattern ISSUE_ID = Pattern.compile("[1-9][0-9]{0,19}");
    static final Pattern ISSUE_KEY =
            Pattern.compile("[A-Za-z][A-Za-z0-9_]{0,254}-[1-9][0-9]{0,19}");
    static final ObjectMapper JSON =
            new ObjectMapper(
                            JsonFactory.builder()
                                    .streamReadConstraints(
                                            StreamReadConstraints.builder()
                                                    .maxNestingDepth(64)
                                                    .build())
                                    .build())
                    .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
                    .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
                    .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);

    private final Operation operation;
    private final String issuePath;
    private final int identityIndex;
    private final int fieldsIndex;
    private final JiraClient client;

    JiraSinkWriter(
            Operation operation,
            String issuePath,
            int identityIndex,
            int fieldsIndex,
            JiraClient client) {
        this.operation = operation;
        this.issuePath = issuePath;
        this.identityIndex = identityIndex;
        this.fieldsIndex = fieldsIndex;
        this.client = client;
    }

    /* Return only after Jira acknowledges this command; an ambiguous failure must not be replayed. */
    @Override
    public void write(SeaTunnelRow row) throws IOException {
        if (row.getRowKind() != RowKind.INSERT) {
            throw invalid("requires INSERT command rows");
        }
        String path = issuePath;
        if (identityIndex >= 0) {
            Object identity = row.getField(identityIndex);
            if (!(identity instanceof String)
                    || (!ISSUE_ID.matcher((String) identity).matches()
                            && !ISSUE_KEY.matcher((String) identity).matches())) {
                throw invalid("requires issue_id_or_key to be a Jira issue ID or key");
            }
            path += "/" + identity;
        }
        if (operation == Operation.DELETE) {
            path += "?deleteSubtasks=false";
        }
        client.mutate(path, fieldsIndex < 0 ? null : encodeFields(row.getField(fieldsIndex)));
    }

    /* Validate before IO, but send original JSON bytes to avoid rounding numbers or losing nulls. */
    private byte[] encodeFields(Object value) throws IOException {
        if (!(value instanceof String) || ((String) value).length() > MAX_JSON_BYTES) {
            throw invalid("requires a STRING containing a bounded JSON object in fields");
        }
        String fields = (String) value;
        if (!StandardCharsets.UTF_8.newEncoder().canEncode(fields)) {
            throw invalid("fields contains invalid Unicode");
        }
        // Send the validated original JSON to preserve numeric precision and explicit nulls.
        byte[] body = ("{\"fields\":" + fields + "}").getBytes(StandardCharsets.UTF_8);
        if (body.length > MAX_JSON_BYTES) {
            throw invalid("request exceeds the 1 MiB JSON limit");
        }

        JsonNode object;
        try {
            object = JSON.readTree(fields);
        } catch (IOException e) {
            throw invalid("fields must be valid JSON with unique member names");
        }
        if (object == null || !object.isObject() || object.isEmpty()) {
            throw invalid("fields must be a non-empty JSON object");
        }
        if (operation == Operation.CREATE
                && (!isNonEmptyObject(object.path("project"))
                        || !isNonEmptyObject(object.path("issuetype"))
                        || !object.path("summary").isTextual()
                        || object.path("summary").asText().trim().isEmpty())) {
            throw invalid(
                    "fields requires a project object, an issuetype object and a non-empty summary");
        }
        return body;
    }

    private static boolean isNonEmptyObject(JsonNode node) {
        return node.isObject() && !node.isEmpty();
    }

    private IOException invalid(String message) {
        return new IOException("Jira Sink " + operation + " " + message);
    }

    @Override
    public void close() {
        client.close();
    }
}
