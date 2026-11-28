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

package org.apache.seatunnel.connectors.seatunnel.bamboohr;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.StreamReadConstraints;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.StreamReadFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Decodes provider documents without coercing HR values or including response bodies in errors.
 *
 * <p>Rows keep JSON numeric precision and the distinction between missing fields and explicit
 * nulls.
 */
final class BambooHRJson {
    private static final ObjectMapper JSON =
            new ObjectMapper(
                            JsonFactory.builder()
                                    .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
                                    .streamReadConstraints(
                                            StreamReadConstraints.builder()
                                                    .maxNestingDepth(80)
                                                    .maxNumberLength(256)
                                                    .maxStringLength(4 * 1024 * 1024)
                                                    .build())
                                    .build())
                    .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
                    .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                    .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));
    private static final Pattern EMPLOYEE_ID = Pattern.compile("[1-9][0-9]{0,31}");
    private static final Set<String> CHANGE_ACTIONS = Set.of("Inserted", "Updated", "Deleted");

    private BambooHRJson() {}

    /**
     * Parses one strict, UTF-8 JSON document. Parser exceptions may contain employee data.
     *
     * <p>Only fixed parser failure messages may reach the engine.
     */
    static JsonNode parse(byte[] bytes) throws IOException {
        try {
            StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(bytes));
        } catch (CharacterCodingException e) {
            throw failure("response is not valid UTF-8 JSON");
        }
        try {
            JsonNode root = JSON.readTree(bytes);
            if (root != null && (root.isObject() || root.isArray())) {
                return root;
            }
        } catch (IOException | RuntimeException ignored) {
            // Do not propagate payload fragments or response-provided names through the exception.
        }
        throw failure("response is not a valid JSON object or array");
    }

    static byte[] report(List<String> fields) throws IOException {
        ObjectNode body = JSON.createObjectNode().put("title", "Apache SeaTunnel employee extract");
        fields.forEach(body.putArray("fields")::add);
        return encode(body);
    }

    static List<JsonNode> records(JsonNode node, String member, int limit) throws IOException {
        JsonNode array = member == null ? node : node.path(member);
        if (!array.isArray() || array.size() > limit) {
            throw failure("invalid response collection or max_records_per_response exceeded");
        }
        List<JsonNode> result = new ArrayList<>(array.size());
        for (JsonNode row : array) {
            if (!row.isObject()) {
                throw failure("response collection contains a non-object record");
            }
            result.add(row);
        }
        return result;
    }

    /**
     * Applies the provider's creation date as a second filter after its time-off overlap query.
     *
     * <p>Historical records without that date are preserved within the provider's response window.
     */
    static boolean inTimeOffWindow(JsonNode row, LocalDate first, LocalDate last)
            throws IOException {
        LocalDate date = date(row.path("created"), "created");
        return date == null || (!date.isBefore(first) && !date.isAfter(last));
    }

    /**
     * Uses the provider's timesheet calendar date, including hour entries whose start is null.
     *
     * <p>The date shared by clock and hour records takes precedence over an optional start
     * timestamp.
     */
    static boolean inTimesheetWindow(JsonNode row, LocalDate first, LocalDate last, ZoneId zone)
            throws IOException {
        LocalDate date = date(row.path("date"), "timesheet date");
        if (date == null) {
            JsonNode start = row.path("start");
            if (!start.isMissingNode() && !start.isNull()) {
                if (!start.isTextual()) {
                    throw failure("invalid timesheet start timestamp");
                }
                try {
                    date =
                            OffsetDateTime.parse(start.textValue())
                                    .atZoneSameInstant(zone)
                                    .toLocalDate();
                } catch (DateTimeException e) {
                    throw failure("invalid timesheet start timestamp");
                }
            }
        }
        return date == null || (!date.isBefore(first) && !date.isAfter(last));
    }

    private static LocalDate date(JsonNode node, String field) throws IOException {
        if (node.isNull() || node.isMissingNode()) {
            return null;
        }
        if (node.isTextual() && node.textValue().length() == 10) {
            try {
                LocalDate value = LocalDate.parse(node.textValue());
                if (value.getYear() >= 1) {
                    return value;
                }
            } catch (DateTimeException ignored) {
                // Report only the known date field name.
            }
        }
        throw failure("invalid " + field + " in response");
    }

    static List<EmployeeRef> directory(JsonNode root, int limit) throws IOException {
        List<JsonNode> rows = records(root, "employees", limit);
        Set<String> seen = new HashSet<>();
        List<EmployeeRef> employees = new ArrayList<>();
        for (JsonNode row : rows) {
            String id = employeeId(row.path("id"));
            if (!seen.add(id)) {
                throw failure("published directory contains a duplicate internal employee ID");
            }
            employees.add(new EmployeeRef(id, null, null));
        }
        employees.sort(Comparator.comparing(ref -> ref.id));
        return employees;
    }

    /**
     * Validates the distinct documented changed-ID map and freezes its effective upper time bound.
     */
    static List<EmployeeRef> changed(JsonNode root, int limit, Instant since, Instant through)
            throws IOException {
        JsonNode map = root.path("employees");
        if (!map.isObject() || map.size() > limit) {
            throw failure("invalid changed-employee map or max_records_per_response exceeded");
        }
        List<EmployeeRef> employees = new ArrayList<>();
        Iterator<Map.Entry<String, JsonNode>> entries = map.fields();
        while (entries.hasNext()) {
            Map.Entry<String, JsonNode> entry = entries.next();
            JsonNode row = entry.getValue();
            String id = employeeId(row.path("id"));
            String action = row.path("action").asText("");
            String timestamp = row.path("lastChanged").asText("");
            if (!id.equals(entry.getKey()) || !CHANGE_ACTIONS.contains(action)) {
                throw failure("invalid changed-employee ID or action");
            }
            Instant changed;
            try {
                changed = OffsetDateTime.parse(timestamp).toInstant();
            } catch (DateTimeException e) {
                throw failure("invalid changed-employee lastChanged timestamp");
            }
            if (!changed.isBefore(since) && !changed.isAfter(through)) {
                employees.add(new EmployeeRef(id, action, timestamp));
            }
        }
        employees.sort(Comparator.comparing(ref -> ref.id));
        return employees;
    }

    static JsonNode employee(JsonNode node, String expectedId) throws IOException {
        if (!node.isObject() || !expectedId.equals(employeeId(node.path("id")))) {
            throw failure("employee detail response does not match the requested internal ID");
        }
        return node;
    }

    private static String employeeId(JsonNode node) throws IOException {
        String text = node.isTextual() || node.isIntegralNumber() ? node.asText() : "";
        if (!EMPLOYEE_ID.matcher(text).matches()) {
            throw failure("invalid internal employee ID");
        }
        return text;
    }

    static String tombstone(String id) throws IOException {
        return asJson(JSON.createObjectNode().put("id", id));
    }

    static String asJson(JsonNode row) throws IOException {
        return new String(encode(row), StandardCharsets.UTF_8);
    }

    static String digestRows(List<String> rows) {
        MessageDigest state = sha256();
        for (String row : rows) {
            state.update(row.getBytes(StandardCharsets.UTF_8));
            state.update((byte) '\n');
        }
        return hex(state.digest());
    }

    static String digestEmployees(List<EmployeeRef> employees) {
        MessageDigest state = sha256();
        for (EmployeeRef employee : employees) {
            state.update(
                    (employee.id + "|" + employee.action + "|" + employee.changedAt + "\n")
                            .getBytes(StandardCharsets.US_ASCII));
        }
        return hex(state.digest());
    }

    private static byte[] encode(JsonNode row) throws IOException {
        try {
            return JSON.writeValueAsBytes(row);
        } catch (IOException | RuntimeException ignored) {
            throw failure("could not serialize a JSON record");
        }
    }

    static String digestText(String text) {
        return hex(sha256().digest(text.getBytes(StandardCharsets.UTF_8)));
    }

    private static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable");
        }
    }

    private static String hex(byte[] bytes) {
        char[] digits = "0123456789abcdef".toCharArray();
        char[] result = new char[bytes.length * 2];
        for (int i = 0; i < bytes.length; i++) {
            result[i * 2] = digits[(bytes[i] & 0xff) >>> 4];
            result[i * 2 + 1] = digits[bytes[i] & 0xf];
        }
        return new String(result);
    }

    static Failure failure(String reason) {
        return new Failure("BambooHR: " + reason);
    }

    /**
     * A safe failure containing no credential, request URL, or response record values.
     *
     * <p>Provider-originated response content is kept out of job diagnostics.
     */
    static final class Failure extends IOException {
        Failure(String reason) {
            super(reason);
        }
    }

    /**
     * A transient employee traversal key and optional provider-reported change metadata.
     *
     * <p>Full snapshots have no change metadata, while deleted employees carry no detail record.
     */
    static final class EmployeeRef {
        final String id;
        final String action;
        final String changedAt;

        EmployeeRef(String id, String action, String changedAt) {
            this.id = id;
            this.action = action;
            this.changedAt = changedAt;
        }

        boolean deleted() {
            return "Deleted".equals(action);
        }
    }
}
