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
package org.apache.seatunnel.connectors.seatunnel.apptivo;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Maps the five supported DAO reads to exact Apptivo records.
 *
 * <p>Every list resource has its own offset and ID field. Cases returns the configuration object
 * directly, using a stable synthetic ID for that singleton.
 */
final class ApptivoApi {
    private final ApptivoConfig config;
    private final ApptivoHttp http;

    // Form-encoded query components never enter checkpoint state or diagnostics.
    private final String credentials;

    ApptivoApi(ApptivoConfig config, ApptivoHttp http) {
        this.config = config;
        this.http = http;
        credentials = "&apiKey=" + encode(config.token) + "&accessKey=" + encode(config.accessKey);
    }

    /**
     * Reads one bounded collection page or the case settings singleton.
     *
     * <p>A server-provided counter may validate continuity, while the normal protocol can use full,
     * short and empty data pages without counters.
     */
    Page fetch(
            ApptivoResource resource, int offset, Long expectedTotal, String tenant, long deadline)
            throws InterruptedException {
        String target = "/app/dao/v6/" + resource.label + "?a=" + resource.action;
        if (!resource.singleton()) {
            target += "&startIndex=" + offset + "&numRecords=" + config.pageSize;
        }
        JsonNode response = http.get(resource, target + credentials, deadline);
        validateStatus(resource, response);
        String firm = observeFirm(resource, tenant, response.path("firmId"));
        if (resource.singleton()) {
            if (!(response.hasNonNull("statuses")
                    || response.hasNonNull("types")
                    || response.hasNonNull("priorities")
                    || response.hasNonNull("assignees")
                    || response.hasNonNull("appName")
                    || response.hasNonNull("firmId"))) {
                throw resource.failure("case response is missing its configuration data");
            }
            return new Page(
                    List.of(new Record("case-config", response)),
                    null,
                    firm,
                    ApptivoConfig.hash(response.toString()),
                    false);
        }

        JsonNode records = response.path("data");
        if (!records.isArray()) {
            throw resource.failure("list response does not contain a data array");
        }
        if (records.size() > config.pageSize) {
            throw resource.failure("list response exceeds the requested page size");
        }
        Long start = counter(resource, response, "startIndex");
        Long declared = counter(resource, response, "totalRecords");
        if (start != null && start != offset
                || expectedTotal != null && declared != null && !expectedTotal.equals(declared)) {
            throw resource.failure("pagination offset or total is inconsistent");
        }
        Long total = declared == null ? expectedTotal : declared;
        long end = (long) offset + records.size();
        if (total != null && (end > total || end < total && records.size() < config.pageSize)) {
            throw resource.failure("pagination offset or total is inconsistent");
        }
        boolean more = total == null ? records.size() == config.pageSize : end < total;
        List<Record> rows = new ArrayList<>(records.size());
        Set<String> ids = new HashSet<>();
        for (JsonNode row : records) {
            if (!row.isObject()) {
                throw resource.failure("list data contains a non-object record");
            }
            String id = identifier(resource, row.path(resource.id), resource.id);
            if (!ids.add(id)) {
                throw resource.failure("list contains duplicate record identities");
            }
            firm = observeFirm(resource, firm, row.path("firmId"));
            rows.add(new Record(id, row));
        }
        http.remaining(resource, deadline);
        return new Page(rows, total, firm, ApptivoConfig.hash(records.toString()), more);
    }

    private static void validateStatus(ApptivoResource resource, JsonNode response) {
        JsonNode status = response.path("status");
        if (status.isMissingNode()) {
            return;
        }
        if (status.isTextual() && "FAILURE".equalsIgnoreCase(status.textValue())) {
            throw resource.failure("provider response reported FAILURE");
        }
        if (!status.isTextual() || !"SUCCESS".equalsIgnoreCase(status.textValue())) {
            throw resource.failure("provider response has an unknown status");
        }
    }

    private static Long counter(ApptivoResource resource, JsonNode page, String field) {
        JsonNode value = page.path(field);
        if (value.isMissingNode()) {
            return null;
        }
        if (!value.isIntegralNumber() || !value.canConvertToLong() || value.longValue() < 0) {
            throw resource.failure("pagination offset or total is inconsistent");
        }
        return value.longValue();
    }

    private static String identifier(ApptivoResource resource, JsonNode value, String field) {
        if (value.isIntegralNumber() || value.isTextual()) {
            String key = value.asText();
            if (!key.isBlank()
                    && key.length() <= 256
                    && key.chars().noneMatch(Character::isISOControl)) {
                return key;
            }
        }
        throw resource.failure("record has an invalid " + field);
    }

    private static String observeFirm(ApptivoResource resource, String expected, JsonNode value) {
        if (value.isMissingNode() || value.isNull()) {
            return expected;
        }
        String actual = identifier(resource, value, "firmId");
        if (expected != null && !expected.equals(actual)) {
            throw resource.failure("response firmId does not match the scan tenant");
        }
        return actual;
    }

    private static String encode(String input) {
        return URLEncoder.encode(input, StandardCharsets.UTF_8).replace("+", "%20");
    }

    /**
     * Represents a validated provider page and its completion and tenant metadata.
     *
     * <p>The content hash checks resume without persisting any of its CRM records.
     */
    static final class Page {
        final List<Record> rows;
        final Long total;
        final String firm;
        final String digest;
        final boolean more;

        Page(List<Record> rows, Long total, String firm, String digest, boolean more) {
            this.rows = rows;
            this.total = total;
            this.firm = firm;
            this.digest = digest;
            this.more = more;
        }
    }

    /**
     * Separates the projected SeaTunnel row ID from a complete provider object.
     *
     * <p>Keeping the provider object whole preserves account-specific custom fields and numbers.
     */
    static final class Record {
        final String id;
        final JsonNode value;

        Record(String id, JsonNode value) {
            this.id = id;
            this.value = value;
        }
    }
}
