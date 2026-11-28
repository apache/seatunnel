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

package org.apache.seatunnel.connectors.seatunnel.alpacabroker;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.io.IOException;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.apache.seatunnel.connectors.seatunnel.alpacabroker.AlpacaBrokerHttp.JSON;
import static org.apache.seatunnel.connectors.seatunnel.alpacabroker.AlpacaBrokerHttp.failure;

/**
 * Decodes the seven fixed resource shapes and advances their documented pagination.
 *
 * <p>Each returned page contains a complete group of rows and an immutable next position.
 */
final class AlpacaBrokerPages {
    private static final int ACCOUNT_LIMIT = 1000;
    private final AlpacaBrokerSettings settings;
    private final AlpacaBrokerHttp http;
    private final String tableId;

    AlpacaBrokerPages(AlpacaBrokerSettings settings, AlpacaBrokerHttp http, String tableId) {
        this.settings = settings;
        this.http = http;
        this.tableId = tableId;
    }

    Page next(AlpacaBrokerPosition input, long deadline) throws Exception {
        if (input.requests >= settings.maxRequests) {
            throw invalid("max_requests exhausted before the source reached its end");
        }
        String operation = settings.resource.key;
        switch (settings.resource) {
            case ASSETS:
                Map<String, String> assetQuery = new LinkedHashMap<>();
                put(assetQuery, "asset_class", settings.assetClass);
                return rows(
                        input.finish(),
                        array(get(operation, assetQuery, deadline, "assets")),
                        "id",
                        null);
            case ACCOUNTS_ACTIVITIES:
                return readActivities(input, deadline);
            case ACCOUNTS:
                AccountPage accounts = readAccountPage(input, false, deadline);
                return rows(input.afterAccounts(accounts.next), accounts.records, "id", null);
            case ACCOUNT_DOCUMENTS:
                return readDocuments(input, deadline);
            case CALENDAR:
                Map<String, String> calendarQuery = dates(input);
                calendarQuery.put("date_type", settings.calendarDateType);
                List<JsonNode> calendar =
                        array(get(operation, calendarQuery, deadline, "calendar"));
                for (JsonNode record : calendar) {
                    date(value(record, "date"));
                    checkDate(
                            record,
                            "trading".equals(settings.calendarDateType)
                                    ? "date"
                                    : "settlement_date",
                            input);
                }
                return rows(input.finish(), calendar, "date", null);
            case CLOCK:
                JsonNode clock = get(operation, Map.of(), deadline, "clock");
                time(value(clock, "timestamp"));
                time(value(clock, "next_open"));
                time(value(clock, "next_close"));
                if (!clock.path("is_open").isBoolean()) {
                    throw invalid("clock.is_open is not a boolean");
                }
                return new Page(
                        input.finish(), List.of(row(value(clock, "timestamp"), null, clock)));
            case COUNTRY_INFO:
                return countries(input, deadline);
            default:
                throw invalid("unknown resource");
        }
    }

    private Page readActivities(AlpacaBrokerPosition input, long deadline) throws Exception {
        Map<String, String> query = new LinkedHashMap<>();
        put(query, "after", settings.startTime);
        put(query, "until", input.fixedTime);
        put(query, "page_token", input.cursor);
        query.put("direction", "asc");
        query.put("page_size", Integer.toString(settings.pageSize));
        List<JsonNode> records =
                array(get(settings.resource.key, query, deadline, "accounts", "activities"));
        if (records.isEmpty()) {
            return new Page(input.finish(), List.of());
        }
        if (records.size() > settings.pageSize) {
            throw invalid("activity page exceeds page_size");
        }
        for (JsonNode record : records) {
            if (input.cursor != null && input.cursor.equals(value(record, "id"))) {
                throw invalid("exclusive activity cursor was repeated");
            }
        }
        String token = value(records.get(records.size() - 1), "id");
        if (input.activityCursors.contains(AlpacaBrokerSettings.digest(token))) {
            throw invalid("activity pagination repeated an earlier cursor");
        }
        // Even a short activity page can have a successor. The empty page is terminal.
        return rows(input.afterActivities(token), records, "id", null);
    }

    private Page readDocuments(AlpacaBrokerPosition input, long deadline) throws Exception {
        if (!input.pendingParents.isEmpty()) {
            String parent = input.pendingParents.get(input.parentOffset);
            List<JsonNode> documents =
                    array(
                            get(
                                    "account_documents",
                                    dates(input),
                                    deadline,
                                    "accounts",
                                    parent,
                                    "documents"));
            for (JsonNode document : documents) {
                checkDate(document, "date", input);
            }
            return rows(input.afterDocument(), documents, "id", parent);
        }
        AccountPage page = readAccountPage(input, true, deadline);
        List<String> parents = new ArrayList<>(page.records.size());
        for (JsonNode account : page.records) {
            parents.add(value(account, "id"));
        }
        return new Page(input.withParents(parents, page.next), List.of());
    }

    private AccountPage readAccountPage(AlpacaBrokerPosition input, boolean parent, long deadline)
            throws Exception {
        Map<String, String> query = new LinkedHashMap<>();
        Instant from =
                input.cursor != null ? time(input.cursor) : parent ? null : settings.startTime;
        put(query, "created_after", from);
        put(query, "created_before", input.fixedTime);
        query.put("sort", "asc");
        List<JsonNode> accounts = array(get("accounts", query, deadline, "accounts"));
        if (accounts.size() > ACCOUNT_LIMIT) {
            throw invalid("account list exceeds the service's 1000-account window");
        }
        List<Instant> times = new ArrayList<>(accounts.size());
        Set<UUID> identities = new HashSet<>();
        Instant previous = from;
        for (JsonNode account : accounts) {
            UUID id = uuid(value(account, "id"));
            if (!identities.add(id)) {
                throw invalid("account page repeated a record ID");
            }
            Instant created = time(value(account, "created_at"));
            if ((previous != null && created.isBefore(previous))
                    || created.isAfter(input.fixedTime)) {
                throw invalid("account creation time is outside the ascending requested window");
            }
            times.add(created);
            previous = created;
        }
        if (accounts.size() < ACCOUNT_LIMIT) {
            return new AccountPage(accounts, null);
        }
        Instant boundary = times.get(times.size() - 1);
        int limit = times.size() - 1;
        while (limit > 0 && boundary.equals(times.get(limit - 1))) {
            limit--;
        }
        if (limit == 0) {
            throw invalid(
                    "1000 accounts share one creation timestamp and the service cannot paginate the tie");
        }
        // An inclusive lower bound re-reads the full boundary cohort on the next request.
        return new AccountPage(accounts.subList(0, limit), boundary.toString());
    }

    private Page countries(AlpacaBrokerPosition input, long deadline) throws Exception {
        JsonNode countries = get("country-info", Map.of(), deadline, "country-info");
        if (!countries.isObject()) {
            throw invalid("country-info must contain an object keyed by country code");
        }
        List<SeaTunnelRow> rows = new ArrayList<>();
        Iterator<Map.Entry<String, JsonNode>> fields = countries.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> country = fields.next();
            if (!country.getKey().matches("[A-Z]{3}") || !country.getValue().isObject()) {
                throw invalid("country-info contains an invalid country entry");
            }
            rows.add(row(country.getKey(), null, country.getValue()));
        }
        return new Page(input.finish(), rows);
    }

    private Page rows(
            AlpacaBrokerPosition next, List<JsonNode> records, String identity, String parent)
            throws IOException {
        List<SeaTunnelRow> rows = new ArrayList<>(records.size());
        Set<String> ids = new HashSet<>();
        for (JsonNode record : records) {
            String id = value(record, identity);
            if (!ids.add(id)) {
                throw invalid("page repeated a record ID");
            }
            rows.add(row(id, parent, record));
        }
        return new Page(next, rows);
    }

    private SeaTunnelRow row(String id, String parent, JsonNode record) throws IOException {
        try {
            SeaTunnelRow row =
                    new SeaTunnelRow(
                            new Object[] {
                                settings.resource.key, id, parent, JSON.writeValueAsString(record)
                            });
            row.setTableId(tableId);
            return row;
        } catch (JsonProcessingException e) {
            throw invalid("could not normalize a JSON record");
        }
    }

    private Map<String, String> dates(AlpacaBrokerPosition input) {
        Map<String, String> query = new LinkedHashMap<>();
        put(query, "start", settings.startDate);
        put(query, "end", input.fixedDate);
        return query;
    }

    private void checkDate(JsonNode record, String field, AlpacaBrokerPosition input)
            throws IOException {
        LocalDate value = date(value(record, field));
        if ((settings.startDate != null && value.isBefore(settings.startDate))
                || value.isAfter(input.fixedDate)) {
            throw invalid("response date is outside the requested inclusive window");
        }
    }

    private JsonNode get(String operation, Map<String, String> query, long deadline, String... path)
            throws Exception {
        List<String> segments = new ArrayList<>();
        segments.add("v1");
        segments.addAll(List.of(path));
        return http.get(operation, segments, query, deadline);
    }

    private static void put(Map<String, String> query, String key, Object value) {
        if (value != null) {
            query.put(key, value.toString());
        }
    }

    private List<JsonNode> array(JsonNode document) throws IOException {
        if (!document.isArray()) {
            throw invalid("expected an array response");
        }
        if (document.size() > 100000) {
            throw invalid("response exceeds the maximum record count");
        }
        List<JsonNode> records = new ArrayList<>(document.size());
        for (JsonNode value : document) {
            if (!value.isObject()) {
                throw invalid("expected JSON object records");
            }
            records.add(value);
        }
        return records;
    }

    private String value(JsonNode record, String field) throws IOException {
        JsonNode value = record.path(field);
        if (!value.isTextual()
                || value.asText().isBlank()
                || value.asText().length() > 512
                || value.asText().chars().anyMatch(c -> Character.isISOControl(c))) {
            throw invalid("missing or invalid " + field);
        }
        return value.asText();
    }

    private Instant time(String text) throws IOException {
        try {
            return OffsetDateTime.parse(text).toInstant();
        } catch (DateTimeException e) {
            throw invalid("response timestamp is invalid");
        }
    }

    private LocalDate date(String text) throws IOException {
        try {
            return LocalDate.parse(text);
        } catch (DateTimeException e) {
            throw invalid("response date is invalid");
        }
    }

    private UUID uuid(String text) throws IOException {
        try {
            UUID value = UUID.fromString(text);
            if (value.toString().equalsIgnoreCase(text)) {
                return value;
            }
        } catch (IllegalArgumentException ignored) {
            // Report the malformed identifier without exposing a customer account ID.
        }
        throw invalid("response account ID is not a UUID");
    }

    private AlpacaBrokerHttp.ReadFailure invalid(String message) {
        return failure(settings.resource.key, message);
    }

    /**
     * Rows and the position committed together when the reader publishes this page.
     *
     * <p>The reader can retry an uncommitted request using its previous checkpoint position.
     */
    static final class Page {
        final AlpacaBrokerPosition position;
        final List<SeaTunnelRow> rows;

        private Page(AlpacaBrokerPosition position, List<SeaTunnelRow> rows) {
            this.position = position;
            this.rows = rows;
        }
    }

    /**
     * Fully covered accounts before an inclusive timestamp used for the next page.
     *
     * <p>The final equal-timestamp group of a capped response is deferred to the inclusive next
     * request.
     */
    private static final class AccountPage {
        private final List<JsonNode> records;
        private final String next;

        private AccountPage(List<JsonNode> records, String next) {
            this.records = records;
            this.next = next;
        }
    }
}
