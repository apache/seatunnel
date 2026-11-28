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

package org.apache.seatunnel.connectors.seatunnel.appfigures;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.io.IOException;
import java.time.DateTimeException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.apache.seatunnel.connectors.seatunnel.appfigures.AppfiguresResource.ADS;
import static org.apache.seatunnel.connectors.seatunnel.appfigures.AppfiguresResource.ADSPEND;
import static org.apache.seatunnel.connectors.seatunnel.appfigures.AppfiguresResource.PRODUCTS;
import static org.apache.seatunnel.connectors.seatunnel.appfigures.AppfiguresResource.SUBSCRIPTIONS;
import static org.apache.seatunnel.connectors.seatunnel.appfigures.AppfiguresResource.USERS;

/**
 * Executes one bounded request and decodes the specific provider response.
 *
 * <p>Provider operations and JSON validation complete before any rows or the next continuation are
 * published to the engine.
 */
final class AppfiguresRead {
    private static final int MAX_SELECTION_LENGTH = 4096;
    private final AppfiguresSettings settings;
    private final AppfiguresHttp http;
    private final String tableId;

    AppfiguresRead(AppfiguresSettings settings, AppfiguresHttp http, String tableId) {
        this.settings = settings;
        this.http = http;
        this.tableId = tableId;
    }

    Response fetch(AppfiguresSplit current, long deadline)
            throws IOException, InterruptedException {
        AppfiguresResource target = current.discovering ? PRODUCTS : settings.resource;
        byte[] bytes = http.get(target, parameters(current, target), deadline);
        String digest = AppfiguresSettings.digest(bytes);
        if (current.digest != null && !current.digest.equals(digest)) {
            throw failure("response changed after checkpoint");
        }
        JsonNode payload = AppfiguresJson.object(bytes, target);
        AppfiguresSplit next = current.copy();
        next.digest = null;
        next.offset = 0;
        List<Row> decoded;
        if (target == USERS
                || (target == PRODUCTS && (payload.has("metadata") || payload.has("results")))) {
            decoded = page(payload, current, next, target);
        } else {
            if (target == PRODUCTS && current.page > 1) {
                throw failure("account products changed response shape during pagination");
            }
            decoded = records(target, payload);
            next.done = true;
        }
        List<SeaTunnelRow> rows = new ArrayList<>();
        if (current.discovering) {
            List<String> selected = new ArrayList<>(current.products);
            int length = String.join(",", selected).length();
            for (Row row : decoded) {
                length += row.id.length() + 1;
                if (length > MAX_SELECTION_LENGTH) {
                    throw failure("store product selection exceeds the report query limit");
                }
                selected.add(row.id);
            }
            next.products = List.copyOf(selected);
            if (next.done) {
                next.reportPhase();
            }
        } else {
            for (Row record : decoded) {
                SeaTunnelRow row =
                        new SeaTunnelRow(
                                new Object[] {
                                    settings.resource.name,
                                    record.id,
                                    AppfiguresJson.write(record.keys, settings.resource),
                                    AppfiguresJson.write(record.payload, settings.resource)
                                });
                row.setTableId(tableId);
                rows.add(row);
            }
        }
        http.requireTime(deadline);
        return new Response(next, digest, rows);
    }

    private Map<String, String> parameters(AppfiguresSplit state, AppfiguresResource target) {
        Map<String, String> query = new LinkedHashMap<>();
        if (target == PRODUCTS) {
            query.put("store", settings.store);
        }
        if (target == USERS || (target == PRODUCTS && state.page > 1)) {
            query.put("page", Integer.toString(state.page));
            query.put(
                    "count",
                    Integer.toString(
                            state.wirePageSize == 0 ? settings.pageSize : state.wirePageSize));
        }

        if (target.report()) {
            query.put("format", "json");
            if (!settings.wireGroups.isEmpty()) {
                query.put("group_by", settings.wireGroups);
                if ((target == ADS || target == ADSPEND) && settings.groups.contains("store")) {
                    query.put("combine", "true");
                }
            }
            if (state.reportStart != null) {
                query.put("start_date", state.reportStart.toString());
                query.put("end_date", state.reportEnd.toString());
            }
            if (!state.products.isEmpty()) {
                query.put("products", String.join(",", state.products));
                if (target == SUBSCRIPTIONS) {
                    query.put("include_inapps", "true");
                }
            }
        }
        return query;
    }

    private List<Row> records(AppfiguresResource target, JsonNode payload)
            throws AppfiguresFailure {
        switch (target) {
            case STATUS:
                if (payload.path("status").asInt(0) != 200 || !payload.path("user").isObject()) {
                    throw failure("status response is missing an authenticated user");
                }
                return List.of(entity(payload.path("user"), null, target));
            case USAGE:
                if (!payload.path("api_requests").isObject()) {
                    throw failure("API usage response is missing api_requests");
                }
                return List.of(new Row("usage", List.of(), payload));
            case DATA_CATEGORIES:
            case PRODUCTS:
                return dictionary(target, payload);
            default:
                List<Row> result = new ArrayList<>();
                report(payload, List.of(), result);
                return result;
        }
    }

    private List<Row> dictionary(AppfiguresResource target, JsonNode payload)
            throws AppfiguresFailure {
        if (payload.size() > settings.maxRows) {
            throw failure("dictionary exceeds max_rows");
        }
        List<Row> rows = new ArrayList<>(payload.size());
        Iterator<Map.Entry<String, JsonNode>> fields = payload.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> item = fields.next();
            rows.add(entity(item.getValue(), item.getKey(), target));
        }
        return rows;
    }

    private Row entity(JsonNode record, String dictionaryKey, AppfiguresResource target)
            throws AppfiguresFailure {
        JsonNode id = record.path("id");
        String identity = id.isTextual() || id.isIntegralNumber() ? id.asText() : "";
        if (!record.isObject()
                || !identity.matches("[0-9]{1,64}")
                || (dictionaryKey != null && !dictionaryKey.equals(identity))) {
            throw failure("invalid provider record identifier");
        }
        if (target == PRODUCTS && !settings.store.equals(record.path("store").asText())) {
            throw failure("product is outside the selected store or lacks its store identity");
        }
        return new Row(
                identity, dictionaryKey == null ? List.of() : List.of(dictionaryKey), record);
    }

    private List<Row> page(
            JsonNode payload,
            AppfiguresSplit current,
            AppfiguresSplit next,
            AppfiguresResource target)
            throws AppfiguresFailure {
        JsonNode results = payload.path("results");
        JsonNode metadata = payload.path("metadata").path("resultset");
        int page = integer(metadata, "page", 1, settings.maxPages);
        int size = integer(metadata, "count", 0, 100);
        int pages = integer(metadata, "total_pages", 0, settings.maxPages);
        int total = integer(metadata, "total_count", 0, settings.maxRows);
        int requestedSize = current.wirePageSize == 0 ? settings.pageSize : current.wirePageSize;
        if (!results.isArray()
                || page != current.page
                || results.size() > size
                || (target == USERS && size > requestedSize)
                || (pages < page && !(page == 1 && pages == 0 && total == 0))
                || (current.expectedPages >= 0
                        && (pages != current.expectedPages || total != current.expectedCount))) {
            throw failure("invalid or changing pagination metadata");
        }
        List<Row> rows = new ArrayList<>(results.size());
        Set<String> observed = new HashSet<>(current.identities);
        for (JsonNode record : results) {
            Row row = entity(record, null, target);
            if (!observed.add(AppfiguresSettings.digest(row.id))) {
                throw failure("provider repeated a record across pages");
            }
            if (target != USERS || acceptsLogin(record)) {
                rows.add(row);
            }
        }
        boolean last = pages <= page;
        if (observed.size() > total
                || (last && observed.size() != total)
                || (!last && size == 0 && current.wirePageSize == 0)) {
            throw failure("pagination ended or advanced without the declared record count");
        }
        next.expectedCount = total;
        next.expectedPages = pages;
        next.identities = Set.copyOf(observed);
        next.wirePageSize =
                current.wirePageSize == 0
                        ? (target == USERS ? requestedSize : size)
                        : current.wirePageSize;
        next.page = page + 1;
        next.done = last;
        return rows;
    }

    private int integer(JsonNode payload, String key, int min, int max) throws AppfiguresFailure {
        JsonNode value = payload.path(key);
        if (!value.isIntegralNumber()
                || !value.canConvertToInt()
                || value.intValue() < min
                || value.intValue() > max) {
            throw failure("invalid pagination field " + key);
        }
        return value.intValue();
    }

    private boolean acceptsLogin(JsonNode user) throws AppfiguresFailure {
        if (settings.userStart == null && settings.userEnd == null) {
            return true;
        }
        JsonNode value = user.path("last_login");
        if (value.isMissingNode() || value.isNull()) {
            return false;
        }
        try {
            LocalDateTime login = LocalDateTime.parse(value.asText());
            return (settings.userStart == null || !login.isBefore(settings.userStart))
                    && (settings.userEnd == null || login.isBefore(settings.userEnd));
        } catch (DateTimeException e) {
            throw failure("provider last_login is not a local date and time");
        }
    }

    private void report(JsonNode data, List<String> keys, List<Row> result)
            throws AppfiguresFailure {
        if (Thread.currentThread().isInterrupted()) {
            throw failure("report decoding interrupted");
        }
        if (!data.isObject()) {
            throw failure("invalid grouped report node");
        }
        if (data.isEmpty()) {
            return;
        }
        if (keys.size() == settings.groups.size()) {
            if (settings.resource.metrics.stream().noneMatch(data::has)) {
                throw failure("report result is missing its documented metric fields");
            }
            if (result.size() == settings.maxRows) {
                throw failure("report exceeds max_rows");
            }
            String id =
                    keys.isEmpty()
                            ? "total"
                            : AppfiguresSettings.digest(
                                    AppfiguresJson.write(keys, settings.resource));
            result.add(new Row(id, keys, data));
            return;
        }
        Iterator<Map.Entry<String, JsonNode>> fields = data.fields();
        while (fields.hasNext()) {
            Map.Entry<String, JsonNode> field = fields.next();
            if (field.getKey().isEmpty() || field.getKey().length() > 512) {
                throw failure("invalid grouped report key");
            }
            List<String> path = new ArrayList<>(keys);
            path.add(field.getKey());
            report(field.getValue(), List.copyOf(path), result);
        }
    }

    private AppfiguresFailure failure(String reason) {
        return new AppfiguresFailure(settings.resource, reason);
    }

    /**
     * One SeaTunnel-ready response and its state after the final row.
     *
     * <p>The intermediate checkpoint records the raw response digest and only the emitted offset.
     */
    static final class Response {
        final AppfiguresSplit next;
        final String digest;
        final List<SeaTunnelRow> rows;

        private Response(AppfiguresSplit next, String digest, List<SeaTunnelRow> rows) {
            this.next = next;
            this.digest = digest;
            this.rows = rows;
        }
    }

    /**
     * A provider record with its business identifier and any enclosing dictionary keys.
     *
     * <p>Keeping the path preserves unknown group keys without inventing dimension labels.
     */
    private static final class Row {
        private final String id;
        private final List<String> keys;
        private final JsonNode payload;

        private Row(String id, List<String> keys, JsonNode payload) {
            this.id = id;
            this.keys = keys;
            this.payload = payload;
        }
    }
}
