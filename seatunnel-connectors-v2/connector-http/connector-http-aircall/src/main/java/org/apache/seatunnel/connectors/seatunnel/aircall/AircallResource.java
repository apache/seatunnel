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
package org.apache.seatunnel.connectors.seatunnel.aircall;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.DateTimeException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

enum AircallResource {
    CALLS("calls", "calls", Kind.PAGE),
    COMPANY("company", "company", Kind.OBJECT),
    CONTACTS("contacts", "contacts", Kind.PAGE),
    NUMBERS("numbers", "numbers", Kind.PAGE),
    TAGS("tags", "tags", Kind.PAGE),
    USER_AVAILABILITY("users/availabilities", "users", Kind.PAGE),
    USERS("users", "users", Kind.PAGE),
    TEAMS("teams", "teams", Kind.PAGE),
    WEBHOOKS("webhooks", "webhooks", Kind.PAGE),
    CONVERSATIONS("conversations", "conversations", Kind.PAGE),
    CONVERSATION_ENGAGEMENTS(
            "conversations/{id}/engagements", "engagements", Kind.PAGE, CONVERSATIONS),
    CAMPAIGNS("campaigns", "campaigns", Kind.CURSOR),
    CAMPAIGN_OUTCOMES("campaign_outcomes", "campaign_outcomes", Kind.CURSOR),
    TRACKERS("trackers", "trackers", Kind.ARRAY),
    SMS_TEMPLATES("sms/templates", "templates", Kind.PAGE),
    WHATSAPP_TEMPLATES("numbers/{id}/templates", "templates", Kind.WHATSAPP, NUMBERS),
    USER_NUMBERS("users/{id}/numbers", "numbers", Kind.PAGE, USERS),
    DIALER_CAMPAIGNS("users/{id}/dialer_campaign", null, Kind.OBJECT, USERS),
    DIALER_CAMPAIGN_PHONE_NUMBERS(
            "users/{id}/dialer_campaign/phone_numbers", "numbers", Kind.ARRAY, USERS),
    CALL_TRANSCRIPTIONS("calls/{id}/transcription", "transcription", Kind.OBJECT, CALLS),
    CALL_SENTIMENTS("calls/{id}/sentiments", "sentiment", Kind.OBJECT, CALLS),
    CALL_TOPICS("calls/{id}/topics", "topic", Kind.OBJECT, CALLS),
    CALL_SUMMARIES("calls/{id}/summary", "summary", Kind.OBJECT, CALLS),
    CALL_ACTION_ITEMS("calls/{id}/action_items", "action_items", Kind.ARRAY, CALLS),
    CALL_PLAYBOOK_RESULTS("calls/{id}/playbook_result", null, Kind.OBJECT, CALLS);

    static final int QUERY_CAP = 10000;
    static final int MAX_CURSOR = 2048;
    final Kind kind;
    final AircallResource parent;
    final boolean intelligence;
    private final String route;
    private final String responseKey;

    AircallResource(String route, String responseKey, Kind kind) {
        this(route, responseKey, kind, null);
    }

    AircallResource(String route, String responseKey, Kind kind, AircallResource parent) {
        this.route = route;
        this.responseKey = responseKey;
        this.kind = kind;
        this.parent = parent;
        // Only the six opt-in resources traverse the calls collection.
        this.intelligence = "calls".equals(parent == null ? null : parent.route);
    }

    String label() {
        return name().toLowerCase(Locale.ROOT);
    }

    boolean windowed() {
        return this == CALLS || this == CONTACTS || this == CONVERSATIONS;
    }

    boolean splittable() {
        return windowed() || this == USERS || this == NUMBERS || this == USER_AVAILABILITY;
    }

    int pageLimit(AircallConfig config) {
        return this == SMS_TEMPLATES ? config.smsLimit : config.pageSize;
    }

    Request request(
            AircallConfig config, AircallSplit state, String parentId, AircallSplit.Cursor cursor) {
        String target = "/v1/" + route;
        if (parent != null) {
            if (parentId == null || !parentId.matches("[1-9][0-9]{0,63}")) {
                throw failure("invalid parent identity");
            }
            target = target.replace("{id}", parentId);
        }
        Map<String, String> query = new LinkedHashMap<>();
        if (kind == Kind.PAGE) {
            query.put("page", Integer.toString(cursor.page));
            query.put("per_page", Integer.toString(pageLimit(config)));
        } else if (kind == Kind.CURSOR) {
            query.put("per_page", Integer.toString(config.pageSize));
            if (cursor.token != null) {
                query.put("cursor", cursor.token);
            }
        } else if (kind == Kind.WHATSAPP && cursor.token != null) {
            query.put("token", cursor.token);
        }
        if (parent == null && state.from != 0 && splittable()) {
            query.put("from", Long.toString(state.from));
            // Conversations expose fractional last-engagement times. Keep the boundary
            // in acquisition and assign it to the next window with the returned timestamp.
            long upper = this == CONVERSATIONS ? state.until : state.until - 1;
            query.put("to", Long.toString(upper));
            query.put("order", "asc");
        }
        if (this == USERS
                || this == NUMBERS
                || this == USER_AVAILABILITY
                || this == CONVERSATION_ENGAGEMENTS) {
            query.put("order", "asc");
        }
        if (this == CALLS) {
            query.put("fetch_call_timeline", "true");
        } else if (this == CALL_PLAYBOOK_RESULTS) {
            query.put("fetch_playbook", "true");
        }
        return new Request(target, query);
    }

    List<JsonNode> records(JsonNode root, AircallConfig config) {
        if (!root.isObject()) {
            throw failure("expected a JSON response object");
        }
        JsonNode value = responseKey == null ? root : root.get(responseKey);
        if (value == null) {
            throw failure("missing resource response field");
        }
        if (intelligence && value.isNull()) {
            return Collections.emptyList();
        }
        if (kind == Kind.OBJECT) {
            if (!value.isObject()) {
                throw failure("invalid resource object");
            }
            return Collections.singletonList(value);
        }
        int limit =
                kind == Kind.PAGE || kind == Kind.CURSOR
                        ? pageLimit(config)
                        : kind == Kind.WHATSAPP ? 1000 : QUERY_CAP;
        if (!value.isArray() || value.size() > limit) {
            throw failure("invalid or oversized resource array");
        }
        List<JsonNode> rows = new ArrayList<>(value.size());
        value.forEach(rows::add);
        return rows;
    }

    String identity(JsonNode row) {
        if (!row.isObject()) {
            throw failure("invalid resource record");
        }
        String key = this == WEBHOOKS ? "webhook_id" : "id";
        JsonNode id = row.get(key);
        if (this == COMPANY && id == null) {
            return "company";
        }
        if (id == null
                || !(id.isIntegralNumber() || id.isTextual())
                || id.asText().isBlank()
                || id.asText().length() > 512) {
            throw failure("missing or invalid resource identity");
        }
        return id.asText();
    }

    Long eventTime(JsonNode row) {
        String field;
        switch (this) {
            case CALLS:
                field = "started_at";
                break;
            case CONVERSATIONS:
                field = "last_engagement_at";
                break;
            default:
                return null;
        }
        JsonNode value = row.get(field);
        if (value != null) {
            if (value.isIntegralNumber() && value.canConvertToLong()) {
                return value.longValue();
            }
            if (value.isTextual()) {
                try {
                    return Instant.parse(value.textValue()).getEpochSecond();
                } catch (DateTimeException ex) {
                    throw failure("invalid timestamp");
                }
            }
        }
        throw failure("missing timestamp");
    }

    boolean knownAbsent(int status, JsonNode error) {
        if (this == WHATSAPP_TEMPLATES && status == 403 && error.isObject()) {
            String detail =
                    (error.path("message").asText() + " " + error.path("Forbidden").asText())
                            .toLowerCase(Locale.ROOT);
            return detail.contains("line is not configured to use whatsapp")
                    || detail.startsWith(" line ")
                            && detail.endsWith("doesn't have whatsapp capability")
                    || detail.equals("line does not belong to the company ");
        }
        if (status != 404 || !error.isObject()) {
            return false;
        }
        if (this == DIALER_CAMPAIGNS || this == DIALER_CAMPAIGN_PHONE_NUMBERS) {
            return "NOT_FOUND".equals(error.path("message").asText())
                    || "User has no active campaign".equals(error.path("message").asText());
        }
        if (this == CONVERSATION_ENGAGEMENTS) {
            return error.path("troubleshoot")
                    .asText("")
                    .toLowerCase(Locale.ROOT)
                    .startsWith("no conversation exists");
        }
        if (!intelligence || !"NotFound".equals(error.path("error").asText())) {
            return false;
        }
        String detail = error.path("troubleshoot").asText("").toLowerCase(Locale.ROOT);
        String term =
                this == CALL_PLAYBOOK_RESULTS
                        ? "playbook"
                        : this == CALL_ACTION_ITEMS ? "action item" : responseKey;
        return detail.contains(term) && detail.contains("not found");
    }

    Failure failure(String detail) {
        return new Failure("Aircall " + label() + " " + detail);
    }

    enum Kind {
        PAGE,
        CURSOR,
        WHATSAPP,
        ARRAY,
        OBJECT
    }

    static final class Request {
        final String path;
        final Map<String, String> params;

        private Request(String path, Map<String, String> params) {
            this.path = path;
            this.params = params;
        }

        String target() {
            if (params.isEmpty()) {
                return path;
            }
            return path
                    + "?"
                    + params.entrySet().stream()
                            .map(entry -> escape(entry.getKey()) + "=" + escape(entry.getValue()))
                            .collect(Collectors.joining("&"));
        }

        private static String escape(String value) {
            return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
        }
    }

    static final class Failure extends IllegalStateException {
        private static final long serialVersionUID = 1L;

        private Failure(String message) {
            super(message);
        }
    }
}
