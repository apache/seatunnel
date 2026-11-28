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

package org.apache.seatunnel.connectors.seatunnel.calendly;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import org.apache.seatunnel.connectors.seatunnel.calendly.CalendlyJson.Failure;
import org.apache.seatunnel.connectors.seatunnel.calendly.CalendlyJson.Reference;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Maps the supported Calendly resources to fixed API paths and request scopes.
 *
 * <p>Parent records contribute validated identifiers or URI query values. They cannot change the
 * origin, endpoint family, or credentials attached to the resulting request.
 */
enum CalendlyResource {
    API_USER("/users/me", null, null, false, null),
    EVENT_TYPES("/event_types", "organization", null, true, "updated_at"),
    GROUPS("/groups", "organization", null, true, null),
    ORGANIZATION_MEMBERSHIPS("/organization_memberships", "organization", null, true, null),
    SCHEDULED_EVENTS("/scheduled_events", "organization", null, true, "updated_at"),
    EVENT_INVITEES("/scheduled_events/{id}/invitees", null, SCHEDULED_EVENTS, true, "created_at"),
    TEAMS("/teams", null, null, true, null),
    EVENT_TYPE_MEMBERSHIPS("/event_type_memberships", "event_type", EVENT_TYPES, true, null),
    EVENT_TYPE_AVAILABILITY_SCHEDULES(
            "/event_type_availability_schedules", "event_type", EVENT_TYPES, false, null),
    GROUP_RELATIONSHIPS("/group_relationships", "group", GROUPS, true, null),
    USER_AVAILABILITY_SCHEDULES(
            "/user_availability_schedules", "user", ORGANIZATION_MEMBERSHIPS, false, null),
    LOCATIONS("/locations", "user", ORGANIZATION_MEMBERSHIPS, false, null),
    ORGANIZATION_INVITATIONS("/organizations/{id}/invitations", null, null, true, null),
    WEBHOOK_SUBSCRIPTIONS("/webhook_subscriptions", "organization", null, true, null),
    ROUTING_FORMS("/routing_forms", "organization", null, true, null),
    ROUTING_FORM_SUBMISSIONS("/routing_form_submissions", "form", ROUTING_FORMS, true, null),
    CONTACTS("/contacts", null, null, true, null),
    OUTGOING_COMMUNICATIONS("/outgoing_communications", "organization", null, true, "created_at"),
    ACTIVITY_LOG_ENTRIES("/activity_log_entries", "organization", null, true, "occurred_at"),
    MEETING_RECAPS("/meeting_recaps", null, null, true, "start_time");

    final String id = name().toLowerCase(Locale.ROOT);
    final String path;
    final String scopeParameter;
    final CalendlyResource parent;
    final boolean counted;
    final String cursorField;

    CalendlyResource(
            String path,
            String scopeParameter,
            CalendlyResource parent,
            boolean counted,
            String cursorField) {
        this.path = path;
        this.scopeParameter = scopeParameter;
        this.parent = parent;
        this.counted = counted;
        this.cursorField = cursorField;
    }

    static CalendlyResource parse(String value) {
        for (CalendlyResource candidate : values()) {
            if (candidate.id.equals(value)) {
                return candidate;
            }
        }
        throw CalendlyConfig.invalid("resource must be a documented Calendly resource");
    }

    boolean pageable() {
        return counted || this == EVENT_TYPE_AVAILABILITY_SCHEDULES;
    }

    Reference directScope(Reference organization) {
        return "organization".equals(scopeParameter) || this == ORGANIZATION_INVITATIONS
                ? organization
                : null;
    }

    /**
     * Reads the verified reference used by a hidden parent row.
     *
     * @param row the parent record
     * @return the fixed-family reference to use in the child request
     * @throws Failure if the parent does not identify a canonical resource
     */
    Reference childScope(JsonNode row) throws Failure {
        switch (this) {
            case EVENT_TYPES:
                return CalendlyJson.reference(row.get("uri"), "/event_types");
            case GROUPS:
                return CalendlyJson.reference(row.get("uri"), "/groups");
            case SCHEDULED_EVENTS:
                return CalendlyJson.reference(row.get("uri"), "/scheduled_events");
            case ORGANIZATION_MEMBERSHIPS:
                return CalendlyJson.reference(row.path("user").get("uri"), "/users");
            case ROUTING_FORMS:
                return CalendlyJson.reference(row.get("uri"), "/routing_forms");
            default:
                throw CalendlyJson.failure("invalid resource parent");
        }
    }

    /**
     * Builds the origin-form request target and sends only documented query fields.
     *
     * <p>Hidden scheduled events are intentionally unfiltered by start time. An older event can
     * hold an invitee created inside the requested date window.
     */
    String target(
            CalendlyConfig config,
            Reference scope,
            String cursor,
            Instant ceiling,
            boolean hidden) {
        String route = path.contains("{id}") ? path.replace("{id}", scope.id) : path;
        Map<String, String> query = new LinkedHashMap<>();
        if (counted) {
            query.put("count", Integer.toString(config.pageSize));
        }
        if (scopeParameter != null) {
            query.put(scopeParameter, scope.uri);
        }
        switch (this) {
            case EVENT_TYPES:
                query.put("sort", "updated_at:desc");
                break;
            case SCHEDULED_EVENTS:
                query.put("sort", "start_time:desc");
                if (!hidden) {
                    query.put("min_start_time", config.start.toString());
                }
                break;
            case EVENT_INVITEES:
                query.put("sort", "created_at:desc");
                break;
            case ORGANIZATION_INVITATIONS:
            case ROUTING_FORM_SUBMISSIONS:
                query.put("sort", "created_at:asc");
                break;
            case WEBHOOK_SUBSCRIPTIONS:
                query.put("scope", "organization");
                break;
            case OUTGOING_COMMUNICATIONS:
                addWindow(query, "min_created_at", "max_created_at", config.lower, ceiling);
                break;
            case ACTIVITY_LOG_ENTRIES:
                query.put("sort", "occurred_at:asc");
                addWindow(query, "min_occurred_at", "max_occurred_at", config.lower, ceiling);
                break;
            case MEETING_RECAPS:
                addWindow(query, "start_time", "end_time", config.lower, ceiling);
                break;
            default:
                break;
        }
        if (cursor != null) {
            query.put("page_token", cursor);
        }
        return query.isEmpty()
                ? route
                : route
                        + "?"
                        + query.entrySet().stream()
                                .map(
                                        e ->
                                                e.getKey()
                                                        + "="
                                                        + URLEncoder.encode(
                                                                e.getValue(),
                                                                StandardCharsets.UTF_8))
                                .collect(Collectors.joining("&"));
    }

    private static void addWindow(
            Map<String, String> query, String min, String max, Instant lower, Instant ceiling) {
        query.put(min, lower.toString());
        query.put(max, ceiling.toString());
    }
}
