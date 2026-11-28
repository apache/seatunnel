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

package org.apache.seatunnel.connectors.seatunnel.ashby;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.io.IOException;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.seatunnel.connectors.seatunnel.ashby.AshbyResource.APPLICATIONS;
import static org.apache.seatunnel.connectors.seatunnel.ashby.AshbyResource.APPLICATION_CRITERIA_EVALUATIONS;
import static org.apache.seatunnel.connectors.seatunnel.ashby.AshbyResource.APPLICATION_HISTORY;
import static org.apache.seatunnel.connectors.seatunnel.ashby.AshbyResource.INTERVIEW_STAGES;

/**
 * Builds and validates one direct, discovery or child page.
 *
 * <p>Only result rows reach the collector. Cursor and synchronization token ownership follow the
 * method and parent used to obtain them.
 */
final class AshbyRead {
    private final AshbySettings settings;
    private final AshbyHttp http;
    private final String tableId;

    AshbyRead(AshbySettings settings, AshbyHttp http, String tableId) {
        this.settings = settings;
        this.http = http;
        this.tableId = tableId;
    }

    Response fetch(AshbySplit state, long deadline) throws IOException, InterruptedException {
        if (state.requests >= settings.maxRequests) {
            throw failure("max_requests_per_pass exhausted");
        }
        AshbySplit next = state.copy();
        next.requests++;
        next.readyAt = null;
        next.pageOffset = 0;
        next.pageDigest = null;

        AshbyResource parentResource = settings.resource.parent();
        boolean discover = parentResource != null && state.parents.isEmpty();
        boolean child = parentResource != null && !discover;
        AshbyResource target = discover ? parentResource : settings.resource;
        AshbySplit.Parent parent = child ? state.parents.get(state.parentIndex) : null;
        ObjectNode request = arguments(target, state, parent, discover);
        JsonNode body = http.post(target, request, deadline);
        String digest = AshbySettings.hash(AshbyJson.encode(body, target));
        if (state.pageDigest != null && !state.pageDigest.equals(digest)) {
            throw failure("provider page changed after checkpoint");
        }
        List<SeaTunnelRow> rows = new ArrayList<>();
        String continuation = continuation(body, target);
        if (continuation != null) {
            next.continuePage(continuation, child, target);
        }
        JsonNode entries = body.path("results");
        if (target.mode != AshbyResource.Mode.SINGLE && entries.size() > settings.pageSize) {
            throw new AshbyFailure(target, "provider page exceeded the requested record bound");
        }

        if (discover) {
            loadParents(body, entries, continuation, next);
        } else {
            Set<String> ids = new HashSet<>();
            for (JsonNode element : entries) {
                if (Thread.currentThread().isInterrupted()) {
                    throw failure("page decoding interrupted");
                }
                String id = id(element, target);
                if (!ids.add(id)) {
                    throw new AshbyFailure(
                            target, "provider repeated a record identifier within a page");
                }
                if (target != APPLICATIONS || inUpdateWindow(element)) {
                    rows.add(row(element, id, parent));
                }
            }
            if (continuation == null) {
                if (!child) {
                    next.finishPass(
                            settings,
                            target.mode == AshbyResource.Mode.TOKEN
                                    ? terminalToken(body, target)
                                    : null);
                } else {
                    next.finishChild();
                    if (next.parents.isEmpty() && next.finalParentPage) {
                        next.finishPass(settings, next.finalParentToken);
                    }
                }
            }
        }
        AshbyHttp.time(target, deadline);
        return new Response(next, digest, rows);
    }

    private ObjectNode arguments(
            AshbyResource target, AshbySplit state, AshbySplit.Parent parent, boolean discovery) {
        ObjectNode args = AshbyJson.JSON.createObjectNode();
        target.flags.forEach(name -> args.put(name, true));
        if (target.mode != AshbyResource.Mode.SINGLE) {
            args.put("limit", settings.pageSize);
            String cursor = parent == null ? state.cursor : state.childCursor;
            if (cursor != null) {
                args.put("cursor", cursor);
            }
        }
        if (parent != null) {
            args.put(
                    settings.resource == INTERVIEW_STAGES ? "interviewPlanId" : "applicationId",
                    parent.id);
        }
        if (target.mode == AshbyResource.Mode.TOKEN
                && state.syncToken != null
                && (!discovery || settings.resource == APPLICATION_HISTORY)) {
            args.put("syncToken", state.syncToken);
        }
        if (!discovery && target.createdFilter() && settings.start != null) {
            args.put("createdAfter", settings.start.toEpochMilli());
        }
        return args;
    }

    private void loadParents(JsonNode body, JsonNode entries, String cursor, AshbySplit next)
            throws AshbyFailure {
        AshbyResource parent = settings.resource.parent();
        List<AshbySplit.Parent> filtered = new ArrayList<>(entries.size());
        Set<String> ids = new HashSet<>();
        for (JsonNode entry : entries) {
            String id = id(entry, parent);
            if (!ids.add(id)) {
                throw new AshbyFailure(
                        parent, "provider repeated a parent identifier within a page");
            }
            if (settings.resource == INTERVIEW_STAGES) {
                JsonNode archived = entry.path("isArchived");
                if (!archived.isMissingNode() && !archived.isNull() && !archived.isBoolean()) {
                    throw failure("invalid interview plan archive flag");
                }
                filtered.add(
                        new AshbySplit.Parent(
                                id,
                                null,
                                null,
                                null,
                                archived.isBoolean() ? archived.booleanValue() : null));
            } else if (settings.resource != APPLICATION_HISTORY || inUpdateWindow(entry)) {
                filtered.add(
                        new AshbySplit.Parent(
                                id,
                                string(entry, "status"),
                                string(entry, "createdAt"),
                                string(entry, "updatedAt"),
                                null));
            }
        }
        next.parents = List.copyOf(filtered);
        next.parentIndex = 0;
        next.finalParentPage = cursor == null;
        if (next.finalParentPage) {
            next.finalParentToken =
                    settings.resource == APPLICATION_HISTORY ? terminalToken(body, parent) : null;
            if (filtered.isEmpty()) {
                next.finishPass(settings, next.finalParentToken);
            }
        }
    }

    private boolean inUpdateWindow(JsonNode row) throws AshbyFailure {
        if (settings.start == null || !row.hasNonNull("updatedAt")) {
            return true;
        }
        String text = string(row, "updatedAt");
        try {
            Instant updated = OffsetDateTime.parse(text).toInstant();
            return !updated.isBefore(settings.start);
        } catch (DateTimeException e) {
            throw failure("invalid provider updatedAt timestamp");
        }
    }

    private String continuation(JsonNode body, AshbyResource target) throws AshbyFailure {
        JsonNode more = body.path("moreDataAvailable");
        JsonNode cursor = body.path("nextCursor");
        boolean hasCursor =
                !cursor.isMissingNode()
                        && !cursor.isNull()
                        && !(cursor.isTextual() && cursor.textValue().isEmpty());
        if (target.mode == AshbyResource.Mode.SINGLE) {
            if ((!more.isMissingNode() && !more.isBoolean())
                    || more.asBoolean(false)
                    || hasCursor) {
                throw new AshbyFailure(
                        target, "provider returned a continuation for a single-response method");
            }
            return null;
        }
        if (!more.isBoolean()) {
            throw new AshbyFailure(target, "response is missing a valid moreDataAvailable flag");
        }
        if (!more.booleanValue()) {
            if (hasCursor) {
                throw new AshbyFailure(target, "terminal page contains a contradictory nextCursor");
            }
            return null;
        }
        if (!cursor.isTextual()
                || !AshbySettings.opaque(cursor.textValue())
                || "start".equals(cursor.textValue())) {
            throw new AshbyFailure(target, "response is missing a usable nextCursor");
        }
        return cursor.textValue();
    }

    private static String terminalToken(JsonNode body, AshbyResource target) throws AshbyFailure {
        JsonNode token = body.path("syncToken");
        if (!token.isTextual() || !AshbySettings.opaque(token.textValue())) {
            throw new AshbyFailure(target, "final page is missing a usable syncToken");
        }
        return token.textValue();
    }

    private static String id(JsonNode element, AshbyResource target) throws AshbyFailure {
        JsonNode key = element.path("id");
        if (!element.isObject()
                || !key.isTextual()
                || !AshbySettings.opaque(key.textValue())
                || key.textValue().length() > 128) {
            throw new AshbyFailure(target, "provider record is missing a usable id");
        }
        return key.textValue();
    }

    private String string(JsonNode value, String field) throws AshbyFailure {
        JsonNode node = value.path(field);
        if (node.isMissingNode() || node.isNull()) {
            return null;
        }
        if (!node.isTextual()
                || !AshbySettings.opaque(node.textValue())
                || node.textValue().length() > 128) {
            throw failure("unexpected application field " + field);
        }
        return node.textValue();
    }

    private SeaTunnelRow row(JsonNode data, String id, AshbySplit.Parent parent)
            throws AshbyFailure {
        String lineage = null;
        ObjectNode payload = (ObjectNode) data;
        if (parent != null) {
            lineage = parent.id;
            payload = payload.deepCopy();
            if (settings.resource == INTERVIEW_STAGES) {
                requireParent(payload, "interviewPlanId", parent.id);
                payload.put("interview_plan_is_archived", parent.archived);
            } else {
                requireParent(payload, "applicationId", parent.id);
                payload.put("application_id", parent.id);
                if (settings.resource == APPLICATION_HISTORY) {
                    payload.put("application_status", parent.status);
                    payload.put("application_created_at", parent.created);
                    payload.put("application_updated_at", parent.updated);
                } else if (settings.resource == APPLICATION_CRITERIA_EVALUATIONS) {
                    id = parent.id + "/" + id;
                }
            }
        }
        SeaTunnelRow row =
                new SeaTunnelRow(
                        new Object[] {
                            settings.resource.key,
                            id,
                            lineage,
                            AshbyJson.encode(payload, settings.resource)
                        });
        row.setTableId(tableId);
        return row;
    }

    private void requireParent(ObjectNode record, String field, String parent) throws AshbyFailure {
        if (record.hasNonNull(field) && !parent.equals(record.path(field).asText())) {
            throw failure("provider child belongs to a different parent");
        }
    }

    private AshbyFailure failure(String reason) {
        return new AshbyFailure(settings.resource, reason);
    }

    /**
     * Validated rows and the source state after the response has reached the collector.
     *
     * <p>The reader owns this transition so a saved row offset cannot advance the sync token.
     */
    static final class Response {
        final AshbySplit next;
        final String digest;
        final List<SeaTunnelRow> rows;

        private Response(AshbySplit next, String digest, List<SeaTunnelRow> rows) {
            this.next = next;
            this.digest = digest;
            this.rows = rows;
        }
    }
}
