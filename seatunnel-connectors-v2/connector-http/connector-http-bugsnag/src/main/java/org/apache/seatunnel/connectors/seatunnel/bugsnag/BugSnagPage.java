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
package org.apache.seatunnel.connectors.seatunnel.bugsnag;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonParser;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.StreamReadConstraints;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Validates BugSnag arrays and usage summaries before publication or child discovery.
 *
 * <p>The page proof covers all raw records and the normalized continuation, including rows outside
 * the selected output window.
 */
final class BugSnagPage {
    private static final int MAX_UNSIZED_RECORDS = 10000;
    private static final ObjectMapper JSON =
            new ObjectMapper(
                            JsonFactory.builder()
                                    .streamReadConstraints(
                                            StreamReadConstraints.builder()
                                                    .maxNestingDepth(80)
                                                    .maxNumberLength(256)
                                                    .build())
                                    .build())
                    .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
                    .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
                    .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                    .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));

    final List<SeaTunnelRow> rows;
    final BugSnagSplit next;
    final String fingerprint;

    private BugSnagPage(List<SeaTunnelRow> rows, BugSnagSplit next, String fingerprint) {
        this.rows = rows;
        this.next = next;
        this.fingerprint = fingerprint;
    }

    /**
     * Computes the next state from one complete response.
     *
     * <p>Page descendants become eligible only after every output row has been published.
     */
    static BugSnagPage read(
            BugSnagTransport client,
            BugSnagConfig config,
            BugSnagSplit state,
            String table,
            long deadline)
            throws BugSnagReadException, InterruptedException {
        BugSnagWork work = state.work.get(0);
        BugSnagTransport.ResponsePage response = client.get(work, state.end, deadline);
        JsonNode data = parse(response.body, work);
        List<JsonNode> records = records(data, work, config.pageSize);
        List<SeaTunnelRow> rows = new ArrayList<>();
        List<BugSnagWork> next = new ArrayList<>();
        List<String> identities = new ArrayList<>(records.size());
        Set<String> seen = new HashSet<>(work.previousIds);
        MessageDigest digest = BugSnagConfig.sha256();

        for (JsonNode record : records) {
            String id = identity(record, work);
            String identifier = BugSnagConfig.digest(id);
            if (!seen.add(identifier)) {
                throw work.failure("duplicate identifier within or across adjacent pages");
            }
            String payload = payload(record, work);
            hashField(digest, id);
            hashField(digest, payload);
            identities.add(identifier);
            descendants(next, config, work, id);
            if (config.resources.contains(work.resource)) {
                Instant timestamp = time(record, work);
                if (timestamp == null
                        || !timestamp.isBefore(config.start) && timestamp.isBefore(state.end)) {
                    rows.add(row(work, id, timestamp, payload, table));
                }
            }
        }
        BugSnagWork continuation = BugSnagLinks.next(response, work, config, state.end, identities);
        if (continuation != null) {
            next.add(continuation);
            hashField(digest, continuation.offset);
            hashField(digest, continuation.base == null ? "" : continuation.base);
        }
        String fingerprint =
                Base64.getUrlEncoder().withoutPadding().encodeToString(digest.digest());
        if (state.published < 0
                || state.published > rows.size()
                || state.published > 0 && !fingerprint.equals(state.fingerprint)) {
            throw work.failure(
                    "provider page changed since checkpoint or has an invalid row offset");
        }
        BugSnagTransport.checkDeadline(work, deadline);
        return new BugSnagPage(rows, state.complete(next), fingerprint);
    }

    private static JsonNode parse(byte[] bytes, BugSnagWork work) throws BugSnagReadException {
        try {
            String text =
                    StandardCharsets.UTF_8.newDecoder().decode(ByteBuffer.wrap(bytes)).toString();
            return JSON.readTree(text);
        } catch (IOException | RuntimeException invalid) {
            throw work.failure("expected a single valid UTF-8 JSON response");
        }
    }

    private static List<JsonNode> records(JsonNode data, BugSnagWork work, int configured)
            throws BugSnagReadException {
        if (work.resource == BugSnagResource.USAGE_SUMMARY) {
            if (data == null
                    || !data.isObject()
                    || !nonNegative(data.path("project_notifications_count"))
                    || !nonNegative(data.path("collaborator_email_notifications_count"))
                    || !data.path("current_user_using_for_email_notification").isBoolean()
                    || data.has("performance_monitor_count")
                            && !nonNegative(data.path("performance_monitor_count"))) {
                throw work.failure("expected a saved search usage summary object");
            }
            return List.of(data);
        }
        int size = work.resource.pageSize(configured);
        int limit = size == 0 ? MAX_UNSIZED_RECORDS : size;
        if (data == null || !data.isArray() || data.size() > limit) {
            throw work.failure("expected a JSON array within the response record limit");
        }
        List<JsonNode> records = new ArrayList<>(data.size());
        data.forEach(records::add);
        return records;
    }

    private static boolean nonNegative(JsonNode field) {
        return field.isNumber() && field.decimalValue().signum() >= 0;
    }

    private static String identity(JsonNode data, BugSnagWork work) throws BugSnagReadException {
        if (!data.isObject()) {
            throw work.failure("expected each BugSnag record to be a JSON object");
        }
        String id =
                work.resource.key == null ? work.search : data.path(work.resource.key).textValue();
        if (id == null
                || id.isBlank()
                || id.length() > 256
                || id.codePoints()
                        .anyMatch(
                                c ->
                                        Character.isISOControl(c)
                                                || c >= Character.MIN_SURROGATE
                                                        && c <= Character.MAX_SURROGATE)) {
            throw work.failure("expected a valid textual provider identifier");
        }
        if (work.organization != null
                        && data.hasNonNull("organization_id")
                        && !work.organization.equals(data.path("organization_id").textValue())
                || work.project != null
                        && data.hasNonNull("project_id")
                        && !work.project.equals(data.path("project_id").textValue())) {
            throw work.failure("record does not belong to the requested parent");
        }
        return id;
    }

    private static String payload(JsonNode data, BugSnagWork work) throws BugSnagReadException {
        try {
            return JSON.writeValueAsString(data);
        } catch (IOException | RuntimeException invalid) {
            throw work.failure("could not encode the provider record as JSON");
        }
    }

    private static void descendants(
            List<BugSnagWork> pending, BugSnagConfig config, BugSnagWork work, String id)
            throws BugSnagReadException {
        switch (work.resource) {
            case ORGANIZATIONS:
                boolean projects =
                        config.resources.stream()
                                .anyMatch(
                                        resource ->
                                                resource == BugSnagResource.PROJECTS
                                                        || resource.scope
                                                                == BugSnagResource.Scope.PROJECT
                                                        || resource.scope
                                                                == BugSnagResource.Scope.SEARCH);
                if (projects
                        || config.resources.contains(BugSnagResource.COLLABORATORS)
                        || config.resources.contains(BugSnagResource.TEAMS)) {
                    requireRouteIdentifier(id, work);
                }
                if (projects) {
                    pending.add(new BugSnagWork(BugSnagResource.PROJECTS, id, null, null));
                }
                addSelected(pending, config, BugSnagResource.COLLABORATORS, id, null, null);
                addSelected(pending, config, BugSnagResource.TEAMS, id, null, null);
                break;
            case PROJECTS:
                boolean searches = config.resources.contains(BugSnagResource.USAGE_SUMMARY);
                if (searches
                        || config.resources.stream()
                                .anyMatch(
                                        resource ->
                                                resource.scope == BugSnagResource.Scope.PROJECT)) {
                    requireRouteIdentifier(id, work);
                }
                if (searches && !config.resources.contains(BugSnagResource.SAVED_SEARCHES)) {
                    pending.add(
                            new BugSnagWork(
                                    BugSnagResource.SAVED_SEARCHES, work.organization, id, null));
                }
                for (BugSnagResource resource : config.resources) {
                    if (resource.scope == BugSnagResource.Scope.PROJECT) {
                        pending.add(new BugSnagWork(resource, work.organization, id, null));
                    }
                }
                break;
            case SAVED_SEARCHES:
                if (config.resources.contains(BugSnagResource.USAGE_SUMMARY)) {
                    requireRouteIdentifier(id, work);
                    pending.add(
                            new BugSnagWork(
                                    BugSnagResource.USAGE_SUMMARY,
                                    work.organization,
                                    work.project,
                                    id));
                }
                break;
            default:
                // Leaf records do not produce any further requests.
                break;
        }
    }

    private static void addSelected(
            List<BugSnagWork> pending,
            BugSnagConfig config,
            BugSnagResource resource,
            String organization,
            String project,
            String search) {
        if (config.resources.contains(resource)) {
            pending.add(new BugSnagWork(resource, organization, project, search));
        }
    }

    private static void requireRouteIdentifier(String id, BugSnagWork work)
            throws BugSnagReadException {
        if (!id.matches("[A-Za-z0-9_.-]{1,128}") || ".".equals(id) || "..".equals(id)) {
            throw work.failure("provider parent identifier is unsafe in an API path segment");
        }
    }

    private static Instant time(JsonNode record, BugSnagWork work) throws BugSnagReadException {
        if (work.resource.time == null) {
            return null;
        }
        String value = record.path(work.resource.time).textValue();
        try {
            if (value != null && value.length() <= 48) {
                return OffsetDateTime.parse(value).toInstant();
            }
        } catch (DateTimeException invalid) {
            // Error diagnostics must not include the untrusted timestamp.
        }
        throw work.failure("record has no valid " + work.resource.time + " timestamp");
    }

    private static SeaTunnelRow row(
            BugSnagWork work, String id, Instant time, String payload, String table) {
        String organization =
                work.resource == BugSnagResource.ORGANIZATIONS ? id : work.organization;
        String project = work.resource == BugSnagResource.PROJECTS ? id : work.project;
        String search = work.resource == BugSnagResource.SAVED_SEARCHES ? id : work.search;
        SeaTunnelRow row =
                new SeaTunnelRow(
                        new Object[] {
                            work.resource.id,
                            id,
                            organization,
                            project,
                            search,
                            time == null ? null : time.toString(),
                            payload
                        });
        row.setTableId(table);
        return row;
    }

    private static void hashField(MessageDigest digest, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        int length = bytes.length;
        digest.update((byte) (length >>> 24));
        digest.update((byte) (length >>> 16));
        digest.update((byte) (length >>> 8));
        digest.update((byte) length);
        digest.update(bytes);
    }
}
