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
package org.apache.seatunnel.connectors.seatunnel.buildkite;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.connectors.seatunnel.buildkite.BuildkiteHttp.Reply;
import org.apache.seatunnel.connectors.seatunnel.buildkite.BuildkiteResource.Shape;
import org.apache.seatunnel.connectors.seatunnel.buildkite.BuildkiteSplit.Frame;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.DateTimeException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Reads documented Buildkite list and singleton JSON routes.
 *
 * <p>Native responses supply only validated identifiers and continuation numbers. The source
 * constructs the target and keeps the original filters across every request.
 */
final class BuildkiteApi {
    private static final int SUITE_PAGE_SIZE = 30;
    private final BuildkiteConfig config;
    private final BuildkiteHttp http;

    BuildkiteApi(BuildkiteConfig config, BuildkiteHttp http) {
        this.config = config;
        this.http = http;
    }

    Page list(Frame frame, Instant end, long deadline) throws InterruptedException {
        BuildkiteResource resource = frame.resource;
        String path = resource.path(frame.org, frame.partition);
        Map<String, String> query = new LinkedHashMap<>();
        Map<String, String> invariant = new LinkedHashMap<>();
        if (resource.shape == Shape.PAGED) {
            query.put("page", Integer.toString(frame.page));
            invariant.put("per_page", Integer.toString(config.pageSize));
        } else if (resource.shape == Shape.SUITES && frame.page > 1) {
            query.put("page", Integer.toString(frame.page));
        }
        if (resource.wireDates) {
            invariant.put("created_from", config.start.toString());
            invariant.put("created_to", end.toString());
        }
        query.putAll(invariant);
        if (resource.shape == Shape.SUITES) {
            // Keep the API default when a deployment exposes organization suite pagination.
            invariant.put("per_page", Integer.toString(SUITE_PAGE_SIZE));
        }
        Reply reply = http.request(resource, target(path, query), deadline);
        int next = BuildkiteLink.next(resource, path, invariant, frame.page, reply.links);

        List<Row> rows;
        if (resource.shape == Shape.SINGLE) {
            if (next != 0 || !reply.json.isObject()) {
                throw resource.failure("expected one metadata object without pagination");
            }
            rows = List.of(row(resource, reply.json));
        } else {
            if (!reply.json.isArray()) {
                throw resource.failure("expected a JSON array");
            }
            if (resource.shape == Shape.ARRAY && next != 0) {
                throw resource.failure("unexpected next Link for a full-list response");
            }
            int cap = config.unpagedItems;
            if (resource.shape == Shape.PAGED) {
                cap = config.pageSize;
            } else if (resource.shape == Shape.SUITES && (frame.page > 1 || next != 0)) {
                cap = Math.min(SUITE_PAGE_SIZE, config.unpagedItems);
            }
            if (reply.json.size() > cap) {
                throw resource.failure("response exceeds the collection item limit");
            }
            rows = new ArrayList<>(reply.json.size());
            for (JsonNode node : reply.json) {
                http.remaining(resource, deadline);
                rows.add(row(resource, node));
            }
        }
        String digest = BuildkiteConfig.hash(reply.json.toString() + "\n" + next);
        http.remaining(resource, deadline);
        return new Page(rows, next, digest);
    }

    private Row row(BuildkiteResource resource, JsonNode node) {
        if (!node.isObject()) {
            throw resource.failure("expected an object for a record");
        }
        String id = required(resource, node, resource.identity());
        String key = null;
        if (resource == BuildkiteResource.ORGANIZATIONS
                || resource == BuildkiteResource.PIPELINES) {
            key = required(resource, node, "slug");
        } else if (resource == BuildkiteResource.CLUSTERS) {
            key = id;
        }
        if (key != null && !BuildkiteResource.pathKey(key)) {
            throw resource.failure("invalid parent path identifier");
        }
        Instant created = null;
        if (resource.dated && config.emits(resource)) {
            try {
                created = BuildkiteConfig.providerTimestamp(required(resource, node, "created_at"));
            } catch (DateTimeException ex) {
                throw resource.failure("invalid created_at timestamp");
            }
        }
        if (resource.hasCredentialMetadata()) {
            ObjectNode description = ((ObjectNode) node).deepCopy();
            description.remove(List.of("token", "api_token", "token_value"));
            return new Row(id, key, created, description.toString());
        }
        return new Row(id, key, created, node.toString());
    }

    private static String required(BuildkiteResource resource, JsonNode node, String field) {
        JsonNode value = node.get(field);
        if (value == null
                || !value.isTextual()
                || value.textValue().isBlank()
                || value.textValue().length() > 4096
                || value.textValue().codePoints().anyMatch(Character::isISOControl)) {
            throw resource.failure("missing or invalid " + field);
        }
        return value.textValue();
    }

    private static String target(String path, Map<String, String> query) {
        StringBuilder result = new StringBuilder(path);
        String delimiter = "?";
        for (Map.Entry<String, String> item : query.entrySet()) {
            result.append(delimiter)
                    .append(item.getKey())
                    .append('=')
                    .append(URLEncoder.encode(item.getValue(), StandardCharsets.UTF_8));
            delimiter = "&";
        }
        return result.toString();
    }

    /**
     * Retains a validated response for the lifetime of its active frame.
     *
     * <p>The digest covers its parsed records and validated next page, including filtered rows.
     */
    static final class Page {
        final List<Row> rows;
        final int next;
        final String digest;

        Page(List<Row> rows, int next, String digest) {
            this.rows = rows;
            this.next = next;
            this.digest = digest;
        }
    }

    /**
     * Projects one record without losing its precise JSON values.
     *
     * <p>The optional path key is present only for records that can be parents.
     */
    static final class Row {
        final String id;
        final String key;
        final Instant created;
        final String record;

        Row(String id, String key, Instant created, String record) {
            this.id = id;
            this.key = key;
            this.created = created;
            this.record = record;
        }
    }
}
