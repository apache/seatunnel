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
package org.apache.seatunnel.connectors.seatunnel.appstoreconnect;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.Serializable;
import java.net.URI;
import java.net.URLDecoder;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.YearMonth;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

import static org.apache.seatunnel.connectors.seatunnel.appstoreconnect.AppStoreConnectResource.Stage;

/**
 * Implements Apple's JSON:API relationships and gzip report request parameters.
 *
 * <p>Server-supplied pagination may move only within the same collection and filter scope. Signed
 * report URLs are validated separately and stripped from exposed metadata.
 */
final class AppStoreConnectApi {
    private final AppStoreConnectConfig config;
    private final AppStoreConnectHttp http;

    AppStoreConnectApi(AppStoreConnectConfig config, AppStoreConnectHttp http) {
        this.config = config;
        this.http = http;
    }

    /**
     * Resolves the current collection using stable parent IDs, never relationship URLs from API
     * data. Each parent has its own cursor chain.
     */
    Page collection(AppStoreConnectResource resource, Stage stage, Parents parents, String cursor)
            throws InterruptedException {
        String path;
        String type;
        Map<String, String> filters = new LinkedHashMap<>();
        switch (stage) {
            case APPS:
                path = "/v1/apps";
                type = "apps";
                break;
            case REVIEWS:
                path = "/v1/apps/" + parents.app + "/customerReviews";
                type = "customerReviews";
                filters.put("sort", "-createdDate");
                break;
            case REQUESTS:
                path = "/v1/apps/" + parents.app + "/analyticsReportRequests";
                type = "analyticsReportRequests";
                filters.put("filter[accessType]", resource.access());
                break;
            case REPORTS:
                path = "/v1/analyticsReportRequests/" + parents.request + "/reports";
                type = "analyticsReports";
                filters.put("filter[name]", resource.family.title);
                break;
            case INSTANCES:
                path = "/v1/analyticsReports/" + parents.report + "/instances";
                type = "analyticsReportInstances";
                filters.put("filter[granularity]", "DAILY");
                break;
            case SEGMENTS:
                path = "/v1/analyticsReportInstances/" + parents.instance + "/segments";
                type = "analyticsReportSegments";
                break;
            default:
                throw resource.failure("requested a non-collection stage");
        }
        if (cursor != null && !safeCursor(cursor)) {
            throw resource.failure("invalid saved pagination cursor");
        }
        filters.put("limit", String.valueOf(config.pageSize));
        Map<String, String> parameters = new LinkedHashMap<>(filters);
        if (cursor != null) {
            parameters.put("cursor", cursor);
        }
        URI endpoint = uri(path, parameters);
        JsonNode document = http.metadata(resource, endpoint, http.deadline());
        JsonNode data = document.path("data");
        if (!data.isArray() || data.size() > config.pageSize) {
            throw resource.failure("collection data is missing or exceeds the requested page size");
        }
        ArrayNode rows = http.json.createArrayNode();
        for (JsonNode item : data) {
            ObjectNode record = record(resource, item, type);
            if (stage == Stage.SEGMENTS) {
                removeSignedUrl(record);
            }
            rows.add(record);
        }
        JsonNode next = document.path("links").path("next");
        if (!(next.isMissingNode() || next.isNull() || next.isTextual())) {
            throw resource.failure("links.next is not a URI string");
        }
        String nextCursor =
                next.isTextual()
                        ? continuation(resource, endpoint, filters, next.textValue())
                        : null;
        return new Page(rows, nextCursor);
    }

    private String continuation(
            AppStoreConnectResource resource,
            URI endpoint,
            Map<String, String> filters,
            String link) {
        try {
            if (link.isEmpty() || link.length() > 8192) {
                throw resource.failure("invalid links.next length");
            }
            URI next =
                    link.startsWith("?")
                            ? config.api.resolve(endpoint.getRawPath() + link)
                            : endpoint.resolve(URI.create(link));
            int port = next.getPort() == 443 ? -1 : next.getPort();
            if (!AppStoreConnectConfig.https(next)
                    || !config.api.getHost().equalsIgnoreCase(next.getHost())
                    || config.api.getPort() != port
                    || !endpoint.getRawPath().equals(next.getRawPath())
                    || next.getRawQuery() == null) {
                throw resource.failure("links.next changed the API origin or collection");
            }
            Map<String, String> values = new LinkedHashMap<>();
            for (String parameter : next.getRawQuery().split("&", -1)) {
                int equals = parameter.indexOf('=');
                if (equals < 1) {
                    throw resource.failure("links.next contains an invalid query parameter");
                }
                String key =
                        URLDecoder.decode(parameter.substring(0, equals), StandardCharsets.UTF_8);
                String value =
                        URLDecoder.decode(parameter.substring(equals + 1), StandardCharsets.UTF_8);
                if (!("cursor".equals(key) || value.equals(filters.get(key)))
                        || values.putIfAbsent(key, value) != null) {
                    throw resource.failure(
                            "links.next changed the filters or repeated a parameter");
                }
            }
            String cursor = values.get("cursor");
            if (!safeCursor(cursor)) {
                throw resource.failure("links.next lacks a valid cursor");
            }
            return cursor;
        } catch (IllegalArgumentException ex) {
            throw resource.failure("invalid links.next URI or encoding");
        }
    }

    private static boolean safeCursor(String cursor) {
        return cursor != null
                && !cursor.isEmpty()
                && cursor.length() <= 4096
                && cursor.chars().allMatch(c -> c >= 33 && c <= 126);
    }

    AppStoreConnectReport dated(AppStoreConnectResource resource, LocalDate date, long deadline)
            throws InterruptedException {
        Map<String, String> query = new LinkedHashMap<>();
        query.put("filter[vendorNumber]", config.vendor);
        String path;
        if (resource.terminal == Stage.FINANCE) {
            path = "/v1/financeReports";
            query.put("filter[reportType]", "FINANCIAL");
            query.put("filter[regionCode]", "ZZ");
            query.put("filter[reportDate]", YearMonth.from(date).toString());
        } else {
            path = "/v1/salesReports";
            query.put("filter[reportType]", resource.reportType);
            query.put("filter[reportSubType]", resource.reportSubType);
            query.put("filter[frequency]", "DAILY");
            query.put("filter[version]", resource.version);
            query.put("filter[reportDate]", date.toString());
        }
        return http.dated(resource, uri(path, query), deadline);
    }

    /**
     * Reads an already requested segment. There is no write to Apple's analytics subscription API.
     * A rejected or expired URL can be renewed once under the original deadline.
     */
    AppStoreConnectReport analytics(
            AppStoreConnectResource resource, Parents parents, long deadline)
            throws InterruptedException {
        for (int renewal = 0; renewal < 2; renewal++) {
            Segment segment = segment(resource, parents, deadline, true);
            AppStoreConnectReport report = http.download(resource, segment, deadline);
            if (report != null) {
                return report;
            }
        }
        throw resource.failure("signed segment URL was rejected after one metadata refresh");
    }

    ObjectNode detail(AppStoreConnectResource resource, Parents parents)
            throws InterruptedException {
        return segment(resource, parents, http.deadline(), false).metadata;
    }

    private Segment segment(
            AppStoreConnectResource resource, Parents parents, long deadline, boolean download)
            throws InterruptedException {
        URI endpoint = uri("/v1/analyticsReportSegments/" + parents.segment, Map.of());
        ObjectNode metadata =
                record(
                        resource,
                        http.metadata(resource, endpoint, deadline).path("data"),
                        "analyticsReportSegments");
        if (!parents.segment.equals(metadata.path("id").textValue())) {
            throw resource.failure("segment metadata returned a different segment ID");
        }
        JsonNode attributes = metadata.path("attributes");
        long size = positiveSize(resource, attributes);
        String checksum = attributes.path("checksum").asText("").toLowerCase(Locale.ROOT);
        if (size <= 0 || !checksum.matches("[0-9a-f]{32}")) {
            throw resource.failure("segment metadata lacks a positive size or MD5 checksum");
        }
        String url = attributes.path("url").asText("");
        URI signed = download ? downloadUri(resource, url) : null;
        if (url.isEmpty()) {
            throw resource.failure("segment metadata lacks its download URL");
        }
        removeSignedUrl(metadata);
        return new Segment(metadata, signed, size, checksum);
    }

    private URI downloadUri(AppStoreConnectResource resource, String url) {
        try {
            if (url.length() > 16384) {
                throw resource.failure("signed download URL is too long");
            }
            URI uri = URI.create(url);
            if (AppStoreConnectConfig.https(uri) && !uri.getRawPath().isEmpty()) {
                String host = uri.getHost().toLowerCase(Locale.ROOT);
                boolean appleS3 =
                        (uri.getPort() == -1 || uri.getPort() == 443)
                                && host.matches(
                                        "(?:[a-z0-9.-]+\\.)?s3(?:[.-][a-z0-9-]+)*\\.amazonaws\\.com");
                if (appleS3 || config.downloadHosts.contains(host)) {
                    return uri;
                }
            }
        } catch (IllegalArgumentException ex) {
            throw resource.failure("invalid signed download URL");
        }
        throw resource.failure("signed download HTTPS host is outside the allowed hosts");
    }

    static long positiveSize(AppStoreConnectResource resource, JsonNode attributes) {
        JsonNode bytes = attributes.path("sizeInBytes");
        if (!bytes.isIntegralNumber() || !bytes.canConvertToLong() || bytes.longValue() < 0) {
            throw resource.failure("unexpected segment sizeInBytes");
        }
        return bytes.longValue();
    }

    static String id(AppStoreConnectResource resource, JsonNode record) {
        String id = record.path("id").asText("");
        if (!record.path("id").isTextual() || !id.matches("[A-Za-z0-9_-]{1,200}")) {
            throw resource.failure("resource has an invalid JSON:API ID");
        }
        return id;
    }

    static LocalDate processingDate(AppStoreConnectResource resource, JsonNode attrs) {
        try {
            return LocalDate.parse(attrs.path("processingDate").asText(""));
        } catch (DateTimeException ex) {
            throw resource.failure("analytics instance has an invalid processingDate");
        }
    }

    static Instant reviewDate(AppStoreConnectResource resource, JsonNode attrs) {
        try {
            return OffsetDateTime.parse(attrs.path("createdDate").asText("")).toInstant();
        } catch (DateTimeException ex) {
            throw resource.failure("review has an invalid createdDate");
        }
    }

    private static ObjectNode record(AppStoreConnectResource resource, JsonNode item, String type) {
        if (!item.isObject()
                || !type.equals(item.path("type").asText(""))
                || !item.path("attributes").isObject()) {
            throw resource.failure("unexpected JSON:API resource type or attributes");
        }
        id(resource, item);
        return (ObjectNode) item;
    }

    private static void removeSignedUrl(ObjectNode record) {
        ((ObjectNode) record.get("attributes")).remove("url");
    }

    private URI uri(String path, Map<String, String> filters) {
        StringBuilder target = new StringBuilder(path);
        for (Map.Entry<String, String> filter : filters.entrySet()) {
            target.append(target.length() == path.length() ? '?' : '&')
                    .append(URLEncoder.encode(filter.getKey(), StandardCharsets.UTF_8))
                    .append('=')
                    .append(
                            URLEncoder.encode(filter.getValue(), StandardCharsets.UTF_8)
                                    .replace("+", "%20"));
        }
        return config.api.resolve(target.toString());
    }

    /**
     * A collection page carries sanitized records and its next opaque cursor.
     *
     * <p>Its digest validates the prefix on restore without persisting provider record data.
     */
    static final class Page {
        final ArrayNode rows;
        final String next;
        final String digest;

        Page(ArrayNode rows, String next) {
            this.rows = rows;
            this.next = next;
            digest = AppStoreConnectConfig.hash(rows.toString() + "\n" + next);
        }
    }

    /**
     * IDs and processing date required to build the next relationship path and output lineage.
     *
     * <p>Only these stable values are kept in checkpoints. Details and download URLs stay local to
     * the current read.
     */
    static final class Parents implements Serializable {
        private static final long serialVersionUID = 1L;
        final String app;
        final String request;
        final String report;
        final String instance;
        final String segment;
        final LocalDate processing;

        Parents(
                String app,
                String request,
                String report,
                String instance,
                String segment,
                LocalDate processing) {
            this.app = app;
            this.request = request;
            this.report = report;
            this.instance = instance;
            this.segment = segment;
            this.processing = processing;
        }

        static Parents root() {
            return new Parents(null, null, null, null, null, null);
        }

        Parents descend(Stage parent, String id, LocalDate date) {
            switch (parent) {
                case APPS:
                    return new Parents(id, null, null, null, null, null);
                case REQUESTS:
                    return new Parents(app, id, null, null, null, null);
                case REPORTS:
                    return new Parents(app, request, id, null, null, null);
                case INSTANCES:
                    return new Parents(app, request, report, id, null, date);
                case SEGMENTS:
                    return new Parents(app, request, report, instance, id, processing);
                default:
                    return this;
            }
        }
    }

    /**
     * One detail response and the integrity metadata for its temporary download locator.
     *
     * <p>It is deliberately not serializable and never stored in source split state.
     */
    static final class Segment {
        final ObjectNode metadata;
        final URI download;
        final long size;
        final String checksum;

        Segment(ObjectNode metadata, URI download, long size, String checksum) {
            this.metadata = metadata;
            this.download = download;
            this.size = size;
            this.checksum = checksum;
        }
    }
}
