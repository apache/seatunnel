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

package org.apache.seatunnel.connectors.seatunnel.datadog;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URIBuilder;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.DateTimeException;
import java.time.Instant;
import java.util.Locale;

/**
 * The provider endpoint, record container and paging rule for each supported resource.
 * Endpoint-specific contracts remain local rather than changing the shared HTTP connector.
 */
enum DatadogResource {
    AUDIT_LOGS("/api/v2/audit/events", "data", true, 1000),
    DASHBOARDS("/api/v1/dashboard", "dashboards", false, 1000),
    DOWNTIMES("/api/v1/downtime", null, false, 1000),
    TEAMS("/api/v2/team", "data", false, 100),
    INCIDENTS("/api/v2/incidents", "data", false, 100),
    LOGS("/api/v2/logs/events", "data", true, 1000),
    METRICS("/api/v2/metrics", "data", false, 1000),
    MONITORS("/api/v1/monitor", null, false, 1000),
    SERVICE_LEVEL_OBJECTIVES("/api/v1/slo", "data", false, 1000),
    SYNTHETIC_TESTS("/api/v1/synthetics/tests", "tests", false, 100),
    USERS("/api/v2/users", "data", false, 100);

    // Endpoint identity is fixed in code; cursor links can supply tokens but cannot redirect keys.
    final String path;
    // Null means the endpoint returns the record array as its root JSON value.
    final String section;
    // Only event endpoints use sliced, half-open time bounds and timestamp filtering.
    final boolean dated;
    // Clamp the common page_size option to the selected endpoint's supported request size.
    final int maxPageSize;
    final String label = name().toLowerCase(Locale.ROOT);

    DatadogResource(String path, String section, boolean dated, int maxPageSize) {
        this.path = path;
        this.section = section;
        this.dated = dated;
        this.maxPageSize = maxPageSize;
    }

    static DatadogResource parse(String value) {
        for (DatadogResource resource : values()) {
            if (resource.label.equals(value)) {
                return resource;
            }
        }
        throw new IllegalArgumentException("Unsupported Datadog resource.");
    }

    /**
     * Builds the fixed relative target; response-supplied links cannot change it. Opaque
     * continuation values are encoded only as query parameters on this endpoint.
     */
    String target(DatadogConfig config, DatadogSplit split) {
        URIBuilder request = new URIBuilder().setPath(path);
        String size = Integer.toString(config.pageSize);
        String position = Long.toString(split.position);
        switch (this) {
            case AUDIT_LOGS:
            case LOGS:
                request.addParameter("filter[from]", split.from.toString())
                        .addParameter("filter[to]", split.until(config.sliceDays).toString())
                        .addParameter("sort", "timestamp")
                        .addParameter("page[limit]", size);
                if (this == LOGS && !config.logsQuery.isEmpty()) {
                    request.addParameter("filter[query]", config.logsQuery);
                }
                break;
            case METRICS:
                request.addParameter("page[size]", size)
                        .addParameter("window[seconds]", Integer.toString(config.metricsWindow));
                break;
            case DASHBOARDS:
                request.addParameter("start", position).addParameter("count", size);
                break;
            case TEAMS:
            case USERS:
                request.addParameter("page[number]", position).addParameter("page[size]", size);
                break;
            case INCIDENTS:
                request.addParameter("page[offset]", position).addParameter("page[size]", size);
                break;
            case MONITORS:
                // The documented ID walk terminates on an empty response and does not use page
                // numbers.
                request.addParameter("id_offset", position);
                break;
            case SERVICE_LEVEL_OBJECTIVES:
                request.addParameter("offset", position).addParameter("limit", size);
                break;
            case SYNTHETIC_TESTS:
                request.addParameter("page_number", position).addParameter("page_size", size);
                break;
            case DOWNTIMES:
                break;
            default:
                throw new AssertionError(this);
        }
        if (split.cursor != null) {
            request.addParameter("page[cursor]", split.cursor);
        }
        try {
            return request.build().toASCIIString();
        } catch (URISyntaxException ex) {
            throw invalid("request parameters cannot be encoded");
        }
    }

    /**
     * A missing container is a provider failure, never an empty successful scan. An explicit empty
     * array remains valid so endpoint pagination can decide whether to continue.
     */
    JsonNode records(JsonNode response) {
        JsonNode records = section == null ? response : response.path(section);
        if (!records.isArray()) {
            throw invalid("missing or invalid result array");
        }
        return records;
    }

    /**
     * A linked continuation is a token source only. The token always goes to the same authenticated
     * API host, endpoint and filters. An authoritative body token takes precedence over link data.
     */
    private String cursor(DatadogConfig config, JsonNode response, String pointer) {
        JsonNode body = response.at(pointer);
        if (!body.isMissingNode() && !body.isNull() && !body.isTextual()) {
            throw invalid("invalid pagination cursor");
        }
        String value = body.asText("");
        if (!value.isEmpty()) {
            return checkedCursor(value);
        }
        JsonNode next = response.at("/links/next");
        if (next.isMissingNode()
                || next.isNull()
                || (next.isTextual() && next.textValue().isEmpty())) {
            return null;
        }
        if (!next.isTextual() || next.textValue().length() > 16384) {
            throw invalid("invalid pagination link");
        }
        try {
            URI link = new URI(next.textValue());
            String host = link.getHost();
            String site = config.host.substring(4);
            boolean apiHost = config.host.equalsIgnoreCase(host);
            boolean webHost = site.equalsIgnoreCase(host) || ("app." + site).equalsIgnoreCase(host);
            if (link.getUserInfo() != null
                    || link.getFragment() != null
                    || (link.getRawAuthority() != null && (!apiHost && !webHost))
                    || (host != null && !"https".equalsIgnoreCase(link.getScheme()))
                    || (link.getScheme() != null && host == null)
                    || (link.getPort() != -1 && link.getPort() != 443)) {
                throw invalid("unrecognized pagination origin");
            }
            String linkedPath = link.getRawPath();
            if (!path.equals(linkedPath)
                    && !(dated && (path.substring(0, path.length() - 1)).equals(linkedPath))
                    && !"".equals(linkedPath)) {
                throw invalid("unrecognized pagination path");
            }
            String token = null;
            for (NameValuePair parameter : new URIBuilder(link).getQueryParams()) {
                if ("page[cursor]".equals(parameter.getName())) {
                    if (token != null || parameter.getValue() == null) {
                        throw invalid("ambiguous pagination link");
                    }
                    token = checkedCursor(parameter.getValue());
                }
            }
            if (token == null) {
                throw invalid("missing pagination link cursor");
            }
            return token;
        } catch (URISyntaxException | IllegalArgumentException ex) {
            throw invalid("malformed pagination link");
        }
    }

    private String checkedCursor(String value) {
        if (value.isEmpty()
                || value.length() > 4096
                || value.chars().anyMatch(Character::isISOControl)) {
            throw invalid("invalid pagination cursor");
        }
        return value;
    }

    /**
     * Evaluates continuation using the original page size, before time-bound row filtering.
     * Filtering must not make a continuing provider page appear terminal.
     */
    Next next(DatadogConfig config, DatadogSplit split, JsonNode response, JsonNode records) {
        int count = records.size();
        switch (this) {
            case AUDIT_LOGS:
            case LOGS:
                return Next.cursor(cursor(config, response, "/meta/page/after"));
            case METRICS:
                return Next.cursor(cursor(config, response, "/meta/pagination/next_cursor"));
            case DOWNTIMES:
                return Next.DONE;
            case DASHBOARDS:
                return count == 0 ? Next.DONE : Next.position(sum(split.position, count));
            case MONITORS:
                if (count == 0) {
                    return Next.DONE;
                }
                long last = split.position;
                for (JsonNode item : records) {
                    Long id = optionalLong(item.path("id"));
                    if (id == null || id <= last) {
                        throw invalid("monitor IDs do not advance");
                    }
                    last = id;
                }
                return Next.position(last);
            case INCIDENTS:
                Long next = optionalLong(response.at("/meta/pagination/next_offset"));
                if (count == 0) {
                    // The endpoint can include the arithmetic next offset on the terminal probe.
                    return Next.DONE;
                }
                return Next.position(
                        next == null
                                ? sum(split.position, count)
                                : advancing(split.position, next));
            case TEAMS:
                Long total = optionalLong(response.at("/meta/pagination/total"));
                if (total != null
                        && remaining(total, split.position, count, config.pageSize) == 0) {
                    return Next.DONE;
                }
                next = optionalLong(response.at("/meta/pagination/next_number"));
                if (next != null) {
                    return Next.position(advancing(split.position, next));
                }
                return numbered(config, split, count, total);
            case USERS:
                total = optionalLong(response.at("/meta/page/total_filtered_count"));
                if (total == null) {
                    total = optionalLong(response.at("/meta/page/total_count"));
                }
                return numbered(config, split, count, total);
            case SYNTHETIC_TESTS:
                return numbered(config, split, count, null);
            case SERVICE_LEVEL_OBJECTIVES:
                total = optionalLong(response.at("/metadata/page/total_filtered_count"));
                if (total != null && remaining(total, split.position, count, 1) == 0) {
                    return Next.DONE;
                }
                if (total != null && count == 0) {
                    throw invalid("inconsistent service level objective count");
                }
                return count == 0 ? Next.DONE : Next.position(sum(split.position, count));
            default:
                throw new AssertionError(this);
        }
    }

    /**
     * Numbered pages represent requested-size slots, so a short result page alone cannot prove
     * completion. A provider total can prove completion; otherwise probe until an empty page.
     */
    private Next numbered(DatadogConfig config, DatadogSplit split, int count, Long total) {
        if ((total == null && count == 0)
                || (total != null
                        && remaining(total, split.position, config.pageSize, config.pageSize)
                                == 0)) {
            return Next.DONE;
        }
        return Next.position(sum(split.position, 1));
    }

    private long advancing(long current, long next) {
        if (next <= current) {
            throw invalid("pagination position does not advance");
        }
        return next;
    }

    private long remaining(long total, long position, int count, int multiplier) {
        try {
            return Math.max(
                    0,
                    Math.subtractExact(
                            total, Math.addExact(Math.multiplyExact(position, multiplier), count)));
        } catch (ArithmeticException ex) {
            throw invalid("pagination count overflow");
        }
    }

    private long sum(long number, long delta) {
        try {
            return Math.addExact(number, delta);
        } catch (ArithmeticException ex) {
            throw invalid("pagination position overflow");
        }
    }

    private Long optionalLong(JsonNode value) {
        if (value.isMissingNode() || value.isNull()) {
            return null;
        }
        if (!value.isIntegralNumber() || !value.canConvertToLong() || value.longValue() < 0) {
            throw invalid("invalid pagination number");
        }
        return value.longValue();
    }

    String id(JsonNode item) {
        if (this == METRICS && item.isTextual() && !item.textValue().isEmpty()) {
            return item.textValue();
        }
        if (!item.isObject()) {
            throw invalid("invalid result record");
        }
        JsonNode id = item.path(this == SYNTHETIC_TESTS ? "public_id" : "id");
        if (id.isMissingNode() || id.isNull()) {
            return null;
        }
        if (!id.isTextual() && !id.isIntegralNumber()) {
            throw invalid("invalid result identifier");
        }
        return id.asText();
    }

    String timestamp(JsonNode item) {
        return dated ? item.at("/attributes/timestamp").textValue() : null;
    }

    /**
     * Filters exact event instants into the current half-open window. A row at the upper boundary
     * is left for the following window rather than emitted twice.
     */
    boolean inWindow(String rawTimestamp, DatadogConfig config, DatadogSplit split) {
        if (!dated) {
            return true;
        }
        try {
            if (rawTimestamp == null) {
                throw invalid("missing event timestamp");
            }
            Instant timestamp = Instant.parse(rawTimestamp);
            return !timestamp.isBefore(split.from)
                    && timestamp.isBefore(split.until(config.sliceDays));
        } catch (DateTimeException ex) {
            throw invalid("invalid event timestamp");
        }
    }

    ProviderFailure invalid(String reason) {
        return new ProviderFailure("Datadog " + label + ": " + reason);
    }

    /**
     * Safe connector diagnostic whose text excludes provider bodies, URLs and credentials. The
     * reader propagates this failure category without retaining arbitrary remote exception text.
     */
    static final class ProviderFailure extends IllegalStateException {
        private ProviderFailure(String message) {
            super(message);
        }
    }

    /**
     * Endpoint-specific successor calculated before row filtering; the reader commits it
     * atomically.
     */
    static final class Next {
        private static final Next DONE = new Next(null, 0, true);
        // Cursor and numeric position are mutually exclusive continuation forms.
        final String cursor;
        final long position;
        // Completion ends the current event window, or the whole catalog when no window exists.
        final boolean done;

        private Next(String cursor, long position, boolean done) {
            this.cursor = cursor;
            this.position = position;
            this.done = done;
        }

        static Next cursor(String cursor) {
            return cursor == null ? DONE : new Next(cursor, 0, false);
        }

        static Next position(long position) {
            return new Next(null, position, false);
        }
    }
}
