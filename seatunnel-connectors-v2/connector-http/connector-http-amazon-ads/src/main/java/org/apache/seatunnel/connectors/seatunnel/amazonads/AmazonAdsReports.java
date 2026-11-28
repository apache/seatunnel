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
package org.apache.seatunnel.connectors.seatunnel.amazonads;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.format.DateTimeParseException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Owns reporting v3 creation, exact duplicate verification, status checks and signed URL renewal.
 * Only a validated report ID and configuration fingerprint cross the checkpoint boundary.
 */
final class AmazonAdsReports {
    private static final String PATH = "/reporting/reports";
    private static final String CREATE_TYPE = "application/vnd.createasyncreportrequest.v3+json";
    private static final String CREATE_REPLY = "application/vnd.createasyncreportresponse.v3+json";
    private static final String STATUS_REPLY = "application/vnd.getasyncreportresponse.v3+json";
    private static final String UUID =
            "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}";
    private static final Pattern DUPLICATE =
            Pattern.compile("duplicate\\s+of\\s*:\\s*(" + UUID + ")", Pattern.CASE_INSENSITIVE);
    private static final Set<String> RUNNING = Set.of("PENDING", "PROCESSING");
    private static final Set<String> REUSABLE = Set.of("PENDING", "PROCESSING", "COMPLETED");
    private static final List<String> DETAIL_FIELDS = List.of("detail", "details", "message");
    private static final List<String> IDENTITY_FIELDS =
            List.of("adProduct", "reportTypeId", "timeUnit", "format", "groupBy");
    private final AmazonAdsConfig config;
    private final AmazonAdsTransport transport;

    AmazonAdsReports(AmazonAdsConfig config, AmazonAdsTransport transport) {
        this.config = config;
        this.transport = transport;
    }

    ObjectNode request(AmazonAdsResource resource, AmazonAdsPartition.Window window) {
        ObjectNode request = transport.mapper().createObjectNode();
        request.put("name", "SeaTunnel " + resource.label() + " " + window.from + " " + window.to);
        request.put("startDate", window.from.toString());
        request.put("endDate", window.to.toString());
        AmazonAdsReportCatalog.forResource(resource).write(request.putObject("configuration"));
        return request;
    }

    static String fingerprint(AmazonAdsResource resource, String profile, ObjectNode request) {
        return AmazonAdsRecord.hash(
                resource.label(),
                profile,
                request.path("startDate").asText(),
                request.path("endDate").asText(),
                request.path("configuration").toString());
    }

    String create(AmazonAdsResource resource, String profile, ObjectNode body, long deadline)
            throws InterruptedException {
        for (int attempt = 1; attempt <= config.attempts; attempt++) {
            AmazonAdsTransport.Reply submitted =
                    transport.ads(
                            resource,
                            profile,
                            PATH,
                            CREATE_TYPE,
                            CREATE_REPLY,
                            body,
                            true,
                            Set.of(200, 425),
                            requestDeadline(deadline));
            if (submitted.status == 200) {
                String id = submitted.payload.path("reportId").asText("");
                if (!validId(id) || !matches(body, submitted.payload)) {
                    throw resource.fail(
                            "create response has a mismatched report identity or configuration");
                }
                return id;
            }
            String candidate = duplicateId(submitted.payload);
            if (candidate != null) {
                AmazonAdsTransport.Reply existing =
                        lookup(resource, profile, candidate, requestDeadline(deadline), true);
                if (existing.status == 200
                        && candidate.equals(existing.payload.path("reportId").asText(""))
                        && REUSABLE.contains(existing.payload.path("status").asText(""))
                        && matches(body, existing.payload)) {
                    return candidate;
                }
            }
            if (attempt < config.attempts) {
                waitFor(
                        resource,
                        config.duplicateMs,
                        deadline,
                        "duplicate report retry exceeds its deadline");
            }
        }
        throw resource.fail("HTTP 425 did not identify a reusable report within the attempt limit");
    }

    AmazonAdsReportSpool awaitDownload(
            AmazonAdsResource resource,
            String profile,
            AmazonAdsPartition.Window window,
            ObjectNode body,
            String reportId,
            long requestedAt,
            long totalDeadline)
            throws InterruptedException {
        int staleUrls = 0;
        String rejectedUrl = null;
        while (true) {
            AmazonAdsTransport.Reply status =
                    lookup(resource, profile, reportId, requestDeadline(totalDeadline), false);
            JsonNode active = status.payload;
            if (!reportId.equals(active.path("reportId").asText("")) || !matches(body, active)) {
                throw resource.fail("report status does not match the checkpointed request");
            }
            String state = active.path("status").asText("");
            if ("FAILED".equals(state)) {
                throw resource.fail("report generation failed");
            }
            if (RUNNING.contains(state)) {
                long left = config.reportMs - Math.max(0, System.currentTimeMillis() - requestedAt);
                if (left <= config.pollMs) {
                    throw resource.fail("report processing timeout exceeded");
                }
                waitFor(
                        resource,
                        config.pollMs,
                        totalDeadline,
                        "report processing timeout exceeded");
                continue;
            }
            if (!"COMPLETED".equals(state)) {
                throw resource.fail("report returned an unknown status");
            }
            String url = active.path("url").asText("");
            if (!active.path("url").isTextual() || url.isBlank()) {
                throw resource.fail("completed report did not return a download URL");
            }
            if (!expired(resource, active) && !url.equals(rejectedUrl)) {
                long downloadDeadline =
                        Math.min(
                                totalDeadline,
                                System.nanoTime()
                                        + TimeUnit.MILLISECONDS.toNanos(config.downloadMs));
                AmazonAdsReportSpool complete =
                        transport.download(resource, url, window, profile, downloadDeadline);
                if (complete != null) {
                    return complete;
                }
                rejectedUrl = url;
            }
            if (++staleUrls >= config.attempts) {
                throw resource.fail("no usable download URL after reacquiring report status");
            }
            waitFor(
                    resource,
                    config.pollMs,
                    totalDeadline,
                    "report download URL refresh exceeds its deadline");
        }
    }

    private AmazonAdsTransport.Reply lookup(
            AmazonAdsResource resource, String profile, String id, long deadline, boolean duplicate)
            throws InterruptedException {
        return transport.ads(
                resource,
                profile,
                PATH + "/" + id,
                null,
                STATUS_REPLY,
                null,
                false,
                duplicate ? Set.of(200, 403, 404) : Set.of(200),
                deadline);
    }

    private long requestDeadline(long deadline) {
        return Math.min(
                deadline, System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(config.requestMs));
    }

    private void waitFor(AmazonAdsResource resource, long millis, long deadline, String failure)
            throws InterruptedException {
        long nanos = TimeUnit.MILLISECONDS.toNanos(millis);
        if (transport.remaining(resource, deadline) <= nanos) {
            throw resource.fail(failure);
        }
        TimeUnit.NANOSECONDS.sleep(nanos);
    }

    private static boolean expired(AmazonAdsResource resource, JsonNode active) {
        JsonNode value = active.get("urlExpiresAt");
        if (value == null || value.isNull()) {
            return false;
        }
        try {
            return !OffsetDateTime.parse(value.asText("")).toInstant().isAfter(Instant.now());
        } catch (DateTimeParseException e) {
            throw resource.fail("report download URL expiration is invalid");
        }
    }

    static boolean validId(String value) {
        return value != null && value.matches(UUID);
    }

    private static String duplicateId(JsonNode body) {
        if (body == null) {
            return null;
        }
        for (String field : DETAIL_FIELDS) {
            String text = body.path(field).asText("");
            Matcher matcher = DUPLICATE.matcher(text);
            if (matcher.find()) {
                return matcher.group(1);
            }
        }
        return null;
    }

    private static boolean matches(ObjectNode wanted, JsonNode response) {
        if (!wanted.path("startDate").equals(response.path("startDate"))
                || !wanted.path("endDate").equals(response.path("endDate"))) {
            return false;
        }
        JsonNode a = wanted.path("configuration");
        JsonNode b = response.path("configuration");
        for (String attribute : IDENTITY_FIELDS) {
            if (!a.path(attribute).equals(b.path(attribute))) {
                return false;
            }
        }
        // JSON report objects expose fields by name; their projection order is immaterial.
        Set<String> requestedColumns = columnSet(a.path("columns"));
        if (requestedColumns == null || !requestedColumns.equals(columnSet(b.path("columns")))) {
            return false;
        }
        JsonNode x = a.path("filters");
        JsonNode y = b.path("filters");
        return x.equals(y) || x.isArray() && x.isEmpty() && (y.isMissingNode() || y.isNull());
    }

    private static Set<String> columnSet(JsonNode node) {
        if (!node.isArray() || node.isEmpty()) {
            return null;
        }
        Set<String> result = new HashSet<>();
        for (JsonNode value : node) {
            if (!value.isTextual() || !result.add(value.textValue())) {
                return null;
            }
        }
        return result;
    }
}
