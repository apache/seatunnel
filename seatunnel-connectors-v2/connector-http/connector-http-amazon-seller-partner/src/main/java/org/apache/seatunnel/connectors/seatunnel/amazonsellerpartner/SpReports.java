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

package org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import okhttp3.HttpUrl;
import okhttp3.Request;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.DateTimeException;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.SpPosition.Phase.CREATE;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.SpPosition.Phase.DOCUMENT;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.SpPosition.Phase.GENERATED_LIST;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.SpPosition.Phase.POLL;

/**
 * Drives report discovery, creation, polling, and independent download requests.
 *
 * <p>Every transition is checkpointable. Ambiguous creation is never retried, and report
 * restoration validates the uncompressed document before resuming output.
 */
final class SpReports {
    private static final String REPORTS = "reports/2021-06-30/reports";
    private static final String DOCUMENTS = "reports/2021-06-30/documents";
    private static final Pattern DISTRIBUTION =
            Pattern.compile("d[a-z0-9]{8,62}\\.cloudfront\\.net");
    private static final Pattern STORAGE =
            Pattern.compile(
                    "(?:[a-z0-9][a-z0-9.-]{0,200}\\.)?s3(?:[.-][a-z0-9-]+)?\\.amazonaws\\.com");
    private static final Set<String> ELIGIBLE = Set.of("IN_QUEUE", "IN_PROGRESS", "DONE");
    private static final int OUTPUT_BATCH = 512;
    private static final int MAX_RESULT_REPORTS = 100;
    private static final int MAX_GENERATED_REPORTS = 5000;

    private final SpSettings settings;
    private final SpAuthorization auth;
    private final SpTransport transport;
    private final SpDocuments documents;
    private String authorizedIntent;
    private SpDocuments.Decoded cache;

    SpReports(SpSettings settings, SpAuthorization auth, SpTransport transport) {
        this.settings = settings;
        this.auth = auth;
        this.transport = transport;
        documents = new SpDocuments(settings);
    }

    List<SpRecord> read(SpPosition state, long deadline) throws IOException, InterruptedException {
        switch (state.phase) {
            case LOOKUP:
                lookup(state, deadline, false);
                break;
            case CREATE:
                create(state, deadline);
                break;
            case GENERATED_LIST:
                generated(state, deadline);
                break;
            case POLL:
                poll(state, deadline);
                break;
            case DOCUMENT:
                return output(state, deadline);
            default:
                throw failure("unexpected report state");
        }
        return List.of();
    }

    private void lookup(SpPosition position, long deadline, boolean ambiguous)
            throws IOException, InterruptedException {
        JsonNode list = list(position, false, deadline);
        for (JsonNode candidate : reports(list)) {
            if (matches(candidate, position)) {
                startPoll(position, identifier(candidate, "reportId"), false);
                return;
            }
        }
        String next = continuation(list);
        position.setCursor(next, false);
        if (next == null) {
            if (ambiguous) {
                throw failure(
                        "report creation outcome is unknown and no exact existing report can be proven");
            }
            authorizedIntent = intent(position);
            position.phase = CREATE;
        }
    }

    private void create(SpPosition position, long deadline)
            throws IOException, InterruptedException {
        if (!intent(position).equals(authorizedIntent)) {
            // A restored creation intent might already have reached the provider.
            lookup(position, deadline, true);
            return;
        }
        authorizedIntent = null;
        ObjectNode request =
                SpJson.JSON.createObjectNode().put("reportType", settings.resource.reportType);
        request.putArray("marketplaceIds").add(settings.marketplace);
        if (settings.resource.window != SpResource.Window.SNAPSHOT) {
            request.put("dataStartTime", position.from.toString());
            request.put("dataEndTime", SpWindows.inclusiveUpper(position).toString());
        }
        if (!settings.reportOptions.isEmpty()) {
            ObjectNode options = request.putObject("reportOptions");
            settings.reportOptions.forEach(options::put);
        }
        JsonNode accepted = auth.createReport(REPORTS, request, deadline);
        startPoll(position, identifier(accepted, "reportId"), true);
    }

    private void generated(SpPosition position, long deadline)
            throws IOException, InterruptedException {
        if (!position.parents.isEmpty()) {
            startPoll(position, position.parents.get(position.parentIndex), false);
            return;
        }
        JsonNode list = list(position, true, deadline);
        String next = continuation(list);
        List<String> ids = new ArrayList<>();
        Set<String> seen = new HashSet<>(position.generatedReportHashes);
        for (JsonNode report : reports(list)) {
            if (!settings.resource.reportType.equals(report.path("reportType").asText())) {
                throw failure("generated report has an unexpected report type");
            }
            String status = SpJson.text(report, "processingStatus", 32, settings.resource.name());
            if (!ELIGIBLE.contains(status)
                    && !"FATAL".equals(status)
                    && !"CANCELLED".equals(status)) {
                throw failure("generated report has an unknown processing status");
            }
            if (ELIGIBLE.contains(status)) {
                requireGeneratedMarketplace(report);
                String id = identifier(report, "reportId");
                if (!seen.add(SpSettings.hash(id))) {
                    throw failure("generated report list contains duplicate IDs");
                }
                if (seen.size() > MAX_GENERATED_REPORTS) {
                    throw failure("generated report listing exceeds its 5000 report limit");
                }
                ids.add(id);
            }
        }
        position.setCursor(next, false);
        position.generatedReportHashes = Set.copyOf(seen);
        position.parents = List.copyOf(ids);
        position.parentIndex = 0;
        position.parentFinished = next == null;
        if (ids.isEmpty() && position.parentFinished) {
            SpWindows.advance(settings, position);
        }
    }

    private JsonNode list(SpPosition state, boolean generated, long deadline)
            throws IOException, InterruptedException {
        Map<String, String> query = new LinkedHashMap<>();
        if (state.cursor != null) {
            query.put("nextToken", state.cursor);
        } else {
            query.put("reportTypes", settings.resource.reportType);
            query.put("processingStatuses", "IN_QUEUE,IN_PROGRESS,DONE");
            query.put("marketplaceIds", settings.marketplace);
            query.put("pageSize", "100");
            if (generated) {
                query.put("createdSince", state.from.toString());
                query.put("createdUntil", SpWindows.inclusiveUpper(state).toString());
            }
        }
        return auth.get("getReports", REPORTS, query, null, deadline);
    }

    private List<JsonNode> reports(JsonNode response) throws SpFailure {
        JsonNode items = response.path("reports");
        if (!items.isArray() || items.size() > MAX_RESULT_REPORTS) {
            throw failure("invalid report list response");
        }
        List<JsonNode> reports = new ArrayList<>(items.size());
        for (JsonNode item : items) {
            if (!item.isObject()) {
                throw failure("expected an object in the report list");
            }
            reports.add(item);
        }
        return reports;
    }

    private boolean matches(JsonNode report, SpPosition state) {
        if (!settings.resource.reportType.equals(report.path("reportType").asText())
                || !marketplace(report)
                || !optionsMatch(report)) {
            return false;
        }
        try {
            if (settings.resource.window == SpResource.Window.SNAPSHOT) {
                if (report.hasNonNull("dataStartTime") || report.hasNonNull("dataEndTime")) {
                    return false;
                }
            } else if (!state.from.equals(time(report.path("dataStartTime")))
                    || !SpWindows.inclusiveUpper(state).equals(time(report.path("dataEndTime")))) {
                return false;
            }
            String status = report.path("processingStatus").asText();
            if ("DONE".equals(status)) {
                if (settings.maxDoneAge == 0) {
                    return false;
                }
                Instant created = time(report.path("createdTime"));
                Instant now = Instant.now();
                return !created.isAfter(now)
                        && created.plus(Duration.ofHours(settings.maxDoneAge)).isAfter(now);
            }
            return "IN_QUEUE".equals(status) || "IN_PROGRESS".equals(status);
        } catch (DateTimeException e) {
            return false;
        }
    }

    private boolean marketplace(JsonNode report) {
        JsonNode ids = report.path("marketplaceIds");
        return ids.isArray() && ids.size() == 1 && settings.marketplace.equals(ids.get(0).asText());
    }

    private void requireGeneratedMarketplace(JsonNode report) throws SpFailure {
        JsonNode ids = report.path("marketplaceIds");
        if (settings.resource.restrictedDocument) {
            if (!marketplace(report)) {
                throw failure(
                        "generated restricted report has no exact configured marketplace scope");
            }
            return;
        }
        // Settlement files are account-level. Preserve any row-level marketplace in the payload.
        if (ids.isMissingNode() || ids.isNull() || (ids.isArray() && ids.isEmpty())) {
            return;
        }
        if (ids.isArray()) {
            for (JsonNode id : ids) {
                if (id.isTextual() && settings.marketplace.equals(id.textValue())) {
                    return;
                }
            }
        }
        throw failure("generated settlement belongs to another marketplace");
    }

    private boolean optionsMatch(JsonNode report) {
        JsonNode options = report.path("reportOptions");
        // External metadata without an options echo cannot prove equivalence. A saved owned ID
        // resumes by ID and does not pass through this external candidate check.
        if (!options.isObject() || options.size() != settings.reportOptions.size()) {
            return false;
        }
        for (Map.Entry<String, String> opt : settings.reportOptions.entrySet()) {
            JsonNode value = options.path(opt.getKey());
            if (!value.isTextual() || !opt.getValue().equals(value.asText())) {
                return false;
            }
        }
        return true;
    }

    private void startPoll(SpPosition position, String id, boolean submitted) {
        position.report = id;
        position.submitted = submitted;
        position.reportDeadline = Instant.now().plusMillis(settings.reportTimeout);
        position.nextPoll = null;
        position.phase = POLL;
    }

    private void poll(SpPosition position, long deadline) throws IOException, InterruptedException {
        long budget = Duration.between(Instant.now(), position.reportDeadline).toMillis();
        if (budget <= 0L) {
            throw failure("report polling deadline exceeded");
        }
        long bound = Math.min(deadline, System.nanoTime() + budget * 1_000_000L);
        JsonNode report =
                auth.get("getReport", REPORTS + "/" + position.report, Map.of(), null, bound);
        if (!position.report.equals(identifier(report, "reportId"))
                || !settings.resource.reportType.equals(report.path("reportType").asText())) {
            throw failure("provider returned metadata for another report");
        }
        if (settings.resource.window == SpResource.Window.GENERATED
                && report.hasNonNull("marketplaceIds")) {
            requireGeneratedMarketplace(report);
        }
        String status = SpJson.text(report, "processingStatus", 32, settings.resource.name());
        switch (status) {
            case "IN_QUEUE":
            case "IN_PROGRESS":
                position.nextPoll = Instant.now().plusMillis(settings.pollInterval);
                if (!position.nextPoll.isBefore(position.reportDeadline)) {
                    throw failure("report polling deadline exceeded");
                }
                return;
            case "DONE":
                position.document = identifier(report, "reportDocumentId");
                try {
                    position.reportDataEnd =
                            report.hasNonNull("dataEndTime")
                                    ? time(report.path("dataEndTime"))
                                    : null;
                } catch (DateTimeException e) {
                    throw failure("invalid report data end time");
                }
                position.phase = DOCUMENT;
                return;
            case "CANCELLED":
                if (settings.cancelledEmpty
                        && position.submitted
                        && !report.hasNonNull("reportDocumentId")) {
                    completed(position);
                    return;
                }
                throw failure(
                        "report CANCELLED and the provider did not establish an empty result");
            case "FATAL":
                boolean options = optionFailure(report, bound);
                throw failure(
                        options
                                ? "report FATAL because its options were rejected"
                                : "report FATAL at the provider");
            default:
                throw failure("unknown processing status");
        }
    }

    private boolean optionFailure(JsonNode report, long deadline) throws InterruptedException {
        if (!report.hasNonNull("reportDocumentId")) {
            return false;
        }
        try {
            Download data = download(identifier(report, "reportDocumentId"), deadline);
            byte[] decoded = documents.expand(data.reply.body, data.compression);
            String reason =
                    new String(decoded, 0, Math.min(decoded.length, 32768), StandardCharsets.UTF_8)
                            .toLowerCase(Locale.ROOT);
            return reason.contains("option")
                    && (reason.contains("invalid")
                            || reason.contains("missing")
                            || reason.contains("required")
                            || reason.contains("unsupported"));
        } catch (IOException ignored) {
            // A failure document can only refine the diagnosis. The FATAL status still wins.
            return false;
        }
    }

    private List<SpRecord> output(SpPosition position, long deadline)
            throws IOException, InterruptedException {
        if (cache == null || !position.document.equals(cache.id)) {
            Download bytes = download(position.document, deadline);
            cache =
                    documents.parse(
                            bytes.reply.body,
                            bytes.compression,
                            bytes.reply.contentType,
                            position.document);
        }
        if ((position.documentDigest != null && !position.documentDigest.equals(cache.digest))
                || position.documentOffset > cache.rows.size()) {
            throw failure("report document changed after checkpoint");
        }
        position.documentDigest = cache.digest;
        int next = Math.min(position.documentOffset + OUTPUT_BATCH, cache.rows.size());
        List<SpRecord> result = new ArrayList<>(next - position.documentOffset);
        for (int index = position.documentOffset; index < next; index++) {
            ObjectNode data = cache.rows.get(index).deepCopy();
            decorate(position, data);
            String id = businessId(data, position.report + "/" + (index + 1));
            result.add(new SpRecord(id, null, data));
        }
        position.documentOffset = next;
        if (next == cache.rows.size()) {
            cache = null;
            completed(position);
        }
        return result;
    }

    private void decorate(SpPosition position, ObjectNode data) {
        String field = settings.resource.windowField;
        Instant effectiveEnd =
                position.reportDataEnd == null
                        ? SpWindows.inclusiveUpper(position)
                        : position.reportDataEnd;
        if (field != null && !data.has(field)) {
            String value =
                    "endDate".equals(field) || "queryEndDate".equals(field)
                            ? SpWindows.date(effectiveEnd).toString()
                            : effectiveEnd.toString();
            data.put(field, value);
        }
        if (settings.resource == SpResource.GET_SALES_AND_TRAFFIC_REPORT
                || settings.resource == SpResource.GET_SALES_AND_TRAFFIC_REPORT_BY_MONTH) {
            data.putIfAbsent(
                    "date",
                    SpJson.JSON
                            .getNodeFactory()
                            .textNode(SpWindows.date(position.from).toString()));
            data.putIfAbsent(
                    "marketplaceId", SpJson.JSON.getNodeFactory().textNode(settings.marketplace));
        }
    }

    private String businessId(ObjectNode record, String fallback) {
        String field =
                settings.resource.name().contains("MERCHANT_LISTINGS")
                        ? "listing-id"
                        : settings.resource == SpResource.GET_XML_BROWSE_TREE_DATA
                                ? "browseNodeId"
                                : settings.resource
                                                == SpResource
                                                        .GET_XML_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL
                                        ? "AmazonOrderID"
                                        : "";
        JsonNode id = record.path(field);
        return id.isTextual() && !id.textValue().isBlank() ? id.textValue() : fallback;
    }

    private void completed(SpPosition position) {
        position.resetReport();
        if (settings.resource.window == SpResource.Window.GENERATED) {
            position.parentIndex++;
            if (position.parentIndex == position.parents.size()) {
                position.parents = List.of();
                position.parentIndex = 0;
                if (position.parentFinished) {
                    SpWindows.advance(settings, position);
                    return;
                }
            }
            position.phase = GENERATED_LIST;
        } else {
            SpWindows.advance(settings, position);
        }
    }

    private Download download(String id, long deadline) throws IOException, InterruptedException {
        for (int attempt = 0; attempt < 3; attempt++) {
            String path = DOCUMENTS + "/" + id;
            SpAuthorization.Scope scope =
                    settings.resource.restrictedDocument
                            ? new SpAuthorization.Scope("/" + path, List.of())
                            : null;
            JsonNode target = auth.get("getReportDocument", path, Map.of(), scope, deadline);
            if (!id.equals(identifier(target, "reportDocumentId"))) {
                throw failure("provider returned a different report document");
            }
            HttpUrl url = allowedUrl(SpJson.text(target, "url", 16384, "getReportDocument"));
            Request request = new Request.Builder().url(url).get().build();
            SpTransport.Reply response =
                    transport.exchange(
                            "report download", request, SpTransport.Safety.READ, true, deadline);
            if (response.status == 200) {
                String compression =
                        target.hasNonNull("compressionAlgorithm")
                                ? SpJson.text(
                                        target, "compressionAlgorithm", 16, "getReportDocument")
                                : null;
                return new Download(response, compression);
            }
            if (response.status != 403) {
                throw failure("report download returned HTTP " + response.status);
            }
        }
        throw failure("presigned report URL remained forbidden after two renewals");
    }

    private HttpUrl allowedUrl(String address) throws SpFailure {
        try {
            URI uri = URI.create(address);
            if (!"https".equals(uri.getScheme())
                    || uri.getHost() == null
                    || uri.getRawUserInfo() != null
                    || uri.getRawFragment() != null
                    || uri.getRawPath().isEmpty()
                    || !uri.equals(uri.normalize())) {
                throw failure("invalid report download address");
            }
            String host = uri.getHost().toLowerCase(Locale.ROOT);
            HttpUrl url = HttpUrl.get(address);
            if (settings.customOrigin != null) {
                HttpUrl sandbox = HttpUrl.get(settings.customOrigin);
                if (sandbox.host().equals(url.host()) && sandbox.port() == url.port()) {
                    return url;
                }
            } else if (url.port() == 443
                    && (DISTRIBUTION.matcher(host).matches() || STORAGE.matcher(host).matches())) {
                return url;
            }
        } catch (IllegalArgumentException ignored) {
            // Never include URI parsing failures or a signed download URL in diagnostics.
        }
        throw failure("report download host is outside the allowed HTTPS origins");
    }

    private String identifier(JsonNode tree, String field) throws SpFailure {
        String id = SpJson.text(tree, field, 256, settings.resource.name());
        if (!id.matches("[A-Za-z0-9][A-Za-z0-9_.:-]*")) {
            throw failure("invalid report identifier");
        }
        return id;
    }

    private String continuation(JsonNode response) throws SpFailure {
        JsonNode next = response.path("nextToken");
        return next.isMissingNode()
                        || next.isNull()
                        || (next.isTextual() && next.asText().isEmpty())
                ? null
                : SpJson.text(response, "nextToken", 8192, "getReports");
    }

    private static Instant time(JsonNode value) {
        return OffsetDateTime.parse(value.asText()).toInstant();
    }

    private static String intent(SpPosition state) {
        return state.fingerprint + "/" + state.from + "/" + state.until;
    }

    private SpFailure failure(String reason) {
        return new SpFailure(settings.resource.name(), reason);
    }

    /**
     * One HTTPS document transfer and its provider compression indicator.
     *
     * <p>The associated pre-signed URL is never retained.
     */
    private static final class Download {
        private final SpTransport.Reply reply;
        private final String compression;

        private Download(SpTransport.Reply reply, String compression) {
            this.reply = reply;
            this.compression = compression;
        }
    }
}
