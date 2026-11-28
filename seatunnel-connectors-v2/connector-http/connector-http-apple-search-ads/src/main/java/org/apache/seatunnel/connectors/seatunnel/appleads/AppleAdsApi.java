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
package org.apache.seatunnel.connectors.seatunnel.appleads;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import java.time.DateTimeException;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Version-specific request builders and response decoders for the eight read operations.
 *
 * <p>Logical row identities are normalized across modes; the provider fields and monetary values
 * are kept exact. Reports count page entities separately from emitted daily facts.
 */
final class AppleAdsApi {
    private final AppleAdsConfig config;
    private final AppleAdsHttp http;

    AppleAdsApi(AppleAdsConfig config, AppleAdsHttp http) {
        this.config = config;
        this.http = http;
    }

    /**
     * Retrieves a single page in one explicit account, parent and date window.
     *
     * <p>The provider's response counters must agree with the requested offset before any rows from
     * the page are emitted.
     */
    Page page(AppleAdsResource resource, Long campaign, Long group, Window window, int offset)
            throws InterruptedException {
        long deadline = http.deadline();
        JsonNode response;
        if (config.platform()) {
            String path =
                    (resource.report() ? "/v1/reports/apps/" : "/v1/") + resource.route + "/query";
            response =
                    http.query(
                            resource,
                            path,
                            resource.report()
                                    ? platformReport(campaign, window, offset)
                                    : platformEntity(resource, campaign, group, offset),
                            deadline);
        } else if (resource.report()) {
            String path =
                    "/api/v5/reports/campaigns"
                            + (campaign == null ? "" : "/" + campaign + "/" + resource.route);
            response = http.query(resource, path, legacyReport(window, offset), deadline);
        } else {
            String path =
                    legacyCollection(resource, campaign, group)
                            + "?offset="
                            + offset
                            + "&limit="
                            + config.pageSize;
            response = http.query(resource, path, null, deadline);
        }

        rejectErrors(resource, response.path("error"));
        rejectErrors(resource, response.path("errors"));
        JsonNode data = data(resource, response);
        JsonNode pagination = response.path("pagination");
        int total;
        int next;
        if (!config.platform()
                && resource == AppleAdsResource.ADS
                && offset == 0
                && (pagination.isMissingNode() || pagination.isNull())
                && data.isArray()) {
            // The legacy Get All Ads endpoint also documents a single complete list response.
            total = data.size();
            next = -1;
        } else {
            int start =
                    counter(resource, pagination, config.platform() ? "offset" : "startIndex", 0);
            int pageSize =
                    counter(
                            resource,
                            pagination,
                            config.platform() ? "pageSize" : "itemsPerPage",
                            1);
            total =
                    counter(
                            resource,
                            pagination,
                            config.platform() ? "totalCount" : "totalResults",
                            0);
            if (start != offset || pageSize > config.pageSize || offset > total) {
                throw resource.fail(
                        "pagination counters disagree with the requested offset or size");
            }
            int onPage = Math.min(pageSize, total - offset);
            if (!(data.isArray() && data.size() == onPage) && !(data.isNull() && onPage == 0)) {
                throw resource.fail("empty, short or oversized page contradicts pagination totals");
            }
            int advanced = offset + onPage;
            next = advanced < total ? advanced : -1;
        }
        String digest = AppleAdsConfig.fingerprint(data.toString());
        List<Row> rows = new ArrayList<>();
        if (data.isArray()) {
            Set<String> identities = new HashSet<>();
            int processed = 0;
            for (JsonNode item : data) {
                if (resource.report()) {
                    reportRows(resource, campaign, window, item, rows, identities);
                } else {
                    Row record = entity(resource, campaign, group, item);
                    accept(resource, record, rows, identities);
                }
                if ((++processed & 127) == 0) {
                    http.remaining(resource, deadline);
                }
            }
        }
        http.remaining(resource, deadline);
        return new Page(rows, total, next, digest);
    }

    private ObjectNode platformEntity(
            AppleAdsResource resource, Long campaign, Long group, int offset) {
        ObjectNode body = http.json.createObjectNode();
        body.putObject("pagination")
                .put("offset", offset)
                .put("pageSize", config.pageSize)
                .put("fetchTotalCount", true);
        body.putArray("sorting").addObject().put("field", "id").put("order", "ASC");
        ObjectNode filter = equalsFilter(body.putArray("filters"));
        if (resource == AppleAdsResource.CAMPAIGNS) {
            filter.put("field", "promotedObjectType").put("value", "APPSTORE_APP");
        } else if (resource == AppleAdsResource.ADGROUPS) {
            filter.put("field", "campaignId").put("value", campaign.longValue());
        } else {
            filter.put("field", "adGroupId").put("value", group.longValue());
        }
        return body;
    }

    private ObjectNode platformReport(Long campaign, Window window, int offset) {
        ObjectNode body = http.json.createObjectNode();
        body.putObject("pagination").put("offset", offset).put("pageSize", config.pageSize);
        if (campaign != null) {
            // Reporting filters use a string-array value rather than the entity numeric scalar.
            equalsFilter(body.putArray("filters"))
                    .put("field", "campaignId")
                    .putArray("value")
                    .add(campaign.toString());
        }
        body.putArray("groupBy").add("countryOrRegion");
        ObjectNode range =
                body.putObject("timeRange")
                        .put("start", window.first.toString())
                        .put("end", window.last.toString())
                        .put("timeZone", config.timezone);
        if (!window.oneDay()) {
            range.put("granularity", "DAILY");
        }
        return body;
    }

    private static ObjectNode equalsFilter(ArrayNode filters) {
        return filters.addObject().put("operator", "EQUALS");
    }

    private ObjectNode legacyReport(Window window, int offset) {
        ObjectNode body =
                http.json
                        .createObjectNode()
                        .put("startTime", window.first.toString())
                        .put("endTime", window.last.toString())
                        .put("timeZone", config.timezone)
                        .put("granularity", "DAILY")
                        .put("returnRowTotals", false)
                        .put("returnGrandTotals", false)
                        .put("returnRecordsWithNoMetrics", false);
        body.putArray("groupBy").add("countryOrRegion");
        ObjectNode selector = body.putObject("selector");
        selector.putArray("orderBy")
                .addObject()
                .put("field", "countryOrRegion")
                .put("sortOrder", "ASCENDING");
        selector.putObject("pagination").put("offset", offset).put("limit", config.pageSize);
        return body;
    }

    private static String legacyCollection(AppleAdsResource resource, Long campaign, Long group) {
        if (resource == AppleAdsResource.CAMPAIGNS) {
            return "/api/v5/campaigns";
        }
        String path = "/api/v5/campaigns/" + campaign + "/adgroups";
        if (resource != AppleAdsResource.ADGROUPS) {
            path +=
                    "/"
                            + group
                            + (resource == AppleAdsResource.KEYWORDS
                                    ? "/targetingkeywords"
                                    : "/ads");
        }
        return path;
    }

    private JsonNode data(AppleAdsResource resource, JsonNode response) {
        JsonNode result = response.path(config.platform() ? "result" : "data");
        if (result.isNull() || !resource.report()) {
            return result;
        }
        return config.platform()
                ? result.path("rows")
                : result.path("reportingDataResponse").path("row");
    }

    private static void rejectErrors(AppleAdsResource resource, JsonNode result) {
        if (!(result.isMissingNode() || result.isNull() || result.isArray() && result.isEmpty())) {
            throw resource.fail("response contained provider errors");
        }
    }

    private static int counter(
            AppleAdsResource resource, JsonNode pagination, String name, int minimum) {
        JsonNode value = pagination.path(name);
        if (!value.isIntegralNumber() || !value.canConvertToInt() || value.intValue() < minimum) {
            throw resource.fail("response lacks a valid pagination " + name);
        }
        return value.intValue();
    }

    private Row entity(AppleAdsResource resource, Long campaign, Long group, JsonNode item) {
        if (!item.isObject()) {
            throw resource.fail("expected an Apple Ads entity object");
        }
        long id = id(resource, item, "id");
        validateScope(resource, item, campaign, group);
        if (resource == AppleAdsResource.CAMPAIGNS) {
            campaign = id;
        } else if (resource == AppleAdsResource.ADGROUPS) {
            group = id;
        }
        return new Row(id, campaign, group, null, null, item, null);
    }

    /**
     * Projects one day's fact(s) from the report envelope and preserves available geography.
     *
     * <p>Platform's one-day shape has totalMetrics only; multi-day Platform and legacy daily
     * requests use their documented granular arrays, excluding any grand totals.
     */
    private void reportRows(
            AppleAdsResource resource,
            Long campaign,
            Window window,
            JsonNode item,
            List<Row> rows,
            Set<String> identities) {
        JsonNode metadata = item.path("metadata");
        if (!metadata.isObject()) {
            throw resource.fail("report row lacks entity metadata");
        }
        long entity = id(resource, metadata, config.platform() ? "id" : resource.legacyId);
        validateScope(resource, metadata, campaign, null);
        Long reportCampaign =
                resource == AppleAdsResource.CAMPAIGNS_REPORT_DAILY
                        ? Long.valueOf(entity)
                        : campaign;
        Long group =
                resource == AppleAdsResource.ADGROUPS_REPORT_DAILY
                        ? Long.valueOf(entity)
                        : optionalId(resource, metadata, "adGroupId");
        if (config.platform() && window.oneDay()) {
            JsonNode total = item.path("totalMetrics");
            if (total.isObject() && !total.isEmpty()) {
                accept(
                        resource,
                        metric(
                                resource,
                                entity,
                                reportCampaign,
                                group,
                                window.first,
                                metadata,
                                total),
                        rows,
                        identities);
            } else if (!(total.isNull() || total.isObject())) {
                throw resource.fail("one-day report lacks totalMetrics");
            }
            return;
        }
        JsonNode granular = item.path(config.platform() ? "granularMetrics" : "granularity");
        if (!granular.isArray()) {
            throw resource.fail("daily report lacks granular metrics");
        }
        for (JsonNode value : granular) {
            LocalDate day;
            try {
                day = LocalDate.parse(value.path("date").asText(""));
            } catch (DateTimeException ex) {
                throw resource.fail("daily report metric has an invalid date");
            }
            if (!value.isObject() || day.isBefore(window.first) || day.isAfter(window.last)) {
                throw resource.fail("daily report metric is outside the requested window");
            }
            accept(
                    resource,
                    metric(resource, entity, reportCampaign, group, day, metadata, value),
                    rows,
                    identities);
        }
    }

    private static Row metric(
            AppleAdsResource resource,
            long id,
            Long campaign,
            Long group,
            LocalDate date,
            JsonNode metadata,
            JsonNode values) {
        JsonNode geography = values.path("countryOrRegion");
        if (geography.isMissingNode() || geography.isNull()) {
            geography = metadata.path("countryOrRegion");
        }
        String country = null;
        if (!geography.isMissingNode() && !geography.isNull()) {
            if (!geography.isTextual()
                    || geography.textValue().isBlank()
                    || geography.textValue().length() > 32) {
                throw resource.fail("invalid countryOrRegion in report row");
            }
            country = geography.textValue();
        }
        return new Row(id, campaign, group, date, country, metadata, values);
    }

    private void validateScope(
            AppleAdsResource resource, JsonNode item, Long campaign, Long group) {
        Long account = optionalId(resource, item, config.mode.contextKey);
        Long fromCampaign = optionalId(resource, item, "campaignId");
        Long fromGroup = optionalId(resource, item, "adGroupId");
        if (account != null && !config.accountId.equals(account.toString())
                || campaign != null && fromCampaign != null && !campaign.equals(fromCampaign)
                || group != null && fromGroup != null && !group.equals(fromGroup)) {
            throw resource.fail("response row does not belong to the requested account or parent");
        }
    }

    private static Long optionalId(AppleAdsResource resource, JsonNode node, String field) {
        return node.hasNonNull(field) ? id(resource, node, field) : null;
    }

    private static long id(AppleAdsResource resource, JsonNode value, String field) {
        JsonNode number = value.path(field);
        if (!number.isIntegralNumber() || !number.canConvertToLong() || number.longValue() <= 0) {
            throw resource.fail("response has an invalid " + field);
        }
        return number.longValue();
    }

    private static void accept(
            AppleAdsResource resource, Row row, List<Row> rows, Set<String> ids) {
        String identity = row.id + "/" + row.day + "/" + row.country;
        if (!ids.add(identity)) {
            throw resource.fail("page contains duplicate Apple Ads row identities");
        }
        if (rows.size() >= 200000) {
            throw resource.fail("daily metric count exceeds the page safety limit");
        }
        rows.add(row);
    }

    /**
     * Holds one provider page and the next raw entity offset, independent of daily fact count.
     *
     * <p>The digest identifies equivalent page content for checkpoint restore.
     */
    static final class Page {
        final List<Row> rows;
        final int total;
        final int next;
        final String digest;

        Page(List<Row> rows, int total, int next, String digest) {
            this.rows = rows;
            this.total = total;
            this.next = next;
            this.digest = digest;
        }
    }

    /**
     * Retains exact provider data while binding each emitted row to its parent, day and geography.
     *
     * <p>Multiple report rows share their metadata node instead of duplicating large payloads.
     */
    static final class Row {
        final long id;
        final Long campaign;
        final Long group;
        final LocalDate day;
        final String country;
        final JsonNode metadata;
        final JsonNode metrics;

        Row(
                long id,
                Long campaign,
                Long group,
                LocalDate day,
                String country,
                JsonNode metadata,
                JsonNode metrics) {
            this.id = id;
            this.campaign = campaign;
            this.group = group;
            this.day = day;
            this.country = country;
            this.metadata = metadata;
            this.metrics = metrics;
        }

        String json(AppleAdsHttp http) {
            if (metrics == null) {
                return metadata.toString();
            }
            ObjectNode report = http.json.createObjectNode();
            report.set("metadata", metadata);
            report.set("metrics", metrics);
            return report.toString();
        }
    }

    /**
     * Closed date range for one report query.
     *
     * <p>The selected version treats a one-day range differently from a granular multi-day range.
     */
    static final class Window {
        final LocalDate first;
        final LocalDate last;

        Window(LocalDate first, LocalDate last) {
            this.first = first;
            this.last = last;
        }

        boolean oneDay() {
            return first.equals(last);
        }
    }
}
