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
package org.apache.seatunnel.connectors.seatunnel.campaignmonitor;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.connectors.seatunnel.campaignmonitor.CampaignMonitorResource.Shape;
import org.apache.seatunnel.connectors.seatunnel.campaignmonitor.CampaignMonitorSplit.Frame;

import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.ResolverStyle;
import java.time.temporal.ChronoField;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Applies the v3.3 response and paging contracts before handing rows to the reader.
 *
 * <p>All routes are built from a validated client ID and the connector's fixed read path.
 */
final class CampaignMonitorApi {
    private static final DateTimeFormatter SENT_DATE =
            new DateTimeFormatterBuilder()
                    .appendPattern("uuuu-MM-dd HH:mm:ss")
                    .optionalStart()
                    .appendFraction(ChronoField.NANO_OF_SECOND, 1, 9, true)
                    .optionalEnd()
                    .toFormatter(Locale.ROOT)
                    .withResolverStyle(ResolverStyle.STRICT);

    private final CampaignMonitorConfig config;
    private final CampaignMonitorHttp http;

    CampaignMonitorApi(CampaignMonitorConfig config, CampaignMonitorHttp http) {
        this.config = config;
        this.http = http;
    }

    Page list(Frame frame, long deadline) throws InterruptedException {
        CampaignMonitorResource resource = frame.resource;
        JsonNode response = http.request(resource, target(frame), deadline);
        if (response.isObject() && response.has("Code") && response.has("Message")) {
            throw resource.failure("API returned an error instead of records");
        }
        List<Row> rows;
        int next = 0;
        if (resource.shape == Shape.OBJECT) {
            rows = List.of(clientDetails(resource, response, frame.clientId));
        } else {
            JsonNode items = resource.shape == Shape.PAGED ? response.path("Results") : response;
            if (!items.isArray()) {
                throw resource.failure("expected an array of records");
            }
            int limit = resource.shape == Shape.PAGED ? config.pageSize : config.maxItems;
            if (items.size() > limit) {
                throw resource.failure("response exceeds the configured record count");
            }
            if (resource.shape == Shape.PAGED) {
                next = nextPage(frame, response, items.size());
            }
            rows = new ArrayList<>(items.size());
            for (JsonNode item : items) {
                http.remaining(resource, deadline);
                Row row = row(resource, item);
                if (row != null) {
                    rows.add(row);
                }
            }
        }
        String digest = CampaignMonitorConfig.hash(response.toString());
        http.remaining(resource, deadline);
        return new Page(rows, next, digest);
    }

    private String target(Frame frame) {
        CampaignMonitorResource resource = frame.resource;
        String path = resource.path(frame.clientId);
        if (resource.shape != Shape.PAGED) {
            return path;
        }
        String query = path + "?page=" + frame.page + "&pagesize=" + config.pageSize;
        if (resource == CampaignMonitorResource.SENT_CAMPAIGNS) {
            query += "&sentFromDate=" + config.start;
            if (config.end != null) {
                query += "&sentToDate=" + config.end;
            }
        }
        return query;
    }

    private int nextPage(Frame frame, JsonNode response, int items) {
        CampaignMonitorResource resource = frame.resource;
        long page = count(resource, response, "PageNumber");
        long size = count(resource, response, "PageSize");
        long records = count(resource, response, "RecordsOnThisPage");
        long total = count(resource, response, "TotalNumberOfRecords");
        long pages = count(resource, response, "NumberOfPages");
        long expectedPages = total == 0 ? 0 : 1 + (total - 1) / config.pageSize;
        long beginning = (long) (frame.page - 1) * config.pageSize;
        boolean empty = total == 0 && frame.page == 1 && pages <= 1;
        if (page != frame.page
                || size != config.pageSize
                || records != items
                || records > Math.max(0, total - beginning)
                || !empty && (pages != expectedPages || frame.page > pages)) {
            throw resource.failure("inconsistent pagination metadata");
        }
        return page < pages ? frame.page + 1 : 0;
    }

    private static long count(CampaignMonitorResource resource, JsonNode data, String key) {
        JsonNode value = data.path(key);
        if (!value.isIntegralNumber() || !value.canConvertToLong() || value.longValue() < 0) {
            throw resource.failure("invalid pagination " + key);
        }
        return value.longValue();
    }

    private static Row clientDetails(
            CampaignMonitorResource resource, JsonNode response, String clientId) {
        if (!response.isObject()) {
            throw resource.failure("expected a client details object");
        }
        String reported = text(resource, response.path("BasicDetails"), "ClientID");
        if (!clientId.equals(reported)) {
            throw resource.failure("client details do not match the requested client");
        }
        // The details API can return a separate credential capable of authenticating requests.
        ((ObjectNode) response).remove(List.of("ApiKey", "apiKey", "api_key"));
        return new Row(reported, null, response.toString());
    }

    private Row row(CampaignMonitorResource resource, JsonNode item) {
        if (!item.isObject()) {
            throw resource.failure("expected an object for a record");
        }
        String id = text(resource, item, resource.identity);
        if (resource == CampaignMonitorResource.CLIENTS && !CampaignMonitorResource.clientKey(id)) {
            throw resource.failure("invalid ClientID path identifier");
        }
        String sent = null;
        if (resource == CampaignMonitorResource.SENT_CAMPAIGNS) {
            sent = text(resource, item, "SentDate");
            LocalDate date;
            try {
                date = LocalDateTime.parse(sent, SENT_DATE).toLocalDate();
            } catch (DateTimeException ex) {
                throw resource.failure("invalid SentDate calendar timestamp");
            }
            if (date.isBefore(config.start) || config.end != null && !date.isBefore(config.end)) {
                return null;
            }
        }
        return new Row(id, sent, item.toString());
    }

    private static String text(CampaignMonitorResource resource, JsonNode record, String field) {
        JsonNode value = record.path(field);
        if (!value.isTextual()
                || value.textValue().isBlank()
                || value.textValue().length() > 4096
                || value.textValue().codePoints().anyMatch(Character::isISOControl)) {
            throw resource.failure("missing or invalid " + field);
        }
        return value.textValue();
    }

    /**
     * Holds confirmed output rows and their native page boundary.
     *
     * <p>The digest includes pagination and any rows outside the configured date filter.
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
     * Carries the native identity, optional sent time and exact JSON record.
     *
     * <p>The sent time remains a provider wall-clock string.
     */
    static final class Row {
        final String id;
        final String sent;
        final String json;

        Row(String id, String sent, String json) {
            this.id = id;
            this.sent = sent;
            this.json = json;
        }
    }
}
