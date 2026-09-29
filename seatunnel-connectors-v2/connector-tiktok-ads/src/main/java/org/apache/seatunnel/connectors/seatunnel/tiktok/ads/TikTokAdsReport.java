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

package org.apache.seatunnel.connectors.seatunnel.tiktok.ads;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonParser;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.StreamReadConstraints;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.api.table.type.DecimalType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

final class TikTokAdsReport {
    private static final ObjectMapper MAPPER =
            new ObjectMapper(
                            JsonFactory.builder()
                                    .streamReadConstraints(
                                            StreamReadConstraints.builder()
                                                    .maxNestingDepth(16)
                                                    .maxStringLength(4096)
                                                    .maxNumberLength(128)
                                                    .build())
                                    .build())
                    .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
                    .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
                    .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);

    private TikTokAdsReport() {}

    static Page parse(
            byte[] body, int requestedPage, TikTokAdsConfig config, Set<List<String>> seen)
            throws IOException {
        JsonNode root;
        try {
            root = MAPPER.readTree(body);
        } catch (IOException | RuntimeException e) {
            // Parser diagnostics can embed the entire response, including an echoed Access-Token.
            throw failure("invalid JSON response");
        }
        int code = integer(root, "code");
        if (code != 0) {
            throw failure(
                    "API returned nonzero code "
                            + code
                            + "; check authorization, request compatibility and quota");
        }
        JsonNode data = object(root, "data");
        JsonNode info = object(data, "page_info");
        int page = integer(info, "page");
        int pageSize = integer(info, "page_size");
        int totalPages = integer(info, "total_page");
        int total = integer(info, "total_number");
        JsonNode records = data.get("list");
        if (records == null
                || !records.isArray()
                || page != requestedPage
                || pageSize != config.getPageSize()
                || total > config.getMaxRows()) {
            throw failure("invalid page metadata or max_report_rows exceeded");
        }
        // Empty reports can advertise zero pages or the requested first page.
        int expectedPages = (int) ((total + (long) pageSize - 1) / pageSize);
        if (total == 0) {
            if (page != 1 || !records.isEmpty() || (totalPages != 0 && totalPages != 1)) {
                throw failure("inconsistent empty report metadata");
            }
        } else if (totalPages != expectedPages
                || page > totalPages
                || records.size() != Math.min(pageSize, total - (long) (page - 1) * pageSize)) {
            throw failure("inconsistent pagination counts");
        }
        List<SeaTunnelRow> rows = new ArrayList<>();
        SeaTunnelRowType type = config.getTable().getSeaTunnelRowType();
        for (JsonNode record : records) {
            JsonNode dimensions = object(record, "dimensions");
            JsonNode metrics = object(record, "metrics");
            List<String> key = new ArrayList<>();
            for (String name : config.getDimensions()) {
                String value = string(dimensions, name);
                if ("ad_id".equals(name) && !value.matches("[0-9]{1,30}")) {
                    throw failure("invalid ad_id dimension");
                }
                if ("stat_time_day".equals(name)) {
                    validateDay(value, config);
                }
                key.add(value);
            }
            if (!seen.add(key)) {
                throw failure("repeated dimension key; report pagination is not stable");
            }
            Object[] fields = new Object[type.getTotalFields()];
            for (int i = 0; i < fields.length; i++) {
                String name = type.getFieldName(i);
                if (config.getDimensions().contains(name)) {
                    fields[i] = string(dimensions, name);
                } else {
                    String value = string(metrics, name);
                    try {
                        if ("spend".equals(name)) {
                            DecimalType decimalType = (DecimalType) type.getFieldType(i);
                            if (!value.matches("[0-9]{1,38}(\\.[0-9]{1,38})?")) {
                                throw failure("invalid spend decimal");
                            }
                            BigDecimal decimal =
                                    new BigDecimal(value)
                                            .setScale(
                                                    decimalType.getScale(),
                                                    RoundingMode.UNNECESSARY);
                            if (decimal.precision() > decimalType.getPrecision()) {
                                throw failure("spend exceeds schema precision");
                            }
                            fields[i] = decimal;
                        } else {
                            if (!value.matches("[0-9]{1,19}")) {
                                throw failure("invalid count metric");
                            }
                            fields[i] = Long.parseLong(value);
                        }
                    } catch (ArithmeticException | NumberFormatException e) {
                        throw failure("metric cannot be represented exactly by schema");
                    }
                }
            }
            rows.add(new SeaTunnelRow(fields));
        }
        return new Page(total, totalPages, rows);
    }

    private static void validateDay(String value, TikTokAdsConfig config) throws IOException {
        // TikTok returns a report-local midnight label, not a UTC timestamp.
        if (!value.matches("[0-9]{4}-[0-9]{2}-[0-9]{2} 00:00:00")) {
            throw failure("invalid stat_time_day label");
        }
        try {
            LocalDate day = TikTokAdsConfig.date(value.substring(0, 10));
            if (day.isBefore(TikTokAdsConfig.date(config.getStartDate()))
                    || day.isAfter(TikTokAdsConfig.date(config.getEndDate()))) {
                throw failure("stat_time_day outside requested dates");
            }
        } catch (IllegalArgumentException e) {
            throw failure("invalid stat_time_day calendar date");
        }
    }

    private static JsonNode object(JsonNode parent, String field) throws IOException {
        JsonNode node = parent == null ? null : parent.get(field);
        if (node == null || !node.isObject()) {
            throw failure("missing or invalid " + field + " object");
        }
        return node;
    }

    private static int integer(JsonNode parent, String field) throws IOException {
        JsonNode node = parent == null ? null : parent.get(field);
        if (node == null
                || !node.isIntegralNumber()
                || !node.canConvertToInt()
                || node.intValue() < 0) {
            throw failure("missing or invalid " + field + " integer");
        }
        return node.intValue();
    }

    private static String string(JsonNode parent, String field) throws IOException {
        JsonNode node = parent.get(field);
        if (node == null || !node.isTextual() || node.textValue().isEmpty()) {
            throw failure("missing or invalid requested field " + field);
        }
        return node.textValue();
    }

    static IOException failure(String message) {
        return new IOException("TikTokAds: " + message);
    }

    static final class Page {
        final int total;
        final int totalPages;
        final List<SeaTunnelRow> rows;

        Page(int total, int totalPages, List<SeaTunnelRow> rows) {
            this.total = total;
            this.totalPages = totalPages;
            this.rows = rows;
        }
    }
}
