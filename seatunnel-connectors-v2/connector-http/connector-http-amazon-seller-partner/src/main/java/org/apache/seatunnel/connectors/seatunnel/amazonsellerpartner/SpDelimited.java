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

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import java.math.BigDecimal;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.YearMonth;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Decodes Amazon's tab-delimited reports, including localized feedback headers.
 *
 * <p>Text is preserved exactly unless a declared report field uses a numeric or temporal type.
 * Malformed quoting and inconsistent columns fail before the report advances.
 */
final class SpDelimited {
    private static final List<String> FEEDBACK =
            List.of("date", "rating", "comments", "response", "order_id", "rater_email");
    private static final Set<String> NUMERIC =
            Set.of(
                    "item-price",
                    "item-tax",
                    "shipping-price",
                    "shipping-tax",
                    "gift-wrap-price",
                    "gift-wrap-tax",
                    "item-promotion-discount",
                    "ship-promotion-discount",
                    "amount",
                    "quantity",
                    "quantity-purchased",
                    "quantity-shipped",
                    "quantity-received",
                    "quantity-available",
                    "total-amount",
                    "unit-price",
                    "price",
                    "tax",
                    "fee",
                    "estimated_monthly_storage_fee",
                    "average_quantity_on_hand",
                    "quantity-for-local-fulfillment",
                    "quantity available",
                    "quantity shipped",
                    "requested-quantity",
                    "cancelled-quantity",
                    "quantity-to-ship",
                    "label cost",
                    "starting warehouse balance",
                    "ending warehouse balance");
    private static final Set<String> SETTLEMENT_TIMES =
            Set.of(
                    "settlement-start-date",
                    "settlement-end-date",
                    "deposit-date",
                    "posted-date-time");
    private static final Map<String, ZoneOffset> ZONES =
            Map.ofEntries(
                    Map.entry("UTC", ZoneOffset.UTC),
                    Map.entry("GMT", ZoneOffset.UTC),
                    Map.entry("PST", ZoneOffset.ofHours(-8)),
                    Map.entry("PDT", ZoneOffset.ofHours(-7)),
                    Map.entry("EST", ZoneOffset.ofHours(-5)),
                    Map.entry("EDT", ZoneOffset.ofHours(-4)),
                    Map.entry("CET", ZoneOffset.ofHours(1)),
                    Map.entry("CEST", ZoneOffset.ofHours(2)),
                    Map.entry("JST", ZoneOffset.ofHours(9)));
    private static final DateTimeFormatter WALL_TIME =
            DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss");
    private static final int MAX_COLUMNS = 1024;
    private static final int MAX_FIELD = 1024 * 1024;

    private final SpSettings settings;
    private final String text;
    private int offset;

    SpDelimited(SpSettings settings, String text) {
        this.settings = settings;
        this.text = text;
        offset = text.startsWith("\ufeff") ? 1 : 0;
    }

    List<ObjectNode> rows() throws SpFailure {
        List<ObjectNode> result = new ArrayList<>();
        List<String> columns = nextNonempty();
        if (columns == null) {
            return result;
        }
        if (settings.resource.format == SpResource.Format.FEEDBACK) {
            if (columns.size() != FEEDBACK.size()) {
                throw invalid("feedback header does not match the documented layout");
            }
            columns = FEEDBACK;
        } else if (new HashSet<>(columns).size() != columns.size()
                || columns.stream().anyMatch(String::isBlank)) {
            throw invalid("empty or duplicate tabular field name");
        }
        List<String> input;
        while ((input = nextNonempty()) != null) {
            if (input.size() != columns.size()) {
                throw invalid("tabular record does not match its header");
            }
            if (result.size() >= settings.maxRows) {
                throw invalid("report exceeds max_report_rows");
            }
            ObjectNode row = SpJson.JSON.createObjectNode();
            for (int i = 0; i < columns.size(); i++) {
                addField(row, columns.get(i), input.get(i));
            }
            result.add(row);
        }
        return result;
    }

    private List<String> nextNonempty() throws SpFailure {
        while (offset < text.length()) {
            if (Thread.currentThread().isInterrupted()) {
                throw invalid("tabular report decoding interrupted");
            }
            List<String> row = next();
            if (row.size() != 1 || !row.get(0).isEmpty()) {
                return row;
            }
        }
        return null;
    }

    private List<String> next() throws SpFailure {
        List<String> row = new ArrayList<>();
        for (; ; ) {
            String value = field();
            row.add(value);
            if (row.size() > MAX_COLUMNS) {
                throw invalid("tabular report has too many columns");
            }
            if (offset == text.length()) {
                return row;
            }
            char delimiter = text.charAt(offset++);
            if (delimiter == '\n') {
                return row;
            }
            if (delimiter == '\r') {
                if (offset < text.length() && text.charAt(offset) == '\n') {
                    offset++;
                }
                return row;
            }
            if (delimiter != '\t') {
                throw invalid("tabular quoted value has trailing data");
            }
        }
    }

    private String field() throws SpFailure {
        StringBuilder value = new StringBuilder();
        boolean quoted = offset < text.length() && text.charAt(offset) == '"';
        if (quoted) {
            offset++;
        }
        while (offset < text.length()) {
            char current = text.charAt(offset);
            if (current == '"') {
                if (!quoted) {
                    throw invalid("unexpected quote in tabular field");
                }
                offset++;
                if (offset == text.length() || text.charAt(offset) != '"') {
                    return value.toString();
                }
            } else if (!quoted && (current == '\r' || current == '\n' || current == '\t')) {
                return value.toString();
            }
            value.append(current);
            offset++;
            if (value.length() > MAX_FIELD) {
                throw invalid("tabular field exceeds its size limit");
            }
        }
        if (quoted) {
            throw invalid("unclosed quoted field in tabular report");
        }
        return value.toString();
    }

    private void addField(ObjectNode row, String name, String value) throws SpFailure {
        if (NUMERIC.contains(name.toLowerCase(Locale.ROOT))
                || (settings.resource.format == SpResource.Format.FEEDBACK
                        && "rating".equals(name))) {
            if (value.isBlank()) {
                row.putNull(name);
            } else {
                try {
                    row.put(name, new BigDecimal(value));
                } catch (NumberFormatException e) {
                    throw invalid("invalid numeric value in a documented report field");
                }
            }
            return;
        }
        try {
            if (settings.resource.format == SpResource.Format.FEEDBACK
                    && "date".equals(name)
                    && !value.isBlank()) {
                row.put(name, feedbackDate(value).toString());
            } else if (settings.resource == SpResource.GET_LEDGER_DETAIL_VIEW_DATA
                    && "Date".equals(name)
                    && !value.isBlank()) {
                row.put(name, ledgerDate(value).toString());
            } else if (settings.resource == SpResource.GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE_V2
                    && (SETTLEMENT_TIMES.contains(name) || "posted-date".equals(name))) {
                if (value.isBlank()) {
                    row.putNull(name);
                } else {
                    row.put(
                            name,
                            "posted-date".equals(name)
                                    ? LocalDate.parse(value).toString()
                                    : normalizeTimestamp(value));
                }
            } else if ("open-date".equals(name) && isListingReport() && !value.isBlank()) {
                row.put(
                        name,
                        settings.resource == SpResource.GET_MERCHANTS_LISTINGS_FYP_REPORT
                                ? LocalDate.parse(normalizeTimestamp(value).substring(0, 10))
                                        .toString()
                                : normalizeTimestamp(value));
            } else {
                row.put(name, value);
            }
        } catch (DateTimeException e) {
            throw invalid("invalid date or timestamp in a documented report field");
        }
    }

    private boolean isListingReport() {
        return settings.resource.name().contains("LISTINGS");
    }

    private static LocalDate ledgerDate(String text) {
        if (text.matches("[0-9]{1,2}/[0-9]{4}")) {
            return YearMonth.parse(text, DateTimeFormatter.ofPattern("M/uuuu")).atDay(1);
        }
        if (text.contains("/")) {
            return LocalDate.parse(text, DateTimeFormatter.ofPattern("M/d/uuuu"));
        }
        return LocalDate.parse(text);
    }

    private LocalDate feedbackDate(String text) {
        if (text.matches("[0-9]{4}-[0-9]{2}-[0-9]{2}")) {
            return LocalDate.parse(text);
        }
        String[] parts = text.split("DE".equals(settings.region) ? "\\." : "/", -1);
        if (parts.length != 3) {
            throw new DateTimeException("invalid localized report date");
        }
        String yearText = "JP".equals(settings.region) ? parts[0] : parts[2];
        boolean yearFirst = "JP".equals(settings.region);
        boolean monthFirst = "US".equals(settings.region);
        try {
            int year = Integer.parseInt(yearText);
            if (yearText.length() == 2) {
                year += year >= 69 ? 1900 : 2000;
            } else if (yearText.length() != 4) {
                throw new DateTimeException("invalid localized report year");
            }
            int month = Integer.parseInt(monthFirst ? parts[0] : parts[1]);
            int day = Integer.parseInt(yearFirst ? parts[2] : monthFirst ? parts[1] : parts[0]);
            return LocalDate.of(year, month, day);
        } catch (NumberFormatException e) {
            throw new DateTimeException("invalid localized report date");
        }
    }

    private static String normalizeTimestamp(String value) {
        try {
            return OffsetDateTime.parse(value).toString();
        } catch (DateTimeException ignored) {
            if (value.length() == 10) {
                return LocalDate.parse(value).toString();
            }
            int space = value.lastIndexOf(' ');
            ZoneOffset zone = space < 0 ? null : ZONES.get(value.substring(space + 1));
            if (zone == null) {
                throw new DateTimeException("unsupported report timestamp zone");
            }
            return LocalDateTime.parse(value.substring(0, space), WALL_TIME)
                    .atOffset(zone)
                    .toString();
        }
    }

    private SpFailure invalid(String reason) {
        return new SpFailure(settings.resource.name(), reason);
    }
}
