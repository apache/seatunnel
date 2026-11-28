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

package org.apache.seatunnel.connectors.seatunnel.appsflyer;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Validates and stages one CSV report before any row can reach the engine. A size ceiling
 * invalidates the entire window and allows a smaller exact request.
 */
final class AppsFlyerRecords {
    private static final Set<String> BOOLEANS =
            Set.of(
                    "is_lat",
                    "is_primary_attribution",
                    "is_receipt_validated",
                    "is_retargeting",
                    "store_reinstall",
                    "wifi");
    private static final Set<String> RAW_TIMES =
            Set.of(
                    "attributed_touch_time",
                    "install_time",
                    "event_time",
                    "contributor1_touch_time",
                    "contributor2_touch_time",
                    "contributor3_touch_time",
                    "gp_click_time",
                    "gp_install_begin",
                    "device_download_time");
    private static final Set<String> NUMBERS =
            Set.of(
                    "average_ecpi",
                    "average_revenue_per_user",
                    "clicks",
                    "click_through_rate",
                    "conversion_rate",
                    "conversions",
                    "impressions",
                    "installs",
                    "loyal_users",
                    "loyal_users_rate",
                    "return_on_investment",
                    "sessions",
                    "total_cost",
                    "total_revenue");
    private static final Set<String> EVENT_KEYS =
            Set.of("event_name", "event_unique_users", "event_counter", "event_sales");
    private static final Set<String> PERCENTAGES =
            Set.of(
                    "click_through_rate",
                    "conversion_rate",
                    "loyal_users_rate",
                    "return_on_investment");
    private static final Map<String, String> ALIASES =
            Map.ofEntries(
                    Map.entry("ad", "af_ad"),
                    Map.entry("ad_id", "af_ad_id"),
                    Map.entry("ad_type", "af_ad_type"),
                    Map.entry("adset", "af_adset"),
                    Map.entry("adset_id", "af_adset_id"),
                    Map.entry("agency_pmd_af_prt", "af_prt"),
                    Map.entry("amazon_fire_id", "amazon_aid"),
                    Map.entry("arpu", "average_revenue_per_user"),
                    Map.entry("attribution_lookback", "af_attribution_lookback"),
                    Map.entry("campaign_c", "campaign"),
                    Map.entry("campaign_id", "af_c_id"),
                    Map.entry("channel", "af_channel"),
                    Map.entry("cost_currency", "af_cost_currency"),
                    Map.entry("cost_model", "af_cost_model"),
                    Map.entry("cost_value", "af_cost_value"),
                    Map.entry("ctr", "click_through_rate"),
                    Map.entry("google_play_broadcast_referrer", "gp_broadcast_referrer"),
                    Map.entry("google_play_click_time", "gp_click_time"),
                    Map.entry("google_play_install_begin_time", "gp_install_begin"),
                    Map.entry("google_play_referrer", "gp_referrer"),
                    Map.entry("keywords", "af_keywords"),
                    Map.entry("loyal_users_installs", "loyal_users_rate"),
                    Map.entry("media_source_pid", "media_source"),
                    Map.entry("partner", "af_prt"),
                    Map.entry("reengagement_window", "af_reengagement_window"),
                    Map.entry("roi", "return_on_investment"),
                    Map.entry("site_id", "af_siteid"),
                    Map.entry("sub_site_id", "af_sub_siteid"));
    private static final Pattern NON_NAME = Pattern.compile("[^\\p{L}\\p{N}]+");
    private static final Pattern CONTRIBUTOR = Pattern.compile("contributor_([123])_(.+)");
    private static final Pattern SUB_PARAMETER = Pattern.compile("sub_param_([1-5])");
    private static final Pattern EVENT =
            Pattern.compile(
                    "\\s*\\((unique users|event counter|sales in usd)\\)$",
                    Pattern.CASE_INSENSITIVE);
    private static final JsonNodeFactory NODES = JsonNodeFactory.withExactBigDecimals(true);
    private static final String ADDITIONAL = "additional_columns";

    private AppsFlyerRecords() {}

    /**
     * Checks the header, UTF-8, row count, date bounds and all selected typed record values. A
     * provider record cap or connector staging limit is never considered a complete report.
     */
    static Snapshot parse(InputStream input, AppsFlyerConfig config, AppsFlyerWindow window)
            throws IOException {
        AppsFlyerCsv csv = new AppsFlyerCsv(input, config.maxField);
        List<String> header = csv.read();
        while (blank(header)) {
            header = csv.read();
        }
        Layout layout = new Layout(header, window);
        int cap = Math.min(window.report.providerCap, config.maxWindowRecords);
        List<Row> rows = new ArrayList<>();
        List<String> record;
        int physical = 0;
        long bytes = 0;
        while ((record = csv.read()) != null) {
            if (blank(record)) {
                continue;
            }
            if (++physical >= cap) {
                throw new Saturated("report reached its row threshold");
            }
            if (record.size() != layout.columns.size()) {
                throw new InvalidReport("CSV record width does not match the report header");
            }
            String time = cursor(record.get(layout.cursor), window, config.zone);
            ObjectNode data = NODES.objectNode();
            ObjectNode additional = NODES.objectNode();
            ObjectNode unmappedContext = NODES.objectNode();
            for (int i = 0; i < record.size(); i++) {
                String key = layout.keys[i];
                String value = record.get(i);
                if (key == null) {
                    additional.put(layout.columns.get(i), value);
                    if (!layout.eventColumns[i]) {
                        unmappedContext.put(layout.columns.get(i), value);
                    }
                } else {
                    put(data, key, value, window.report.raw);
                }
            }
            for (AppsFlyerResource resource : config.selected) {
                if (resource.report != window.report) {
                    continue;
                }
                if (resource.events) {
                    for (Map.Entry<String, int[]> event : layout.events.entrySet()) {
                        int[] indices = event.getValue();
                        ObjectNode report = data.deepCopy();
                        report.put("event_name", event.getKey());
                        putMetric(report, "event_unique_users", record, indices[0]);
                        putMetric(report, "event_counter", record, indices[1]);
                        putMetric(report, "event_sales", record, indices[2]);
                        if (!unmappedContext.isEmpty()) {
                            report.set(ADDITIONAL, unmappedContext);
                        }
                        bytes = add(rows, resource, time, report, bytes, config);
                    }
                } else {
                    ObjectNode report = data.deepCopy();
                    if (!additional.isEmpty()) {
                        report.set(ADDITIONAL, additional);
                    }
                    bytes = add(rows, resource, time, report, bytes, config);
                }
            }
        }
        return new Snapshot(rows);
    }

    private static long add(
            List<Row> rows,
            AppsFlyerResource resource,
            String time,
            ObjectNode data,
            long storedBytes,
            AppsFlyerConfig config)
            throws IOException {
        String payload = data.toString();
        long total = storedBytes + payload.getBytes(StandardCharsets.UTF_8).length;
        if (rows.size() >= config.maxWindowRecords || total > config.maxBuffer) {
            throw new Saturated("report exceeds its staged output limit");
        }
        rows.add(new Row(resource, time, payload));
        return total;
    }

    private static void putMetric(ObjectNode out, String key, List<String> row, int index)
            throws IOException {
        if (index < 0) {
            out.putNull(key);
        } else {
            putNumber(out, key, row.get(index), false);
        }
    }

    private static void put(ObjectNode out, String key, String value, boolean raw)
            throws IOException {
        if (raw && BOOLEANS.contains(key)) {
            if (typedNull(value)) {
                out.putNull(key);
            } else if ("true".equalsIgnoreCase(value) || "1".equals(value)) {
                out.put(key, true);
            } else if ("false".equalsIgnoreCase(value) || "0".equals(value)) {
                out.put(key, false);
            } else {
                throw new InvalidReport("invalid typed boolean in CSV record");
            }
        } else if (raw && RAW_TIMES.contains(key)) {
            if (typedNull(value)) {
                out.putNull(key);
            } else {
                out.put(key, time(value).normalized);
            }
        } else if (!raw && NUMBERS.contains(key)) {
            putNumber(out, key, value, PERCENTAGES.contains(key));
        } else if (!raw && "date".equals(key)) {
            try {
                out.put(key, LocalDate.parse(value).toString());
            } catch (DateTimeException e) {
                throw new InvalidReport("invalid aggregate date");
            }
        } else {
            out.put(key, value);
        }
    }

    private static boolean typedNull(String value) {
        return value.isEmpty() || "N/A".equals(value) || "NULL".equals(value);
    }

    private static void putNumber(ObjectNode out, String key, String value, boolean percentage)
            throws IOException {
        if (typedNull(value)) {
            out.putNull(key);
            return;
        }
        try {
            String numeric =
                    percentage && value.endsWith("%")
                            ? value.substring(0, value.length() - 1)
                            : value;
            if (numeric.length() > 1024) {
                throw new NumberFormatException();
            }
            BigDecimal number = new BigDecimal(numeric);
            if (Math.abs((long) number.scale()) > 1024) {
                throw new NumberFormatException();
            }
            out.put(key, number);
        } catch (NumberFormatException e) {
            throw new InvalidReport("invalid exact decimal in CSV record");
        }
    }

    private static String cursor(String value, AppsFlyerWindow window, ZoneId zone)
            throws IOException {
        if (window.report.raw) {
            Timestamp parsed = time(value);
            if (parsed.absolute != null) {
                if (parsed.absolute.isBefore(window.start)
                        || !parsed.absolute.isBefore(window.end)) {
                    throw new InvalidReport("raw timestamp falls outside the requested window");
                }
            } else if (zone.getRules().getValidOffsets(parsed.local).isEmpty()) {
                throw new InvalidReport("raw timestamp does not exist in the configured timezone");
            } else if (parsed.local.isBefore(LocalDateTime.ofInstant(window.start, zone))
                    || !parsed.local.isBefore(LocalDateTime.ofInstant(window.end, zone))) {
                throw new InvalidReport("raw timestamp falls outside the requested local window");
            }
            return parsed.normalized;
        }
        try {
            LocalDate date = LocalDate.parse(value);
            if (date.isBefore(window.start.atZone(zone).toLocalDate())
                    || !date.isBefore(window.end.atZone(zone).toLocalDate())) {
                throw new InvalidReport("aggregate date falls outside the requested window");
            }
            return date.toString();
        } catch (DateTimeException e) {
            throw new InvalidReport("invalid aggregate date");
        }
    }

    private static Timestamp time(String value) throws IOException {
        String iso = value.replace(' ', 'T');
        try {
            return new Timestamp(OffsetDateTime.parse(iso).toInstant(), null);
        } catch (DateTimeException ignored) {
            try {
                return new Timestamp(null, LocalDateTime.parse(iso));
            } catch (DateTimeException e) {
                throw new InvalidReport("invalid timestamp in CSV record");
            }
        }
    }

    private static boolean blank(List<String> record) {
        return record != null && record.size() == 1 && record.get(0).isEmpty();
    }

    private static String normalize(String label) {
        String key = NON_NAME.matcher(label.strip().toLowerCase(Locale.ROOT)).replaceAll("_");
        key = key.replaceFirst("^_+|_+$", "");
        Matcher contributor = CONTRIBUTOR.matcher(key);
        if (contributor.matches()) {
            String suffix = contributor.group(2);
            return "contributor"
                    + contributor.group(1)
                    + ("partner".equals(suffix) ? "_af_prt" : "_" + suffix);
        }
        Matcher index = SUB_PARAMETER.matcher(key);
        return index.matches() ? "af_sub" + index.group(1) : ALIASES.getOrDefault(key, key);
    }

    /**
     * Maps provider columns and event suffixes without losing unknown or colliding column names. A
     * known cursor in the header is required to distinguish reports from 200 error bodies.
     */
    private static final class Layout {
        private final List<String> columns;
        private final String[] keys;
        private final boolean[] eventColumns;
        private final Map<String, int[]> events = new LinkedHashMap<>();
        private final int cursor;

        private Layout(List<String> columns, AppsFlyerWindow window) throws IOException {
            if (columns == null || columns.size() < 2) {
                throw new InvalidReport("expected a complete CSV report header");
            }
            this.columns = columns;
            keys = new String[columns.size()];
            eventColumns = new boolean[columns.size()];
            Set<String> used = new HashSet<>();
            Set<String> rawNames = new HashSet<>();
            used.add(ADDITIONAL);
            if (!window.report.raw) {
                used.addAll(EVENT_KEYS);
            }
            int cursorIndex = -1;
            for (int i = 0; i < columns.size(); i++) {
                String name = columns.get(i);
                if (name.isBlank() || !rawNames.add(name)) {
                    throw new InvalidReport("empty or duplicate CSV column name");
                }
                Matcher metric = EVENT.matcher(name);
                if (!window.report.raw && metric.find() && metric.start() > 0) {
                    addEvent(name.substring(0, metric.start()).strip(), metric.group(1), i);
                    eventColumns[i] = true;
                    continue;
                }
                String normalized = normalize(name);
                if (window.report.cursor.equals(normalized) && cursorIndex >= 0) {
                    throw new InvalidReport("ambiguous cursor column in CSV report");
                }
                if (!normalized.isEmpty() && used.add(normalized)) {
                    keys[i] = normalized;
                    if (window.report.cursor.equals(normalized)) {
                        cursorIndex = i;
                    }
                }
            }
            if (cursorIndex < 0) {
                throw new InvalidReport("CSV report is missing its required cursor column");
            }
            cursor = cursorIndex;
        }

        private void addEvent(String name, String suffix, int index) throws IOException {
            int metric;
            switch (suffix.toLowerCase(Locale.ROOT)) {
                case "unique users":
                    metric = 0;
                    break;
                case "event counter":
                    metric = 1;
                    break;
                default:
                    metric = 2;
            }
            int[] entry = events.computeIfAbsent(name, ignored -> new int[] {-1, -1, -1});
            if (name.isEmpty() || entry[metric] >= 0) {
                throw new InvalidReport("duplicate or unnamed event metric in CSV header");
            }
            entry[metric] = index;
        }
    }

    /**
     * Preserves absolute offsets when supplied and leaves offset-free provider times local. Query
     * bound comparison uses the corresponding absolute or wall-clock representation.
     */
    private static final class Timestamp {
        final Instant absolute;
        final LocalDateTime local;
        final String normalized;

        Timestamp(Instant absolute, LocalDateTime local) {
            this.absolute = absolute;
            this.local = local;
            normalized = absolute == null ? local.toString() : absolute.toString();
        }
    }

    /**
     * Represents a CSV structure or value failure whose reason is safe for job logs. Parsing
     * diagnostics omit all raw fields and header values.
     */
    static final class InvalidReport extends IOException {
        InvalidReport(String reason) {
            super(reason);
        }
    }

    /**
     * Marks a full or oversized report that can safely be split before output. The reader fails if
     * the provider cannot represent smaller non-overlapping bounds.
     */
    static final class Saturated extends IOException {
        Saturated(String reason) {
            super(reason);
        }
    }

    /**
     * Carries one selected resource view of a validated CSV record. Its exact data and original
     * report-time semantics are preserved as strings.
     */
    static final class Row {
        final AppsFlyerResource resource;
        final String time;
        final String payload;

        Row(AppsFlyerResource resource, String time, String payload) {
            this.resource = resource;
            this.time = time;
            this.payload = payload;
        }
    }

    /**
     * Stores the validated output and a digest used only for completed-response row replay. A
     * partially emitted result must match every remaining row and their order.
     */
    static final class Snapshot {
        final List<Row> rows;
        final String digest;

        Snapshot(List<Row> rows) {
            this.rows = rows;
            MessageDigest hash = AppsFlyerConfig.digest();
            for (Row row : rows) {
                hash.update((byte) row.resource.ordinal());
                byte[] bytes = row.payload.getBytes(StandardCharsets.UTF_8);
                hash.update(ByteBuffer.allocate(Integer.BYTES).putInt(bytes.length).array());
                hash.update(bytes);
            }
            digest = AppsFlyerConfig.digestId(hash.digest());
        }
    }
}
