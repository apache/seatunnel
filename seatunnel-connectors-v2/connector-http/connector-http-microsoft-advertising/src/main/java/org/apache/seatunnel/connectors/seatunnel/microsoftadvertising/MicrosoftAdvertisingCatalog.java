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
package org.apache.seatunnel.connectors.seatunnel.microsoftadvertising;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Microsoft Advertising resource definitions and v13 reporting request capabilities.
 *
 * <p>Fixed variants preserve their selected column order. Custom variants use the official column
 * vocabulary for the chosen request object before any report is submitted.
 */
final class MicrosoftAdvertisingCatalog {
    private static final Map<String, ReportType> TYPES = reportTypes();
    private static final Map<String, Resource> FIXED = fixedResources();
    private static final Set<String> AGGREGATIONS =
            Set.of(
                    "Summary",
                    "Hourly",
                    "Daily",
                    "Weekly",
                    "Monthly",
                    "WeeklyStartingMonday",
                    "Yearly",
                    "DayOfWeek",
                    "HourOfDay");

    private static final Set<String> GROUPED_PERIODS =
            Set.of(
                    "Today",
                    "Yesterday",
                    "LastSevenDays",
                    "ThisMonth",
                    "LastMonth",
                    "LastThreeMonths",
                    "LastSixMonths");

    private MicrosoftAdvertisingCatalog() {}

    static List<Resource> select(List<String> requested, List<JsonNode> custom) {
        if (custom.size() > 100) {
            throw invalidCustom();
        }
        Map<String, Resource> resources = new LinkedHashMap<>(FIXED);
        for (JsonNode specification : custom) {
            Resource report = customReport(specification);
            if (resources.putIfAbsent(report.name, report) != null) {
                throw invalidCustom();
            }
        }
        if (requested.isEmpty()) {
            return List.copyOf(resources.values());
        }
        Set<String> seen = new HashSet<>();
        List<Resource> selected = new ArrayList<>();
        for (String name : requested) {
            Resource resource = resources.get(name);
            if (resource == null || !seen.add(name)) {
                throw new IllegalArgumentException("Invalid MicrosoftAdvertising option resources");
            }
            selected.add(resource);
        }
        return List.copyOf(selected);
    }

    private static Resource customReport(JsonNode config) {
        if (!config.isObject()) {
            throw invalidCustom();
        }
        String name = text(config, "name");
        String object = text(config, "reporting_object");
        String aggregation = text(config, "report_aggregation");
        JsonNode listed = config.path("report_columns");
        JsonNode exactName = config.path("disable_custom_report_names_camel_to_snake_conversion");
        ReportType type = TYPES.get(object);
        if (!exactName.isMissingNode() && !exactName.isBoolean()
                || name.length() > 100
                || !name.matches("[A-Za-z0-9][A-Za-z0-9 _.-]*")
                || type == null
                || object.equals("BudgetSummaryReportRequest")
                || !AGGREGATIONS.contains(aggregation)
                || !type.columns.contains("TimePeriod") && !aggregation.equals("Summary")
                || aggregation.equals("Hourly") && object.equals("ShareOfVoiceReportRequest")
                || !listed.isArray()
                || listed.isEmpty()
                || listed.size() > type.columns.size()) {
            throw invalidCustom();
        }
        if (!exactName.asBoolean(false)) {
            name =
                    name.replaceAll("([A-Z]+)([A-Z][a-z])", "$1_$2")
                            .replaceAll("([a-z0-9])([A-Z])", "$1_$2")
                            .replaceAll("[ .-]+", "_")
                            .toLowerCase(Locale.ROOT);
        }
        List<String> columns = new ArrayList<>();
        Set<String> seen = new HashSet<>();
        for (JsonNode value : listed) {
            String column = value.asText("");
            if (!value.isTextual() || !type.columns.contains(column) || !seen.add(column)) {
                throw invalidCustom();
            }
            columns.add(column);
        }
        if ("Summary".equals(aggregation)) {
            if (seen.contains("TimePeriod")) {
                throw invalidCustom();
            }
        } else if (type.columns.contains("TimePeriod") && !seen.contains("TimePeriod")) {
            columns.add("TimePeriod");
        }
        String period = null;
        JsonNode predefined = config.path("predefined_time");
        if (aggregation.equals("DayOfWeek") || aggregation.equals("HourOfDay")) {
            period = predefined.isMissingNode() ? "LastSevenDays" : predefined.asText("");
            if (!GROUPED_PERIODS.contains(period)) {
                throw invalidCustom();
            }
        } else if (!predefined.isMissingNode()) {
            throw invalidCustom();
        }
        return new Resource(
                name,
                Kind.REPORT,
                object,
                type.aggregation ? aggregation : null,
                type.time,
                null,
                columns,
                List.of(),
                period);
    }

    private static String text(JsonNode object, String name) {
        JsonNode value = object.path(name);
        if (!value.isTextual() || value.textValue().isBlank()) {
            throw invalidCustom();
        }
        return value.textValue();
    }

    private static IllegalArgumentException invalidCustom() {
        return new IllegalArgumentException("Invalid MicrosoftAdvertising option custom_reports");
    }

    private static Map<String, ReportType> reportTypes() {
        Map<String, ReportType> result = new LinkedHashMap<>();
        for (String[] row : rows("report-columns.tsv", 3)) {
            ReportType type = new ReportType(row[1], Set.of(row[2].split(",")));
            if (result.put(row[0], type) != null) {
                throw new IllegalStateException("Duplicate Microsoft Advertising request type");
            }
        }
        return Collections.unmodifiableMap(result);
    }

    private static Map<String, Resource> fixedResources() {
        Map<String, Resource> result = new LinkedHashMap<>();
        for (String name : List.of("accounts", "campaigns", "ad_groups", "ads")) {
            result.put(
                    name,
                    new Resource(name, Kind.ENTITY, null, null, false, null, List.of(), List.of()));
        }
        for (String[] row : rows("fixed-reports.tsv", 5)) {
            Resource report =
                    report(
                            row[0],
                            row[1],
                            row[2].equals("-") ? null : row[2],
                            List.of(row[3].split(",")),
                            row[4].equals("-") ? List.of() : List.of(row[4].split(",")));
            if (result.put(report.name, report) != null) {
                throw new IllegalStateException("Duplicate Microsoft Advertising resource");
            }
        }
        bulk(result, "labels", "Labels", "Label");
        bulk(result, "ad_group_labels", "AdGroupLabels", "Ad Group Label");
        bulk(result, "app_install_ad_labels", "AppInstallAdLabels", "App Install Ad Label");
        bulk(result, "app_install_ads", "AppInstallAds", "App Install Ad");
        bulk(result, "keywords", "Keywords", "Keyword");
        bulk(result, "keyword_labels", "KeywordLabels", "Keyword Label");
        bulk(result, "campaign_labels", "CampaignLabels", "Campaign Label");
        bulk(result, "budget", "Budgets", "Budget");
        if (result.size() != 77) {
            throw new IllegalStateException("Incomplete Microsoft Advertising resource catalog");
        }
        return Collections.unmodifiableMap(result);
    }

    private static void bulk(
            Map<String, Resource> resources, String name, String entity, String csvType) {
        List<String> identity =
                name.endsWith("_labels") ? List.of("Id", "Parent Id") : List.of("Id");
        resources.put(
                name,
                new Resource(name, Kind.BULK, entity, null, false, csvType, List.of(), identity));
    }

    private static Resource report(
            String name,
            String object,
            String aggregation,
            List<String> columns,
            List<String> identity) {
        ReportType type = TYPES.get(object);
        if (type == null || !type.columns.containsAll(columns)) {
            throw new IllegalStateException("Invalid Microsoft Advertising report catalog");
        }
        return new Resource(
                name, Kind.REPORT, object, aggregation, type.time, null, columns, identity);
    }

    private static List<String[]> rows(String filename, int fields) {
        InputStream input = MicrosoftAdvertisingCatalog.class.getResourceAsStream(filename);
        if (input == null) {
            throw new IllegalStateException(
                    "Missing Microsoft Advertising report catalog resource");
        }
        List<String[]> result = new ArrayList<>();
        try (BufferedReader reader =
                new BufferedReader(new InputStreamReader(input, StandardCharsets.UTF_8))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (!line.isBlank() && !line.startsWith("#")) {
                    String[] row = line.split("\t", -1);
                    if (row.length != fields) {
                        throw new IllegalStateException(
                                "Invalid Microsoft Advertising report catalog record");
                    }
                    result.add(row);
                }
            }
        } catch (IOException ex) {
            throw new IllegalStateException(
                    "Could not read the Microsoft Advertising report catalog");
        }
        return result;
    }

    /**
     * Selects direct paged reads and the two independent provider job protocols.
     *
     * <p>Each job protocol carries its own identifiers, scopes and file format.
     */
    enum Kind {
        ENTITY,
        REPORT,
        BULK
    }

    /**
     * Immutable configuration for a single output resource.
     *
     * <p>Request columns and optional identity dimensions have a defined order.
     */
    static final class Resource implements Serializable {
        private static final long serialVersionUID = 1L;

        final String name;
        final Kind kind;
        final String requestType;
        final String aggregation;
        final boolean reportTime;
        final String csvType;
        final List<String> columns;
        final List<String> identity;
        final String predefinedTime;

        Resource(
                String name,
                Kind kind,
                String requestType,
                String aggregation,
                boolean reportTime,
                String csvType,
                List<String> columns,
                List<String> identity) {
            this(
                    name,
                    kind,
                    requestType,
                    aggregation,
                    reportTime,
                    csvType,
                    columns,
                    identity,
                    null);
        }

        Resource(
                String name,
                Kind kind,
                String requestType,
                String aggregation,
                boolean reportTime,
                String csvType,
                List<String> columns,
                List<String> identity,
                String predefinedTime) {
            this.name = name;
            this.kind = kind;
            this.requestType = requestType;
            this.aggregation = aggregation;
            this.reportTime = reportTime;
            this.csvType = csvType;
            this.columns = List.copyOf(columns);
            this.identity = List.copyOf(identity);
            this.predefinedTime = predefinedTime;
        }

        LocalDate[] predefinedDates(LocalDate today) {
            LocalDate month = today.withDayOfMonth(1);
            switch (predefinedTime) {
                case "Today":
                    return new LocalDate[] {today, today};
                case "Yesterday":
                    return new LocalDate[] {today.minusDays(1), today.minusDays(1)};
                case "LastSevenDays":
                    return new LocalDate[] {today.minusDays(7), today.minusDays(1)};
                case "ThisMonth":
                    return new LocalDate[] {month, today};
                case "LastMonth":
                    return new LocalDate[] {month.minusMonths(1), month.minusDays(1)};
                case "LastThreeMonths":
                    return new LocalDate[] {month.minusMonths(3), month.minusDays(1)};
                case "LastSixMonths":
                    return new LocalDate[] {month.minusMonths(6), month.minusDays(1)};
                default:
                    throw new IllegalStateException(
                            "Unknown Microsoft Advertising predefined time");
            }
        }

        LocalDate oldest(LocalDate asOf) {
            if (!reportTime) {
                return LocalDate.MIN;
            }
            switch (requestType) {
                case "BudgetSummaryReportRequest":
                    return asOf.minusMonths(24);
                case "CallDetailReportRequest":
                case "SearchCampaignChangeHistoryReportRequest":
                case "ShareOfVoiceReportRequest":
                    return asOf.minusMonths(6);
                case "ProductMatchCountReportRequest":
                    return asOf;
                default:
                    if (!"Hourly".equals(aggregation)) {
                        return asOf.minusMonths(36);
                    }
                    switch (requestType) {
                        case "GeographicPerformanceReportRequest":
                        case "UserLocationPerformanceReportRequest":
                            return asOf.minusDays(14);
                        case "GoalsAndFunnelsReportRequest":
                        case "SearchQueryPerformanceReportRequest":
                        case "ProductSearchQueryPerformanceReportRequest":
                        case "DSASearchQueryPerformanceReportRequest":
                            return asOf.minusMonths(1);
                        case "PublisherUsagePerformanceReportRequest":
                            return asOf.minusMonths(2);
                        default:
                            return asOf.minusMonths(6);
                    }
            }
        }
    }

    /**
     * Request fields and column vocabulary documented for one v13 report object.
     *
     * <p>A fixed report can omit request members that its concrete object does not declare.
     */
    private static final class ReportType {
        final boolean aggregation;
        final boolean time;
        final Set<String> columns;

        ReportType(String flags, Set<String> columns) {
            aggregation = flags.charAt(0) == 'a';
            time = flags.charAt(1) == 't';
            this.columns = columns;
        }
    }
}
