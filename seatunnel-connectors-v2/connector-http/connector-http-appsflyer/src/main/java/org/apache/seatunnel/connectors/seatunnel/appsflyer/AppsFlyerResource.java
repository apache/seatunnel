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

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.TreeSet;

/**
 * Defines the eighteen output resources over the fourteen AppsFlyer v5 requests. Event views use
 * the same complete CSV as their matching base report.
 */
enum AppsFlyerResource {
    IN_APP_EVENTS(Report.IN_APP_EVENTS),
    ORGANIC_IN_APP_EVENTS(Report.ORGANIC_IN_APP_EVENTS),
    RETARGETING_IN_APP_EVENTS(Report.RETARGETING_IN_APP_EVENTS),
    INSTALLS(Report.INSTALLS),
    ORGANIC_INSTALLS(Report.ORGANIC_INSTALLS),
    RETARGETING_INSTALLS(Report.RETARGETING_INSTALLS),
    UNINSTALL_EVENTS(Report.UNINSTALL_EVENTS),
    ORGANIC_UNINSTALL_EVENTS(Report.ORGANIC_UNINSTALL_EVENTS),
    DAILY_REPORT(Report.DAILY),
    RETARGETING_DAILY_REPORT(Report.RETARGETING_DAILY),
    PARTNERS_REPORT(Report.PARTNERS),
    RETARGETING_PARTNERS_REPORT(Report.RETARGETING_PARTNERS),
    PARTNERS_EVENTS_REPORT(Report.PARTNERS, true),
    RETARGETING_PARTNERS_EVENTS_REPORT(Report.RETARGETING_PARTNERS, true),
    GEO_REPORT(Report.GEO),
    RETARGETING_GEO_REPORT(Report.RETARGETING_GEO),
    GEO_EVENTS_REPORT(Report.GEO, true),
    RETARGETING_GEO_EVENTS_REPORT(Report.RETARGETING_GEO, true);

    private static final String HEX = "0123456789ABCDEF";
    final Report report;
    final boolean events;

    AppsFlyerResource(Report report) {
        this(report, false);
    }

    AppsFlyerResource(Report report, boolean events) {
        this.report = report;
        this.events = events;
    }

    String key() {
        return name().toLowerCase(Locale.ROOT);
    }

    static List<String> names() {
        List<String> resources = new ArrayList<>();
        for (AppsFlyerResource resource : values()) {
            resources.add(resource.key());
        }
        return Collections.unmodifiableList(resources);
    }

    static AppsFlyerResource named(String value) {
        try {
            return valueOf(value.toUpperCase(Locale.ROOT));
        } catch (IllegalArgumentException | NullPointerException e) {
            throw AppsFlyerConfig.invalid("resources contains an unsupported name");
        }
    }

    static String encode(String value) {
        StringBuilder output = new StringBuilder();
        for (byte octet : value.getBytes(StandardCharsets.UTF_8)) {
            int ch = octet & 255;
            if ((ch >= 'a' && ch <= 'z')
                    || (ch >= 'A' && ch <= 'Z')
                    || (ch >= '0' && ch <= '9')
                    || ch == '-'
                    || ch == '_'
                    || ch == '.'
                    || ch == '~') {
                output.append((char) ch);
            } else {
                output.append('%').append(HEX.charAt(ch >>> 4)).append(HEX.charAt(ch & 15));
            }
        }
        return output.toString();
    }

    /**
     * Preserves the provider-specific raw additional field requests. Aggregate queries use their
     * own documented parameters and omit these raw fields.
     */
    private enum Fields {
        RAW,
        ORGANIC_EVENTS,
        UNINSTALL,
        NONE
    }

    /**
     * Captures one v5 endpoint, its report family, and its row and date granularity. ID, campaign
     * type, date and timezone are carried into the wire request explicitly.
     */
    enum Report {
        IN_APP_EVENTS("in_app_events_report", "event_time", 31, Fields.RAW),
        ORGANIC_IN_APP_EVENTS(
                "organic_in_app_events_report", "event_time", 31, Fields.ORGANIC_EVENTS),
        RETARGETING_IN_APP_EVENTS("in-app-events-retarget", "event_time", 31, Fields.RAW),
        INSTALLS("installs_report", "install_time", 60, Fields.RAW),
        ORGANIC_INSTALLS("organic_installs_report", "install_time", 60, Fields.RAW),
        RETARGETING_INSTALLS("installs-retarget", "install_time", 60, Fields.RAW),
        UNINSTALL_EVENTS("uninstall_events_report", "event_time", 60, Fields.UNINSTALL),
        ORGANIC_UNINSTALL_EVENTS(
                "organic_uninstall_events_report", "event_time", 60, Fields.UNINSTALL),
        DAILY("daily_report", false),
        RETARGETING_DAILY("daily_report", true),
        PARTNERS("partners_by_date_report", false),
        RETARGETING_PARTNERS("partners_by_date_report", true),
        GEO("geo_by_date_report", false),
        RETARGETING_GEO("geo_by_date_report", true);

        private static final Set<String> SHARED_RAW_FIELDS =
                Set.of(
                        "ad_unit",
                        "amazon_aid",
                        "blocked_reason",
                        "blocked_reason_rule",
                        "blocked_reason_value",
                        "blocked_sub_reason",
                        "contributor1_match_type",
                        "contributor2_match_type",
                        "contributor3_match_type",
                        "custom_data",
                        "deeplink_url",
                        "device_category",
                        "device_download_time",
                        "device_model",
                        "gp_broadcast_referrer",
                        "gp_click_time",
                        "gp_install_begin",
                        "gp_referrer",
                        "impressions",
                        "install_app_store",
                        "keyword_id",
                        "keyword_match_type",
                        "match_type",
                        "mediation_network",
                        "monetization_network",
                        "network_account_id",
                        "oaid",
                        "placement",
                        "rejected_reason",
                        "rejected_reason_value",
                        "segment",
                        "store_reinstall");
        private static final Set<String> ATTRIBUTION_FIELDS =
                Set.of("app_type", "att", "campaign_type", "conversion_type");

        final String endpoint;
        final String cursor;
        final int days;
        final int providerCap;
        final boolean raw;
        final boolean retargeting;
        private final Fields fields;

        Report(String endpoint, String cursor, int days, Fields fields) {
            this.endpoint = endpoint;
            this.cursor = cursor;
            this.days = days;
            this.providerCap = 1000000;
            this.raw = true;
            this.retargeting = false;
            this.fields = fields;
        }

        Report(String endpoint, boolean retargeting) {
            this.endpoint = endpoint;
            this.cursor = "date";
            this.days = 60;
            this.providerCap = 200000;
            this.raw = false;
            this.retargeting = retargeting;
            this.fields = Fields.NONE;
        }

        /**
         * Builds only the parameters supported by this raw or aggregate v5 endpoint. Raw uses
         * half-open datetime bounds and aggregates use inclusive dates on the wire.
         */
        String target(AppsFlyerConfig config, AppsFlyerWindow window) {
            String prefix = raw ? "/api/raw-data/export/app/" : "/api/agg-data/export/app/";
            String query =
                    prefix
                            + config.encodedApp
                            + "/"
                            + endpoint
                            + "/v5?from="
                            + encode(window.from(this, config.zone))
                            + "&to="
                            + encode(window.to(this, config.zone))
                            + "&timezone="
                            + encode(config.zone.getId());
            if (raw) {
                return query
                        + "&currency=preferred&maximum_rows="
                        + providerCap
                        + "&additional_fields="
                        + encode(additionalFields());
            }
            return retargeting ? query + "&reattr=true" : query;
        }

        private String additionalFields() {
            Set<String> selected = new TreeSet<>(SHARED_RAW_FIELDS);
            if (fields != Fields.UNINSTALL) {
                selected.addAll(ATTRIBUTION_FIELDS);
            }
            if (fields != Fields.ORGANIC_EVENTS) {
                selected.add("is_lat");
            }
            return String.join(",", selected);
        }
    }
}
