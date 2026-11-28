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
package org.apache.seatunnel.connectors.seatunnel.appstoreconnect;

import java.util.Locale;

/**
 * Catalog of the bounded App Store Connect datasets.
 *
 * <p>Analytics entries distinguish parent metadata, final segments and the two request access
 * types.
 */
enum AppStoreConnectResource {
    LIST_ID_APPS(Stage.APPS),
    SUBSCRIBER_REPORT(Stage.SALES, "SUBSCRIBER", "DETAILED", "1_3"),
    SALES_REPORT(Stage.SALES, "SALES", "SUMMARY", "1_0"),
    SUBSCRIPTION_REPORT(Stage.SALES, "SUBSCRIPTION", "SUMMARY", "1_3"),
    SUBSCRIPTION_EVENT_REPORT(Stage.SALES, "SUBSCRIPTION_EVENT", "SUMMARY", "1_3"),
    FINANCE_REPORT(Stage.FINANCE),
    CUSTOMER_REVIEWS_PER_APP(Stage.REVIEWS),
    ANALYTICS_REPORT_REQUESTS_ONGOING(Stage.REQUESTS, false, null),
    ANALYTICS_REPORT_REQUESTS_HISTORICAL(Stage.REQUESTS, true, null),
    ANALYTICS_INSTALLATIONS_REPORTS(Stage.REPORTS, false, Family.INSTALLATIONS),
    ANALYTICS_INSTALLATIONS_INSTANCES(Stage.INSTANCES, false, Family.INSTALLATIONS),
    ANALYTICS_INSTALLATIONS_SEGMENTS(Stage.SEGMENTS, false, Family.INSTALLATIONS),
    ANALYTICS_INSTALLATIONS_SEGMENT_DETAILS(Stage.DETAIL, false, Family.INSTALLATIONS),
    APP_STORE_INSTALLATIONS_AND_DELETIONS(Stage.DOWNLOAD, false, Family.INSTALLATIONS),
    ANALYTICS_INSTALLATIONS_REPORTS_HISTORICAL(Stage.REPORTS, true, Family.INSTALLATIONS),
    ANALYTICS_INSTALLATIONS_INSTANCES_HISTORICAL(Stage.INSTANCES, true, Family.INSTALLATIONS),
    ANALYTICS_INSTALLATIONS_SEGMENTS_HISTORICAL(Stage.SEGMENTS, true, Family.INSTALLATIONS),
    ANALYTICS_INSTALLATIONS_SEGMENT_DETAILS_HISTORICAL(Stage.DETAIL, true, Family.INSTALLATIONS),
    APP_STORE_INSTALLATIONS_AND_DELETIONS_HISTORICAL(Stage.DOWNLOAD, true, Family.INSTALLATIONS),
    ANALYTICS_APP_DOWNLOAD_REPORTS(Stage.REPORTS, false, Family.DOWNLOADS),
    ANALYTICS_APP_DOWNLOAD_INSTANCES(Stage.INSTANCES, false, Family.DOWNLOADS),
    ANALYTICS_APP_DOWNLOAD_SEGMENTS(Stage.SEGMENTS, false, Family.DOWNLOADS),
    ANALYTICS_APP_DOWNLOAD_SEGMENT_DETAILS(Stage.DETAIL, false, Family.DOWNLOADS),
    APP_DOWNLOAD(Stage.DOWNLOAD, false, Family.DOWNLOADS),
    ANALYTICS_APP_DOWNLOAD_REPORTS_HISTORICAL(Stage.REPORTS, true, Family.DOWNLOADS),
    ANALYTICS_APP_DOWNLOAD_INSTANCES_HISTORICAL(Stage.INSTANCES, true, Family.DOWNLOADS),
    ANALYTICS_APP_DOWNLOAD_SEGMENTS_HISTORICAL(Stage.SEGMENTS, true, Family.DOWNLOADS),
    ANALYTICS_APP_DOWNLOAD_SEGMENT_DETAILS_HISTORICAL(Stage.DETAIL, true, Family.DOWNLOADS),
    APP_DOWNLOAD_HISTORICAL(Stage.DOWNLOAD, true, Family.DOWNLOADS);

    /**
     * Identifies the response shape and the relationship that gets expanded.
     *
     * <p>Scalar report and detail stages terminate the collection walk.
     */
    enum Stage {
        APPS,
        REVIEWS,
        REQUESTS,
        REPORTS,
        INSTANCES,
        SEGMENTS,
        DETAIL,
        DOWNLOAD,
        SALES,
        FINANCE
    }

    /**
     * Maps each analytics family to Apple's name filter for the reports relationship.
     *
     * <p>Both families share the endpoints, so their names determine the report data.
     */
    enum Family {
        INSTALLATIONS("App Store Installation and Deletion Standard"),
        DOWNLOADS("App Downloads Standard");

        final String title;

        Family(String title) {
            this.title = title;
        }
    }

    // Stage emitted by this resource after traversal through the preceding parents.
    final Stage terminal;

    // Access type separates historical snapshots from ongoing requests for the same app.
    final boolean historical;

    // Report name disambiguates the analytics families sharing the same endpoints.
    final Family family;

    // Sales endpoint requires an approved tuple of report type, subtype and version.
    final String reportType;

    final String reportSubType;
    final String version;

    AppStoreConnectResource(Stage terminal) {
        this(terminal, false, null, null, null, null);
    }

    AppStoreConnectResource(Stage terminal, boolean historical, Family family) {
        this(terminal, historical, family, null, null, null);
    }

    AppStoreConnectResource(Stage terminal, String reportType, String subtype, String version) {
        this(terminal, false, null, reportType, subtype, version);
    }

    AppStoreConnectResource(
            Stage terminal,
            boolean historical,
            Family family,
            String reportType,
            String subtype,
            String version) {
        this.terminal = terminal;
        this.historical = historical;
        this.family = family;
        this.reportType = reportType;
        this.reportSubType = subtype;
        this.version = version;
    }

    String key() {
        return name().toLowerCase(Locale.ROOT);
    }

    String access() {
        return historical ? "ONE_TIME_SNAPSHOT" : "ONGOING";
    }

    boolean dated() {
        return terminal == Stage.SALES || terminal == Stage.FINANCE;
    }

    Failure failure(String reason) {
        return new Failure("AppStoreConnect " + key() + ": " + reason);
    }

    /**
     * Connector diagnostic with deliberately sanitized provider context.
     *
     * <p>Endpoint queries, provider error text and credentials are excluded from the exception.
     */
    static final class Failure extends IllegalStateException {
        private static final long serialVersionUID = 1L;

        Failure(String message) {
            super(message);
        }
    }
}
