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

package org.apache.seatunnel.connectors.seatunnel.appfigures;

import java.util.Set;

/**
 * The eleven native Appfigures read routes and their response contracts.
 *
 * <p>Report metric keys identify valid leaves without assuming the order of returned grouping keys.
 */
enum AppfiguresResource {
    STATUS("status", ""),
    DATA_CATEGORIES("data_categories", "data/categories"),
    USERS("users", "users"),
    USAGE("usage", "usage"),
    PRODUCTS("products_mine_search", "products/mine"),
    SALES("reports_sales", "reports/sales", Set.of("downloads", "revenue", "net_downloads")),
    REVENUE("reports_revenue", "reports/revenue", Set.of("sales", "total", "iap", "ads")),
    SUBSCRIPTIONS(
            "reports_subscriptions",
            "reports/subscriptions",
            Set.of("active_subscriptions", "all_active_subscriptions", "mrr", "churn")),
    ADS("reports_ads", "reports/ads", Set.of("requests", "impressions", "revenue", "clicks")),
    ADSPEND(
            "reports_adspend",
            "reports/adspend",
            Set.of("installs", "cost", "impressions", "clicks")),
    RATINGS("reports_ratings", "reports/ratings", Set.of("breakdown", "average", "total", "new"));

    final String name;
    final String route;
    final Set<String> metrics;

    AppfiguresResource(String name, String route) {
        this(name, route, Set.of());
    }

    AppfiguresResource(String name, String route, Set<String> metrics) {
        this.name = name;
        this.route = route;
        this.metrics = metrics;
    }

    boolean report() {
        return !metrics.isEmpty();
    }

    Set<String> pivots() {
        switch (this) {
            case SALES:
                return Set.of("product", "country", "date", "store", "device");
            case REVENUE:
            case SUBSCRIPTIONS:
                return Set.of("product", "country", "date", "store");
            case ADS:
            case ADSPEND:
                return Set.of("product", "country", "date", "store", "network");
            case RATINGS:
                return Set.of("product", "country", "date");
            default:
                return Set.of();
        }
    }

    String wirePivot(String pivot) {
        if (this != SALES) {
            return pivot;
        }
        switch (pivot) {
            case "product":
                return "products";
            case "country":
                return "countries";
            case "store":
                return "stores";
            case "date":
                return "dates";
            default:
                return pivot;
        }
    }

    static AppfiguresResource select(String configured) {
        for (AppfiguresResource resource : values()) {
            if (resource.name.equalsIgnoreCase(configured)) {
                return resource;
            }
        }
        throw AppfiguresSettings.invalid("resource", "expected a supported Appfigures resource");
    }
}
