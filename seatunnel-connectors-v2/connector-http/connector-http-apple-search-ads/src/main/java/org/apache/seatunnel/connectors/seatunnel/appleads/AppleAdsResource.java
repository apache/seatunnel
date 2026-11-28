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

import java.util.Locale;

/**
 * Identifies the four Apple Ads entity datasets and their daily reports.
 *
 * <p>Legacy report metadata has entity-specific ID names. Platform report metadata uses a common id
 * property while preserving the same logical resource labels.
 */
enum AppleAdsResource {
    CAMPAIGNS("campaigns", null, null),
    ADGROUPS("adgroups", CAMPAIGNS, null),
    KEYWORDS("keywords", ADGROUPS, null),
    ADS("ads", ADGROUPS, null),
    CAMPAIGNS_REPORT_DAILY("campaigns", null, "campaignId"),
    ADGROUPS_REPORT_DAILY("adgroups", CAMPAIGNS, "adGroupId"),
    KEYWORDS_REPORT_DAILY("keywords", CAMPAIGNS, "keywordId"),
    ADS_REPORT_DAILY("ads", CAMPAIGNS, "adId");

    // Current API segments are shared between the entity catalog and the corresponding reports.
    final String route;

    // Report windows scope their fact data independently of the unfiltered parent discovery.
    final AppleAdsResource parent;

    // Only v5 uses these entity-dependent fields in the report metadata.
    final String legacyId;

    AppleAdsResource(String route, AppleAdsResource parent, String legacyId) {
        this.route = route;
        this.parent = parent;
        this.legacyId = legacyId;
    }

    String label() {
        return name().toLowerCase(Locale.ROOT);
    }

    boolean report() {
        return legacyId != null;
    }

    AppleAdsResource firstStage() {
        return parent == null ? this : CAMPAIGNS;
    }

    AppleAdsResource after(AppleAdsResource stage) {
        return parent == ADGROUPS && stage == CAMPAIGNS ? ADGROUPS : this;
    }

    Failure fail(String reason) {
        return new Failure("AppleAds " + label() + ": " + reason);
    }

    /**
     * Exposes only an operation label and sanitized diagnostic.
     *
     * <p>Raw provider responses, account secrets and request headers are excluded.
     */
    static final class Failure extends IllegalStateException {
        private static final long serialVersionUID = 1L;

        Failure(String message) {
            super(message);
        }
    }
}
