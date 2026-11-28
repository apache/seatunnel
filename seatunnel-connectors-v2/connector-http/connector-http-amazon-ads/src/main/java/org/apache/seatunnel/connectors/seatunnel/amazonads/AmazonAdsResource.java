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
package org.apache.seatunnel.connectors.seatunnel.amazonads;

import java.util.Locale;

/**
 * Registers one named output resource for each Ads API operation or report projection. Shared paths
 * still keep their profile, pagination and report dimensions separate.
 */
enum AmazonAdsResource {
    PROFILES(
            Kind.PROFILES,
            "/v2/profiles?profileTypeFilter=seller,vendor&accessLevel=view",
            null,
            null,
            "profileId"),
    PORTFOLIOS(
            Kind.BODY_TOKEN,
            "/portfolios/list",
            "portfolios",
            "application/vnd.spPortfolio.v3+json",
            "portfolioId"),
    SPONSORED_DISPLAY_CAMPAIGNS(Kind.OFFSET, "/sd/campaigns", null, null, "campaignId"),
    SPONSORED_DISPLAY_AD_GROUPS(Kind.OFFSET, "/sd/adGroups", null, null, "adGroupId"),
    SPONSORED_DISPLAY_PRODUCT_ADS(Kind.OFFSET, "/sd/productAds", null, null, "adId"),
    SPONSORED_DISPLAY_TARGETINGS(Kind.OFFSET, "/sd/targets", null, null, "targetId"),
    SPONSORED_DISPLAY_CREATIVES(Kind.OFFSET, "/sd/creatives", null, null, "creativeId"),
    SPONSORED_DISPLAY_BUDGET_RULES(
            Kind.QUERY_TOKEN,
            "/sp/budgetRules",
            "budgetRulesForAdvertiserResponse",
            null,
            "ruleId"),
    SPONSORED_BRANDS_KEYWORDS(Kind.OFFSET, "/sb/keywords", null, null, "keywordId"),
    SPONSORED_BRANDS_CAMPAIGNS(
            Kind.BODY_TOKEN,
            "/sb/v4/campaigns/list",
            "campaigns",
            "application/vnd.sbcampaignresource.v4+json",
            "campaignId"),
    SPONSORED_BRANDS_AD_GROUPS(
            Kind.BODY_TOKEN,
            "/sb/v4/adGroups/list",
            "adGroups",
            "application/vnd.sbadgroupresource.v4+json",
            "adGroupId"),
    SPONSORED_BRANDS_ADS(
            Kind.BODY_TOKEN,
            "/sb/v4/ads/list",
            "ads",
            "application/vnd.sbadresource.v4+json",
            "adId"),
    SPONSORED_PRODUCT_CAMPAIGNS(
            Kind.BODY_TOKEN,
            "/sp/campaigns/list",
            "campaigns",
            "application/vnd.spCampaign.v3+json",
            "campaignId"),
    SPONSORED_PRODUCT_AD_GROUPS(
            Kind.BODY_TOKEN,
            "/sp/adGroups/list",
            "adGroups",
            "application/vnd.spAdGroup.v3+json",
            "adGroupId"),
    SPONSORED_PRODUCT_KEYWORDS(
            Kind.BODY_TOKEN,
            "/sp/keywords/list",
            "keywords",
            "application/vnd.spKeyword.v3+json",
            "keywordId"),
    SPONSORED_PRODUCT_NEGATIVE_KEYWORDS(
            Kind.BODY_TOKEN,
            "/sp/negativeKeywords/list",
            "negativeKeywords",
            "application/vnd.spNegativeKeyword.v3+json",
            "keywordId"),
    SPONSORED_PRODUCT_CAMPAIGN_NEGATIVE_KEYWORDS(
            Kind.BODY_TOKEN,
            "/sp/campaignNegativeKeywords/list",
            "campaignNegativeKeywords",
            "application/vnd.spCampaignNegativeKeyword.v3+json",
            "keywordId"),
    SPONSORED_PRODUCT_ADS(
            Kind.BODY_TOKEN,
            "/sp/productAds/list",
            "productAds",
            "application/vnd.spProductAd.v3+json",
            "adId"),
    SPONSORED_PRODUCT_TARGETINGS(
            Kind.BODY_TOKEN,
            "/sp/targets/list",
            "targetingClauses",
            "application/vnd.spTargetingClause.v3+json",
            "targetId"),
    SPONSORED_PRODUCT_AD_GROUP_BID_RECOMMENDATIONS(
            Kind.BID,
            "/sp/targets/bid/recommendations",
            null,
            "application/vnd.spthemebasedbidrecommendation.v4+json",
            null),
    SPONSORED_PRODUCT_AD_GROUP_SUGGESTED_KEYWORDS(
            Kind.SUGGESTIONS,
            "/sp/targets/keywords/recommendations",
            "keywordTargetList",
            "application/vnd.spkeywordsrecommendation.v4+json",
            null),
    ATTRIBUTION_REPORT_PRODUCTS(Kind.ATTRIBUTION, "/attribution/report", "reports", null, null),
    ATTRIBUTION_REPORT_PERFORMANCE_ADGROUP(
            Kind.ATTRIBUTION, "/attribution/report", "reports", null, null),
    ATTRIBUTION_REPORT_PERFORMANCE_CAMPAIGN(
            Kind.ATTRIBUTION, "/attribution/report", "reports", null, null),
    ATTRIBUTION_REPORT_PERFORMANCE_CREATIVE(
            Kind.ATTRIBUTION, "/attribution/report", "reports", null, null),
    SPONSORED_BRANDS_V3_REPORT_STREAM(),
    SPONSORED_BRANDS_CAMPAIGNS_REPORT_STREAM(),
    SPONSORED_BRANDS_ADGROUPS_REPORT_STREAM(),
    SPONSORED_BRANDS_ADS_REPORT_STREAM(),
    SPONSORED_DISPLAY_CAMPAIGNS_REPORT_STREAM(),
    SPONSORED_DISPLAY_ADGROUPS_REPORT_STREAM(),
    SPONSORED_DISPLAY_PRODUCTADS_REPORT_STREAM(),
    SPONSORED_DISPLAY_TARGETS_REPORT_STREAM(),
    SPONSORED_DISPLAY_ASINS_REPORT_STREAM(),
    SPONSORED_PRODUCTS_CAMPAIGNS_REPORT_STREAM(),
    SPONSORED_PRODUCTS_ADGROUPS_REPORT_STREAM(),
    SPONSORED_PRODUCTS_KEYWORDS_REPORT_STREAM(),
    SPONSORED_PRODUCTS_TARGETS_REPORT_STREAM(),
    SPONSORED_PRODUCTS_SEARCH_TERMS_REPORT_STREAM(),
    SPONSORED_PRODUCTS_PRODUCTADS_REPORT_STREAM(),
    SPONSORED_PRODUCTS_ASINS_KEYWORDS_REPORT_STREAM(),
    SPONSORED_PRODUCTS_ASINS_TARGETS_REPORT_STREAM(),
    SPONSORED_BRANDS_V3_REPORT_STREAM_DAILY(),
    SPONSORED_BRANDS_CAMPAIGNS_REPORT_STREAM_DAILY(),
    SPONSORED_BRANDS_ADGROUPS_REPORT_STREAM_DAILY(),
    SPONSORED_BRANDS_ADS_REPORT_STREAM_DAILY(),
    SPONSORED_DISPLAY_CAMPAIGNS_REPORT_STREAM_DAILY(),
    SPONSORED_DISPLAY_ADGROUPS_REPORT_STREAM_DAILY(),
    SPONSORED_DISPLAY_PRODUCTADS_REPORT_STREAM_DAILY(),
    SPONSORED_DISPLAY_TARGETS_REPORT_STREAM_DAILY(),
    SPONSORED_DISPLAY_ASINS_REPORT_STREAM_DAILY(),
    SPONSORED_PRODUCTS_CAMPAIGNS_REPORT_STREAM_DAILY(),
    SPONSORED_PRODUCTS_ADGROUPS_REPORT_STREAM_DAILY(),
    SPONSORED_PRODUCTS_KEYWORDS_REPORT_STREAM_DAILY(),
    SPONSORED_PRODUCTS_TARGETS_REPORT_STREAM_DAILY(),
    SPONSORED_PRODUCTS_SEARCH_TERMS_REPORT_STREAM_DAILY(),
    SPONSORED_PRODUCTS_PRODUCTADS_REPORT_STREAM_DAILY(),
    SPONSORED_PRODUCTS_ASINS_KEYWORDS_REPORT_STREAM_DAILY(),
    SPONSORED_PRODUCTS_ASINS_TARGETS_REPORT_STREAM_DAILY();

    final Kind kind;
    final String path;
    final String array;
    final String media;
    final String key;

    AmazonAdsResource() {
        this(Kind.REPORT, null, null, null, null);
    }

    AmazonAdsResource(Kind kind, String path, String array, String media, String key) {
        this.kind = kind;
        this.path = path;
        this.array = array;
        this.media = media == null ? "application/json" : media;
        this.key = key;
    }

    String label() {
        return name().toLowerCase(Locale.ROOT);
    }

    boolean dependsOnAdGroup() {
        return kind == Kind.BID || kind == Kind.SUGGESTIONS;
    }

    static AmazonAdsResource fromLabel(String label) {
        for (AmazonAdsResource resource : values()) {
            if (resource.label().equals(label)) {
                return resource;
            }
        }
        throw new IllegalArgumentException("AmazonAds resources contains an unsupported name");
    }

    Failure fail(String detail) {
        return new Failure("AmazonAds " + label() + ": " + detail);
    }

    /**
     * Distinguishes provider request and continuation contracts.
     *
     * <p>Each operation binds the method and response shape to the scope that supplied its parent.
     */
    enum Kind {
        PROFILES,
        BODY_TOKEN,
        QUERY_TOKEN,
        OFFSET,
        BID,
        SUGGESTIONS,
        ATTRIBUTION,
        REPORT
    }

    /**
     * Connector-owned sanitized failure. Never attaches a raw provider exception or signed URL.
     *
     * <p>A resource can report its phase or status while omitting credential and response contents.
     */
    static final class Failure extends IllegalStateException {
        private static final long serialVersionUID = 1L;

        Failure(String message) {
            super(message);
        }
    }
}
