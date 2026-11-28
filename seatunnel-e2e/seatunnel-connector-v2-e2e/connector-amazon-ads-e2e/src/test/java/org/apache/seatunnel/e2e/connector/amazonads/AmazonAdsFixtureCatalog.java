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
package org.apache.seatunnel.e2e.connector.amazonads;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Independent wire expectations for service operations and report projections. The fixture uses
 * operation data and hashed metric sets rather than importing connector classes.
 */
final class AmazonAdsFixtureCatalog {
    static final List<Operation> DIRECT = operations();
    static final List<Projection> REPORTS = reports();

    private AmazonAdsFixtureCatalog() {}

    private static List<Operation> operations() {
        List<Operation> values = new ArrayList<>();
        values.add(
                new Operation(
                        "portfolios",
                        "/portfolios/list",
                        "portfolios",
                        "portfolioId",
                        "application/vnd.spPortfolio.v3+json",
                        "JSON_TOKEN"));
        values.add(
                new Operation(
                        "sponsored_display_campaigns",
                        "/sd/campaigns",
                        "",
                        "campaignId",
                        "application/json",
                        "OFFSET"));
        values.add(
                new Operation(
                        "sponsored_display_ad_groups",
                        "/sd/adGroups",
                        "",
                        "adGroupId",
                        "application/json",
                        "OFFSET"));
        values.add(
                new Operation(
                        "sponsored_display_product_ads",
                        "/sd/productAds",
                        "",
                        "adId",
                        "application/json",
                        "OFFSET"));
        values.add(
                new Operation(
                        "sponsored_display_targetings",
                        "/sd/targets",
                        "",
                        "targetId",
                        "application/json",
                        "OFFSET"));
        values.add(
                new Operation(
                        "sponsored_display_creatives",
                        "/sd/creatives",
                        "",
                        "creativeId",
                        "application/json",
                        "OFFSET"));
        values.add(
                new Operation(
                        "sponsored_display_budget_rules",
                        "/sp/budgetRules",
                        "budgetRulesForAdvertiserResponse",
                        "ruleId",
                        "application/json",
                        "QUERY_TOKEN"));
        values.add(
                new Operation(
                        "sponsored_brands_keywords",
                        "/sb/keywords",
                        "",
                        "keywordId",
                        "application/json",
                        "OFFSET"));
        values.add(
                new Operation(
                        "sponsored_brands_campaigns",
                        "/sb/v4/campaigns/list",
                        "campaigns",
                        "campaignId",
                        "application/vnd.sbcampaignresource.v4+json",
                        "JSON_TOKEN"));
        values.add(
                new Operation(
                        "sponsored_brands_ad_groups",
                        "/sb/v4/adGroups/list",
                        "adGroups",
                        "adGroupId",
                        "application/vnd.sbadgroupresource.v4+json",
                        "JSON_TOKEN"));
        values.add(
                new Operation(
                        "sponsored_brands_ads",
                        "/sb/v4/ads/list",
                        "ads",
                        "adId",
                        "application/vnd.sbadresource.v4+json",
                        "JSON_TOKEN"));
        values.add(
                new Operation(
                        "sponsored_product_campaigns",
                        "/sp/campaigns/list",
                        "campaigns",
                        "campaignId",
                        "application/vnd.spCampaign.v3+json",
                        "JSON_TOKEN"));
        values.add(
                new Operation(
                        "sponsored_product_ad_groups",
                        "/sp/adGroups/list",
                        "adGroups",
                        "adGroupId",
                        "application/vnd.spAdGroup.v3+json",
                        "JSON_TOKEN"));
        values.add(
                new Operation(
                        "sponsored_product_keywords",
                        "/sp/keywords/list",
                        "keywords",
                        "keywordId",
                        "application/vnd.spKeyword.v3+json",
                        "JSON_TOKEN"));
        values.add(
                new Operation(
                        "sponsored_product_negative_keywords",
                        "/sp/negativeKeywords/list",
                        "negativeKeywords",
                        "keywordId",
                        "application/vnd.spNegativeKeyword.v3+json",
                        "JSON_TOKEN"));
        values.add(
                new Operation(
                        "sponsored_product_campaign_negative_keywords",
                        "/sp/campaignNegativeKeywords/list",
                        "campaignNegativeKeywords",
                        "keywordId",
                        "application/vnd.spCampaignNegativeKeyword.v3+json",
                        "JSON_TOKEN"));
        values.add(
                new Operation(
                        "sponsored_product_ads",
                        "/sp/productAds/list",
                        "productAds",
                        "adId",
                        "application/vnd.spProductAd.v3+json",
                        "JSON_TOKEN"));
        values.add(
                new Operation(
                        "sponsored_product_targetings",
                        "/sp/targets/list",
                        "targetingClauses",
                        "targetId",
                        "application/vnd.spTargetingClause.v3+json",
                        "JSON_TOKEN"));
        values.add(
                new Operation(
                        "sponsored_product_ad_group_bid_recommendations",
                        "/sp/targets/bid/recommendations",
                        "",
                        "",
                        "application/vnd.spthemebasedbidrecommendation.v4+json",
                        "BID"));
        values.add(
                new Operation(
                        "sponsored_product_ad_group_suggested_keywords",
                        "/sp/targets/keywords/recommendations",
                        "keywordTargetList",
                        "",
                        "application/vnd.spkeywordsrecommendation.v4+json",
                        "SUGGEST"));
        values.add(
                new Operation(
                        "attribution_report_products",
                        "/attribution/report",
                        "reports",
                        "",
                        "application/json",
                        "AT:PRODUCTS"));
        values.add(
                new Operation(
                        "attribution_report_performance_adgroup",
                        "/attribution/report",
                        "reports",
                        "",
                        "application/json",
                        "AT:ADGROUP"));
        values.add(
                new Operation(
                        "attribution_report_performance_campaign",
                        "/attribution/report",
                        "reports",
                        "",
                        "application/json",
                        "AT:CAMPAIGN"));
        values.add(
                new Operation(
                        "attribution_report_performance_creative",
                        "/attribution/report",
                        "reports",
                        "",
                        "application/json",
                        "AT:CREATIVE"));
        return List.copyOf(values);
    }

    private static List<Projection> reports() {
        List<Projection> values = new ArrayList<>();
        values.add(
                new Projection(
                        "sponsored_brands_v3_report_stream",
                        "SPONSORED_BRANDS",
                        "sbPurchasedProduct",
                        "SUMMARY",
                        "purchasedAsin",
                        "",
                        "67f436222adaa8d921b8477839c292495c4bab52bb1aea294188dbd3bf0f7745"));
        values.add(
                new Projection(
                        "sponsored_brands_campaigns_report_stream",
                        "SPONSORED_BRANDS",
                        "sbCampaigns",
                        "SUMMARY",
                        "campaign",
                        "",
                        "92266eefd79708a7162c2d6911ee2ac068aaf2e357607ba7dc9fbe0a7370b065"));
        values.add(
                new Projection(
                        "sponsored_brands_adgroups_report_stream",
                        "SPONSORED_BRANDS",
                        "sbAdGroup",
                        "SUMMARY",
                        "adGroup",
                        "",
                        "29d4d9a5339d4aea4e08bd07f83cf9b4151e6ac59491ae922455b027f49d92b2"));
        values.add(
                new Projection(
                        "sponsored_brands_ads_report_stream",
                        "SPONSORED_BRANDS",
                        "sbAds",
                        "SUMMARY",
                        "ads",
                        "",
                        "5155c7d0fef7af6f8731c17913e0ea6dda6cb27153b9cf1d79c274f092781ee1"));
        values.add(
                new Projection(
                        "sponsored_display_campaigns_report_stream",
                        "SPONSORED_DISPLAY",
                        "sdCampaigns",
                        "SUMMARY",
                        "campaign",
                        "",
                        "59fe9a975180053bcb3b48732572fa9e099eb409928e89c21355378f463f4dac"));
        values.add(
                new Projection(
                        "sponsored_display_adgroups_report_stream",
                        "SPONSORED_DISPLAY",
                        "sdAdGroup",
                        "SUMMARY",
                        "adGroup",
                        "",
                        "8e2985181811df1959684b53331d51bb76518886b1a9a386db9479d8b9740cee"));
        values.add(
                new Projection(
                        "sponsored_display_productads_report_stream",
                        "SPONSORED_DISPLAY",
                        "sdAdvertisedProduct",
                        "SUMMARY",
                        "advertiser",
                        "",
                        "1d00f290ae66c64b76c716bef2cd7eccde8f6488aecf04b6be56dd5b79a35c42"));
        values.add(
                new Projection(
                        "sponsored_display_targets_report_stream",
                        "SPONSORED_DISPLAY",
                        "sdTargeting",
                        "SUMMARY",
                        "targeting",
                        "",
                        "5307d5b2d57b4dfe0842893e2d3f91618de3a120a4440248bb210e2a9695d0f3"));
        values.add(
                new Projection(
                        "sponsored_display_asins_report_stream",
                        "SPONSORED_DISPLAY",
                        "sdPurchasedProduct",
                        "SUMMARY",
                        "asin",
                        "",
                        "b42083f656766fca082cdaf1747272e3a674fd8bde2939477800576776772554"));
        values.add(
                new Projection(
                        "sponsored_products_campaigns_report_stream",
                        "SPONSORED_PRODUCTS",
                        "spCampaigns",
                        "SUMMARY",
                        "campaign",
                        "",
                        "656abc1cef94037fe823197c9aaee5d0a7d226f3028c6241774afa2d48121493"));
        values.add(
                new Projection(
                        "sponsored_products_adgroups_report_stream",
                        "SPONSORED_PRODUCTS",
                        "spCampaigns",
                        "SUMMARY",
                        "campaign,adGroup",
                        "",
                        "220035fc32929ae4797178b7c13ecbe200e74661ac57abf90499053e008902c9"));
        values.add(
                new Projection(
                        "sponsored_products_keywords_report_stream",
                        "SPONSORED_PRODUCTS",
                        "spTargeting",
                        "SUMMARY",
                        "targeting",
                        "BROAD EXACT PHRASE",
                        "947cfc35bf46ac17ae0869a959e1fc338372e9876b5ebcd0b1d05ffe9ca8298a"));
        values.add(
                new Projection(
                        "sponsored_products_targets_report_stream",
                        "SPONSORED_PRODUCTS",
                        "spTargeting",
                        "SUMMARY",
                        "targeting",
                        "TARGETING_EXPRESSION TARGETING_EXPRESSION_PREDEFINED",
                        "364c72612916ad69e5ab48db2d389e335cbe5ed11e971af3b990a36a7f232b61"));
        values.add(
                new Projection(
                        "sponsored_products_search_terms_report_stream",
                        "SPONSORED_PRODUCTS",
                        "spSearchTerm",
                        "SUMMARY",
                        "searchTerm",
                        "BROAD EXACT PHRASE TARGETING_EXPRESSION TARGETING_EXPRESSION_PREDEFINED",
                        "1a7c5cda0e2a6c0fb1f1833ffc7318359d21d0e0c680aa59a32f79c78d80061d"));
        values.add(
                new Projection(
                        "sponsored_products_productads_report_stream",
                        "SPONSORED_PRODUCTS",
                        "spAdvertisedProduct",
                        "SUMMARY",
                        "advertiser",
                        "",
                        "4401d771512b8ced18692602e5d4c5e7e6a487d94d319307dbfbb99048c8043f"));
        values.add(
                new Projection(
                        "sponsored_products_asins_keywords_report_stream",
                        "SPONSORED_PRODUCTS",
                        "spPurchasedProduct",
                        "SUMMARY",
                        "asin",
                        "",
                        "8ee0d25269953bd360d6713a89f1d9448a9ac4ffc0361f889a6f7a24780a2063"));
        values.add(
                new Projection(
                        "sponsored_products_asins_targets_report_stream",
                        "SPONSORED_PRODUCTS",
                        "spPurchasedProduct",
                        "SUMMARY",
                        "asin",
                        "",
                        "d301d99ef72dd2eb680ccc7fdb4c7cc457d782657647e7c8ac72dcf2e4aabc92"));
        values.add(
                new Projection(
                        "sponsored_brands_v3_report_stream_daily",
                        "SPONSORED_BRANDS",
                        "sbPurchasedProduct",
                        "DAILY",
                        "purchasedAsin",
                        "",
                        "bdf4bd10feae098905670c58a225184e68f292c33c77d3ea6cb716335a1b5842"));
        values.add(
                new Projection(
                        "sponsored_brands_campaigns_report_stream_daily",
                        "SPONSORED_BRANDS",
                        "sbCampaigns",
                        "DAILY",
                        "campaign",
                        "",
                        "cd4f37b61f178f0a92608d809ead5105181db7268ac40594d40ff8d89fc99998"));
        values.add(
                new Projection(
                        "sponsored_brands_adgroups_report_stream_daily",
                        "SPONSORED_BRANDS",
                        "sbAdGroup",
                        "DAILY",
                        "adGroup",
                        "",
                        "c4506b7d0ce9e1ef4f4b4aaa4731356f89c5ead88a5baf3925b30cde238725b5"));
        values.add(
                new Projection(
                        "sponsored_brands_ads_report_stream_daily",
                        "SPONSORED_BRANDS",
                        "sbAds",
                        "DAILY",
                        "ads",
                        "",
                        "71787276e82de9ce768ffbb1e8e17b8734bf63eeb8900d2bbd3e7356d2f49753"));
        values.add(
                new Projection(
                        "sponsored_display_campaigns_report_stream_daily",
                        "SPONSORED_DISPLAY",
                        "sdCampaigns",
                        "DAILY",
                        "campaign",
                        "",
                        "4ed37a1ea3d6e7f5b65c1117cf0920e38df98409b352e12769a602596201dd6c"));
        values.add(
                new Projection(
                        "sponsored_display_adgroups_report_stream_daily",
                        "SPONSORED_DISPLAY",
                        "sdAdGroup",
                        "DAILY",
                        "adGroup",
                        "",
                        "4c848cfcf02e91ad45390a54b81ff7e48a600fd9c526d0a87cbf4a3301b49c1c"));
        values.add(
                new Projection(
                        "sponsored_display_productads_report_stream_daily",
                        "SPONSORED_DISPLAY",
                        "sdAdvertisedProduct",
                        "DAILY",
                        "advertiser",
                        "",
                        "d965c53f8de6b3db27d734ce8d94002060c30a774ff311c2596ba52b0b72e4ba"));
        values.add(
                new Projection(
                        "sponsored_display_targets_report_stream_daily",
                        "SPONSORED_DISPLAY",
                        "sdTargeting",
                        "DAILY",
                        "targeting",
                        "",
                        "6af0b2dfea68e31011f9de343eb1d03edb250879e934227f09000ab662373059"));
        values.add(
                new Projection(
                        "sponsored_display_asins_report_stream_daily",
                        "SPONSORED_DISPLAY",
                        "sdPurchasedProduct",
                        "DAILY",
                        "asin",
                        "",
                        "ca68adae55416fb69c834c694120b7e79df48b14da255b760b7c6472d992a2c7"));
        values.add(
                new Projection(
                        "sponsored_products_campaigns_report_stream_daily",
                        "SPONSORED_PRODUCTS",
                        "spCampaigns",
                        "DAILY",
                        "campaign",
                        "",
                        "0d67288dae0c239c05da6bee13f583f962d5a422d6af8ce1edcfb4b866d1ef89"));
        values.add(
                new Projection(
                        "sponsored_products_adgroups_report_stream_daily",
                        "SPONSORED_PRODUCTS",
                        "spCampaigns",
                        "DAILY",
                        "campaign,adGroup",
                        "",
                        "98564e4e13d4fa4ebcaf4ed842ec352cd01e490c092217fbac1c842faf12bd48"));
        values.add(
                new Projection(
                        "sponsored_products_keywords_report_stream_daily",
                        "SPONSORED_PRODUCTS",
                        "spTargeting",
                        "DAILY",
                        "targeting",
                        "BROAD EXACT PHRASE",
                        "2d3237a6ed2033ce3e95ae97a3dcd2cea910742b57b009b02b6cf2530ed5a799"));
        values.add(
                new Projection(
                        "sponsored_products_targets_report_stream_daily",
                        "SPONSORED_PRODUCTS",
                        "spTargeting",
                        "DAILY",
                        "targeting",
                        "TARGETING_EXPRESSION TARGETING_EXPRESSION_PREDEFINED",
                        "0249ba6cb7fe1e20d69211b3bbc50e13c2225a953b975727548f9c8135d9af9d"));
        values.add(
                new Projection(
                        "sponsored_products_search_terms_report_stream_daily",
                        "SPONSORED_PRODUCTS",
                        "spSearchTerm",
                        "DAILY",
                        "searchTerm",
                        "BROAD EXACT PHRASE TARGETING_EXPRESSION TARGETING_EXPRESSION_PREDEFINED",
                        "64764e190ab28a76011aaa42b33661cf53939e25a42d92f75eb9d9b5ae44c95e"));
        values.add(
                new Projection(
                        "sponsored_products_productads_report_stream_daily",
                        "SPONSORED_PRODUCTS",
                        "spAdvertisedProduct",
                        "DAILY",
                        "advertiser",
                        "",
                        "8f20de39b6ea504e2448b168f94e14c8d2720b15f78e3ae9ea5fd809d85732b0"));
        values.add(
                new Projection(
                        "sponsored_products_asins_keywords_report_stream_daily",
                        "SPONSORED_PRODUCTS",
                        "spPurchasedProduct",
                        "DAILY",
                        "asin",
                        "",
                        "ea12dd246788f7c2d252e6712e09ac9e854a7adcea19776d07b844bde5f19a92"));
        values.add(
                new Projection(
                        "sponsored_products_asins_targets_report_stream_daily",
                        "SPONSORED_PRODUCTS",
                        "spPurchasedProduct",
                        "DAILY",
                        "asin",
                        "",
                        "b7631af62ddb5cfb243e4ecd55fd872f0c37fd43f5beb6e09133005d1fee0209"));
        return List.copyOf(values);
    }

    static List<String> textList(JsonNode values) {
        List<String> result = new ArrayList<>();
        values.forEach(node -> result.add(node.asText()));
        return result;
    }

    static String columnDigest(JsonNode value) {
        List<String> fields = textList(value);
        Collections.sort(fields);
        try {
            byte[] bytes =
                    MessageDigest.getInstance("SHA-256")
                            .digest(String.join("\n", fields).getBytes(StandardCharsets.UTF_8));
            StringBuilder result = new StringBuilder(bytes.length * 2);
            for (byte b : bytes) {
                result.append(Character.forDigit((b & 255) >>> 4, 16))
                        .append(Character.forDigit(b & 15, 16));
            }
            return result.toString();
        } catch (NoSuchAlgorithmException ex) {
            throw new AssertionError(ex);
        }
    }

    /**
     * Describes an observed read path, media version, response field and page contract.
     *
     * <p>The provider fixture verifies these independently of the connector enum and request
     * builder.
     */
    static final class Operation {
        final String name;
        final String path;
        final String field;
        final String key;
        final String media;
        final String mode;

        Operation(String name, String path, String field, String key, String media, String mode) {
            this.name = name;
            this.path = path;
            this.field = field;
            this.key = key;
            this.media = media;
            this.mode = mode;
        }
    }

    /**
     * Matches a versioned report request to its logical output resource, independently of the
     * report name.
     *
     * <p>Canonical column set hashes distinguish the projections and their time units.
     */
    static final class Projection {
        final String name;
        final String product;
        final String reportType;
        final String timeUnit;
        final String dimensions;
        final String keywordTypes;
        final String columns;

        Projection(
                String name,
                String product,
                String reportType,
                String timeUnit,
                String dimensions,
                String keywordTypes,
                String columns) {
            this.name = name;
            this.product = product;
            this.reportType = reportType;
            this.timeUnit = timeUnit;
            this.dimensions = dimensions;
            this.keywordTypes = keywordTypes;
            this.columns = columns;
        }

        boolean matches(JsonNode configuration) {
            if (!product.equals(configuration.path("adProduct").asText())
                    || !reportType.equals(configuration.path("reportTypeId").asText())
                    || !timeUnit.equals(configuration.path("timeUnit").asText())
                    || !dimensions.equals(String.join(",", textList(configuration.path("groupBy"))))
                    || !"GZIP_JSON".equals(configuration.path("format").asText())
                    || !columns.equals(columnDigest(configuration.path("columns")))) {
                return false;
            }
            JsonNode filters = configuration.path("filters");
            if (keywordTypes.isEmpty()) {
                return filters.isArray() && filters.isEmpty();
            }
            if (filters.size() != 1
                    || !"keywordType".equals(filters.path(0).path("field").asText())) {
                return false;
            }
            List<String> actual = textList(filters.path(0).path("values"));
            Collections.sort(actual);
            return keywordTypes.equals(String.join(" ", actual));
        }
    }
}
