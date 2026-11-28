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

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

/**
 * Defines the Amazon report projections. Each template keeps both time units explicit and retains
 * any report-specific grouping, column and filter differences.
 */
final class AmazonAdsReportCatalog {
    private static final Map<AmazonAdsResource, Template> TEMPLATES = templates();

    private AmazonAdsReportCatalog() {}

    static Template forResource(AmazonAdsResource resource) {
        Template result = TEMPLATES.get(resource);
        if (result == null) {
            throw resource.fail("no report projection is registered");
        }
        return result;
    }

    private static Map<AmazonAdsResource, Template> templates() {
        Map<AmazonAdsResource, Template> pair = new EnumMap<>(AmazonAdsResource.class);
        add(
                pair,
                AmazonAdsResource.SPONSORED_BRANDS_V3_REPORT_STREAM,
                AmazonAdsResource.SPONSORED_BRANDS_V3_REPORT_STREAM_DAILY,
                "SPONSORED_BRANDS",
                "sbPurchasedProduct",
                "purchasedAsin",
                "",
                false,
                "adGroupName attributionType campaignBudgetCurrencyCode campaignName newToBrandPurchases14d"
                        + " newToBrandPurchasesPercentage14d newToBrandSales14d newToBrandSalesPercentage14d"
                        + " newToBrandUnitsSold14d newToBrandUnitsSoldPercentage14d orders14d productCategory productName"
                        + " purchasedAsin sales14d unitsSold14d");
        add(
                pair,
                AmazonAdsResource.SPONSORED_BRANDS_CAMPAIGNS_REPORT_STREAM,
                AmazonAdsResource.SPONSORED_BRANDS_CAMPAIGNS_REPORT_STREAM_DAILY,
                "SPONSORED_BRANDS",
                "sbCampaigns",
                "campaign",
                "",
                true,
                "addToCart addToCartClicks addToCartRate addToList addToListFromClicks brandStorePageView"
                        + " brandedSearches brandedSearchesClicks campaignBudgetAmount campaignBudgetCurrencyCode"
                        + " campaignBudgetType campaignId campaignName campaignStatus clicks cost costType detailPageViews"
                        + " detailPageViewsClicks eCPAddToCart impressions kindleEditionNormalizedPagesRead14d"
                        + " kindleEditionNormalizedPagesRoyalties14d longTermROAS longTermSales newToBrandDetailPageViewRate"
                        + " newToBrandDetailPageViews newToBrandDetailPageViewsClicks newToBrandECPDetailPageView"
                        + " newToBrandPurchases newToBrandPurchasesClicks newToBrandPurchasesPercentage newToBrandPurchasesRate"
                        + " newToBrandSales newToBrandSalesClicks newToBrandSalesPercentage newToBrandUnitsSold"
                        + " newToBrandUnitsSoldClicks newToBrandUnitsSoldPercentage purchases purchasesClicks purchasesPromoted"
                        + " qualifiedBorrows qualifiedBorrowsFromClicks royaltyQualifiedBorrows"
                        + " royaltyQualifiedBorrowsFromClicks sales salesClicks salesPromoted topOfSearchImpressionShare"
                        + " unitsSold unitsSoldClicks video5SecondViewRate video5SecondViews videoCompleteViews"
                        + " videoFirstQuartileViews videoMidpointViews videoThirdQuartileViews videoUnmutes"
                        + " viewClickThroughRate viewabilityRate viewableImpressions");
        add(
                pair,
                AmazonAdsResource.SPONSORED_BRANDS_ADGROUPS_REPORT_STREAM,
                AmazonAdsResource.SPONSORED_BRANDS_ADGROUPS_REPORT_STREAM_DAILY,
                "SPONSORED_BRANDS",
                "sbAdGroup",
                "adGroup",
                "",
                true,
                "adGroupId adGroupName adStatus addToCart addToCartClicks addToCartRate addToList"
                        + " addToListFromClicks brandedSearches brandedSearchesClicks campaignBudgetAmount"
                        + " campaignBudgetCurrencyCode campaignBudgetType campaignId campaignName campaignStatus clicks cost"
                        + " costType detailPageViews detailPageViewsClicks eCPAddToCart impressions"
                        + " kindleEditionNormalizedPagesRead14d kindleEditionNormalizedPagesRoyalties14d"
                        + " newToBrandDetailPageViewRate newToBrandDetailPageViews newToBrandDetailPageViewsClicks"
                        + " newToBrandECPDetailPageView newToBrandPurchases newToBrandPurchasesClicks"
                        + " newToBrandPurchasesPercentage newToBrandPurchasesRate newToBrandSales newToBrandSalesClicks"
                        + " newToBrandSalesPercentage newToBrandUnitsSold newToBrandUnitsSoldClicks"
                        + " newToBrandUnitsSoldPercentage purchases purchasesClicks purchasesPromoted qualifiedBorrows"
                        + " qualifiedBorrowsFromClicks royaltyQualifiedBorrows royaltyQualifiedBorrowsFromClicks sales"
                        + " salesClicks salesPromoted unitsSold unitsSoldClicks video5SecondViewRate video5SecondViews"
                        + " videoCompleteViews videoFirstQuartileViews videoMidpointViews videoThirdQuartileViews videoUnmutes"
                        + " viewabilityRate");
        add(
                pair,
                AmazonAdsResource.SPONSORED_BRANDS_ADS_REPORT_STREAM,
                AmazonAdsResource.SPONSORED_BRANDS_ADS_REPORT_STREAM_DAILY,
                "SPONSORED_BRANDS",
                "sbAds",
                "ads",
                "",
                true,
                "adGroupId adGroupName adId addToCart addToCartClicks addToCartRate addToList addToListFromClicks"
                        + " brandedSearches brandedSearchesClicks campaignBudgetAmount campaignBudgetCurrencyCode"
                        + " campaignBudgetType campaignId campaignName campaignStatus clicks cost costType detailPageViews"
                        + " detailPageViewsClicks eCPAddToCart impressions kindleEditionNormalizedPagesRead14d"
                        + " kindleEditionNormalizedPagesRoyalties14d newToBrandDetailPageViewRate newToBrandDetailPageViews"
                        + " newToBrandDetailPageViewsClicks newToBrandECPDetailPageView newToBrandPurchases"
                        + " newToBrandPurchasesClicks newToBrandPurchasesPercentage newToBrandPurchasesRate newToBrandSales"
                        + " newToBrandSalesClicks newToBrandSalesPercentage newToBrandUnitsSold newToBrandUnitsSoldClicks"
                        + " newToBrandUnitsSoldPercentage purchases purchasesClicks purchasesPromoted qualifiedBorrows"
                        + " qualifiedBorrowsFromClicks royaltyQualifiedBorrows royaltyQualifiedBorrowsFromClicks sales"
                        + " salesClicks salesPromoted unitsSold unitsSoldClicks video5SecondViewRate video5SecondViews"
                        + " videoCompleteViews videoFirstQuartileViews videoMidpointViews videoThirdQuartileViews videoUnmutes"
                        + " viewabilityRate viewableImpressions");
        add(
                pair,
                AmazonAdsResource.SPONSORED_DISPLAY_CAMPAIGNS_REPORT_STREAM,
                AmazonAdsResource.SPONSORED_DISPLAY_CAMPAIGNS_REPORT_STREAM_DAILY,
                "SPONSORED_DISPLAY",
                "sdCampaigns",
                "campaign",
                "",
                true,
                "addToCart addToCartClicks addToCartRate addToCartViews addToList addToListFromClicks"
                        + " addToListFromViews brandedSearchRate brandedSearches brandedSearchesClicks brandedSearchesViews"
                        + " campaignBudgetAmount campaignBudgetCurrencyCode campaignId campaignName campaignStatus clicks cost"
                        + " costType cumulativeReach detailPageViews detailPageViewsClicks eCPAddToCart eCPBrandSearch"
                        + " impressions impressionsFrequencyAverage impressionsViews leadFormOpens leads linkOuts"
                        + " newToBrandDetailPageViewClicks newToBrandDetailPageViewRate newToBrandDetailPageViewViews"
                        + " newToBrandDetailPageViews newToBrandECPDetailPageView newToBrandPurchases newToBrandPurchasesClicks"
                        + " newToBrandSales newToBrandSalesClicks newToBrandUnitsSold newToBrandUnitsSoldClicks purchases"
                        + " purchasesClicks purchasesPromotedClicks qualifiedBorrows qualifiedBorrowsFromClicks"
                        + " qualifiedBorrowsFromViews royaltyQualifiedBorrows royaltyQualifiedBorrowsFromClicks"
                        + " royaltyQualifiedBorrowsFromViews sales salesClicks salesPromotedClicks unitsSold unitsSoldClicks"
                        + " videoCompleteViews videoFirstQuartileViews videoMidpointViews videoThirdQuartileViews videoUnmutes"
                        + " viewClickThroughRate viewabilityRate");
        add(
                pair,
                AmazonAdsResource.SPONSORED_DISPLAY_ADGROUPS_REPORT_STREAM,
                AmazonAdsResource.SPONSORED_DISPLAY_ADGROUPS_REPORT_STREAM_DAILY,
                "SPONSORED_DISPLAY",
                "sdAdGroup",
                "adGroup",
                "",
                true,
                "adGroupId adGroupName addToCart addToCartClicks addToCartRate addToCartViews addToList"
                        + " addToListFromClicks addToListFromViews bidOptimization brandedSearchRate brandedSearches"
                        + " brandedSearchesClicks brandedSearchesViews campaignBudgetCurrencyCode campaignId campaignName"
                        + " clicks cost cumulativeReach detailPageViews detailPageViewsClicks eCPAddToCart eCPBrandSearch"
                        + " impressions impressionsFrequencyAverage impressionsViews leadFormOpens leads linkOuts"
                        + " newToBrandDetailPageViewClicks newToBrandDetailPageViewRate newToBrandDetailPageViewViews"
                        + " newToBrandDetailPageViews newToBrandECPDetailPageView newToBrandPurchases newToBrandPurchasesClicks"
                        + " newToBrandSales newToBrandSalesClicks newToBrandUnitsSold newToBrandUnitsSoldClicks purchases"
                        + " purchasesClicks purchasesPromotedClicks qualifiedBorrows qualifiedBorrowsFromClicks"
                        + " qualifiedBorrowsFromViews royaltyQualifiedBorrows royaltyQualifiedBorrowsFromClicks"
                        + " royaltyQualifiedBorrowsFromViews sales salesClicks salesPromotedClicks unitsSold unitsSoldClicks"
                        + " videoCompleteViews videoFirstQuartileViews videoMidpointViews videoThirdQuartileViews videoUnmutes"
                        + " viewClickThroughRate viewabilityRate");
        add(
                pair,
                AmazonAdsResource.SPONSORED_DISPLAY_PRODUCTADS_REPORT_STREAM,
                AmazonAdsResource.SPONSORED_DISPLAY_PRODUCTADS_REPORT_STREAM_DAILY,
                "SPONSORED_DISPLAY",
                "sdAdvertisedProduct",
                "advertiser",
                "",
                true,
                "adGroupId adGroupName adId addToCart addToCartClicks addToCartRate addToCartViews addToList"
                        + " addToListFromClicks addToListFromViews bidOptimization brandedSearchRate brandedSearches"
                        + " brandedSearchesClicks brandedSearchesViews campaignBudgetCurrencyCode campaignId campaignName"
                        + " clicks cost cumulativeReach detailPageViews detailPageViewsClicks eCPAddToCart eCPBrandSearch"
                        + " impressions impressionsFrequencyAverage impressionsViews leadFormOpens leads linkOuts"
                        + " newToBrandDetailPageViewClicks newToBrandDetailPageViewRate newToBrandDetailPageViewViews"
                        + " newToBrandDetailPageViews newToBrandECPDetailPageView newToBrandPurchases newToBrandPurchasesClicks"
                        + " newToBrandSales newToBrandSalesClicks newToBrandUnitsSold newToBrandUnitsSoldClicks promotedAsin"
                        + " promotedSku purchases purchasesClicks purchasesPromotedClicks qualifiedBorrows"
                        + " qualifiedBorrowsFromClicks qualifiedBorrowsFromViews royaltyQualifiedBorrows"
                        + " royaltyQualifiedBorrowsFromClicks royaltyQualifiedBorrowsFromViews sales salesClicks"
                        + " salesPromotedClicks unitsSold unitsSoldClicks videoCompleteViews videoFirstQuartileViews"
                        + " videoMidpointViews videoThirdQuartileViews videoUnmutes viewClickThroughRate viewabilityRate");
        add(
                pair,
                AmazonAdsResource.SPONSORED_DISPLAY_TARGETS_REPORT_STREAM,
                AmazonAdsResource.SPONSORED_DISPLAY_TARGETS_REPORT_STREAM_DAILY,
                "SPONSORED_DISPLAY",
                "sdTargeting",
                "targeting",
                "",
                true,
                "adGroupId adGroupName addToCart addToCartClicks addToCartRate addToCartViews addToList"
                        + " addToListFromClicks addToListFromViews brandedSearchRate brandedSearches brandedSearchesClicks"
                        + " brandedSearchesViews campaignBudgetCurrencyCode campaignId campaignName clicks cost detailPageViews"
                        + " detailPageViewsClicks eCPAddToCart eCPBrandSearch impressions impressionsViews leadFormOpens leads"
                        + " linkOuts newToBrandPurchases newToBrandPurchasesClicks newToBrandSales newToBrandSalesClicks"
                        + " newToBrandUnitsSold newToBrandUnitsSoldClicks purchases purchasesClicks purchasesPromotedClicks"
                        + " qualifiedBorrows qualifiedBorrowsFromClicks qualifiedBorrowsFromViews royaltyQualifiedBorrows"
                        + " royaltyQualifiedBorrowsFromClicks royaltyQualifiedBorrowsFromViews sales salesClicks"
                        + " salesPromotedClicks targetingExpression targetingId targetingText unitsSold unitsSoldClicks"
                        + " videoCompleteViews videoFirstQuartileViews videoMidpointViews videoThirdQuartileViews videoUnmutes"
                        + " viewClickThroughRate viewabilityRate");
        add(
                pair,
                AmazonAdsResource.SPONSORED_DISPLAY_ASINS_REPORT_STREAM,
                AmazonAdsResource.SPONSORED_DISPLAY_ASINS_REPORT_STREAM_DAILY,
                "SPONSORED_DISPLAY",
                "sdPurchasedProduct",
                "asin",
                "",
                true,
                "adGroupId adGroupName addToList addToListFromClicks addToListFromViews asinBrandHalo"
                        + " campaignBudgetCurrencyCode campaignId campaignName conversionsBrandHalo conversionsBrandHaloClicks"
                        + " promotedAsin promotedSku qualifiedBorrows qualifiedBorrowsFromClicks qualifiedBorrowsFromViews"
                        + " royaltyQualifiedBorrows royaltyQualifiedBorrowsFromClicks royaltyQualifiedBorrowsFromViews"
                        + " salesBrandHalo salesBrandHaloClicks unitsSoldBrandHalo unitsSoldBrandHaloClicks");
        add(
                pair,
                AmazonAdsResource.SPONSORED_PRODUCTS_CAMPAIGNS_REPORT_STREAM,
                AmazonAdsResource.SPONSORED_PRODUCTS_CAMPAIGNS_REPORT_STREAM_DAILY,
                "SPONSORED_PRODUCTS",
                "spCampaigns",
                "campaign",
                "",
                false,
                "attributedSalesSameSku14d attributedSalesSameSku1d attributedSalesSameSku30d"
                        + " attributedSalesSameSku7d campaignApplicableBudgetRuleId campaignApplicableBudgetRuleName"
                        + " campaignBudgetAmount campaignId campaignName campaignRuleBasedBudgetAmount campaignStatus clicks"
                        + " cost impressions purchases14d purchases1d purchases30d purchases7d purchasesSameSku14d"
                        + " purchasesSameSku1d purchasesSameSku30d purchasesSameSku7d sales14d sales1d sales30d sales7d"
                        + " unitsSoldClicks14d unitsSoldClicks1d unitsSoldClicks30d unitsSoldClicks7d unitsSoldSameSku14d"
                        + " unitsSoldSameSku1d unitsSoldSameSku30d unitsSoldSameSku7d");
        add(
                pair,
                AmazonAdsResource.SPONSORED_PRODUCTS_ADGROUPS_REPORT_STREAM,
                AmazonAdsResource.SPONSORED_PRODUCTS_ADGROUPS_REPORT_STREAM_DAILY,
                "SPONSORED_PRODUCTS",
                "spCampaigns",
                "campaign adGroup",
                "",
                false,
                "adGroupId adGroupName attributedSalesSameSku14d attributedSalesSameSku1d attributedSalesSameSku30d"
                        + " attributedSalesSameSku7d campaignId campaignName clicks cost impressions purchases14d purchases1d"
                        + " purchases30d purchases7d purchasesSameSku14d purchasesSameSku1d purchasesSameSku30d"
                        + " purchasesSameSku7d sales14d sales1d sales30d sales7d unitsSoldClicks14d unitsSoldClicks1d"
                        + " unitsSoldClicks30d unitsSoldClicks7d unitsSoldSameSku14d unitsSoldSameSku1d unitsSoldSameSku30d"
                        + " unitsSoldSameSku7d");
        add(
                pair,
                AmazonAdsResource.SPONSORED_PRODUCTS_KEYWORDS_REPORT_STREAM,
                AmazonAdsResource.SPONSORED_PRODUCTS_KEYWORDS_REPORT_STREAM_DAILY,
                "SPONSORED_PRODUCTS",
                "spTargeting",
                "targeting",
                "BROAD EXACT PHRASE",
                false,
                "adGroupId adGroupName attributedSalesSameSku14d attributedSalesSameSku1d attributedSalesSameSku30d"
                        + " attributedSalesSameSku7d campaignId campaignName clicks cost impressions keyword keywordId"
                        + " matchType purchases14d purchases1d purchases30d purchases7d purchasesSameSku14d purchasesSameSku1d"
                        + " purchasesSameSku30d purchasesSameSku7d sales14d sales1d sales30d sales7d unitsSoldClicks14d"
                        + " unitsSoldClicks1d unitsSoldClicks30d unitsSoldClicks7d unitsSoldSameSku14d unitsSoldSameSku1d"
                        + " unitsSoldSameSku30d unitsSoldSameSku7d");
        add(
                pair,
                AmazonAdsResource.SPONSORED_PRODUCTS_TARGETS_REPORT_STREAM,
                AmazonAdsResource.SPONSORED_PRODUCTS_TARGETS_REPORT_STREAM_DAILY,
                "SPONSORED_PRODUCTS",
                "spTargeting",
                "targeting",
                "TARGETING_EXPRESSION TARGETING_EXPRESSION_PREDEFINED",
                false,
                "adGroupId adGroupName attributedSalesSameSku14d attributedSalesSameSku1d attributedSalesSameSku30d"
                        + " attributedSalesSameSku7d campaignId campaignName clicks cost impressions keyword keywordId"
                        + " keywordType purchases14d purchases1d purchases30d purchases7d purchasesSameSku14d"
                        + " purchasesSameSku1d purchasesSameSku30d purchasesSameSku7d sales14d sales1d sales30d sales7d"
                        + " targeting unitsSoldClicks14d unitsSoldClicks1d unitsSoldClicks30d unitsSoldClicks7d"
                        + " unitsSoldSameSku14d unitsSoldSameSku1d unitsSoldSameSku30d unitsSoldSameSku7d");
        add(
                pair,
                AmazonAdsResource.SPONSORED_PRODUCTS_SEARCH_TERMS_REPORT_STREAM,
                AmazonAdsResource.SPONSORED_PRODUCTS_SEARCH_TERMS_REPORT_STREAM_DAILY,
                "SPONSORED_PRODUCTS",
                "spSearchTerm",
                "searchTerm",
                "BROAD EXACT PHRASE TARGETING_EXPRESSION TARGETING_EXPRESSION_PREDEFINED",
                false,
                "acosClicks14d acosClicks7d adGroupId adGroupName adKeywordStatus addToList"
                        + " attributedSalesSameSku14d attributedSalesSameSku1d attributedSalesSameSku30d"
                        + " attributedSalesSameSku7d campaignBudgetAmount campaignBudgetCurrencyCode campaignBudgetType"
                        + " campaignId campaignName campaignStatus clickThroughRate clicks cost costPerClick impressions"
                        + " keyword keywordBid keywordId keywordType kindleEditionNormalizedPagesRead14d"
                        + " kindleEditionNormalizedPagesRoyalties14d matchType portfolioId purchases14d purchases1d"
                        + " purchases30d purchases7d purchasesSameSku14d purchasesSameSku1d purchasesSameSku30d"
                        + " purchasesSameSku7d qualifiedBorrows roasClicks14d roasClicks7d royaltyQualifiedBorrows sales14d"
                        + " sales1d sales30d sales7d salesOtherSku7d searchTerm targeting unitsSoldClicks14d unitsSoldClicks1d"
                        + " unitsSoldClicks30d unitsSoldClicks7d unitsSoldOtherSku7d unitsSoldSameSku14d unitsSoldSameSku1d"
                        + " unitsSoldSameSku30d unitsSoldSameSku7d");
        add(
                pair,
                AmazonAdsResource.SPONSORED_PRODUCTS_PRODUCTADS_REPORT_STREAM,
                AmazonAdsResource.SPONSORED_PRODUCTS_PRODUCTADS_REPORT_STREAM_DAILY,
                "SPONSORED_PRODUCTS",
                "spAdvertisedProduct",
                "advertiser",
                "",
                false,
                "adGroupId adGroupName adId advertisedAsin attributedSalesSameSku14d attributedSalesSameSku1d"
                        + " attributedSalesSameSku30d attributedSalesSameSku7d campaignBudgetCurrencyCode campaignId"
                        + " campaignName clicks cost impressions purchases14d purchases1d purchases30d purchases7d"
                        + " purchasesSameSku14d purchasesSameSku1d purchasesSameSku30d purchasesSameSku7d sales14d sales1d"
                        + " sales30d sales7d unitsSoldClicks14d unitsSoldClicks1d unitsSoldClicks30d unitsSoldClicks7d"
                        + " unitsSoldSameSku14d unitsSoldSameSku1d unitsSoldSameSku30d unitsSoldSameSku7d");
        add(
                pair,
                AmazonAdsResource.SPONSORED_PRODUCTS_ASINS_KEYWORDS_REPORT_STREAM,
                AmazonAdsResource.SPONSORED_PRODUCTS_ASINS_KEYWORDS_REPORT_STREAM_DAILY,
                "SPONSORED_PRODUCTS",
                "spPurchasedProduct",
                "asin",
                "",
                false,
                "adGroupId adGroupName advertisedAsin advertisedSku campaignBudgetCurrencyCode campaignId"
                        + " campaignName keyword keywordId matchType purchasedAsin salesOtherSku14d salesOtherSku1d"
                        + " salesOtherSku30d salesOtherSku7d unitsSoldClicks14d unitsSoldClicks1d unitsSoldClicks30d"
                        + " unitsSoldClicks7d unitsSoldOtherSku14d unitsSoldOtherSku1d unitsSoldOtherSku30d unitsSoldOtherSku7d");
        add(
                pair,
                AmazonAdsResource.SPONSORED_PRODUCTS_ASINS_TARGETS_REPORT_STREAM,
                AmazonAdsResource.SPONSORED_PRODUCTS_ASINS_TARGETS_REPORT_STREAM_DAILY,
                "SPONSORED_PRODUCTS",
                "spPurchasedProduct",
                "asin",
                "",
                false,
                "adGroupId adGroupName advertisedAsin advertisedSku campaignBudgetCurrencyCode campaignId"
                        + " campaignName keywordId keywordType matchType purchasedAsin salesOtherSku14d salesOtherSku1d"
                        + " salesOtherSku30d salesOtherSku7d targeting unitsSoldClicks14d unitsSoldClicks1d unitsSoldClicks30d"
                        + " unitsSoldClicks7d unitsSoldOtherSku14d unitsSoldOtherSku1d unitsSoldOtherSku30d unitsSoldOtherSku7d");
        return Collections.unmodifiableMap(pair);
    }

    private static void add(
            Map<AmazonAdsResource, Template> mapping,
            AmazonAdsResource summary,
            AmazonAdsResource daily,
            String product,
            String reportType,
            String grouping,
            String keywordTypes,
            boolean summaryDates,
            String columns) {
        List<String> base = List.of(columns.split(" "));
        List<String> dimensions = List.of(grouping.split(" "));
        List<String> filter =
                keywordTypes.isEmpty() ? Collections.emptyList() : List.of(keywordTypes.split(" "));
        List<String> summaryProjection = new ArrayList<>(base);
        if (summaryDates) {
            summaryProjection.add("startDate");
            summaryProjection.add("endDate");
        }
        List<String> dailyProjection = new ArrayList<>(base);
        dailyProjection.add("date");
        mapping.put(
                summary,
                new Template(
                        product, reportType, "SUMMARY", dimensions, filter, summaryProjection));
        mapping.put(
                daily,
                new Template(product, reportType, "DAILY", dimensions, filter, dailyProjection));
    }

    /**
     * Stores a single service-defined metric projection without mutable request state.
     *
     * <p>The projection is assembled once and can be attached to multiple profile and date
     * requests.
     */
    static final class Template {
        final String product;
        final String type;
        final String timeUnit;
        final List<String> groupBy;
        final List<String> keywordTypes;
        final List<String> columns;

        private Template(
                String product,
                String type,
                String timeUnit,
                List<String> groupBy,
                List<String> keywordTypes,
                List<String> columns) {
            this.product = product;
            this.type = type;
            this.timeUnit = timeUnit;
            this.groupBy = List.copyOf(groupBy);
            this.keywordTypes = List.copyOf(keywordTypes);
            this.columns = List.copyOf(columns);
        }

        void write(ObjectNode config) {
            config.put("adProduct", product);
            config.put("reportTypeId", type);
            config.put("timeUnit", timeUnit);
            config.put("format", "GZIP_JSON");
            ArrayNode columnArray = config.putArray("columns");
            columns.forEach(columnArray::add);
            ArrayNode groups = config.putArray("groupBy");
            groupBy.forEach(groups::add);
            ArrayNode filters = config.putArray("filters");
            if (!keywordTypes.isEmpty()) {
                ArrayNode values =
                        filters.addObject().put("field", "keywordType").putArray("values");
                keywordTypes.forEach(values::add);
            }
        }
    }
}
