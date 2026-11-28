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

import java.util.Map;

/**
 * The supported SP-API routes and report record shapes.
 *
 * <p>Each report has a fixed document interpretation and a declared request window. Public resource
 * names remain distinct where multiple views share the same Amazon report type.
 */
enum SpResource {
    Orders(Api.ORDERS),
    OrderItems(Api.ORDER_ITEMS),
    ListFinancialEventGroups(Api.FINANCE_GROUPS),
    ListFinancialEvents(Api.FINANCE_EVENTS),
    FbaInboundShipments(Api.FBA_SHIPMENTS),
    FbaInboundShipmentItems(Api.FBA_ITEMS),
    VendorOrders(Api.VENDOR_ORDERS),
    VendorOrdersStatus(Api.VENDOR_STATUS),
    VendorDirectFulfillmentShipping(Api.VENDOR_SHIPPING),

    GET_VENDOR_FORECASTING_FRESH_REPORT("GET_VENDOR_FORECASTING_REPORT", "forecastByAsin", "FRESH"),
    GET_VENDOR_FORECASTING_RETAIL_REPORT(
            "GET_VENDOR_FORECASTING_REPORT", "forecastByAsin", "RETAIL"),
    GET_VENDOR_SALES_REPORT("salesByAsin", Window.DAY, "endDate"),
    GET_VENDOR_INVENTORY_REPORT("inventoryByAsin", Window.DAY, "endDate"),
    GET_VENDOR_TRAFFIC_REPORT("trafficByAsin", Window.DAY, "endDate"),
    GET_VENDOR_NET_PURE_PRODUCT_MARGIN_REPORT("netPureProductMarginByAsin", Window.DAY, "endDate"),
    GET_VENDOR_REAL_TIME_INVENTORY_REPORT(
            Account.VENDOR, Format.JSON, Window.DAY, 1, "reportData", "endTime"),

    GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL(30, false),
    GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL(30, false),
    GET_FBA_STORAGE_FEE_CHARGES_DATA,
    GET_AMAZON_FULFILLED_SHIPMENTS_DATA_GENERAL(30, true),
    GET_FLAT_FILE_OPEN_LISTINGS_DATA,
    GET_FBA_FULFILLMENT_REMOVAL_ORDER_DETAIL_DATA,
    GET_FLAT_FILE_ACTIONABLE_ORDER_DATA_SHIPPING(Format.TSV, true),
    GET_ORDER_REPORT_DATA_SHIPPING(
            Account.SELLER,
            Format.ORDER_XML,
            Window.GENERATED,
            89,
            null,
            null,
            null,
            Map.of(),
            true),
    GET_LEDGER_DETAIL_VIEW_DATA,
    GET_LEDGER_SUMMARY_VIEW_DATA,
    GET_FBA_REIMBURSEMENTS_DATA,
    GET_FBA_FULFILLMENT_CUSTOMER_RETURNS_DATA,
    GET_SELLER_FEEDBACK_DATA(Format.FEEDBACK, false),
    GET_AFN_INVENTORY_DATA,
    GET_AFN_INVENTORY_DATA_BY_COUNTRY,
    GET_FBA_INVENTORY_PLANNING_DATA,
    GET_FLAT_FILE_RETURNS_DATA_BY_RETURN_DATE(60, true),
    GET_FLAT_FILE_ARCHIVED_ORDERS_DATA_BY_ORDER_DATE(365, false),
    GET_MERCHANTS_LISTINGS_FYP_REPORT,
    GET_MERCHANT_CANCELLED_LISTINGS_DATA,
    GET_MERCHANT_LISTINGS_DATA_BACK_COMPAT,
    GET_STRANDED_INVENTORY_UI_DATA,
    GET_FBA_MYI_UNSUPPRESSED_INVENTORY_DATA,
    GET_FBA_FULFILLMENT_CUSTOMER_SHIPMENT_PROMOTION_DATA,
    GET_FBA_ESTIMATED_FBA_FEES_TXT_DATA,
    GET_RESTOCK_INVENTORY_RECOMMENDATIONS_REPORT,
    GET_FBA_FULFILLMENT_CUSTOMER_SHIPMENT_REPLACEMENT_DATA,
    GET_FBA_FULFILLMENT_REMOVAL_SHIPMENT_DETAIL_DATA,
    GET_XML_BROWSE_TREE_DATA(Format.BROWSE_XML, false),
    GET_XML_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL(Format.ORDER_XML, 30, false),
    GET_MERCHANT_LISTINGS_ALL_DATA,
    GET_MERCHANT_LISTINGS_DATA,
    GET_MERCHANT_LISTINGS_INACTIVE_DATA,
    GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE_V2(Format.TSV, Window.GENERATED, 89),

    GET_BRAND_ANALYTICS_MARKET_BASKET_REPORT("dataByAsin"),
    GET_BRAND_ANALYTICS_SEARCH_TERMS_REPORT("dataByDepartmentAndSearchTerm"),
    GET_BRAND_ANALYTICS_REPEAT_PURCHASE_REPORT("dataByAsin"),
    GET_BRAND_ANALYTICS_ALTERNATE_PURCHASE_REPORT("dataByAsin"),
    GET_BRAND_ANALYTICS_ITEM_COMPARISON_REPORT("dataByAsin"),
    GET_SALES_AND_TRAFFIC_REPORT("salesAndTrafficByAsin", Window.DAY),
    GET_SALES_AND_TRAFFIC_REPORT_BY_DATE("salesAndTrafficByDate", Window.DAY),
    GET_SALES_AND_TRAFFIC_REPORT_BY_MONTH("salesAndTrafficByAsin", Window.MONTH);

    /**
     * Selling partner role family.
     *
     * <p>Selection is validated before authenticating, since seller and vendor routes have
     * different permissions.
     */
    enum Account {
        SELLER,
        VENDOR
    }

    /**
     * The non-report operation used to read a response.
     *
     * <p>Parent-based streams reuse their discovery route and attach the parent identity to each
     * returned child record.
     */
    enum Api {
        ORDERS(Account.SELLER, "orders/v0/orders", "Orders", "AmazonOrderId"),
        ORDER_ITEMS(Account.SELLER, "orders/v0/orders", "OrderItems", "OrderItemId"),
        FINANCE_GROUPS(
                Account.SELLER,
                "finances/v0/financialEventGroups",
                "FinancialEventGroupList",
                "FinancialEventGroupId"),
        FINANCE_EVENTS(Account.SELLER, "finances/v0/financialEvents", "FinancialEvents", null),
        FBA_SHIPMENTS(Account.SELLER, "fba/inbound/v0/shipments", "ShipmentData", "ShipmentId"),
        FBA_ITEMS(Account.SELLER, "fba/inbound/v0/shipments", "ItemData", "SellerSKU"),
        VENDOR_ORDERS(
                Account.VENDOR, "vendor/orders/v1/purchaseOrders", "orders", "purchaseOrderNumber"),
        VENDOR_STATUS(
                Account.VENDOR,
                "vendor/orders/v1/purchaseOrdersStatus",
                "ordersStatus",
                "purchaseOrderNumber"),
        VENDOR_SHIPPING(
                Account.VENDOR,
                "vendor/directFulfillment/shipping/v1/shippingLabels",
                "shippingLabels",
                "purchaseOrderNumber");

        final Account account;
        final String path;
        final String records;
        final String id;

        Api(Account account, String path, String records, String id) {
            this.account = account;
            this.path = path;
            this.records = records;
            this.id = id;
        }
    }

    /**
     * The report document record format.
     *
     * <p>Order and browse-tree XML have distinct record paths. Feedback has localized header labels
     * and a stable output layout.
     */
    enum Format {
        TSV,
        JSON,
        ORDER_XML,
        BROWSE_XML,
        FEEDBACK
    }

    /**
     * Report time-window policy.
     *
     * <p>Generated reports are discoverable through the list operation and cannot be submitted
     * through the report creation route.
     */
    enum Window {
        RANGE,
        DAY,
        MONTH,
        PRIOR_WEEK,
        SNAPSHOT,
        GENERATED
    }

    final Api api;
    final Account account;
    final String reportType;
    final Format format;
    final Window window;
    final int maxDays;
    final String jsonArray;
    final String windowField;
    final Map<String, String> fixedOptions;
    final boolean restrictedDocument;

    SpResource(Api api) {
        this.api = api;
        account = api.account;
        reportType = null;
        format = null;
        window = null;
        maxDays = 0;
        jsonArray = null;
        windowField = null;
        fixedOptions = Map.of();
        restrictedDocument = false;
    }

    SpResource() {
        this(365, true);
    }

    SpResource(int maxDays, boolean attachWindow) {
        this(
                Account.SELLER,
                Format.TSV,
                Window.RANGE,
                maxDays,
                null,
                attachWindow ? "dataEndTime" : null);
    }

    SpResource(Format format, boolean restricted) {
        this(format, 365, restricted);
    }

    SpResource(Format format, int maxDays, boolean restricted) {
        this(
                Account.SELLER,
                format,
                Window.RANGE,
                maxDays,
                null,
                "dataEndTime",
                null,
                Map.of(),
                restricted);
    }

    SpResource(Format format, Window window, int maxDays) {
        this(Account.SELLER, format, window, maxDays, null, null);
    }

    SpResource(String array) {
        this(
                Account.SELLER,
                Format.JSON,
                Window.PRIOR_WEEK,
                7,
                array,
                null,
                null,
                Map.of("reportPeriod", "WEEK"),
                false);
    }

    SpResource(String array, Window window) {
        this(
                Account.SELLER,
                Format.JSON,
                window,
                1,
                array,
                "queryEndDate",
                "GET_SALES_AND_TRAFFIC_REPORT",
                Map.of("dateGranularity", window == Window.MONTH ? "MONTH" : "DAY"),
                false);
    }

    SpResource(String array, Window window, String windowField) {
        this(Account.VENDOR, Format.JSON, window, 1, array, windowField);
    }

    SpResource(String reportType, String array, String program) {
        this(
                Account.VENDOR,
                Format.JSON,
                Window.SNAPSHOT,
                1,
                array,
                null,
                reportType,
                Map.of("sellingProgram", program),
                false);
    }

    SpResource(
            Account account,
            Format format,
            Window window,
            int maxDays,
            String jsonArray,
            String windowField) {
        this(account, format, window, maxDays, jsonArray, windowField, null, Map.of(), false);
    }

    SpResource(
            Account account,
            Format format,
            Window window,
            int maxDays,
            String jsonArray,
            String windowField,
            String reportType,
            Map<String, String> fixedOptions,
            boolean restrictedDocument) {
        api = null;
        this.account = account;
        this.reportType = reportType == null ? name() : reportType;
        this.format = format;
        this.window = window;
        this.maxDays = maxDays;
        this.jsonArray = jsonArray;
        this.windowField = windowField;
        this.fixedOptions = fixedOptions;
        this.restrictedDocument = restrictedDocument;
    }

    static SpResource select(String value) {
        for (SpResource resource : values()) {
            if (resource.name().equalsIgnoreCase(value)) {
                return resource;
            }
        }
        throw new IllegalArgumentException("AmazonSellerPartner requires a supported resource");
    }

    boolean isVendorDailyAnalytics() {
        return account == Account.VENDOR
                && window == Window.DAY
                && this != GET_VENDOR_REAL_TIME_INVENTORY_REPORT;
    }

    boolean isReport() {
        return api == null;
    }
}
