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

package org.apache.seatunnel.e2e.connector.amazonsellerpartner;

import org.apache.seatunnel.shade.com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.EngineType;
import org.apache.seatunnel.e2e.common.container.TestContainer;
import org.apache.seatunnel.e2e.common.junit.DisabledOnContainer;
import org.apache.seatunnel.e2e.common.util.ContainerUtil;
import org.apache.seatunnel.e2e.common.util.JobIdGenerator;

import org.awaitility.Awaitility;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.junit.jupiter.api.parallel.Execution;
import org.junit.jupiter.api.parallel.ExecutionMode;
import org.testcontainers.containers.Container;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.utility.DockerImageName;
import org.testcontainers.utility.MountableFile;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.math.BigDecimal;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.time.DayOfWeek;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.temporal.TemporalAdjusters;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * Executes real SeaTunnel jobs against a separate JDK 11 SP-API HTTPS service.
 *
 * <p>Assertions cover the emitted provider rows as well as the exact external request and restore
 * trajectory.
 */
@Execution(ExecutionMode.SAME_THREAD)
class AmazonSellerPartnerSourceIT extends TestSuiteBase implements TestResource {
    private static final ObjectMapper JSON =
            new ObjectMapper().enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS);
    private static final String REPORTS = "/reports/2021-06-30/reports";
    private static final String DOCUMENT = "/reports/2021-06-30/documents/doc1";
    private static final String ORDER_ROUTE = "/orders/v0/orders";
    private static final String SHIPMENT_ROUTE = "/fba/inbound/v0/shipments";
    private static final String ORDER1 = "111-1234567-1234567";
    private static final String ORDER2 = "111-1234567-1234568";
    private static final String SELLER_MARKET = "ATVPDKIKX0DER";
    private static final String PRIVATE_SIGNATURE = "fixture-private-signature";
    private static final String TOKEN_ROUTE = "/tokens/2021-03-01/restrictedDataToken";
    private static final String CERT = "/tmp/seatunnel-sp-fixture.pem";
    private static final String WIDE = "9007199254740993.123456789012345678";
    private final LocalDate sampleDay = LocalDate.now(ZoneOffset.UTC).minusDays(8);
    private GenericContainer<?> fixture;
    private Path keys;

    /**
     * Starts the fixture with one generated TLS identity and the existing Jackson jars.
     *
     * <p>Only its control port is mapped to the test harness. The engines use the isolated
     * container network for HTTPS calls.
     */
    @BeforeAll
    @Override
    public void startUp() throws Exception {
        keys = Files.createTempDirectory("seatunnel-sp-fixture-");
        certificate("fixture", SpApiFixture.HOST);
        certificate("unrelated", SpApiFixture.HOST);
        fixture =
                new GenericContainer<>(DockerImageName.parse("eclipse-temurin:11-jdk"))
                        .withNetwork(NETWORK)
                        .withNetworkAliases(SpApiFixture.HOST, "unknown-sp-host")
                        .withExposedPorts(SpApiFixture.CONTROL_PORT)
                        .withCopyFileToContainer(
                                MountableFile.forHostPath(classes(SpApiFixture.class)), "/sp/test")
                        .withCopyFileToContainer(
                                MountableFile.forHostPath(keys.resolve("fixture.p12")),
                                "/sp/server.p12")
                        .waitingFor(Wait.forHttp("/ready").forStatusCode(200));
        Set<Path> libraries = new LinkedHashSet<>();
        libraries.add(classes(JsonFactory.class));
        libraries.add(classes(JsonProperty.class));
        libraries.add(classes(ObjectMapper.class));
        StringBuilder classpath = new StringBuilder("/sp/test");
        int offset = 0;
        for (Path library : libraries) {
            String destination = "/sp/lib" + offset++ + ".jar";
            fixture.withCopyFileToContainer(MountableFile.forHostPath(library), destination);
            classpath.append(':').append(destination);
        }
        fixture.withCommand(
                "java",
                "--add-modules",
                "jdk.httpserver",
                "-cp",
                classpath.toString(),
                SpApiFixture.class.getName(),
                "/sp/server.p12");
        fixture.start();
    }

    /**
     * Removes all fixture resources once the class finishes.
     *
     * <p>The generated certificates are limited to this test container and its engine copies.
     */
    @AfterAll
    @Override
    public void tearDown() throws Exception {
        try {
            if (fixture != null) {
                fixture.close();
            }
        } finally {
            if (keys != null) {
                for (String name : List.of("fixture", "unrelated")) {
                    Files.deleteIfExists(keys.resolve(name + ".pem"));
                    Files.deleteIfExists(keys.resolve(name + ".p12"));
                }
                Files.deleteIfExists(keys);
            }
        }
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void sellerOrdersAndTheirItemsTraverseBothCursorChains(TestContainer engine) throws Exception {
        trust(engine);
        ObjectNode firstOrder = order(ORDER1).putNull("Optional");
        ObjectNode lastOrder = order(ORDER2);
        ObjectNode first = payload("Orders", arr(firstOrder));
        first.with("payload").put("NextToken", "orders ?/+%二");
        setup(
                program(
                        get(ORDER_ROUTE, first),
                        get(ORDER_ROUTE, payload("Orders", arr(lastOrder)))));
        List<Row> orders = run(engine, options("Orders"));
        Assertions.assertEquals(
                List.of(ORDER1, ORDER2), List.of(orders.get(0).id, orders.get(1).id));
        Assertions.assertTrue(orders.get(0).data.path("Optional").isNull());
        JsonNode firstCall = requests().get(0);
        Assertions.assertEquals(SELLER_MARKET, firstCall.at("/query/MarketplaceIds").asText());
        Assertions.assertEquals(
                start().toString(), firstCall.at("/query/LastUpdatedAfter").asText());
        Assertions.assertEquals("orders ?/+%二", requests().get(1).at("/query/NextToken").asText());

        String firstPath = ORDER_ROUTE + "/" + ORDER1 + "/orderItems";
        String secondPath = ORDER_ROUTE + "/" + ORDER2 + "/orderItems";
        ObjectNode firstItem =
                payload(
                        "OrderItems",
                        arr(obj().put("OrderItemId", "item-1").put("QuantityOrdered", 2)));
        firstItem.with("payload").put("NextToken", "items/+?");
        setup(
                program(
                        get(ORDER_ROUTE, first),
                        get(firstPath, firstItem),
                        get(
                                firstPath,
                                payload("OrderItems", arr(obj().put("OrderItemId", "item-2")))),
                        get(ORDER_ROUTE, payload("Orders", arr(lastOrder))),
                        get(
                                secondPath,
                                payload("OrderItems", arr(obj().put("OrderItemId", "item-3"))))));
        List<Row> items = run(engine, options("OrderItems"));
        Assertions.assertEquals(3, items.size());
        Assertions.assertEquals(ORDER1, items.get(0).parent);
        Assertions.assertEquals(ORDER1, items.get(0).data.path("AmazonOrderId").asText());
        Assertions.assertEquals(
                firstOrder.path("LastUpdateDate"), items.get(0).data.path("LastUpdateDate"));
        Assertions.assertEquals(2, items.get(0).data.path("QuantityOrdered").asInt());
        Assertions.assertEquals(ORDER2, items.get(2).parent);
        Assertions.assertEquals("items/+?", requests().get(2).at("/query/NextToken").asText());
        Assertions.assertEquals("orders ?/+%二", requests().get(3).at("/query/NextToken").asText());

        ObjectNode duplicate = payload("Orders", arr());
        duplicate.with("payload").put("NextToken", "orders ?/+%二");
        setup(program(get(ORDER_ROUTE, first), get(ORDER_ROUTE, duplicate)));
        rejected(engine, options("Orders"), "provider repeated its cursor");
        Assertions.assertEquals(2, requests().size());
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void financeUsesItsOwnWindowAndContinuesThroughEmptyPages(TestContainer engine)
            throws Exception {
        trust(engine);
        String groups = "/finances/v0/financialEventGroups";
        String events = "/finances/v0/financialEvents";
        ObjectNode noGroups = payload("FinancialEventGroupList", arr());
        noGroups.with("payload").put("NextToken", "finance-groups-next");
        ObjectNode group = obj().put("FinancialEventGroupId", "event-group");
        group.putObject("OriginalTotal")
                .put("CurrencyCode", "USD")
                .put("CurrencyAmount", new BigDecimal(WIDE));
        setup(
                program(
                        get(groups, noGroups),
                        get(groups, payload("FinancialEventGroupList", arr(group)))));
        List<Row> groupRows =
                run(engine, options("ListFinancialEventGroups").put("financial_page_size", 21));
        Assertions.assertEquals(1, groupRows.size());
        Assertions.assertEquals("event-group", groupRows.get(0).id);
        exact(groupRows.get(0).data.at("/OriginalTotal/CurrencyAmount"));
        Assertions.assertEquals("21", requests().get(1).at("/query/MaxResultsPerPage").asText());
        Assertions.assertEquals(
                start().toString(),
                requests().get(1).at("/query/FinancialEventGroupStartedAfter").asText());
        Assertions.assertEquals(
                "finance-groups-next", requests().get(1).at("/query/NextToken").asText());

        ObjectNode empty = payload("FinancialEvents", obj());
        empty.with("payload").put("NextToken", "events-next");
        ObjectNode data = obj();
        data.putArray("ShipmentEventList")
                .addObject()
                .put("AmazonOrderId", ORDER1)
                .putNull("Optional");
        setup(program(get(events, empty), get(events, payload("FinancialEvents", data))));
        List<Row> eventRows =
                run(engine, options("ListFinancialEvents").put("financial_page_size", 10));
        Assertions.assertEquals(1, eventRows.size());
        Assertions.assertEquals(
                ORDER1, eventRows.get(0).data.at("/ShipmentEventList/0/AmazonOrderId").asText());
        Assertions.assertEquals(
                end().toString(), eventRows.get(0).data.path("PostedBefore").asText());
        Assertions.assertEquals("events-next", requests().get(1).at("/query/NextToken").asText());
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void inboundShipmentsUseDateThenTokenAndPreserveItemLineage(TestContainer engine)
            throws Exception {
        trust(engine);
        ObjectNode shipment =
                payload(
                        "ShipmentData",
                        arr(obj().put("ShipmentId", "FBA1").put("ShipmentStatus", "SHIPPED")));
        shipment.with("payload").put("NextToken", "inbound/+next");
        ObjectNode later =
                payload(
                        "ShipmentData",
                        arr(obj().put("ShipmentId", "FBA2").put("ShipmentStatus", "RECEIVING")));
        setup(program(get(SHIPMENT_ROUTE, shipment), get(SHIPMENT_ROUTE, later)));
        ObjectNode options =
                options("FbaInboundShipments")
                        .put("inbound_replication_mode", "FIXED")
                        .put("inbound_start_datetime", start().toString())
                        .put("inbound_end_datetime", end().toString());
        List<Row> rows = run(engine, options);
        Assertions.assertEquals(2, rows.size());
        Assertions.assertEquals("FBA2", rows.get(1).id);
        Assertions.assertEquals("RECEIVING", rows.get(1).data.path("ShipmentStatus").asText());
        Assertions.assertEquals("DATE_RANGE", requests().get(0).at("/query/QueryType").asText());
        Assertions.assertEquals(
                start().toString(), requests().get(0).at("/query/LastUpdatedAfter").asText());
        Assertions.assertEquals("NEXT_TOKEN", requests().get(1).at("/query/QueryType").asText());
        Assertions.assertFalse(requests().get(1).path("query").has("LastUpdatedAfter"));

        ObjectNode first =
                payload("ItemData", arr(obj().put("SellerSKU", "sku-a").put("QuantityShipped", 3)));
        ObjectNode second =
                payload(
                        "ItemData",
                        arr(obj().put("SellerSKU", "sku-b").put("QuantityReceived", 2)));
        setup(
                program(
                        get(SHIPMENT_ROUTE, shipment),
                        get(SHIPMENT_ROUTE + "/FBA1/items", first),
                        get(SHIPMENT_ROUTE, later),
                        get(SHIPMENT_ROUTE + "/FBA2/items", second)));
        List<Row> items =
                run(engine, options.deepCopy().put("resource", "FbaInboundShipmentItems"));
        Assertions.assertEquals(2, items.size());
        Assertions.assertEquals("FBA2/sku-b", items.get(1).id);
        Assertions.assertEquals("FBA2", items.get(1).parent);
        Assertions.assertEquals("FBA2", items.get(1).data.path("ShipmentId").asText());
        Assertions.assertEquals(2, items.get(1).data.path("QuantityReceived").asInt());
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void vendorOrdersStatusesAndLabelsKeepTheirSpecificQueriesAndPayload(TestContainer engine)
            throws Exception {
        trust(engine);
        for (String[] choice :
                List.of(
                        new String[] {
                            "VendorOrders", "/vendor/orders/v1/purchaseOrders", "orders", "changed"
                        },
                        new String[] {
                            "VendorOrdersStatus",
                            "/vendor/orders/v1/purchaseOrdersStatus",
                            "ordersStatus",
                            "created"
                        },
                        new String[] {
                            "VendorDirectFulfillmentShipping",
                            "/vendor/directFulfillment/shipping/v1/shippingLabels",
                            "shippingLabels",
                            "created"
                        })) {
            ObjectNode first =
                    payload(
                            choice[2],
                            arr(
                                    obj().put("purchaseOrderNumber", "purchase-1")
                                            .put("status", "OPEN")));
            first.with("payload").putObject("pagination").put("nextToken", "vendor-next");
            ObjectNode next =
                    payload(
                            choice[2],
                            arr(
                                    obj().put("purchaseOrderNumber", "purchase-2")
                                            .put("status", "SHIPPED")));
            boolean restricted = "VendorDirectFulfillmentShipping".equals(choice[0]);
            String auth = restricted ? "rdt" : "lwa";
            ObjectNode settings = options(choice[0]).put("account_type", "VENDOR");
            if (restricted) {
                setup(program());
                rejected(engine, settings, "requires restricted data access");
                Assertions.assertEquals(0, requests().size());
            }
            setup(
                    program(
                            get(choice[1], first).put("auth", auth),
                            get(choice[1], next).put("auth", auth)));
            List<Row> rows = run(engine, settings.put("include_pii", restricted));
            if (restricted) {
                JsonNode scope = forPath(TOKEN_ROUTE).get(0).at("/body/restrictedResources/0");
                Assertions.assertEquals(choice[1], scope.path("path").asText());
                Assertions.assertFalse(scope.has("dataElements"));
            }
            Assertions.assertEquals(2, rows.size(), choice[0]);
            Assertions.assertEquals("purchase-2", rows.get(1).id);
            Assertions.assertEquals("SHIPPED", rows.get(1).data.path("status").asText());
            Assertions.assertEquals(
                    end().toString(), rows.get(1).data.path(choice[3] + "Before").asText());
            Assertions.assertEquals(
                    "vendor-next", requests().get(1).at("/query/nextToken").asText());
            Assertions.assertEquals(
                    end().toString(),
                    requests().get(1).path("query").path(choice[3] + "Before").asText());
        }
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void everyTabularReportUsesItsActualReportTypeAndProducesExactRows(TestContainer engine)
            throws Exception {
        trust(engine);
        for (String[] report :
                List.of(
                        new String[] {
                            "GET_FLAT_FILE_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL", "amazon-order-id"
                        },
                        new String[] {
                            "GET_FLAT_FILE_ALL_ORDERS_DATA_BY_LAST_UPDATE_GENERAL",
                            "amazon-order-id"
                        },
                        new String[] {"GET_FBA_STORAGE_FEE_CHARGES_DATA", "fnsku"},
                        new String[] {
                            "GET_AMAZON_FULFILLED_SHIPMENTS_DATA_GENERAL", "shipment-item-id"
                        },
                        new String[] {"GET_FLAT_FILE_OPEN_LISTINGS_DATA", "sku"},
                        new String[] {"GET_FBA_FULFILLMENT_REMOVAL_ORDER_DETAIL_DATA", "order-id"},
                        new String[] {
                            "GET_FLAT_FILE_ACTIONABLE_ORDER_DATA_SHIPPING", "order-item-id"
                        },
                        new String[] {"GET_LEDGER_DETAIL_VIEW_DATA", "Reference ID"},
                        new String[] {"GET_LEDGER_SUMMARY_VIEW_DATA", "MSKU"},
                        new String[] {"GET_FBA_REIMBURSEMENTS_DATA", "reimbursement-id"},
                        new String[] {
                            "GET_FBA_FULFILLMENT_CUSTOMER_RETURNS_DATA", "detailed-disposition"
                        },
                        new String[] {"GET_AFN_INVENTORY_DATA", "fulfillment-channel-sku"},
                        new String[] {"GET_AFN_INVENTORY_DATA_BY_COUNTRY", "seller-sku"},
                        new String[] {"GET_FBA_INVENTORY_PLANNING_DATA", "sku"},
                        new String[] {"GET_FLAT_FILE_RETURNS_DATA_BY_RETURN_DATE", "Amazon RMA ID"},
                        new String[] {
                            "GET_FLAT_FILE_ARCHIVED_ORDERS_DATA_BY_ORDER_DATE", "amazon-order-id"
                        },
                        new String[] {"GET_MERCHANTS_LISTINGS_FYP_REPORT", "SKU"},
                        new String[] {"GET_MERCHANT_CANCELLED_LISTINGS_DATA", "seller-sku"},
                        new String[] {"GET_MERCHANT_LISTINGS_DATA_BACK_COMPAT", "listing-id"},
                        new String[] {"GET_STRANDED_INVENTORY_UI_DATA", "stranded-reason"},
                        new String[] {"GET_FBA_MYI_UNSUPPRESSED_INVENTORY_DATA", "fnsku"},
                        new String[] {
                            "GET_FBA_FULFILLMENT_CUSTOMER_SHIPMENT_PROMOTION_DATA",
                            "item-promotion-id"
                        },
                        new String[] {"GET_FBA_ESTIMATED_FBA_FEES_TXT_DATA", "fnsku"},
                        new String[] {
                            "GET_RESTOCK_INVENTORY_RECOMMENDATIONS_REPORT", "Merchant SKU"
                        },
                        new String[] {
                            "GET_FBA_FULFILLMENT_CUSTOMER_SHIPMENT_REPLACEMENT_DATA",
                            "replacement-customer-order-id"
                        },
                        new String[] {
                            "GET_FBA_FULFILLMENT_REMOVAL_SHIPMENT_DETAIL_DATA", "tracking-number"
                        },
                        new String[] {"GET_MERCHANT_LISTINGS_ALL_DATA", "listing-id"},
                        new String[] {"GET_MERCHANT_LISTINGS_DATA", "listing-id"},
                        new String[] {"GET_MERCHANT_LISTINGS_INACTIVE_DATA", "listing-id"})) {
            String resource = report[0];
            String identityField = report[1];
            boolean restricted = "GET_FLAT_FILE_ACTIONABLE_ORDER_DATA_SHIPPING".equals(resource);
            String dateColumn =
                    "GET_LEDGER_DETAIL_VIEW_DATA".equals(resource)
                            ? "\tDate"
                            : resource.contains("LISTINGS") ? "\topen-date" : "";
            String dateValue =
                    "GET_LEDGER_DETAIL_VIEW_DATA".equals(resource)
                            ? "\t02/2024"
                            : resource.contains("LISTINGS") ? "\t2024-02-29 12:34:56 PST" : "";
            String raw =
                    "\ufeff"
                            + identityField
                            + "\tamount\tdescription\tquantity"
                            + dateColumn
                            + "\r\n"
                            + "listing-001\t"
                            + WIDE
                            + "\t\"München \"\"q\"\"\nline\"\t"
                            + dateValue
                            + "\r\n";
            setup(reportProgram(resource, raw, true, restricted));
            ObjectNode options = options(resource).put("include_pii", restricted);
            List<Row> rows = run(engine, options);
            Assertions.assertEquals(1, rows.size(), resource);
            exact(rows.get(0).data.path("amount"));
            Assertions.assertEquals(
                    "listing-001", rows.get(0).data.path(identityField).asText(), resource);
            Assertions.assertEquals(
                    "München \"q\"\nline", rows.get(0).data.path("description").asText(), resource);
            Assertions.assertTrue(rows.get(0).data.path("quantity").isNull(), resource);
            if ("GET_LEDGER_DETAIL_VIEW_DATA".equals(resource)) {
                Assertions.assertEquals("2024-02-01", rows.get(0).data.path("Date").asText());
            } else if ("GET_MERCHANTS_LISTINGS_FYP_REPORT".equals(resource)) {
                Assertions.assertEquals("2024-02-29", rows.get(0).data.path("open-date").asText());
            } else if (resource.contains("LISTINGS")) {
                Assertions.assertEquals(
                        "2024-02-29T12:34:56-08:00", rows.get(0).data.path("open-date").asText());
            }
            assertReportWire(resource, SELLER_MARKET, start(), end().minusSeconds(1), restricted);
        }
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void forecastVendorAnalyticsBrandAndSalesReportPathsAreDistinct(TestContainer engine)
            throws Exception {
        trust(engine);
        for (String[] item :
                List.of(
                        new String[] {
                            "GET_VENDOR_FORECASTING_FRESH_REPORT",
                            "forecastByAsin",
                            "VENDOR",
                            "FRESH"
                        },
                        new String[] {
                            "GET_VENDOR_FORECASTING_RETAIL_REPORT",
                            "forecastByAsin",
                            "VENDOR",
                            "RETAIL"
                        },
                        new String[] {"GET_VENDOR_SALES_REPORT", "salesByAsin", "VENDOR", ""},
                        new String[] {
                            "GET_VENDOR_INVENTORY_REPORT", "inventoryByAsin", "VENDOR", ""
                        },
                        new String[] {"GET_VENDOR_TRAFFIC_REPORT", "trafficByAsin", "VENDOR", ""},
                        new String[] {
                            "GET_VENDOR_NET_PURE_PRODUCT_MARGIN_REPORT",
                            "netPureProductMarginByAsin",
                            "VENDOR",
                            ""
                        },
                        new String[] {
                            "GET_VENDOR_REAL_TIME_INVENTORY_REPORT", "reportData", "VENDOR", ""
                        },
                        new String[] {
                            "GET_BRAND_ANALYTICS_MARKET_BASKET_REPORT", "dataByAsin", "SELLER", ""
                        },
                        new String[] {
                            "GET_BRAND_ANALYTICS_SEARCH_TERMS_REPORT",
                            "dataByDepartmentAndSearchTerm",
                            "SELLER",
                            ""
                        },
                        new String[] {
                            "GET_BRAND_ANALYTICS_REPEAT_PURCHASE_REPORT", "dataByAsin", "SELLER", ""
                        },
                        new String[] {
                            "GET_BRAND_ANALYTICS_ALTERNATE_PURCHASE_REPORT",
                            "dataByAsin",
                            "SELLER",
                            ""
                        },
                        new String[] {
                            "GET_BRAND_ANALYTICS_ITEM_COMPARISON_REPORT", "dataByAsin", "SELLER", ""
                        },
                        new String[] {
                            "GET_SALES_AND_TRAFFIC_REPORT", "salesAndTrafficByAsin", "SELLER", ""
                        },
                        new String[] {
                            "GET_SALES_AND_TRAFFIC_REPORT_BY_DATE",
                            "salesAndTrafficByDate",
                            "SELLER",
                            ""
                        },
                        new String[] {
                            "GET_SALES_AND_TRAFFIC_REPORT_BY_MONTH",
                            "salesAndTrafficByAsin",
                            "SELLER",
                            ""
                        })) {
            String resource = item[0];
            String reportType =
                    !item[3].isEmpty()
                            ? "GET_VENDOR_FORECASTING_REPORT"
                            : resource.startsWith("GET_SALES_AND_TRAFFIC_REPORT")
                                    ? "GET_SALES_AND_TRAFFIC_REPORT"
                                    : resource;
            ObjectNode packet = obj();
            packet.putArray("unrelated").addObject().put("wrong", true);
            ObjectNode row =
                    packet.putArray(item[1])
                            .addObject()
                            .put("asin", "B00EXAMPLE")
                            .put("decimal", new BigDecimal(WIDE))
                            .putNull("optional");
            row.putObject("nested").put("label", "東京");
            setup(reportProgram(reportType, packet.toString(), false, false));
            ObjectNode options = options(resource).put("account_type", item[2]);
            boolean hasDistributor =
                    "GET_VENDOR_SALES_REPORT".equals(resource)
                            || "GET_VENDOR_INVENTORY_REPORT".equals(resource);
            if (hasDistributor) {
                ObjectNode declaration = obj().put("stream_name", resource);
                ArrayNode values = declaration.putArray("options_list");
                values.addObject()
                        .put("option_name", "distributorView")
                        .put("option_value", "SOURCING");
                values.addObject()
                        .put("option_name", "sellingProgram")
                        .put("option_value", "RETAIL");
                options.put("report_options_list", arr(declaration).toString());
            }
            Instant from = start();
            Instant to = end().minusSeconds(1);
            if (resource.startsWith("GET_BRAND_ANALYTICS")) {
                LocalDate week =
                        sampleDay
                                .plusDays(1)
                                .with(TemporalAdjusters.previousOrSame(DayOfWeek.SUNDAY));
                from = midnight(week.minusDays(7));
                to = midnight(week).minusSeconds(1);
            } else if (resource.endsWith("_BY_MONTH")) {
                LocalDate first = sampleDay.withDayOfMonth(1);
                from = midnight(first.minusMonths(1));
                to = midnight(first).minusSeconds(1);
                options.put("start_time", from.toString())
                        .put("end_time", midnight(first).toString())
                        .put("asin_granularity", "CHILD");
            } else if (!item[3].isEmpty()) {
                from = null;
                to = null;
            }
            List<Row> actual = run(engine, options);
            Assertions.assertEquals(1, actual.size(), resource);
            Assertions.assertEquals(
                    "B00EXAMPLE", actual.get(0).data.path("asin").asText(), resource);
            exact(actual.get(0).data.path("decimal"));
            Assertions.assertTrue(actual.get(0).data.path("optional").isNull(), resource);
            Assertions.assertEquals(
                    "東京", actual.get(0).data.at("/nested/label").asText(), resource);
            assertReportWire(reportType, SELLER_MARKET, from, to, false);
            JsonNode sent = requests().get(1).path("body").path("reportOptions");
            if (!item[3].isEmpty()) {
                Assertions.assertEquals(item[3], sent.path("sellingProgram").asText());
            } else if ("VENDOR".equals(item[2])
                    && resource.endsWith("_REPORT")
                    && !"GET_VENDOR_REAL_TIME_INVENTORY_REPORT".equals(resource)) {
                Assertions.assertEquals("DAY", sent.path("reportPeriod").asText());
                if (hasDistributor) {
                    Assertions.assertEquals("SOURCING", sent.path("distributorView").asText());
                    Assertions.assertEquals("RETAIL", sent.path("sellingProgram").asText());
                }
            } else if (resource.startsWith("GET_BRAND_ANALYTICS")) {
                Assertions.assertEquals("WEEK", sent.path("reportPeriod").asText());
            } else if (resource.endsWith("_BY_MONTH")) {
                Assertions.assertEquals("MONTH", sent.path("dateGranularity").asText());
                Assertions.assertEquals("CHILD", sent.path("asinGranularity").asText());
            }
        }
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void feedbackBothXmlLayoutsAndGeneratedSettlementHaveRealTypedOutput(TestContainer engine)
            throws Exception {
        trust(engine);
        String feedback = "GET_SELLER_FEEDBACK_DATA";
        setup(
                reportProgram(
                        feedback,
                        "Datum\tBewertung\tKommentar\tAntwort\tBestellung\tKontakt\n"
                                + "29.02.24\t5\t\"groß\nartig\"\t\t123\t\n",
                        true,
                        false));
        List<Row> rows = run(engine, options(feedback).put("region", "DE"));
        Assertions.assertEquals(1, rows.size());
        Assertions.assertEquals("2024-02-29", rows.get(0).data.path("date").asText());
        Assertions.assertEquals(5, rows.get(0).data.path("rating").asInt());
        Assertions.assertEquals("groß\nartig", rows.get(0).data.path("comments").asText());
        assertReportWire(feedback, "A1PA6795UKMFR9", start(), end().minusSeconds(1), false);

        String browse = "GET_XML_BROWSE_TREE_DATA";
        setup(
                reportProgram(
                        browse,
                        "<Result><Node><browseNodeId>9</browseNodeId>"
                                + "<attribute name=\"name\">東京</attribute><attribute name=\"dept\">books</attribute>"
                                + "</Node></Result>",
                        false,
                        false));
        Row node = run(engine, options(browse)).get(0);
        Assertions.assertEquals("9", node.id);
        Assertions.assertEquals(2, node.data.path("attribute").size());
        Assertions.assertEquals("東京", node.data.at("/attribute/0/text").asText());
        assertReportWire(browse, SELLER_MARKET, start(), end().minusSeconds(1), false);

        for (String report :
                List.of(
                        "GET_ORDER_REPORT_DATA_SHIPPING",
                        "GET_XML_ALL_ORDERS_DATA_BY_ORDER_DATE_GENERAL")) {
            boolean restricted = "GET_ORDER_REPORT_DATA_SHIPPING".equals(report);
            String item = restricted ? "Item" : "OrderItem";
            String document =
                    "<AmazonEnvelope><Message><OrderReport><AmazonOrderID>"
                            + ORDER1
                            + "</AmazonOrderID><"
                            + item
                            + "><SKU>sku-1</SKU></"
                            + item
                            + "><"
                            + item
                            + "><SKU>sku-2</SKU><Quantity>7</Quantity><ItemPrice><Amount>"
                            + WIDE
                            + "</Amount></ItemPrice></"
                            + item
                            + ">"
                            + "</OrderReport></Message></AmazonEnvelope>";
            if (restricted) {
                for (String[] scope :
                        List.of(
                                new String[] {},
                                new String[] {"A13V1IB3VIYZZH"},
                                new String[] {SELLER_MARKET, "A13V1IB3VIYZZH"})) {
                    ObjectNode invalid = metadata(report, "other", "DONE");
                    if (scope.length > 0) {
                        ArrayNode marketplaces = invalid.putArray("marketplaceIds");
                        for (String market : scope) {
                            marketplaces.add(market);
                        }
                    }
                    setup(program(get(REPORTS, list(arr(invalid)))));
                    rejected(
                            engine,
                            options(report).put("include_pii", true),
                            "no exact configured marketplace scope");
                    Assertions.assertEquals(1, requests().size());
                    Assertions.assertTrue(forPath(TOKEN_ROUTE).isEmpty());
                }
                ObjectNode scheduled = metadata(report, "report1", "DONE");
                scheduled.putArray("marketplaceIds").add(SELLER_MARKET);
                setup(
                        program(
                                get(REPORTS, list(arr(scheduled))),
                                get(REPORTS + "/report1", done(report)),
                                target("one", true, true),
                                file("one", document, true)));
            } else {
                setup(reportProgram(report, document, true, false));
            }
            List<Row> xml = run(engine, options(report).put("include_pii", restricted));
            Assertions.assertEquals(1, xml.size());
            String itemPath = "/" + item + "/1";
            Assertions.assertEquals("sku-2", xml.get(0).data.at(itemPath + "/SKU").asText());
            Assertions.assertEquals(7, xml.get(0).data.at(itemPath + "/Quantity").asInt());
            exact(xml.get(0).data.at(itemPath + "/ItemPrice/Amount"));
            Assertions.assertEquals(ORDER1, xml.get(0).data.path("AmazonOrderID").asText());
            if (restricted) {
                Assertions.assertEquals(
                        report, requests().get(0).at("/query/reportTypes").asText());
                Assertions.assertFalse(requests().toString().contains("\"method\":\"POST\""));
                Assertions.assertEquals(
                        DOCUMENT,
                        forPath(TOKEN_ROUTE)
                                .get(0)
                                .at("/body/restrictedResources/0/path")
                                .asText());
            } else {
                assertReportWire(report, SELLER_MARKET, start(), end().minusSeconds(1), false);
            }
        }

        String settlement = "GET_V2_SETTLEMENT_REPORT_DATA_FLAT_FILE_V2";
        ObjectNode batch =
                metadata(settlement, "report1", "DONE")
                        .put("dataEndTime", end().minusSeconds(1).toString());
        batch.putArray("marketplaceIds").add(SELLER_MARKET).add("A13V1IB3VIYZZH");
        ObjectNode inventory = list(arr(batch)).put("nextToken", "generated-next");
        setup(
                program(
                        get(REPORTS, inventory),
                        get(REPORTS + "/report1", done(settlement)),
                        target("one", true, false),
                        file(
                                "one",
                                "amount\tposted-date-time\tdeposit-date\tmarketplace-name\n"
                                        + WIDE
                                        + "\t2024-02-29 23:29:31 UTC\t\tAmazon.fr\n",
                                true),
                        get(REPORTS, list(arr()))));
        Row settled = run(engine, options(settlement)).get(0);
        exact(settled.data.path("amount"));
        Assertions.assertEquals("Amazon.fr", settled.data.path("marketplace-name").asText());
        Assertions.assertNull(settled.market);
        Assertions.assertEquals(
                "2024-02-29T23:29:31Z", settled.data.path("posted-date-time").asText());
        Assertions.assertTrue(settled.data.path("deposit-date").isNull());
        Assertions.assertEquals(
                start().toString(), requests().get(0).at("/query/createdSince").asText());
        Assertions.assertEquals(1, requests().get(4).path("query").size());
        Assertions.assertEquals(
                "generated-next", requests().get(4).at("/query/nextToken").asText());
        Assertions.assertFalse(requests().toString().contains("\"method\":\"POST\""));
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void restrictedScopeRenewalAndRoleDenialNeverDowngradeToLwa(TestContainer engine)
            throws Exception {
        trust(engine);
        ObjectNode denied =
                obj().putArray("errors")
                        .addObject()
                        .put("code", "Unauthorized")
                        .put("message", "The access token you provided has expired.");
        ObjectNode invalid = obj();
        invalid.putArray("errors").add(denied);
        ObjectNode withPii = order(ORDER1);
        withPii.putObject("BuyerInfo").put("BuyerEmail", "buyer@example.invalid");
        withPii.putObject("ShippingAddress").put("City", "Seattle");
        setup(
                program(
                        step("GET", ORDER_ROUTE, invalid).put("status", 403).put("auth", "rdt"),
                        get(ORDER_ROUTE, payload("Orders", arr(withPii))).put("auth", "rdt")));
        List<Row> secured = run(engine, options("Orders").put("include_pii", true));
        Assertions.assertEquals(
                "buyer@example.invalid", secured.get(0).data.at("/BuyerInfo/BuyerEmail").asText());
        Assertions.assertEquals(
                "Seattle", secured.get(0).data.at("/ShippingAddress/City").asText());
        List<JsonNode> tokens = forPath(TOKEN_ROUTE);
        Assertions.assertEquals(2, tokens.size());
        for (JsonNode event : tokens) {
            JsonNode scopes = event.at("/body/restrictedResources");
            Assertions.assertEquals(1, scopes.size());
            Assertions.assertEquals("GET", scopes.get(0).path("method").asText());
            Assertions.assertEquals(ORDER_ROUTE, scopes.get(0).path("path").asText());
            Assertions.assertEquals(
                    arr(
                            JSON.getNodeFactory().textNode("buyerInfo"),
                            JSON.getNodeFactory().textNode("shippingAddress")),
                    scopes.get(0).path("dataElements"));
        }

        ObjectNode request = program();
        request.putObject("rdt").put("status", 403);
        setup(request);
        rejected(engine, options("Orders").put("include_pii", true), "approved restricted role");
        Assertions.assertEquals(0, requests().size());
        Assertions.assertEquals(1, forPath(TOKEN_ROUTE).size());

        String child = ORDER_ROUTE + "/" + ORDER1 + "/orderItems";
        ObjectNode item = obj().put("OrderItemId", "pii-item");
        item.putObject("BuyerInfo").put("GiftMessageText", "fixture gift text");
        setup(
                program(
                        get(ORDER_ROUTE, payload("Orders", arr(order(ORDER1)))),
                        get(child, payload("OrderItems", arr(item))).put("auth", "rdt")));
        Row secureChild = run(engine, options("OrderItems").put("include_pii", true)).get(0);
        Assertions.assertEquals(
                "fixture gift text", secureChild.data.at("/BuyerInfo/GiftMessageText").asText());
        JsonNode resource = forPath(TOKEN_ROUTE).get(0).at("/body/restrictedResources/0");
        Assertions.assertEquals(child, resource.path("path").asText());
        Assertions.assertEquals(1, resource.path("dataElements").size());
        Assertions.assertEquals("buyerInfo", resource.at("/dataElements/0").asText());

        setup(
                program(
                        step("GET", ORDER_ROUTE, invalid).put("status", 401),
                        get(ORDER_ROUTE, payload("Orders", arr(order(ORDER1))))));
        Assertions.assertEquals(1, run(engine, options("Orders")).size());
        Assertions.assertEquals(2, forPath("/auth/o2/token").size());

        ObjectNode expiryPage = payload("Orders", arr(order(ORDER1)));
        expiryPage.with("payload").put("NextToken", "ordered-next");
        ObjectNode expiry =
                program(
                        get(ORDER_ROUTE, expiryPage).put("auth", "rdt").put("pause_ms", 1200),
                        get(ORDER_ROUTE, payload("Orders", arr())).put("auth", "rdt"));
        expiry.putObject("rdt").put("expires", 1);
        setup(expiry);
        Assertions.assertEquals(1, run(engine, options("Orders").put("include_pii", true)).size());
        Assertions.assertEquals(2, forPath(TOKEN_ROUTE).size());
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void reuseRequiresAnExactWindowMarketplaceStatusAndOptions(TestContainer engine)
            throws Exception {
        trust(engine);
        String resource = "GET_LEDGER_SUMMARY_VIEW_DATA";
        ObjectNode options = options(resource).put("max_done_report_age_hours", 36);
        String raw = "amount\n" + WIDE + "\n";
        ObjectNode wrongMarket = candidate(resource, "wrong-market", "DONE");
        wrongMarket.putArray("marketplaceIds").add("A13V1IB3VIYZZH");
        ObjectNode wrongPeriod = candidate(resource, "wrong-period", "IN_PROGRESS");
        wrongPeriod.put("dataStartTime", start().plusSeconds(1).toString());
        ObjectNode old = candidate(resource, "old", "DONE");
        old.put("createdTime", Instant.now().minusSeconds(200000L).toString());
        old.putObject("reportOptions");
        ObjectNode missing = candidate(resource, "unknown-options", "IN_QUEUE");
        missing.remove("reportOptions");
        ObjectNode different = candidate(resource, "different-options", "IN_PROGRESS");
        different.putObject("reportOptions").put("aggregateByLocation", "COUNTRY");
        ObjectNode match = candidate(resource, "report1", "DONE");
        match.putObject("reportOptions");
        ObjectNode next = list(arr(match));
        setup(
                program(
                        get(
                                REPORTS,
                                list(arr(wrongMarket, wrongPeriod, old, missing, different))
                                        .put("nextToken", "reuse-lookup")),
                        get(REPORTS, next),
                        get(REPORTS + "/report1", done(resource)),
                        target("one", false, false),
                        file("one", raw, false)));
        exact(run(engine, options).get(0).data.path("amount"));
        Assertions.assertEquals("reuse-lookup", requests().get(1).at("/query/nextToken").asText());
        Assertions.assertEquals(1, requests().get(1).path("query").size());
        Assertions.assertFalse(requests().toString().contains("\"method\":\"POST\""));

        ObjectNode create = reportProgram(resource, raw, false, false);
        create.withArray("steps").set(0, get(REPORTS, list(arr(match, missing, different))));
        ObjectNode optionSpec = obj().put("stream_name", resource);
        optionSpec
                .putArray("options_list")
                .addObject()
                .put("option_name", "aggregateByLocation")
                .put("option_value", "FC");
        setup(create);
        Assertions.assertEquals(
                1,
                run(
                                engine,
                                options.deepCopy()
                                        .put("max_done_report_age_hours", 0)
                                        .put("report_options_list", arr(optionSpec).toString()))
                        .size());
        Assertions.assertEquals(
                "FC", requests().get(1).at("/body/reportOptions/aggregateByLocation").asText());

        setup(program());
        rejected(
                engine,
                options.deepCopy()
                        .put("report_options_list", arr(optionSpec, optionSpec).toString()),
                "duplicate stream entry");
        Assertions.assertEquals(0, requests().size());
        ObjectNode duplicate = optionSpec.deepCopy();
        duplicate.withArray("options_list").add(duplicate.path("options_list").get(0).deepCopy());
        rejected(
                engine,
                options.deepCopy().put("report_options_list", arr(duplicate).toString()),
                "duplicate option");
        Assertions.assertEquals(0, requests().size());
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void reportPollAndExpiredPresignRenewalAreSeparateBoundedCalls(TestContainer engine)
            throws Exception {
        trust(engine);
        String resource = "GET_FBA_STORAGE_FEE_CHARGES_DATA";
        setup(
                program(
                        get(REPORTS, list(arr())),
                        created(),
                        get(REPORTS + "/report1", metadata(resource, "report1", "IN_QUEUE")),
                        get(REPORTS + "/report1", metadata(resource, "report1", "IN_PROGRESS")),
                        get(REPORTS + "/report1", done(resource)),
                        target("expired", false, false),
                        step("GET", "/download/expired", obj())
                                .put("auth", "none")
                                .put("status", 403),
                        target("renewed", false, false),
                        file("renewed", "amount\n" + WIDE + "\n", false)));
        exact(
                run(engine, options(resource).put("poll_interval_ms", 150))
                        .get(0)
                        .data
                        .path("amount"));
        Assertions.assertEquals(2, forPath(DOCUMENT).size());
        Assertions.assertEquals(1, forPath("/download/expired").size());
        Assertions.assertEquals(1, forPath("/download/renewed").size());
        for (JsonNode call :
                List.of(forPath("/download/expired").get(0), forPath("/download/renewed").get(0))) {
            Assertions.assertEquals("none", call.path("auth").asText());
            Assertions.assertFalse(call.path("amz_date").asBoolean());
            Assertions.assertFalse(call.path("signed").asBoolean());
            Assertions.assertEquals(PRIVATE_SIGNATURE, call.at("/query/X-Amz-Signature").asText());
        }

        setup(
                program(
                        get(REPORTS, list(arr())),
                        created(),
                        get(REPORTS + "/report1", metadata(resource, "report1", "IN_QUEUE")),
                        get(REPORTS + "/report1", metadata(resource, "report1", "IN_PROGRESS"))));
        rejected(
                engine,
                options(resource).put("report_timeout_ms", 1000).put("poll_interval_ms", 900),
                "report polling deadline exceeded");
        Assertions.assertTrue(forPath(REPORTS + "/report1").size() <= 2);
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void unknownFatalCancelledAndAmbiguousSubmissionCannotBecomeSuccessfulRows(TestContainer engine)
            throws Exception {
        trust(engine);
        String resource = "GET_FBA_STORAGE_FEE_CHARGES_DATA";
        for (String status : List.of("CANCELLED", "ALIEN_STATE")) {
            setup(
                    program(
                            get(REPORTS, list(arr())),
                            created(),
                            get(REPORTS + "/report1", metadata(resource, "report1", status))));
            rejected(
                    engine,
                    options(resource),
                    "CANCELLED".equals(status) ? "report CANCELLED" : "unknown processing status");
        }
        setup(
                program(
                        get(REPORTS, list(arr())),
                        created(),
                        get(REPORTS + "/report1", metadata(resource, "report1", "CANCELLED"))));
        Assertions.assertTrue(
                run(engine, options(resource).put("treat_cancelled_as_empty", true)).isEmpty());

        setup(
                program(
                        get(REPORTS, list(arr())),
                        created(),
                        get(
                                REPORTS + "/report1",
                                metadata(resource, "report1", "FATAL")
                                        .put("reportDocumentId", "doc1")),
                        target("fatal", true, false),
                        file(
                                "fatal",
                                "missing required report option " + SpApiFixture.PRIVATE_ERROR,
                                true)));
        rejected(engine, options(resource), "report FATAL because its options were rejected");
        Assertions.assertEquals(1, forPath("/download/fatal").size());

        setup(program(get(REPORTS, list(arr())), step("POST", REPORTS, obj()).put("status", 503)));
        rejected(
                engine,
                options(resource).put("max_attempts", 5),
                "report creation outcome is unknown");
        Assertions.assertEquals(
                1,
                requests().stream()
                        .filter(call -> "POST".equals(call.path("method").asText()))
                        .count());
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void stressesRetryBudgetsAndTransparentMalformedDocumentFailures(TestContainer engine)
            throws Exception {
        trust(engine);
        String format = "GET_FBA_STORAGE_FEE_CHARGES_DATA";
        ObjectNode throttle = step("GET", REPORTS, obj()).put("status", 429);
        throttle.putObject("headers")
                .put("Retry-After", "12")
                .put("x-amzn-RateLimit-Limit", "0.0167");
        setup(program(throttle));
        rejected(engine, options(format).put("request_timeout_ms", 1800), "retry delay exceeds");
        Assertions.assertEquals(1, requests().size());
        setup(program(throttle));
        rejected(
                engine, options(format).put("stop_sync_on_rate_limit", true), "HTTP 429 exhausted");
        Assertions.assertEquals(1, requests().size());

        setup(reportProgram(format, "amount\tbroken\n50\t\"unterminated", false, false));
        rejected(engine, options(format), "unclosed quoted");
        ObjectNode wrongGzip = reportProgram(format, "amount\n50\n", false, false);
        wrongGzip.withArray("steps").set(3, target("one", true, false));
        setup(wrongGzip);
        rejected(engine, options(format), "advertised gzip");
        setup(reportProgram(format, "description\n" + "z".repeat(8192) + "\n", true, false));
        rejected(
                engine,
                options(format).put("max_response_bytes", 1024),
                "decompressed report exceeds");

        String xml = "GET_XML_BROWSE_TREE_DATA";
        setup(
                reportProgram(
                        xml,
                        "<!DOCTYPE Result [<!ENTITY probe SYSTEM \""
                                + SpApiFixture.ORIGIN
                                + "/unexpected-entity\">]><Result><Node>&probe;</Node></Result>",
                        false,
                        false));
        rejected(engine, options(xml), "XML");
        Assertions.assertEquals(0, forPath("/unexpected-entity").size());

        String json = "GET_SALES_AND_TRAFFIC_REPORT_BY_DATE";
        setup(reportProgram(json, "{\"unexpected\":[]}", false, false));
        rejected(engine, options(json), "JSON report array is missing");
        setup(
                reportProgram(
                        json,
                        "{\"salesAndTrafficByDate\":[{\"date\":\"a\",\"date\":\"b\"}]}",
                        false,
                        false));
        rejected(engine, options(json), "expected a complete JSON response");
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void sandboxTrustAccountMismatchAndEscapingDownloadHostsFailBeforeDataLeaks(
            TestContainer engine) throws Exception {
        trust(engine);
        setup(program());
        String other = "/tmp/seatunnel-sp-unrelated.pem";
        engine.copyAbsolutePathToContainer(keys.resolve("unrelated.pem").toString(), other);
        rejected(engine, options("Orders").put("trust_certificate", other), "TLS trust");
        rejected(
                engine,
                options("Orders")
                        .put("sandbox_origin", "https://unknown-sp-host:" + SpApiFixture.TLS_PORT),
                "TLS trust");
        rejected(engine, options("Orders").put("environment", "PRODUCTION"), "custom sandbox");
        rejected(engine, options("Orders").put("account_type", "VENDOR"), "another account type");
        rejected(engine, options("VendorOrders"), "another account type");
        Assertions.assertTrue(trace().isEmpty());

        String resource = "GET_FBA_STORAGE_FEE_CHARGES_DATA";
        ObjectNode wrong =
                obj().put("reportDocumentId", "doc1")
                        .put(
                                "url",
                                "https://127.0.0.1:8444/escape?X-Amz-Signature="
                                        + PRIVATE_SIGNATURE);
        setup(
                program(
                        get(REPORTS, list(arr())),
                        created(),
                        get(REPORTS + "/report1", done(resource)),
                        get(DOCUMENT, wrong)));
        rejected(engine, options(resource), "outside the allowed HTTPS origins");
        Assertions.assertEquals(0, forPath("/escape").size());

        ObjectNode redirect =
                step("GET", "/download/redirect", obj()).put("auth", "none").put("status", 302);
        redirect.putObject("headers").put("Location", SpApiFixture.ORIGIN + "/unexpected-return");
        setup(
                program(
                        get(REPORTS, list(arr())),
                        created(),
                        get(REPORTS + "/report1", done(resource)),
                        target("redirect", false, false),
                        redirect));
        rejected(engine, options(resource), "HTTP 302");
        Assertions.assertEquals(0, forPath("/unexpected-return").size());
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void requestDeadlineAndEngineCancelCloseActualHeldHttpBodies(TestContainer engine)
            throws Exception {
        trust(engine);
        for (boolean trickled : List.of(false, true)) {
            setup(program(held("GET", ORDER_ROUTE, "lwa").put("trickle", trickled)));
            rejected(
                    engine,
                    options("Orders").put("request_timeout_ms", 1600),
                    "request deadline exceeded");
            disconnect(ORDER_ROUTE, 4500L);
            Assertions.assertEquals(1, requests().size());
        }

        String resource = "GET_FBA_STORAGE_FEE_CHARGES_DATA";
        setup(
                program(
                        get(REPORTS, list(arr())),
                        created(),
                        get(REPORTS + "/report1", done(resource)),
                        target("held", false, false),
                        held("GET", "/download/held", "none")));
        Path conf = config(options(resource).put("request_timeout_ms", 120000), true, false);
        try (EngineJob job = new EngineJob(engine, conf, null)) {
            waitForHold("/download/held");
            job.cancel();
            disconnect("/download/held", 12000L);
            Assertions.assertEquals(5, requests().size());
        } finally {
            Files.deleteIfExists(conf);
        }
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void restoresReportCollectionWhileCreationPollingOrTransferWasOutstanding(TestContainer engine)
            throws Exception {
        trust(engine);
        String resource = "GET_FBA_STORAGE_FEE_CHARGES_DATA";
        for (String transition : List.of("create", "poll", "download")) {
            List<ObjectNode> phases = new ArrayList<>();
            phases.add(get(REPORTS, list(arr())));
            String pending;
            if ("create".equals(transition)) {
                pending = REPORTS;
                phases.add(held("POST", REPORTS, "lwa"));
                ObjectNode exact = candidate(resource, "report1", "IN_PROGRESS");
                exact.putObject("reportOptions");
                phases.add(get(REPORTS, list(arr(exact))));
            } else {
                phases.add(created());
                pending = REPORTS + "/report1";
                if ("poll".equals(transition)) {
                    phases.add(held("GET", pending, "lwa"));
                }
            }
            phases.add(get(REPORTS + "/report1", done(resource)));
            if ("download".equals(transition)) {
                pending = "/download/old";
                phases.add(target("old", false, false));
                phases.add(held("GET", pending, "none"));
            }
            phases.add(target("restored", true, false));
            phases.add(
                    file(
                            "restored",
                            "amount\tdescription\n" + WIDE + "\t" + transition + "\n",
                            true));
            setup(program(phases.toArray(new ObjectNode[0])));
            Path conf = config(options(resource).put("request_timeout_ms", 120000), true, false);
            try (EngineJob initial = new EngineJob(engine, conf, null)) {
                waitForHold(pending);
                newerCheckpoint(engine, initial.id);
                initial.savepoint();
                disconnect(pending, 15000L);
                String baseline = engine.getServerLogs();
                Container.ExecResult restored =
                        engine.restoreJobWithCheckpoint(resource(conf), initial.id, jobId());
                success(restored);
                List<Row> rows = emitted(engine, baseline, restored, resource);
                Assertions.assertEquals(1, rows.size(), transition);
                exact(rows.get(0).data.path("amount"));
                Assertions.assertEquals(transition, rows.get(0).data.path("description").asText());
                Assertions.assertEquals(
                        1,
                        requests().stream()
                                .filter(r -> "POST".equals(r.path("method").asText()))
                                .count());
                Assertions.assertEquals(phases.size(), requests().size(), transition);
            } finally {
                Files.deleteIfExists(conf);
            }
        }
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void restoresTheExactNextPageAndSubsequentWindowWithOriginalSettings(TestContainer engine)
            throws Exception {
        trust(engine);
        ObjectNode before = payload("Orders", arr(order(ORDER1)));
        before.with("payload").put("NextToken", "persisted-orders-token");
        ObjectNode second = payload("Orders", arr(order(ORDER2)));
        ObjectNode thirdOrder =
                order("111-1234567-1234569")
                        .put("LastUpdateDate", end().plusSeconds(20).toString());
        setup(
                program(
                        get(ORDER_ROUTE, before),
                        held("GET", ORDER_ROUTE, "lwa"),
                        get(ORDER_ROUTE, second),
                        get(ORDER_ROUTE, payload("Orders", arr(thirdOrder)))));
        ObjectNode opts =
                options("Orders")
                        .put("end_time", end().plusSeconds(86400).toString())
                        .put("period_in_days", 1)
                        .put("request_timeout_ms", 120000);
        Path conf = config(opts, true, false);
        Path mismatched = config(opts.deepCopy().put("region", "DE"), true, false);
        String prior = engine.getServerLogs();
        try (EngineJob initial = new EngineJob(engine, conf, null)) {
            waitForHold(ORDER_ROUTE);
            Awaitility.await()
                    .atMost(20, TimeUnit.SECONDS)
                    .until(() -> rows(fresh(engine, prior), "Orders").size() == 1);
            newerCheckpoint(engine, initial.id);
            initial.savepoint();
            disconnect(ORDER_ROUTE, 15000L);

            String mismatchLog = engine.getServerLogs();
            Container.ExecResult bad =
                    engine.restoreJobWithCheckpoint(resource(mismatched), initial.id, jobId());
            Assertions.assertNotEquals(0, bad.getExitCode());
            String mismatch = transcript(engine, mismatchLog, bad);
            Assertions.assertTrue(
                    mismatch.contains("options differ")
                            || mismatch.contains("changed source options"),
                    mismatch);
            Assertions.assertEquals(2, requests().size());

            String baseline = engine.getServerLogs();
            Container.ExecResult restored =
                    engine.restoreJobWithCheckpoint(resource(conf), initial.id, jobId());
            success(restored);
            List<Row> resumed = emitted(engine, baseline, restored, "Orders");
            Assertions.assertEquals(
                    List.of(ORDER2, "111-1234567-1234569"),
                    List.of(resumed.get(0).id, resumed.get(1).id));
            Assertions.assertEquals(
                    "persisted-orders-token", requests().get(2).at("/query/NextToken").asText());
            Assertions.assertEquals(
                    start().toString(), requests().get(2).at("/query/LastUpdatedAfter").asText());
            Assertions.assertEquals(
                    end().toString(), requests().get(3).at("/query/LastUpdatedAfter").asText());
            Assertions.assertFalse(requests().get(3).path("query").has("NextToken"));
        } finally {
            Files.deleteIfExists(conf);
            Files.deleteIfExists(mismatched);
        }
    }

    @TestTemplate
    @DisabledOnContainer(
            value = {},
            type = {EngineType.FLINK, EngineType.SPARK})
    void terminalReportRestoreEmitsNoDuplicateRowsOrReportRequests(TestContainer engine)
            throws Exception {
        trust(engine);
        String report = "GET_FBA_STORAGE_FEE_CHARGES_DATA";
        setup(reportProgram(report, "amount\n" + WIDE + "\n", false, false));
        Path conf = config(options(report), true, true);
        String baseline = engine.getServerLogs();
        try (EngineJob initial = new EngineJob(engine, conf, null)) {
            Awaitility.await()
                    .atMost(70, TimeUnit.SECONDS)
                    .until(() -> rows(fresh(engine, baseline), report).size() == 1);
            newerCheckpoint(engine, initial.id);
            initial.savepoint();
            Assertions.assertEquals(5, requests().size());
            String prior = engine.getServerLogs();
            try (EngineJob resumed = new EngineJob(engine, conf, initial.id)) {
                status(engine, resumed.id, "RUNNING");
                newerCheckpoint(engine, resumed.id);
                Assertions.assertEquals(5, requests().size());
                Assertions.assertTrue(rows(fresh(engine, prior), report).isEmpty());
                resumed.cancel();
            }
        } finally {
            Files.deleteIfExists(conf);
        }
    }

    private void assertReportWire(
            String type, String market, Instant from, Instant to, boolean restricted)
            throws IOException {
        List<JsonNode> actual = requests();
        Assertions.assertEquals(5, actual.size(), type);
        Assertions.assertEquals(REPORTS, actual.get(0).path("path").asText());
        Assertions.assertEquals(type, actual.get(0).at("/query/reportTypes").asText());
        Assertions.assertEquals("POST", actual.get(1).path("method").asText());
        JsonNode posted = actual.get(1).path("body");
        Assertions.assertEquals(type, posted.path("reportType").asText());
        Assertions.assertEquals(market, posted.at("/marketplaceIds/0").asText());
        if (from == null) {
            Assertions.assertFalse(posted.has("dataStartTime"));
            Assertions.assertFalse(posted.has("dataEndTime"));
        } else {
            Assertions.assertEquals(from.toString(), posted.path("dataStartTime").asText());
            Assertions.assertEquals(to.toString(), posted.path("dataEndTime").asText());
        }
        Assertions.assertEquals(restricted ? "rdt" : "lwa", actual.get(3).path("auth").asText());
        Assertions.assertEquals("none", actual.get(4).path("auth").asText());
        Assertions.assertFalse(actual.get(4).path("amz_date").asBoolean());
        if (restricted) {
            JsonNode scope = forPath(TOKEN_ROUTE).get(0).at("/body/restrictedResources/0");
            Assertions.assertEquals(DOCUMENT, scope.path("path").asText());
            Assertions.assertFalse(scope.has("dataElements"));
        }
    }

    private ObjectNode options(String resource) {
        return obj().put("resource", resource)
                .put("environment", "SANDBOX")
                .put("client_id", SpApiFixture.CLIENT)
                .put("client_secret", SpApiFixture.SECRET)
                .put("token", SpApiFixture.REFRESH)
                .put("sandbox_origin", SpApiFixture.ORIGIN)
                .put("trust_certificate", CERT)
                .put("start_time", start().toString())
                .put("end_time", end().toString())
                .put("poll_interval_ms", 100);
    }

    private ObjectNode order(String id) {
        return obj().put("AmazonOrderId", id)
                .put("LastUpdateDate", start().plusSeconds(20).toString());
    }

    private Instant start() {
        return midnight(sampleDay);
    }

    private Instant end() {
        return midnight(sampleDay.plusDays(1));
    }

    private static Instant midnight(LocalDate day) {
        return day.atStartOfDay().toInstant(ZoneOffset.UTC);
    }

    private static ObjectNode metadata(String type, String report, String status) {
        return obj().put("reportType", type)
                .put("reportId", report)
                .put("processingStatus", status);
    }

    private ObjectNode candidate(String type, String report, String status) {
        ObjectNode result =
                metadata(type, report, status)
                        .put("createdTime", Instant.now().minusSeconds(5).toString())
                        .put("dataStartTime", start().toString())
                        .put("dataEndTime", end().minusSeconds(1).toString());
        result.putArray("marketplaceIds").add(SELLER_MARKET);
        return result;
    }

    private static ObjectNode done(String type) {
        return metadata(type, "report1", "DONE").put("reportDocumentId", "doc1");
    }

    private static ObjectNode reportProgram(
            String type, String data, boolean gzip, boolean restricted) {
        return program(
                get(REPORTS, list(arr())),
                created(),
                get(REPORTS + "/report1", done(type)),
                target("one", gzip, restricted),
                file("one", data, gzip));
    }

    private static ObjectNode list(ArrayNode reports) {
        ObjectNode response = obj();
        response.set("reports", reports);
        return response;
    }

    private static ObjectNode created() {
        return step("POST", REPORTS, obj().put("reportId", "report1")).put("status", 202);
    }

    private static ObjectNode target(String key, boolean gzip, boolean restricted) {
        ObjectNode response =
                obj().put("reportDocumentId", "doc1")
                        .put(
                                "url",
                                SpApiFixture.ORIGIN
                                        + "/download/"
                                        + key
                                        + "?X-Amz-Signature="
                                        + PRIVATE_SIGNATURE);
        if (gzip) {
            response.put("compressionAlgorithm", "GZIP");
        }
        return get(DOCUMENT, response).put("auth", restricted ? "rdt" : "lwa");
    }

    private static ObjectNode file(String key, String data, boolean gzip) {
        return obj().put("path", "/download/" + key)
                .put("auth", "none")
                .put("raw", data)
                .put("gzip", gzip);
    }

    private static ObjectNode held(String method, String path, String auth) {
        return obj().put("method", method).put("path", path).put("auth", auth).put("hold", true);
    }

    private static ObjectNode program(ObjectNode... steps) {
        ObjectNode result = obj();
        ArrayNode actions = result.putArray("steps");
        for (ObjectNode step : steps) {
            actions.add(step);
        }
        return result;
    }

    private static ObjectNode step(String method, String path, JsonNode response) {
        ObjectNode result = obj().put("method", method).put("path", path);
        result.set("json", response);
        return result;
    }

    private static ObjectNode get(String path, JsonNode data) {
        return step("GET", path, data);
    }

    private static ObjectNode payload(String key, JsonNode value) {
        ObjectNode result = obj();
        result.putObject("payload").set(key, value);
        return result;
    }

    private static ObjectNode obj() {
        return JSON.createObjectNode();
    }

    private static ArrayNode arr(JsonNode... values) {
        ArrayNode result = JSON.createArrayNode();
        for (JsonNode value : values) {
            result.add(value);
        }
        return result;
    }

    private static void exact(JsonNode number) {
        Assertions.assertTrue(number.isNumber(), number.toString());
        Assertions.assertEquals(0, new BigDecimal(WIDE).compareTo(number.decimalValue()));
    }

    private void setup(ObjectNode recipe) throws IOException {
        control("/setup", recipe);
    }

    private JsonNode trace() throws IOException {
        return control("/trace", null);
    }

    private List<JsonNode> requests() throws IOException {
        List<JsonNode> result = new ArrayList<>();
        for (JsonNode call : trace()) {
            String path = call.path("path").asText();
            if (!path.equals("/auth/o2/token") && !path.equals(TOKEN_ROUTE)) {
                result.add(call);
            }
        }
        return result;
    }

    private List<JsonNode> forPath(String path) throws IOException {
        List<JsonNode> result = new ArrayList<>();
        for (JsonNode call : trace()) {
            if (path.equals(call.path("path").asText())) {
                result.add(call);
            }
        }
        return result;
    }

    private void waitForHold(String path) {
        Awaitility.await()
                .atMost(75, TimeUnit.SECONDS)
                .until(() -> forPath(path).stream().anyMatch(r -> r.path("held").asBoolean()));
    }

    private void disconnect(String path, long maximum) throws IOException {
        Awaitility.await()
                .atMost(12, TimeUnit.SECONDS)
                .until(() -> forPath(path).stream().anyMatch(r -> r.has("disconnect_ms")));
        JsonNode event =
                forPath(path).stream()
                        .filter(r -> r.has("disconnect_ms"))
                        .findFirst()
                        .orElseThrow();
        Assertions.assertTrue(event.path("disconnect_ms").asLong() < maximum);
    }

    private JsonNode control(String route, ObjectNode body) throws IOException {
        HttpURLConnection connection =
                (HttpURLConnection)
                        new URL(
                                        "http",
                                        fixture.getHost(),
                                        fixture.getMappedPort(SpApiFixture.CONTROL_PORT),
                                        route)
                                .openConnection();
        connection.setConnectTimeout(4000);
        connection.setReadTimeout(8000);
        try {
            if (body != null) {
                connection.setRequestMethod("POST");
                connection.setDoOutput(true);
                try (OutputStream output = connection.getOutputStream()) {
                    JSON.writeValue(output, body);
                }
            }
            Assertions.assertEquals(200, connection.getResponseCode());
            try (InputStream input = connection.getInputStream()) {
                return JSON.readTree(input);
            }
        } finally {
            connection.disconnect();
        }
    }

    private static Path classes(Class<?> owner) throws Exception {
        return Path.of(owner.getProtectionDomain().getCodeSource().getLocation().toURI());
    }

    private void trust(TestContainer engine) {
        engine.copyAbsolutePathToContainer(keys.resolve("fixture.pem").toString(), CERT);
    }

    private void certificate(String name, String dns) throws Exception {
        Path file = keys.resolve(name + ".p12");
        String password = SpApiFixture.KEYSTORE_PASSWORD;
        Process process =
                new ProcessBuilder(
                                Path.of(System.getProperty("java.home"), "bin/keytool").toString(),
                                "-genkeypair",
                                "-noprompt",
                                "-alias",
                                "fixture",
                                "-dname",
                                "CN=" + dns,
                                "-keyalg",
                                "RSA",
                                "-keysize",
                                "2048",
                                "-validity",
                                "3",
                                "-storetype",
                                "PKCS12",
                                "-keystore",
                                file.toString(),
                                "-storepass",
                                password,
                                "-ext",
                                "SAN=DNS:" + dns)
                        .redirectErrorStream(true)
                        .start();
        try {
            Assertions.assertTrue(process.waitFor(25, TimeUnit.SECONDS));
            Assertions.assertEquals(
                    0,
                    process.exitValue(),
                    new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8));
        } finally {
            process.destroyForcibly();
        }
        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        try (InputStream input = Files.newInputStream(file)) {
            keyStore.load(input, password.toCharArray());
        }
        byte[] der = keyStore.getCertificate("fixture").getEncoded();
        String pem = Base64.getMimeEncoder(64, new byte[] {'\n'}).encodeToString(der);
        Files.writeString(
                keys.resolve(name + ".pem"),
                "-----BEGIN CERTIFICATE-----\n" + pem + "\n-----END CERTIFICATE-----\n",
                StandardCharsets.US_ASCII);
    }

    private static List<Row> run(TestContainer engine, ObjectNode options) throws Exception {
        Path file = config(options, false, false);
        String baseline = engine.getServerLogs();
        try {
            Container.ExecResult result = engine.executeJob(resource(file));
            success(result);
            return emitted(engine, baseline, result, options.path("resource").asText());
        } finally {
            Files.deleteIfExists(file);
        }
    }

    private static String rejected(TestContainer engine, ObjectNode options, String expected)
            throws Exception {
        Path file = config(options, false, false);
        String baseline = engine.getServerLogs();
        try {
            Container.ExecResult result = engine.executeJob(resource(file));
            String logs = transcript(engine, baseline, result);
            Assertions.assertNotEquals(0, result.getExitCode(), logs);
            Assertions.assertTrue(
                    logs.contains(expected), () -> "Expected " + expected + "\n" + logs);
            for (String secret :
                    List.of(
                            SpApiFixture.SECRET,
                            SpApiFixture.REFRESH,
                            "fixture-lwa-",
                            "fixture-rdt-",
                            SpApiFixture.PRIVATE_ERROR,
                            PRIVATE_SIGNATURE)) {
                Assertions.assertFalse(
                        logs.contains(secret), "provider details were exposed in diagnostics");
            }
            return logs;
        } finally {
            Files.deleteIfExists(file);
        }
    }

    private static Path config(ObjectNode options, boolean streaming, boolean keepJobOpen)
            throws IOException {
        String mode = streaming ? "STREAMING\n checkpoint.interval=500" : "BATCH";
        String tail =
                keepJobOpen
                        ? "\n FakeSource {\n plugin_output=unrelated\n schema={fields {marker=string}}\n"
                                + " row.num=1\n split.read-interval=100\n rows=[{kind=INSERT,fields=[\"unrelated\"]}]\n }"
                        : "";
        String names = keepJobOpen ? "[sp,unrelated]" : "[sp]";
        Path directory = ContainerUtil.getCurrentModulePath().resolve("src/test/resources");
        Files.createDirectories(directory);
        Path conf = Files.createTempFile(directory, "sp-source-", ".conf");
        return Files.writeString(
                conf,
                "env {\n parallelism=1\n job.retry.times=0\n job.mode="
                        + mode
                        + "\n}\nsource {\n AmazonSellerPartner "
                        + options.deepCopy().put("plugin_output", "sp")
                        + tail
                        + "\n}\nsink {\n Console { plugin_input="
                        + names
                        + " }\n}\n");
    }

    private static List<Row> emitted(
            TestContainer engine, String baseline, Container.ExecResult result, String selected)
            throws IOException {
        List<Row> logs = rows(fresh(engine, baseline), selected);
        return logs.isEmpty() ? rows(result.getStdout(), selected) : logs;
    }

    private static List<Row> rows(String stdout, String resource) throws IOException {
        List<Row> records = new ArrayList<>();
        String field = " : " + resource + ", ";
        for (String line : stdout.split("\\R")) {
            int prefix = line.indexOf("SeaTunnelRow#kind=");
            int offset = prefix < 0 ? -1 : line.indexOf(field, prefix);
            if (offset >= 0) {
                String[] columns = line.substring(offset + field.length()).split(", ", 4);
                Assertions.assertEquals(4, columns.length, line);
                records.add(
                        new Row(
                                columns[0],
                                columns[1].isEmpty() ? null : columns[1],
                                columns[2].isEmpty() ? null : columns[2],
                                JSON.readTree(columns[3])));
            }
        }
        return records;
    }

    private static String transcript(
            TestContainer engine, String baseline, Container.ExecResult result) {
        return result.getStdout() + "\n" + result.getStderr() + "\n" + fresh(engine, baseline);
    }

    private static String fresh(TestContainer engine, String baseline) {
        String logs = engine.getServerLogs();
        return logs.startsWith(baseline) ? logs.substring(baseline.length()) : "";
    }

    private static void success(Container.ExecResult result) {
        Assertions.assertEquals(
                0, result.getExitCode(), result.getStdout() + "\n" + result.getStderr());
    }

    private static String resource(Path file) {
        return "/" + file.getFileName();
    }

    private static String jobId() {
        return Long.toString(JobIdGenerator.newJobId());
    }

    private static void status(TestContainer engine, String id, String expected) {
        Awaitility.await()
                .atMost(35, TimeUnit.SECONDS)
                .until(() -> expected.equals(engine.getJobStatus(id)));
    }

    private static void newerCheckpoint(TestContainer engine, String id) {
        long before = engine.getCompletedCheckpointCount(id);
        Awaitility.await()
                .atMost(30, TimeUnit.SECONDS)
                .until(() -> engine.getCompletedCheckpointCount(id) > before);
    }

    /**
     * Captured output from the real engine Console sink.
     *
     * <p>The JSON tree keeps numeric types available for exact assertions.
     */
    private static final class Row {
        private final String id;
        private final String parent;
        private final String market;
        private final JsonNode data;

        private Row(String id, String parent, String market, JsonNode data) {
            this.id = id;
            this.parent = parent;
            this.market = market;
            this.data = data;
        }
    }

    /**
     * A running or restored engine job controlled by the test driver.
     *
     * <p>The CLI runs asynchronously so the test can stop the engine while a provider request is
     * observed in flight.
     */
    private static final class EngineJob implements AutoCloseable {
        final String id = jobId();
        final TestContainer engine;
        final ExecutorService commands = Executors.newSingleThreadExecutor();
        final Future<Container.ExecResult> result;

        EngineJob(TestContainer engine, Path conf, String source) {
            this.engine = engine;
            result =
                    commands.submit(
                            () ->
                                    source == null
                                            ? engine.executeJob(resource(conf), id)
                                            : engine.restoreJobWithCheckpoint(
                                                    resource(conf), source, id));
        }

        void savepoint() throws Exception {
            success(engine.savepointJob(id));
            status(engine, id, "SAVEPOINT_DONE");
            success(result.get(50, TimeUnit.SECONDS));
        }

        void cancel() throws Exception {
            success(engine.cancelJob(id));
            status(engine, id, "CANCELED");
            result.get(50, TimeUnit.SECONDS);
        }

        @Override
        public void close() throws Exception {
            try {
                String state = engine.getJobStatus(id);
                if (Set.of("RUNNING", "SCHEDULED").contains(state)) {
                    engine.cancelJob(id);
                }
            } finally {
                commands.shutdownNow();
            }
        }
    }
}
