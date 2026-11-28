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
package org.apache.seatunnel.connectors.seatunnel.adobecommerce;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.time.format.ResolverStyle;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

enum AdobeCommerceResource {
    CARTS("carts/search", "id", "entity_id", "updated_at"),
    ORDERS("orders", "entity_id", "entity_id", "updated_at"),
    COUPONS("coupons/search", "coupon_id", "coupon_id", "created_at"),
    INVOICES("invoices", "entity_id", "entity_id", "updated_at"),
    PRODUCTS("products", "id", "entity_id", "updated_at"),
    CUSTOMERS("customers/search", "id", "entity_id", "updated_at"),
    SHIPMENTS("shipments", "entity_id", "entity_id", "updated_at"),
    TAX_RATES("taxRates/search", "id", "tax_calculation_rate_id", null),
    CREDITMEMOS("creditmemos", "entity_id", "entity_id", "updated_at"),
    SALES_RULES("salesRules/search", "rule_id", "rule_id", "from_date"),
    STORE_VIEWS("store/storeViews", "id", Shape.ARRAY),
    TAX_CLASSES("taxClasses/search", "class_id", "class_id", null),
    STORE_GROUPS("store/storeGroups", "id", Shape.ARRAY),
    TRANSACTIONS("transactions", "transaction_id", "transaction_id", "created_at"),
    STORE_WEBSITES("store/websites", "id", Shape.ARRAY),
    CUSTOMER_GROUPS("customerGroups/search", "id", "customer_group_id", null),
    INVENTORY_STOCKS("inventory/stocks", "stock_id", "stock_id", null),
    INVENTORY_SOURCES("inventory/sources", "source_code", "source_code", null),
    DIRECTORY_CURRENCY("directory/currency", "base_currency_code", Shape.OBJECT),
    DIRECTORY_COUNTRIES("directory/countries", "id", Shape.ARRAY),
    INVENTORY_SOURCE_ITEMS("inventory/source-items", "source_code", "source_code", null);

    static final int MAX_UNPAGED_ROWS = 10000;
    private static final DateTimeFormatter DATETIME =
            DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss", Locale.ROOT)
                    .withResolverStyle(ResolverStyle.STRICT);
    private final String route;
    private final String idField;
    private final String sortField;
    private final String cursor;
    private final Shape shape;

    AdobeCommerceResource(String route, String idField, String sortField, String cursor) {
        this.route = route;
        this.idField = idField;
        this.sortField = sortField;
        this.cursor = cursor;
        this.shape = Shape.PAGED;
    }

    AdobeCommerceResource(String route, String idField, Shape shape) {
        this.route = route;
        this.idField = idField;
        this.shape = shape;
        this.sortField = null;
        this.cursor = null;
    }

    static List<String> names() {
        return Collections.unmodifiableList(
                Arrays.stream(values())
                        .map(AdobeCommerceResource::label)
                        .collect(Collectors.toList()));
    }

    String label() {
        return name().toLowerCase(Locale.ROOT);
    }

    boolean paged() {
        return shape == Shape.PAGED;
    }

    String target(AdobeCommerceSettings settings, AdobeCommerceSplit state) {
        String path = settings.prefix + "/" + route;
        if (!paged()) {
            return path;
        }
        StringBuilder query = new StringBuilder();
        append(query, "pageSize", Integer.toString(settings.limit));
        append(query, "currentPage", Integer.toString(state.page));
        sort(query, 0, sortField);
        if (this == INVENTORY_SOURCE_ITEMS) {
            sort(query, 1, "sku");
        }
        if (cursor != null && settings.start != null) {
            filter(query, 0, "gteq", bound(settings.start));
            filter(query, 1, "lt", bound(state.end));
        }
        return path + "?" + query;
    }

    private static void append(StringBuilder query, String name, String value) {
        if (query.length() != 0) {
            query.append('&');
        }
        query.append(encode("searchCriteria[" + name + "]")).append('=').append(encode(value));
    }

    private static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8).replace("+", "%20");
    }

    private static void sort(StringBuilder query, int position, String field) {
        String key = "sortOrders][" + position;
        append(query, key + "][field", field);
        append(query, key + "][direction", "ASC");
    }

    private void filter(StringBuilder query, int group, String condition, String bound) {
        String key = "filter_groups][" + group + "][filters][0";
        append(query, key + "][field", cursor);
        append(query, key + "][condition_type", condition);
        append(query, key + "][value", bound);
    }

    private String bound(Instant time) {
        LocalDateTime utc = LocalDateTime.ofInstant(time, ZoneOffset.UTC);
        if (this != SALES_RULES) {
            return DATETIME.format(utc);
        }
        LocalDate date = utc.toLocalDate();
        // Convert the instant to the first whole UTC date at or after the bound.
        if (!time.equals(date.atStartOfDay().toInstant(ZoneOffset.UTC))) {
            date = date.plusDays(1);
        }
        return date.toString();
    }

    List<JsonNode> records(JsonNode root, int pageSize) {
        if (shape == Shape.OBJECT) {
            if (!root.isObject()) {
                throw error("invalid singleton response");
            }
            return Collections.singletonList(root);
        }
        JsonNode array = paged() ? root.get("items") : root;
        if (array == null
                || !array.isArray()
                || array.size() > (paged() ? pageSize : MAX_UNPAGED_ROWS)) {
            throw error("invalid or oversized resource array");
        }
        List<JsonNode> records = new ArrayList<>(array.size());
        array.forEach(records::add);
        return records;
    }

    String id(JsonNode row) {
        String first = idValue(row, idField);
        return this == INVENTORY_SOURCE_ITEMS
                ? JsonNodeFactory.instance
                        .arrayNode()
                        .add(first)
                        .add(idValue(row, "sku"))
                        .toString()
                : first;
    }

    private String idValue(JsonNode row, String field) {
        JsonNode node = row.get(field);
        if (node == null || !(node.isTextual() || node.isIntegralNumber())) {
            throw error("missing resource identity");
        }
        String value = node.asText();
        if (value.isBlank() || value.length() > 512) {
            throw error("invalid resource identity");
        }
        return value;
    }

    void verifyWindow(JsonNode row, Instant start, Instant end) {
        if (start == null || cursor == null) {
            return;
        }
        JsonNode node = row.get(cursor);
        if (node == null || !node.isTextual()) {
            throw error("missing resource cursor in a bounded scan");
        }
        Instant time;
        try {
            String text = node.textValue();
            time =
                    this == SALES_RULES
                            ? LocalDate.parse(text).atStartOfDay().toInstant(ZoneOffset.UTC)
                            : text.endsWith("Z")
                                    ? Instant.parse(text)
                                    : LocalDateTime.parse(text, DATETIME).toInstant(ZoneOffset.UTC);
        } catch (DateTimeException ex) {
            throw error("invalid resource cursor value");
        }
        if (time.isBefore(start) || !time.isBefore(end)) {
            throw error("provider returned a record outside the UTC scan window");
        }
    }

    boolean hasNext(JsonNode root, int page, int count, AdobeCommerceSettings settings) {
        if (!paged()) {
            return false;
        }
        long total = unsigned(root.path("total_count"));
        if (total < 0) {
            throw error("missing or invalid total_count");
        }
        JsonNode echo = root.get("search_criteria");
        if (echo != null
                && (!echo.isObject()
                        || (echo.has("current_page") && unsigned(echo.get("current_page")) != page)
                        || (echo.has("page_size")
                                && unsigned(echo.get("page_size")) != settings.limit))) {
            throw error("search_criteria does not match the requested page");
        }
        long offset = (long) (page - 1) * settings.limit;
        long expected = Math.max(0L, Math.min(settings.limit, total - offset));
        if (count != expected) {
            throw error("page length is inconsistent with total_count");
        }
        boolean more = offset + settings.limit < total;
        if (more && page >= settings.maxPages) {
            throw error("max_pages exceeded");
        }
        return more;
    }

    private static long unsigned(JsonNode node) {
        if (node.isIntegralNumber() && node.canConvertToLong()) {
            return node.longValue();
        }
        if (node.isTextual() && node.textValue().matches("[0-9]{1,19}")) {
            try {
                return Long.parseLong(node.textValue());
            } catch (NumberFormatException ex) {
                return -1;
            }
        }
        return -1;
    }

    Failure error(String reason) {
        return new Failure("AdobeCommerce " + label() + " " + reason);
    }

    static final class Failure extends IllegalStateException {
        private static final long serialVersionUID = 1L;

        private Failure(String message) {
            super(message);
        }
    }

    private enum Shape {
        PAGED,
        ARRAY,
        OBJECT
    }
}
