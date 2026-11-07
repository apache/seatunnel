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

package org.apache.seatunnel.connectors.seatunnel.paypal.source;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonParser;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.List;

final class PayPalResponse {
    private static final ObjectMapper MAPPER =
            new ObjectMapper()
                    .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
                    .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
    final int totalItems;
    final int totalPages;
    final String account;
    final List<SeaTunnelRow> rows;

    PayPalResponse(JsonNode root, PayPalConfig config, int page) {
        rejectError(root);
        totalItems = integer(root, "total_items");
        totalPages = integer(root, "total_pages");
        if (totalItems > 10000) {
            throw failure(
                    "Result exceeds the 10000-record safety cap; narrow start_date/end_date and reconcile boundaries explicitly");
        }
        int expectedPages = (totalItems + config.pageSize - 1) / config.pageSize;
        if (integer(root, "page") != page
                || (totalPages != expectedPages && !(totalItems == 0 && totalPages == 1))) {
            throw failure("Inconsistent page metadata");
        }
        account = text(root, "account_number", true);
        if (!PayPalConfig.date(text(root, "start_date", true)).equals(config.start)
                || !PayPalConfig.date(text(root, "end_date", true)).equals(config.end)) {
            throw failure(
                    "Response coverage differs from requested dates; wait for reporting availability or explicitly narrow the window");
        }
        JsonNode details = root.path("transaction_details");
        int expectedRows = Math.min(config.pageSize, totalItems - (page - 1) * config.pageSize);
        if (!details.isArray() || details.size() != expectedRows) {
            throw failure("Missing, truncated or inconsistent transaction_details");
        }
        rows = new ArrayList<>(details.size());
        for (JsonNode detail : details) {
            if (!detail.isObject() || !detail.path("transaction_info").isObject()) {
                throw failure("Invalid transaction_info object");
            }
            JsonNode info = detail.get("transaction_info");
            Object[] amount = money(info.get("transaction_amount"));
            Object[] fee = money(info.get("fee_amount"));
            rows.add(
                    new SeaTunnelRow(
                            new Object[] {
                                account,
                                text(info, "transaction_id", false),
                                text(info, "transaction_event_code", false),
                                text(info, "transaction_status", false),
                                timestamp(info, "transaction_initiation_date"),
                                timestamp(info, "transaction_updated_date"),
                                amount[0],
                                amount[1],
                                fee[0],
                                fee[1],
                                detail.toString()
                            }));
        }
    }

    static JsonNode parse(byte[] bytes) {
        try {
            JsonNode root = MAPPER.readTree(bytes);
            if (root == null || !root.isObject()) {
                throw new IllegalArgumentException();
            }
            return root;
        } catch (Exception e) {
            throw failure("Invalid JSON response (body and parser cause withheld)");
        }
    }

    static void rejectError(JsonNode node) {
        boolean tooLarge = "RESULTSET_TOO_LARGE".equals(node.path("name").asText());
        for (JsonNode detail : node.path("details")) {
            tooLarge |= "RESULTSET_TOO_LARGE".equals(detail.path("issue").asText());
        }
        if (tooLarge) {
            throw failure(
                    "RESULTSET_TOO_LARGE: narrow start_date/end_date; automatic lossless splitting is not supported");
        }
        if (node.has("name") || node.has("error") || node.has("details")) {
            throw failure("API returned an error envelope (body withheld)");
        }
    }

    static int integer(JsonNode node, String field) {
        JsonNode value = node.path(field);
        if (!value.isIntegralNumber() || !value.canConvertToInt() || value.intValue() < 0) {
            throw failure("Missing or invalid " + field);
        }
        return value.intValue();
    }

    static String text(JsonNode node, String field, boolean required) {
        JsonNode value = node.get(field);
        if (!required && (value == null || value.isNull())) {
            return null;
        }
        if (value == null || !value.isTextual() || (required && value.textValue().isEmpty())) {
            throw failure("Missing or invalid " + field);
        }
        return value.textValue();
    }

    private static String timestamp(JsonNode node, String field) {
        String value = text(node, field, false);
        return value == null ? null : PayPalConfig.date(value).toString();
    }

    private static Object[] money(JsonNode node) {
        if (node == null || node.isNull()) {
            return new Object[] {null, null};
        }
        String value = text(node, "value", true);
        String currency = text(node, "currency_code", true);
        if (!node.isObject()
                || value.length() > 32
                || !value.matches("-?([0-9]+|[0-9]*\\.[0-9]+)")
                || !currency.matches("[A-Z]{3}")) {
            throw failure("Malformed currency amount");
        }
        try {
            BigDecimal decimal = new BigDecimal(value).setScale(9, RoundingMode.UNNECESSARY);
            if (decimal.precision() > 38) {
                throw new ArithmeticException();
            }
            return new Object[] {decimal, currency};
        } catch (ArithmeticException e) {
            throw failure("Currency amount cannot be represented exactly as DECIMAL(38,9)");
        }
    }

    static IllegalStateException failure(String message) {
        return new IllegalStateException("PayPal: " + message);
    }
}
