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

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.SpResource.Api.FBA_ITEMS;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.SpResource.Api.FBA_SHIPMENTS;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.SpResource.Api.FINANCE_EVENTS;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.SpResource.Api.FINANCE_GROUPS;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.SpResource.Api.ORDERS;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.SpResource.Api.ORDER_ITEMS;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.SpResource.Api.VENDOR_ORDERS;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.SpResource.Api.VENDOR_SHIPPING;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.SpResource.Api.VENDOR_STATUS;

/**
 * Reads the direct Seller and Vendor SP-API resources.
 *
 * <p>Continuation requests retain the operation-specific parameters required by Amazon. Parent
 * discovery and child pages have separate cursor chains.
 */
final class SpApiPages {
    private static final int MAX_PAGE_RECORDS = 2000;
    private final SpSettings settings;
    private final SpAuthorization auth;

    SpApiPages(SpSettings settings, SpAuthorization auth) {
        this.settings = settings;
        this.auth = auth;
    }

    List<SpRecord> read(SpPosition position, long deadline)
            throws IOException, InterruptedException {
        SpResource.Api api = settings.resource.api;
        if (api == ORDER_ITEMS || api == FBA_ITEMS) {
            return readChildren(position, api, deadline);
        }
        JsonNode payload = fetch(api, position, settings.includePii, deadline);
        String continuation = cursor(payload, isVendor(api));
        List<JsonNode> records = elements(payload, api);
        position.setCursor(continuation, false);
        List<SpRecord> output = new ArrayList<>();
        for (JsonNode row : records) {
            if (api != ORDERS || inOrderWindow(row, position)) {
                ObjectNode decorated = ((ObjectNode) row).deepCopy();
                if (api == FINANCE_EVENTS) {
                    decorated.put("PostedBefore", position.until.toString());
                } else if (api == VENDOR_ORDERS) {
                    decorated.put("changedBefore", position.until.toString());
                } else if (api == VENDOR_STATUS || api == VENDOR_SHIPPING) {
                    decorated.put("createdBefore", position.until.toString());
                }
                output.add(record(api, decorated, null));
            }
        }
        if (continuation == null) {
            SpWindows.advance(settings, position);
        }
        return output;
    }

    private List<SpRecord> readChildren(SpPosition state, SpResource.Api api, long deadline)
            throws IOException, InterruptedException {
        if (state.parents.isEmpty()) {
            return findParents(state, api, deadline);
        }
        String parent = state.parents.get(state.parentIndex);
        boolean items = api == ORDER_ITEMS;
        String path = api.path + "/" + parent + (items ? "/orderItems" : "/items");
        Map<String, String> args = new LinkedHashMap<>();
        if (state.childCursor != null) {
            args.put("NextToken", state.childCursor);
        }
        SpAuthorization.Scope permission =
                settings.includePii && items
                        ? new SpAuthorization.Scope("/" + path, List.of("buyerInfo"))
                        : null;
        JsonNode response =
                payload(auth.get(settings.resource.name(), path, args, permission, deadline));
        String next = items ? cursor(response, false) : null;
        List<SpRecord> output = new ArrayList<>();
        for (JsonNode item : elements(response, api)) {
            ObjectNode value = ((ObjectNode) item).deepCopy();
            if (items) {
                value.put("AmazonOrderId", parent);
                value.put("LastUpdateDate", state.parentDates.get(parent));
            } else {
                value.put("ShipmentId", parent);
            }
            output.add(record(api, value, parent));
        }
        state.setCursor(next, true);
        if (next == null) {
            state.parentIndex++;
            if (state.parentIndex == state.parents.size()) {
                state.parents = List.of();
                state.parentDates = Map.of();
                state.parentIndex = 0;
                if (state.parentFinished) {
                    SpWindows.advance(settings, state);
                }
            }
        }
        return output;
    }

    private List<SpRecord> findParents(SpPosition state, SpResource.Api api, long deadline)
            throws IOException, InterruptedException {
        SpResource.Api parentApi = api == ORDER_ITEMS ? ORDERS : FBA_SHIPMENTS;
        // An OrderItems traversal needs order IDs and timestamps, not parent PII.
        JsonNode result = fetch(parentApi, state, false, deadline);
        String next = cursor(result, false);
        state.setCursor(next, false);
        Map<String, String> parents = new LinkedHashMap<>();
        for (JsonNode row : elements(result, parentApi)) {
            if (parentApi == ORDERS && !inOrderWindow(row, state)) {
                continue;
            }
            String parent = SpJson.text(row, parentApi.id, 128, settings.resource.name());
            if (!parent.matches("[A-Za-z0-9_-]{1,128}")) {
                throw error("invalid parent identifier");
            }
            String time =
                    parentApi == ORDERS
                            ? SpJson.text(row, "LastUpdateDate", 96, settings.resource.name())
                            : "";
            if (parents.putIfAbsent(parent, time) != null) {
                throw error("duplicate parent identifier in one page");
            }
        }
        state.parents = List.copyOf(parents.keySet());
        state.parentDates = Map.copyOf(parents);
        state.parentIndex = 0;
        state.parentFinished = next == null;
        if (state.parents.isEmpty() && state.parentFinished) {
            SpWindows.advance(settings, state);
        }
        return List.of();
    }

    private JsonNode fetch(SpResource.Api operation, SpPosition state, boolean pii, long deadline)
            throws IOException, InterruptedException {
        Map<String, String> query = new LinkedHashMap<>();
        SpAuthorization.Scope scope = null;
        switch (operation) {
            case ORDERS:
                query.put("MarketplaceIds", settings.marketplace);
                query.put("LastUpdatedAfter", state.from.toString());
                query.put("LastUpdatedBefore", state.until.toString());
                query.put("MaxResultsPerPage", "100");
                if (pii) {
                    scope =
                            new SpAuthorization.Scope(
                                    "/" + operation.path, List.of("buyerInfo", "shippingAddress"));
                }
                break;
            case FINANCE_GROUPS:
                query.put("FinancialEventGroupStartedAfter", state.from.toString());
                query.put("FinancialEventGroupStartedBefore", state.until.toString());
                query.put("MaxResultsPerPage", Integer.toString(settings.financialPageSize));
                break;
            case FINANCE_EVENTS:
                query.put("PostedAfter", state.from.toString());
                query.put("PostedBefore", state.until.toString());
                query.put("MaxResultsPerPage", Integer.toString(settings.financialPageSize));
                break;
            case FBA_SHIPMENTS:
                query.put("MarketplaceId", settings.marketplace);
                query.put("QueryType", state.cursor == null ? "DATE_RANGE" : "NEXT_TOKEN");
                if (state.cursor == null) {
                    query.put("LastUpdatedAfter", state.from.toString());
                    query.put("LastUpdatedBefore", state.until.minusNanos(1).toString());
                }
                break;
            case VENDOR_ORDERS:
            case VENDOR_STATUS:
            case VENDOR_SHIPPING:
                if (operation == VENDOR_SHIPPING) {
                    scope = new SpAuthorization.Scope("/" + operation.path, List.of());
                }
                String field = operation == VENDOR_ORDERS ? "changed" : "created";
                query.put(field + "After", state.from.minusNanos(1).toString());
                query.put(field + "Before", state.until.toString());
                break;
            default:
                throw error("unsupported parent request");
        }
        if (state.cursor != null) {
            query.put(isVendor(operation) ? "nextToken" : "NextToken", state.cursor);
        }
        return payload(auth.get(settings.resource.name(), operation.path, query, scope, deadline));
    }

    private JsonNode payload(JsonNode root) throws SpFailure {
        JsonNode value = root.path("payload");
        JsonNode errors = root.path("errors");
        if (!value.isObject()
                || !(errors.isMissingNode()
                        || errors.isNull()
                        || (errors.isArray() && errors.isEmpty()))) {
            throw error("expected a successful SP-API payload");
        }
        return value;
    }

    private List<JsonNode> elements(JsonNode response, SpResource.Api api) throws SpFailure {
        JsonNode records = response.path(api.records);
        if (records.isMissingNode() || records.isNull()) {
            return List.of();
        }
        if (api == FINANCE_EVENTS && records.isObject()) {
            return records.isEmpty() ? List.of() : List.of(records);
        }
        if (!records.isArray() || records.size() > MAX_PAGE_RECORDS) {
            throw error("invalid API record list");
        }
        List<JsonNode> result = new ArrayList<>(records.size());
        for (JsonNode record : records) {
            if (!record.isObject()) {
                throw error("expected object records in the API response");
            }
            result.add(record);
        }
        return result;
    }

    private String cursor(JsonNode payload, boolean vendor) throws SpFailure {
        JsonNode parent = vendor ? payload.path("pagination") : payload;
        String name = vendor ? "nextToken" : "NextToken";
        JsonNode token = parent.path(name);
        if (token.isMissingNode()
                || token.isNull()
                || (token.isTextual() && token.asText().isEmpty())) {
            return null;
        }
        return SpJson.text(parent, name, 8192, settings.resource.name());
    }

    private boolean inOrderWindow(JsonNode order, SpPosition state) throws SpFailure {
        try {
            Instant timestamp =
                    OffsetDateTime.parse(
                                    SpJson.text(
                                            order, "LastUpdateDate", 96, settings.resource.name()))
                            .toInstant();
            if (timestamp.isBefore(state.from) || timestamp.isAfter(state.until)) {
                throw error("order has an update time outside the requested interval");
            }
            return timestamp.isBefore(state.until);
        } catch (DateTimeException e) {
            throw error("invalid order update time");
        }
    }

    private SpRecord record(SpResource.Api api, ObjectNode value, String parent) throws SpFailure {
        String id =
                api.id == null
                        ? SpSettings.hash(SpJson.write(value, settings.resource.name()))
                        : SpJson.text(value, api.id, 512, settings.resource.name());
        if (api == FBA_ITEMS) {
            id = parent + "/" + id;
        }
        return new SpRecord(id, parent, value);
    }

    private static boolean isVendor(SpResource.Api api) {
        return api.account == SpResource.Account.VENDOR;
    }

    private SpFailure error(String reason) {
        return new SpFailure(settings.resource.name(), reason);
    }
}
