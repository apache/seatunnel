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

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ArrayNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Implements the read-only GET, cursor POST and hierarchical ad-group operations.
 *
 * <p>Pagination rules are selected by endpoint rather than treating every response as offset-based.
 */
final class AmazonAdsDirect {
    private static final String PRODUCTS =
            "attributedAddToCartClicks14d,attributedDetailPageViewsClicks14d,attributedNewToBrandPurchases14d,attributedNewToBrandSales14d,attributedNewToBrandUnitsSold14d,attributedPurchases14d,attributedSales14d,brandHaloAttributedAddToCartClicks14d,brandHaloAttributedPurchases14d,brandHaloAttributedSales14d,brandHaloDetailPageViewsClicks14d,brandHaloNewToBrandPurchases14d,brandHaloNewToBrandSales14d,brandHaloNewToBrandUnitsSold14d,brandHaloUnitsSold14d,unitsSold14d";
    private static final String PERFORMANCE =
            "Click-throughs,attributedAddToCartClicks14d,attributedDetailPageViewsClicks14d,attributedPurchases14d,attributedSales14d,attributedTotalAddToCartClicks14d,attributedTotalDetailPageViewsClicks14d,attributedTotalPurchases14d,totalAttributedSales14d,totalUnitsSold14d,unitsSold14d";
    private static final int ATTRIBUTION_SIZE = 300;
    private static final int BUDGET_SIZE = 30;
    private final AmazonAdsConfig config;
    private final AmazonAdsTransport transport;

    AmazonAdsDirect(AmazonAdsConfig config, AmazonAdsTransport transport) {
        this.config = config;
        this.transport = transport;
    }

    Page fetch(
            AmazonAdsResource resource,
            AmazonAdsPartition profile,
            AmazonAdsRecord group,
            Instant cutoff,
            AmazonAdsSplit.Cursor cursor,
            long deadline)
            throws InterruptedException {
        ObjectMapper json = transport.mapper();
        String path = resource.path;
        String profileId = profile == null ? null : profile.profile;
        ObjectNode post = null;
        switch (resource.kind) {
            case BODY_TOKEN:
                post = json.createObjectNode();
                if (resource == AmazonAdsResource.PORTFOLIOS) {
                    post.put("includeExtendedDataFields", true);
                } else {
                    post.put("maxResults", config.pageSize);
                }
                if (cursor.token != null) {
                    post.put("nextToken", cursor.token);
                }
                if (!config.states.isEmpty()
                        && (resource == AmazonAdsResource.SPONSORED_PRODUCT_CAMPAIGNS
                                || resource == AmazonAdsResource.SPONSORED_BRANDS_CAMPAIGNS)) {
                    ArrayNode states = post.putObject("stateFilter").putArray("include");
                    config.states.forEach(states::add);
                }
                break;
            case QUERY_TOKEN:
                path +=
                        "?pageSize="
                                + BUDGET_SIZE
                                + (cursor.token == null
                                        ? ""
                                        : "&nextToken=" + encode(cursor.token));
                break;
            case OFFSET:
                path += "?startIndex=" + cursor.offset + "&count=" + config.pageSize;
                break;
            case ATTRIBUTION:
                post = attribution(json, profile, resource, cutoff, cursor.token);
                break;
            case BID:
            case SUGGESTIONS:
                post = recommendation(json, resource, group);
                break;
            default:
                break;
        }
        JsonNode root = transport.read(resource, profileId, path, resource.media, post, deadline);
        return extract(resource, root, group, cursor, deadline);
    }

    private static ObjectNode recommendation(
            ObjectMapper json, AmazonAdsResource resource, AmazonAdsRecord group) {
        ObjectNode request = json.createObjectNode();
        request.put("adGroupId", group.id);
        request.put("campaignId", AmazonAdsRecord.identifier(resource, group.value, "campaignId"));
        if (resource.kind == AmazonAdsResource.Kind.BID) {
            request.put("recommendationType", "BIDS_FOR_EXISTING_AD_GROUP");
            ArrayNode targets = request.putArray("targetingExpressions");
            for (String type :
                    List.of("CLOSE_MATCH", "LOOSE_MATCH", "SUBSTITUTES", "COMPLEMENTS")) {
                targets.addObject().put("type", type);
            }
        } else {
            request.put("recommendationType", "KEYWORDS_FOR_ADGROUP");
        }
        return request;
    }

    private ObjectNode attribution(
            ObjectMapper json,
            AmazonAdsPartition profile,
            AmazonAdsResource resource,
            Instant cutoff,
            String token) {
        AmazonAdsPartition.Window window = profile.attribution(config, resource, cutoff);
        ObjectNode request = json.createObjectNode();
        request.put("startDate", window.compactFrom())
                .put("endDate", window.compactTo())
                .put("count", ATTRIBUTION_SIZE);
        request.put("cursorId", token == null ? "" : token);
        if (resource == AmazonAdsResource.ATTRIBUTION_REPORT_PRODUCTS) {
            return request.put("reportType", "PRODUCTS").put("metrics", PRODUCTS);
        }
        String group;
        switch (resource) {
            case ATTRIBUTION_REPORT_PERFORMANCE_ADGROUP:
                group = "ADGROUP";
                break;
            case ATTRIBUTION_REPORT_PERFORMANCE_CAMPAIGN:
                group = "CAMPAIGN";
                break;
            default:
                group = "CREATIVE";
        }
        return request.put("reportType", "PERFORMANCE")
                .put("groupBy", group)
                .put(
                        "metrics",
                        "CREATIVE".equals(group) ? PERFORMANCE : PERFORMANCE + ",brb_bonus_amount");
    }

    private Page extract(
            AmazonAdsResource resource,
            JsonNode root,
            AmazonAdsRecord group,
            AmazonAdsSplit.Cursor cursor,
            long deadline)
            throws InterruptedException {
        List<AmazonAdsRecord> rows = new ArrayList<>();
        if (resource.kind == AmazonAdsResource.Kind.BID) {
            if (!root.isObject() || !root.path("bidRecommendations").isArray()) {
                throw resource.fail("invalid bid recommendations response");
            }
            rows.add(new AmazonAdsRecord(group.id, withParents(resource, root, group)));
            return new Page(rows, false, null, -1);
        }
        JsonNode entries = resource.array == null ? root : root.path(resource.array);
        int limit =
                resource.kind == AmazonAdsResource.Kind.PROFILES
                        ? config.maxProfiles
                        : resource == AmazonAdsResource.PORTFOLIOS
                                ? 10000
                                : resource.kind == AmazonAdsResource.Kind.ATTRIBUTION
                                        ? ATTRIBUTION_SIZE
                                        : resource.kind == AmazonAdsResource.Kind.QUERY_TOKEN
                                                ? BUDGET_SIZE
                                                : resource.kind
                                                                == AmazonAdsResource.Kind
                                                                        .SUGGESTIONS
                                                        ? 10000
                                                        : config.pageSize;
        if (!entries.isArray() || entries.size() > limit) {
            throw resource.fail("response collection exceeds the bound or has an invalid shape");
        }
        Set<String> ids = new HashSet<>();
        for (JsonNode node : entries) {
            transport.remaining(resource, deadline);
            if (!node.isObject()) {
                throw resource.fail("response contains a non-object record");
            }
            JsonNode value =
                    resource.dependsOnAdGroup() ? withParents(resource, node, group) : node;
            String id =
                    resource.key == null
                            ? AmazonAdsRecord.hash(value.toString())
                            : AmazonAdsRecord.identifier(resource, node, resource.key);
            if (resource.key != null && !ids.add(id)) {
                throw resource.fail("response repeated a record identity");
            }
            rows.add(new AmazonAdsRecord(id, value));
        }
        String next = null;
        if (resource.kind == AmazonAdsResource.Kind.BODY_TOKEN
                || resource.kind == AmazonAdsResource.Kind.QUERY_TOKEN) {
            next = token(resource, root.get("nextToken"));
        } else if (resource.kind == AmazonAdsResource.Kind.ATTRIBUTION) {
            next = token(resource, root.get("cursorId"));
        }
        boolean more =
                next != null
                        || resource.kind == AmazonAdsResource.Kind.OFFSET && rows.size() == limit;
        long total = cursor.total;
        if (root.isObject() && root.hasNonNull("totalResults")) {
            JsonNode n = root.path("totalResults");
            if (!n.isNumber()) {
                throw resource.fail("invalid totalResults metadata");
            }
            try {
                total = n.decimalValue().longValueExact();
            } catch (ArithmeticException ex) {
                throw resource.fail("invalid totalResults metadata");
            }
            if (total < 0 || cursor.total >= 0 && cursor.total != total) {
                throw resource.fail("pagination total changed or indicates missing rows");
            }
        }
        long fetched = cursor.offset + rows.size();
        if (total >= 0 && (total < fetched || !more && total != fetched)) {
            throw resource.fail("pagination total changed or indicates missing rows");
        }
        Page page = new Page(rows, more, next, total);
        if (more && cursor.page + 1 >= config.maxPages || next != null && cursor.saw(next)) {
            throw resource.fail("continuation repeats or exceeds max_pages");
        }
        if (resource.kind == AmazonAdsResource.Kind.OFFSET
                && page.keys.equals(cursor.previousKeys)
                && !rows.isEmpty()) {
            throw resource.fail("offset pagination repeated a page");
        }
        return page;
    }

    private static JsonNode withParents(
            AmazonAdsResource resource, JsonNode value, AmazonAdsRecord group) {
        ObjectNode result = (ObjectNode) value.deepCopy();
        result.put("adGroupId", group.id);
        result.put("campaignId", AmazonAdsRecord.identifier(resource, group.value, "campaignId"));
        return result;
    }

    private static String token(AmazonAdsResource resource, JsonNode node) {
        if (node == null || node.isNull() || node.isTextual() && node.textValue().isEmpty()) {
            return null;
        }
        if (!node.isTextual()
                || node.textValue().length() > 8192
                || node.textValue().isBlank()
                || node.textValue().codePoints().anyMatch(Character::isISOControl)) {
            throw resource.fail("response continuation is invalid");
        }
        return node.textValue();
    }

    static String encode(String value) {
        return URLEncoder.encode(value, StandardCharsets.UTF_8);
    }

    /**
     * Retains a single response and its integrity and continuation metadata.
     *
     * <p>A reader keeps only its current parent and dependent pages while checkpointing row
     * positions.
     */
    static final class Page {
        final List<AmazonAdsRecord> rows;
        final boolean more;
        final String next;
        final long total;
        final String digest;
        final String keys;

        private Page(List<AmazonAdsRecord> rows, boolean more, String next, long total) {
            this.rows = rows;
            this.more = more;
            this.next = next;
            this.total = total;
            digest =
                    AmazonAdsRecord.hash(
                            AmazonAdsRecord.hash(
                                    rows.stream()
                                            .map(r -> r.value.toString())
                                            .toArray(String[]::new)),
                            Boolean.toString(more),
                            next == null ? "" : next,
                            Long.toString(total));
            keys =
                    AmazonAdsRecord.hash(
                            rows.stream().map(r -> r.id).sorted().toArray(String[]::new));
        }
    }
}
