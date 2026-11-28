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
package org.apache.seatunnel.connectors.seatunnel.callrail;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import okhttp3.HttpUrl;

import java.math.BigInteger;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Advances the two CallRail pagination contracts within a single account and date scope.
 *
 * <p>Numbered pages use response metadata. Relative calls use the original vendor URI after
 * verifying the same endpoint and fixed query arguments.
 */
final class CallRailPaging {
    private static final int MAX_LINK_LENGTH = 16384;
    private static final String UTC = "Etc/UTC";

    private CallRailPaging() {}

    static HttpUrl request(CallRailConfig config, CallRailSplit state, CallRailResource resource) {
        if (state.nextUrl != null) {
            // Only the validated continuation can populate this field.
            return HttpUrl.get(state.nextUrl);
        }
        HttpUrl.Builder url =
                HttpUrl.get(config.origin)
                        .newBuilder()
                        .addPathSegment("v3")
                        .addPathSegment("a")
                        .addPathSegment(config.accountId)
                        .addPathSegment(resource.path + ".json")
                        .addQueryParameter("per_page", Integer.toString(config.pageSize));
        if (resource.relative(config)) {
            url.addQueryParameter("relative_pagination", "true").addQueryParameter("offset", "0");
        } else {
            url.addQueryParameter("page", Integer.toString(state.page));
        }
        if (resource.dateFiltered) {
            url.addQueryParameter("start_date", state.windowStart.toString())
                    .addQueryParameter("end_date", state.requestEndDate())
                    .addQueryParameter("time_zone", UTC);
        }
        if (resource.fields != null) {
            url.addQueryParameter("fields", resource.fields);
        }
        return url.build();
    }

    /**
     * Checks all continuation metadata before advancing the response.
     *
     * <p>An empty or short body does not override a valid nonterminal signal.
     */
    static CallRailSplit advance(
            JsonNode root,
            CallRailConfig config,
            CallRailSplit state,
            CallRailResource resource,
            List<String> ids)
            throws CallRailReadException {
        if (resource.relative(config)) {
            JsonNode flag = root.path("has_next_page");
            if (!flag.isBoolean()) {
                throw resource.failure(state.page, "missing relative pagination flag");
            }
            JsonNode link = root.path("next_page");
            String next = null;
            if (flag.booleanValue()) {
                next = relative(link.textValue(), config, state, resource).toString();
            } else if (!link.isNull()
                    && !link.isMissingNode()
                    && !(link.isTextual() && link.textValue().isEmpty())) {
                throw resource.failure(state.page, "unexpected next_page on a terminal response");
            }
            return state.advance(config, resource, flag.booleanValue(), config.pageSize, next, ids);
        }
        if (root.has("has_next_page") || root.has("next_page")) {
            throw resource.failure(state.page, "received relative metadata in a numbered scan");
        }
        int returned = integer(root.path("page"), "page", state, resource);
        int perPage = integer(root.path("per_page"), "per_page", state, resource);
        int totalPages = integer(root.path("total_pages"), "total_pages", state, resource);
        JsonNode total = root.path("total_records");
        if (returned != state.page
                || perPage < 1
                || perPage > config.pageSize
                || state.observedSize > 0 && perPage != state.observedSize
                || perPage < ids.size()
                || !total.isIntegralNumber()
                || total.bigIntegerValue().signum() < 0
                || (totalPages == 0
                        ? state.page != 1 || !ids.isEmpty() || total.bigIntegerValue().signum() != 0
                        : totalPages < state.page)) {
            throw resource.failure(state.page, "inconsistent numbered pagination metadata");
        }
        if (total.isIntegralNumber()
                && total.bigIntegerValue().compareTo(BigInteger.valueOf(ids.size())) < 0) {
            throw resource.failure(state.page, "total_records is smaller than the page");
        }
        return state.advance(config, resource, state.page < totalPages, perPage, null, ids);
    }

    private static int integer(
            JsonNode field, String key, CallRailSplit state, CallRailResource resource)
            throws CallRailReadException {
        if (!field.isIntegralNumber() || !field.canConvertToInt() || field.intValue() < 0) {
            throw resource.failure(state.page, "invalid numbered metadata " + key);
        }
        return field.intValue();
    }

    private static HttpUrl relative(
            String target, CallRailConfig config, CallRailSplit state, CallRailResource resource)
            throws CallRailReadException {
        if (target == null || target.length() > MAX_LINK_LENGTH || target.indexOf('\\') >= 0) {
            throw resource.failure(state.page, "invalid relative continuation URL");
        }
        try {
            URI syntax = new URI(target);
            if (syntax.getRawUserInfo() != null || syntax.getRawFragment() != null) {
                throw resource.failure(state.page, "invalid relative continuation URL");
            }
        } catch (URISyntaxException invalid) {
            throw resource.failure(state.page, "invalid relative continuation URL");
        }
        HttpUrl previous = request(config, state, resource);
        HttpUrl next = previous.resolve(target);
        String scoped = "/v3/a/" + config.accountId + "/calls";
        if (next == null
                || !next.scheme().equals(previous.scheme())
                || !next.host().equals(previous.host())
                || next.port() != previous.port()
                || !next.username().isEmpty()
                || !next.password().isEmpty()
                || !(next.encodedPath().equals(scoped)
                        || next.encodedPath().equals(scoped + ".json"))) {
            throw resource.failure(
                    state.page, "relative continuation changed origin or account route");
        }
        Map<String, String> values = new HashMap<>();
        for (int i = 0; i < next.querySize(); i++) {
            String key = next.queryParameterName(i);
            String value = next.queryParameterValue(i);
            if (value == null || values.putIfAbsent(key, value) != null) {
                throw resource.failure(state.page, "invalid or duplicate continuation query key");
            }
        }
        Map<String, String> fixed =
                Map.of(
                        "start_date", state.windowStart.toString(),
                        "end_date", state.requestEndDate(),
                        "time_zone", UTC,
                        "per_page", Integer.toString(config.pageSize),
                        "relative_pagination", "true",
                        "fields", resource.fields,
                        "offset", Integer.toString(state.page));
        for (Map.Entry<String, String> entry : fixed.entrySet()) {
            if (!entry.getValue().equals(values.remove(entry.getKey()))) {
                throw resource.failure(
                        state.page, "relative continuation changed scope, fields or offset");
            }
        }
        Map<String, String> generated =
                Map.ofEntries(
                        Map.entry("action", "index"),
                        Map.entry("agency_id", config.accountId),
                        Map.entry("api_sort[][start_time]", "desc"),
                        Map.entry("api_sort[]", "start_time"),
                        Map.entry("controller", "api/v3/calls"),
                        Map.entry("format", "json"),
                        Map.entry("no_one_to_many_associations", "true"),
                        Map.entry("order", "desc"),
                        Map.entry("page", "1"),
                        Map.entry("sort", "start_time"));
        if (!generated.entrySet().containsAll(values.entrySet())) {
            throw resource.failure(
                    state.page, "relative continuation added an unsupported query value");
        }
        return next;
    }
}
