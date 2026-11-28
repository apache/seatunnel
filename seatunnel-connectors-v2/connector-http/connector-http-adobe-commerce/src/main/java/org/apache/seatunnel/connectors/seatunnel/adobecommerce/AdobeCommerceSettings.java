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

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;

import static org.apache.seatunnel.connectors.seatunnel.adobecommerce.AdobeCommerceSourceOptions.BASE_URL;
import static org.apache.seatunnel.connectors.seatunnel.adobecommerce.AdobeCommerceSourceOptions.END_TIME;
import static org.apache.seatunnel.connectors.seatunnel.adobecommerce.AdobeCommerceSourceOptions.MAX_ATTEMPTS;
import static org.apache.seatunnel.connectors.seatunnel.adobecommerce.AdobeCommerceSourceOptions.MAX_PAGES;
import static org.apache.seatunnel.connectors.seatunnel.adobecommerce.AdobeCommerceSourceOptions.MAX_RESPONSE_BYTES;
import static org.apache.seatunnel.connectors.seatunnel.adobecommerce.AdobeCommerceSourceOptions.PAGE_DEADLINE_MS;
import static org.apache.seatunnel.connectors.seatunnel.adobecommerce.AdobeCommerceSourceOptions.PAGE_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.adobecommerce.AdobeCommerceSourceOptions.RESOURCES;
import static org.apache.seatunnel.connectors.seatunnel.adobecommerce.AdobeCommerceSourceOptions.SOCKET_TIMEOUT_MS;
import static org.apache.seatunnel.connectors.seatunnel.adobecommerce.AdobeCommerceSourceOptions.START_TIME;
import static org.apache.seatunnel.connectors.seatunnel.adobecommerce.AdobeCommerceSourceOptions.STORE_CODE;
import static org.apache.seatunnel.connectors.seatunnel.adobecommerce.AdobeCommerceSourceOptions.TOKEN;

final class AdobeCommerceSettings implements Serializable {
    private static final long serialVersionUID = 1L;
    final String host;
    final int port;
    final String authority;
    final String prefix;
    final String token;
    final List<AdobeCommerceResource> resources;
    final Instant start;
    private final Instant explicitEnd;
    final int limit;
    final int maxPages;
    final int attempts;
    final int socketTimeout;
    final int pageDeadline;
    final int maxBytes;
    private final byte[] identity;

    AdobeCommerceSettings(ReadonlyConfig options) {
        URI origin = deployment(options.get(BASE_URL));
        host = origin.getHost().toLowerCase(Locale.ROOT);
        port = origin.getPort() == -1 ? 443 : origin.getPort();
        authority = port == 443 ? host : host + ":" + port;
        String store = options.get(STORE_CODE);
        if (store == null || !store.matches("[a-zA-Z0-9_-]{0,64}")) {
            throw invalid(STORE_CODE);
        }
        String path = origin.getRawPath().replaceFirst("/$", "");
        prefix = path + "/rest/" + (store.isEmpty() ? "" : store + "/") + "V1";
        token = options.get(TOKEN);
        if (token == null || token.length() > 2048 || !token.matches("[A-Za-z0-9._~+/=-]+")) {
            throw invalid(TOKEN);
        }
        List<String> names = options.get(RESOURCES);
        if (names == null
                || names.isEmpty()
                || names.size() > AdobeCommerceResource.values().length
                || !AdobeCommerceResource.names().containsAll(names)
                || new HashSet<>(names).size() != names.size()) {
            throw invalid(RESOURCES);
        }
        resources =
                Collections.unmodifiableList(
                        names.stream()
                                .map(
                                        name ->
                                                AdobeCommerceResource.valueOf(
                                                        name.toUpperCase(Locale.ROOT)))
                                .collect(Collectors.toList()));
        start = utc(options.get(START_TIME), START_TIME);
        explicitEnd = utc(options.get(END_TIME), END_TIME);
        if (explicitEnd != null && (start == null || !explicitEnd.isAfter(start))) {
            throw invalid(END_TIME);
        }
        limit = bounded(options, PAGE_SIZE, 1, 1000);
        maxPages = bounded(options, MAX_PAGES, 1, 100000);
        attempts = bounded(options, MAX_ATTEMPTS, 1, 8);
        socketTimeout = bounded(options, SOCKET_TIMEOUT_MS, 100, 120000);
        pageDeadline = bounded(options, PAGE_DEADLINE_MS, 500, 600000);
        maxBytes = bounded(options, MAX_RESPONSE_BYTES, 128, 33554432);
        String scope =
                authority
                        + prefix
                        + "\n"
                        + names
                        + "\n"
                        + start
                        + "\n"
                        + explicitEnd
                        + "\n"
                        + limit;
        identity = digest().digest(scope.getBytes(StandardCharsets.UTF_8));
    }

    private static URI deployment(String value) {
        if (value != null && value.length() <= 1024) {
            try {
                URI uri = new URI(value);
                if ("https".equalsIgnoreCase(uri.getScheme())
                        && uri.getHost() != null
                        && !uri.getHost().startsWith("[")
                        && uri.getRawUserInfo() == null
                        && uri.getRawQuery() == null
                        && uri.getRawFragment() == null
                        && uri.getPort() != 0
                        && uri.getPort() <= 65535
                        && uri.getRawPath().matches("(?:/[a-zA-Z0-9._~-]+)*/?")
                        && uri.normalize().getRawPath().equals(uri.getRawPath())
                        && !uri.getRawPath().matches(".*/rest(?:/.*)?")) {
                    return uri;
                }
            } catch (URISyntaxException ex) {
                // URL diagnostics omit the input, which can contain credentials.
            }
        }
        throw invalid(BASE_URL);
    }

    private static Instant utc(String text, Option<String> key) {
        if (text == null) {
            return null;
        }
        try {
            if (text.matches("[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}Z")) {
                Instant instant = Instant.parse(text);
                int year = instant.atOffset(ZoneOffset.UTC).getYear();
                if (year >= 1000 && year <= 9998) {
                    return instant;
                }
            }
        } catch (DateTimeException malformed) {
            // Match whole-second UTC API timestamps.
        }
        throw invalid(key);
    }

    AdobeCommerceSplit initial(Instant now) {
        Instant end =
                start == null
                        ? null
                        : explicitEnd != null ? explicitEnd : now.truncatedTo(ChronoUnit.SECONDS);
        if (end != null && !end.isAfter(start)) {
            throw invalid(START_TIME);
        }
        return new AdobeCommerceSplit(identity, end);
    }

    void verify(AdobeCommerceSplit state) {
        if (state == null
                || state.version != 1
                || !MessageDigest.isEqual(identity, state.identity)
                || (start == null) != (state.end == null)
                || (state.end != null && !state.end.isAfter(start))
                || (explicitEnd != null && !explicitEnd.equals(state.end))
                || state.resource < 0
                || state.resource > resources.size()
                || state.page < 1
                || state.page > maxPages
                || state.emitted < 0
                || state.emitted >= AdobeCommerceResource.MAX_UNPAGED_ROWS
                || (state.emitted == 0 ? state.digest.length != 0 : state.digest.length != 32)
                || state.history.size() > AdobeCommerceSplit.HISTORY_LIMIT) {
            throw new IllegalStateException(
                    "AdobeCommerce checkpoint is incompatible with the scan");
        }
        boolean terminal = state.resource == resources.size();
        boolean paged = !terminal && resources.get(state.resource).paged();
        if ((paged && state.emitted >= limit)
                || (!paged && (state.page != 1 || !state.history.isEmpty()))
                || (terminal && state.emitted != 0)) {
            throw new IllegalStateException(
                    "AdobeCommerce checkpoint has an invalid page position");
        }
    }

    static MessageDigest digest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException("SHA-256 is unavailable");
        }
    }

    private static int bounded(
            ReadonlyConfig options, Option<Integer> option, int minimum, int maximum) {
        Integer value = options.get(option);
        if (value == null || value < minimum || value > maximum) {
            throw invalid(option);
        }
        return value;
    }

    private static IllegalArgumentException invalid(Option<?> option) {
        return new IllegalArgumentException("Invalid AdobeCommerce option " + option.key());
    }
}
