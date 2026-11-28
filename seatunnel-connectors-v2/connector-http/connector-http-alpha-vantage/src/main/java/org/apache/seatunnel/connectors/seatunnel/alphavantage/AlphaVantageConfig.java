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

package org.apache.seatunnel.connectors.seatunnel.alphavantage;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Validates the fixed provider origin and binds checkpoint state to exactly one query selection.
 */
final class AlphaVantageConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    static final String HOST = "www.alphavantage.co";

    final URI origin;
    final String token;
    final String symbol;
    final List<AlphaVantageResource> resources;
    final String interval;
    final boolean adjusted;
    final String size;
    final int maxRecords;
    final int maxResponse;
    final int connectTimeout;
    final int readTimeout;
    final int requestTimeout;
    final int requestInterval;
    final int maxAttempts;
    final int maxRequests;
    final String fingerprint;

    AlphaVantageConfig(ReadonlyConfig input) {
        origin = origin(input.get(AlphaVantageOptions.API_URL));
        token = input.get(AlphaVantageOptions.TOKEN);
        if (token == null
                || token.isEmpty()
                || token.length() > 4096
                || token.chars().anyMatch(ch -> ch < 33 || ch > 126)) {
            throw invalid("token must be a nonempty API key");
        }
        symbol = input.get(AlphaVantageOptions.SYMBOL);
        if (symbol == null
                || symbol.isEmpty()
                || symbol.length() > 128
                || symbol.codePoints()
                        .anyMatch(
                                ch ->
                                        Character.isWhitespace(ch)
                                                || Character.isISOControl(ch)
                                                || (ch >= 0xd800 && ch <= 0xdfff))) {
            throw invalid("symbol must contain 1 to 128 characters without spaces or controls");
        }
        Set<AlphaVantageResource> selected = new LinkedHashSet<>();
        for (String name : input.get(AlphaVantageOptions.RESOURCES)) {
            AlphaVantageResource resource;
            try {
                resource = AlphaVantageResource.valueOf(name.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException | NullPointerException e) {
                throw invalid("resources contains an unsupported name");
            }
            if (!selected.add(resource)) {
                throw invalid("resources cannot contain duplicates");
            }
        }
        if (selected.isEmpty()) {
            throw invalid("resources cannot be empty");
        }
        resources = Collections.unmodifiableList(new ArrayList<>(selected));
        interval = input.get(AlphaVantageOptions.INTERVAL);
        if (interval == null
                || !Set.of("1min", "5min", "15min", "30min", "60min").contains(interval)) {
            throw invalid("interval must be 1min, 5min, 15min, 30min or 60min");
        }
        adjusted = input.get(AlphaVantageOptions.ADJUSTED);
        size = input.get(AlphaVantageOptions.OUTPUT_SIZE);
        if (size == null || !Set.of("compact", "full").contains(size)) {
            throw invalid("outputsize must be compact or full");
        }
        maxRecords = number(input, AlphaVantageOptions.MAX_RECORDS, 1, 100000);
        maxResponse = number(input, AlphaVantageOptions.MAX_RESPONSE, 1024, 16777216);
        connectTimeout = number(input, AlphaVantageOptions.CONNECT_TIMEOUT, 1, 300000);
        readTimeout = number(input, AlphaVantageOptions.READ_TIMEOUT, 1, 300000);
        requestTimeout = number(input, AlphaVantageOptions.REQUEST_TIMEOUT, 1, 300000);
        requestInterval = number(input, AlphaVantageOptions.REQUEST_INTERVAL, 1, 60000);
        maxAttempts = number(input, AlphaVantageOptions.MAX_ATTEMPTS, 1, 10);
        maxRequests = number(input, AlphaVantageOptions.MAX_REQUESTS, 1, 80);
        // API keys can rotate without changing the query's identity or appearing in the checkpoint.
        fingerprint =
                snapshotId(
                        (origin
                                        + "|"
                                        + symbol.length()
                                        + ":"
                                        + symbol
                                        + "|"
                                        + resources
                                        + "|"
                                        + interval
                                        + "|"
                                        + adjusted
                                        + "|"
                                        + size)
                                .getBytes(StandardCharsets.UTF_8));
    }

    private static int number(ReadonlyConfig input, Option<Integer> key, int min, int max) {
        int value = input.get(key);
        if (value < min || value > max) {
            throw invalid(key.key() + " must be between " + min + " and " + max);
        }
        return value;
    }

    private static URI origin(String value) {
        try {
            URI parsed = new URI(value);
            if (!"https".equalsIgnoreCase(parsed.getScheme())
                    || !HOST.equalsIgnoreCase(parsed.getHost())
                    || parsed.getRawUserInfo() != null
                    || parsed.getRawQuery() != null
                    || parsed.getRawFragment() != null
                    || parsed.getPort() == 0
                    || parsed.getPort() > 65535
                    || (!parsed.getRawPath().isEmpty() && !"/".equals(parsed.getRawPath()))) {
                throw invalid("api_url must be the HTTPS origin of www.alphavantage.co");
            }
            return new URI("https", null, HOST, parsed.getPort(), null, null, null);
        } catch (URISyntaxException | NullPointerException e) {
            throw invalid("api_url must be the HTTPS origin of www.alphavantage.co");
        }
    }

    static String snapshotId(byte[] bytes) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(digest().digest(bytes));
    }

    static MessageDigest digest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    static IllegalArgumentException invalid(String detail) {
        return new IllegalArgumentException("Alpha Vantage source: " + detail);
    }
}
