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

package org.apache.seatunnel.connectors.seatunnel.onehundredms;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import static org.apache.seatunnel.connectors.seatunnel.onehundredms.OneHundredMsSourceOptions.ACCESS_KEY;
import static org.apache.seatunnel.connectors.seatunnel.onehundredms.OneHundredMsSourceOptions.API_URL;
import static org.apache.seatunnel.connectors.seatunnel.onehundredms.OneHundredMsSourceOptions.CONNECT_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.onehundredms.OneHundredMsSourceOptions.MAX_ATTEMPTS;
import static org.apache.seatunnel.connectors.seatunnel.onehundredms.OneHundredMsSourceOptions.MAX_PAGES;
import static org.apache.seatunnel.connectors.seatunnel.onehundredms.OneHundredMsSourceOptions.MAX_PARENTS;
import static org.apache.seatunnel.connectors.seatunnel.onehundredms.OneHundredMsSourceOptions.MAX_RECORDS;
import static org.apache.seatunnel.connectors.seatunnel.onehundredms.OneHundredMsSourceOptions.MAX_RESPONSE_BYTES;
import static org.apache.seatunnel.connectors.seatunnel.onehundredms.OneHundredMsSourceOptions.PAGE_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.onehundredms.OneHundredMsSourceOptions.READ_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.onehundredms.OneHundredMsSourceOptions.REQUEST_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.onehundredms.OneHundredMsSourceOptions.RESOURCES;
import static org.apache.seatunnel.connectors.seatunnel.onehundredms.OneHundredMsSourceOptions.SECRET_KEY;
import static org.apache.seatunnel.connectors.seatunnel.onehundredms.OneHundredMsSourceOptions.START_DATE;
import static org.apache.seatunnel.connectors.seatunnel.onehundredms.OneHundredMsSourceOptions.TOKEN;

/** Validated connector options. Only request-shaping fields enter the checkpoint fingerprint. */
final class OneHundredMsConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    final URI origin;
    final String token;
    final String accessKey;
    final String secretKey;
    final List<OneHundredMsResource> resources;
    final Instant startDate;
    final int pageSize;
    final int maxPages;
    final int maxParents;
    final int maxResponseBytes;
    final int maxRecords;
    final int connectTimeout;
    final int readTimeout;
    final int requestTimeout;
    final int attempts;
    final String planFingerprint;

    OneHundredMsConfig(ReadonlyConfig config) {
        origin = origin(config.get(API_URL));
        token = credential(config, TOKEN, 8192, true);
        accessKey = credential(config, ACCESS_KEY, 512, true);
        secretKey = credential(config, SECRET_KEY, 4096, false);
        if (token == null
                ? accessKey == null || secretKey == null
                : accessKey != null || secretKey != null) {
            throw invalid("supply either token or both access_key and secret_key");
        }
        Set<OneHundredMsResource> selected = new LinkedHashSet<>();
        for (String name : config.get(RESOURCES)) {
            OneHundredMsResource resource;
            try {
                resource = OneHundredMsResource.valueOf(name.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException | NullPointerException e) {
                throw invalid("resources contains an unsupported name");
            }
            if (!selected.add(resource)) {
                throw invalid("resources must not contain duplicates");
            }
        }
        if (selected.isEmpty()) {
            throw invalid("resources must not be empty");
        }
        resources = Collections.unmodifiableList(new ArrayList<>(selected));
        String start = config.get(START_DATE);
        try {
            if (start != null && !start.endsWith("Z")) {
                throw invalid("start_date must be an ISO-8601 UTC instant ending in Z");
            }
            startDate = start == null ? null : Instant.parse(start);
        } catch (DateTimeParseException e) {
            throw invalid("start_date must be an ISO-8601 UTC instant ending in Z");
        }
        pageSize = range(config, PAGE_SIZE, 10, 100);
        maxPages = range(config, MAX_PAGES, 1, 20000);
        maxParents = range(config, MAX_PARENTS, 1, 20000);
        maxResponseBytes = range(config, MAX_RESPONSE_BYTES, 1024, 16777216);
        maxRecords = range(config, MAX_RECORDS, 1, 20000);
        connectTimeout = range(config, CONNECT_TIMEOUT, 1, 300000);
        readTimeout = range(config, READ_TIMEOUT, 1, 300000);
        requestTimeout = range(config, REQUEST_TIMEOUT, 1, 300000);
        attempts = range(config, MAX_ATTEMPTS, 1, 10);
        planFingerprint = digest(origin + "|" + resources + "|" + startDate + "|" + pageSize);
    }

    private static URI origin(String value) {
        try {
            URI uri = new URI(value);
            if (!"https".equalsIgnoreCase(uri.getScheme())
                    || uri.getHost() == null
                    || uri.getHost().length() > 253
                    || uri.getPort() == 0
                    || uri.getPort() > 65535
                    || uri.getRawUserInfo() != null
                    || uri.getRawQuery() != null
                    || uri.getRawFragment() != null
                    || (!uri.getRawPath().isEmpty() && !"/".equals(uri.getRawPath()))) {
                throw invalid("api_url must be a trusted HTTPS origin");
            }
            return new URI(
                    "https",
                    null,
                    uri.getHost().toLowerCase(Locale.ROOT),
                    uri.getPort(),
                    null,
                    null,
                    null);
        } catch (URISyntaxException e) {
            throw invalid("api_url must be a trusted HTTPS origin");
        }
    }

    private static String credential(
            ReadonlyConfig config, Option<String> key, int max, boolean ascii) {
        String value = config.get(key);
        if (value != null
                && (value.trim().isEmpty()
                        || value.length() > max
                        || (ascii && value.chars().anyMatch(c -> c < 33 || c > 126)))) {
            throw invalid(key.key() + " has an invalid credential format");
        }
        return value;
    }

    private static int range(ReadonlyConfig config, Option<Integer> key, int min, int max) {
        int result = config.get(key);
        if (result < min || result > max) {
            throw invalid(key.key() + " must be between " + min + " and " + max);
        }
        return result;
    }

    static String digest(String value) {
        try {
            byte[] digest =
                    MessageDigest.getInstance("SHA-256")
                            .digest(value.getBytes(StandardCharsets.UTF_8));
            return Base64.getUrlEncoder().withoutPadding().encodeToString(digest);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    static IllegalArgumentException invalid(String message) {
        return new IllegalArgumentException("100ms source: " + message);
    }
}
