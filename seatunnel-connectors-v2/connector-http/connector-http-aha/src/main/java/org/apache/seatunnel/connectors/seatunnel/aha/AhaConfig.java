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

package org.apache.seatunnel.connectors.seatunnel.aha;

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

import static org.apache.seatunnel.connectors.seatunnel.aha.AhaSourceOptions.API_URL;
import static org.apache.seatunnel.connectors.seatunnel.aha.AhaSourceOptions.CONNECT_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.aha.AhaSourceOptions.MAX_ATTEMPTS;
import static org.apache.seatunnel.connectors.seatunnel.aha.AhaSourceOptions.MAX_PAGES;
import static org.apache.seatunnel.connectors.seatunnel.aha.AhaSourceOptions.MAX_PARENTS;
import static org.apache.seatunnel.connectors.seatunnel.aha.AhaSourceOptions.MAX_RESPONSE_BYTES;
import static org.apache.seatunnel.connectors.seatunnel.aha.AhaSourceOptions.PAGE_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.aha.AhaSourceOptions.READ_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.aha.AhaSourceOptions.REQUEST_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.aha.AhaSourceOptions.RESOURCES;
import static org.apache.seatunnel.connectors.seatunnel.aha.AhaSourceOptions.TOKEN;

/** Validates the exact tenant origin and keeps authorization out of checkpoint fingerprints. */
final class AhaConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    final URI origin;
    final String token;
    final List<AhaResource> resources;
    final int pageSize;
    final int maxPages;
    final int maxParents;
    final int maxBytes;
    final int connectTimeout;
    final int readTimeout;
    final int totalTimeout;
    final int maxAttempts;
    final String fingerprint;

    AhaConfig(ReadonlyConfig input) {
        origin = origin(input.get(API_URL));
        token = input.get(TOKEN);
        if (token == null
                || token.isEmpty()
                || token.length() > 8192
                || token.chars().anyMatch(ch -> ch < 33 || ch > 126)) {
            throw invalid("token must be a nonempty Aha API key or OAuth2 access token");
        }
        Set<AhaResource> selected = new LinkedHashSet<>();
        for (String name : input.get(RESOURCES)) {
            AhaResource resource;
            try {
                resource = AhaResource.valueOf(name.toUpperCase(Locale.ROOT));
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
        pageSize = bound(input, PAGE_SIZE, 1, 200);
        maxPages = bound(input, MAX_PAGES, 1, 20000);
        maxParents = bound(input, MAX_PARENTS, 1, 20000);
        maxBytes = bound(input, MAX_RESPONSE_BYTES, 1024, 16777216);
        connectTimeout = bound(input, CONNECT_TIMEOUT, 1, 300000);
        readTimeout = bound(input, READ_TIMEOUT, 1, 300000);
        totalTimeout = bound(input, REQUEST_TIMEOUT, 1, 300000);
        maxAttempts = bound(input, MAX_ATTEMPTS, 1, 10);
        fingerprint = digest(origin + "|" + resources + "|" + pageSize);
    }

    private static URI origin(String value) {
        try {
            URI uri = new URI(value);
            String host = uri.getHost() == null ? "" : uri.getHost().toLowerCase(Locale.ROOT);
            if (!"https".equalsIgnoreCase(uri.getScheme())
                    || !host.matches("[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?\\.aha\\.io")
                    || uri.getPort() == 0
                    || uri.getPort() > 65535
                    || uri.getRawUserInfo() != null
                    || uri.getRawQuery() != null
                    || uri.getRawFragment() != null
                    || (!uri.getRawPath().isEmpty() && !"/".equals(uri.getRawPath()))) {
                throw invalid(
                        "api_url must be an HTTPS origin on the account's tenant.aha.io host");
            }
            return new URI("https", null, host, uri.getPort(), null, null, null);
        } catch (URISyntaxException | NullPointerException e) {
            throw invalid("api_url must be an HTTPS origin on the account's tenant.aha.io host");
        }
    }

    private static int bound(ReadonlyConfig input, Option<Integer> key, int min, int max) {
        int value = input.get(key);
        if (value < min || value > max) {
            throw invalid(key.key() + " must be between " + min + " and " + max);
        }
        return value;
    }

    static String digest(String value) {
        try {
            byte[] bytes =
                    MessageDigest.getInstance("SHA-256")
                            .digest(value.getBytes(StandardCharsets.UTF_8));
            return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    static IllegalArgumentException invalid(String detail) {
        return new IllegalArgumentException("Aha source: " + detail);
    }
}
