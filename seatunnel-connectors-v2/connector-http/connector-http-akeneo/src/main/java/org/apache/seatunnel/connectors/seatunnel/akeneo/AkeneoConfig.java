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
package org.apache.seatunnel.connectors.seatunnel.akeneo;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

final class AkeneoConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    final String origin;
    final String host;
    final String authority;
    final int port;
    final String clientAuthorization;
    final String username;
    final String password;
    final Set<AkeneoResource> selection;
    final List<AkeneoResource> groups;
    final boolean legacyMeasurement;
    final int pageSize;
    final int maxPages;
    final int interval;
    final int attempts;
    final int socketTimeout;
    final int pageDeadline;
    final int maxBytes;
    final String scope;

    AkeneoConfig(ReadonlyConfig options) {
        URI uri = parseOrigin(options.get(AkeneoSourceOptions.BASE_URL));
        host = uri.getHost().toLowerCase(Locale.ROOT);
        port = uri.getPort() == -1 ? 443 : uri.getPort();
        authority = host + (port == 443 ? "" : ":" + port);
        origin = "https://" + authority;
        String clientId = credential(options, AkeneoSourceOptions.CLIENT_ID);
        if (clientId.indexOf(':') >= 0) {
            throw new IllegalArgumentException("Akeneo client_id must not contain a colon");
        }
        String secret = credential(options, AkeneoSourceOptions.CLIENT_SECRET);
        clientAuthorization =
                "Basic "
                        + Base64.getEncoder()
                                .encodeToString(
                                        (clientId + ":" + secret).getBytes(StandardCharsets.UTF_8));
        username = credential(options, AkeneoSourceOptions.USERNAME);
        password = credential(options, AkeneoSourceOptions.PASSWORD);
        EnumSet<AkeneoResource> selected = EnumSet.noneOf(AkeneoResource.class);
        for (String value : options.get(AkeneoSourceOptions.RESOURCES)) {
            AkeneoResource resource = AkeneoResource.fromOption(value);
            if (!selected.add(resource)) {
                throw new IllegalArgumentException("Akeneo resources must be unique");
            }
        }
        selection =
                Collections.unmodifiableSet(
                        selected.isEmpty() ? EnumSet.allOf(AkeneoResource.class) : selected);
        EnumSet<AkeneoResource> roots = EnumSet.copyOf(selection);
        if (roots.remove(AkeneoResource.FAMILY_VARIANTS)) {
            roots.add(AkeneoResource.FAMILIES);
        }
        groups = Collections.unmodifiableList(roots.stream().collect(Collectors.toList()));
        String measurement = options.get(AkeneoSourceOptions.MEASUREMENT_API);
        if (!"legacy".equals(measurement) && !"current".equals(measurement)) {
            throw new IllegalArgumentException("Akeneo measurement_api must be current or legacy");
        }
        legacyMeasurement = "legacy".equals(measurement);
        pageSize = range(options, AkeneoSourceOptions.PAGE_SIZE, 1, 100);
        maxPages = range(options, AkeneoSourceOptions.MAX_PAGES, 1, 100000);
        interval = range(options, AkeneoSourceOptions.REQUEST_INTERVAL_MS, 10, 60000);
        attempts = range(options, AkeneoSourceOptions.MAX_ATTEMPTS, 1, 8);
        socketTimeout = range(options, AkeneoSourceOptions.SOCKET_TIMEOUT_MS, 100, 120000);
        pageDeadline = range(options, AkeneoSourceOptions.PAGE_DEADLINE_MS, 500, 600000);
        maxBytes = range(options, AkeneoSourceOptions.MAX_RESPONSE_BYTES, 1024, 33554432);
        scope =
                AkeneoPage.hash(
                        origin,
                        clientId,
                        username,
                        selection.toString(),
                        measurement,
                        Integer.toString(pageSize));
    }

    private static URI parseOrigin(String value) {
        try {
            URI uri = new URI(value);
            String path = uri.getRawPath();
            if ("https".equalsIgnoreCase(uri.getScheme())
                    && uri.getHost() != null
                    && uri.getRawUserInfo() == null
                    && uri.getRawQuery() == null
                    && uri.getRawFragment() == null
                    && (path.isEmpty() || "/".equals(path))
                    && (uri.getPort() == -1 || (uri.getPort() >= 1 && uri.getPort() <= 65535))) {
                return uri;
            }
        } catch (URISyntaxException | NullPointerException ignored) {
            // Do not expose the supplied URL.
        }
        throw new IllegalArgumentException(
                "Akeneo base_url must be an HTTPS origin without user info, query or fragment");
    }

    private static String credential(ReadonlyConfig options, Option<String> option) {
        String value = options.get(option);
        if (value == null
                || value.isBlank()
                || value.length() > 4096
                || value.codePoints().anyMatch(Character::isISOControl)) {
            throw new IllegalArgumentException("Akeneo requires a valid " + option.key());
        }
        return value;
    }

    private static int range(ReadonlyConfig options, Option<Integer> option, int min, int max) {
        int value = options.get(option);
        if (value < min || value > max) {
            throw new IllegalArgumentException(
                    "Akeneo " + option.key() + " is outside the supported range");
        }
        return value;
    }
}
