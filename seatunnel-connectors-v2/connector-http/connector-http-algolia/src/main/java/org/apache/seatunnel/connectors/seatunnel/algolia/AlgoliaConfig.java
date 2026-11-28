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
package org.apache.seatunnel.connectors.seatunnel.algolia;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

/** Validated request, selection and time-window settings bound to one application origin. */
final class AlgoliaConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    final String host;
    final String authority;
    final int port;
    final String applicationId;
    final String token;
    final Set<AlgoliaResource> selection;
    final List<AlgoliaResource> roots;
    final List<AlgoliaResource> children;
    final AlgoliaQuery query;
    final Instant start;
    final Instant requestedEnd;
    final int indicesSize;
    final int logsSize;
    final int maxPages;
    final int interval;
    final int attempts;
    final int socketTimeout;
    final int pageDeadline;
    final int maxBytes;
    final String scope;

    AlgoliaConfig(ReadonlyConfig options) {
        applicationId = options.get(AlgoliaSourceOptions.APPLICATION_ID);
        if (applicationId == null || !applicationId.matches("[A-Za-z0-9]{1,64}")) {
            throw new IllegalArgumentException("Algolia application_id must be alphanumeric");
        }
        token = options.get(AlgoliaSourceOptions.TOKEN);
        if (token == null
                || token.isEmpty()
                || token.length() > 4096
                || !token.chars().allMatch(c -> c >= 33 && c <= 126)) {
            throw new IllegalArgumentException("Algolia requires a valid token");
        }
        URI origin =
                origin(
                        options.getOptional(AlgoliaSourceOptions.BASE_URL)
                                .orElse(
                                        "https://"
                                                + applicationId.toLowerCase(Locale.ROOT)
                                                + ".algolia.net"));
        host = origin.getHost().toLowerCase(Locale.ROOT);
        port = origin.getPort() == -1 ? 443 : origin.getPort();
        authority = host + (port == 443 ? "" : ":" + port);
        EnumSet<AlgoliaResource> selected = EnumSet.noneOf(AlgoliaResource.class);
        for (String name : options.get(AlgoliaSourceOptions.RESOURCES)) {
            if (!selected.add(AlgoliaResource.forName(name))) {
                throw new IllegalArgumentException("Algolia resources must be unique");
            }
        }
        selection =
                Collections.unmodifiableSet(
                        selected.isEmpty() ? EnumSet.allOf(AlgoliaResource.class) : selected);
        children =
                Collections.unmodifiableList(
                        selection.stream()
                                .filter(AlgoliaResource::child)
                                .collect(Collectors.toList()));
        EnumSet<AlgoliaResource> root = EnumSet.copyOf(selection);
        root.removeIf(AlgoliaResource::child);
        if (!children.isEmpty()) {
            root.add(AlgoliaResource.INDICES);
        }
        roots = Collections.unmodifiableList(root.stream().collect(Collectors.toList()));
        query = new AlgoliaQuery(options.get(AlgoliaSourceOptions.SEARCH_QUERY));
        start = parseInstant(options.get(AlgoliaSourceOptions.START_DATE), "start_date");
        requestedEnd =
                options.getOptional(AlgoliaSourceOptions.END_DATE)
                        .map(d -> parseInstant(d, "end_date"))
                        .orElse(null);
        if (requestedEnd != null && !start.isBefore(requestedEnd)) {
            throw new IllegalArgumentException("Algolia end_date must be later than start_date");
        }
        indicesSize = bounded(options, AlgoliaSourceOptions.INDICES_PAGE_SIZE, 1, 1000);
        logsSize = bounded(options, AlgoliaSourceOptions.LOGS_PAGE_SIZE, 1, 1000);
        maxPages = bounded(options, AlgoliaSourceOptions.MAX_PAGES, 1, 20000);
        interval = bounded(options, AlgoliaSourceOptions.MIN_INTERVAL_MS, 10, 60000);
        attempts = bounded(options, AlgoliaSourceOptions.MAX_ATTEMPTS, 1, 8);
        socketTimeout = bounded(options, AlgoliaSourceOptions.SOCKET_TIMEOUT_MS, 100, 120000);
        pageDeadline = bounded(options, AlgoliaSourceOptions.PAGE_DEADLINE_MS, 500, 600000);
        maxBytes = bounded(options, AlgoliaSourceOptions.MAX_RESPONSE_BYTES, 1024, 33554432);
        scope =
                AlgoliaPage.hash(
                        authority,
                        applicationId,
                        selection.toString(),
                        query.raw,
                        start.toString(),
                        String.valueOf(requestedEnd),
                        Integer.toString(indicesSize),
                        Integer.toString(logsSize));
    }

    AlgoliaSplit startAt(Instant coordinatorTime) {
        Instant end = requestedEnd == null ? coordinatorTime : requestedEnd;
        if (!start.isBefore(end)) {
            throw new IllegalArgumentException("Algolia start_date must precede the scan cutoff");
        }
        return new AlgoliaSplit(scope, end);
    }

    private static URI origin(String value) {
        try {
            URI uri = new URI(value);
            if ("https".equalsIgnoreCase(uri.getScheme())
                    && uri.getHost() != null
                    && uri.getRawUserInfo() == null
                    && uri.getRawQuery() == null
                    && uri.getRawFragment() == null
                    && (uri.getRawPath().isEmpty() || "/".equals(uri.getRawPath()))
                    && (uri.getPort() == -1 || (uri.getPort() >= 1 && uri.getPort() <= 65535))) {
                return uri;
            }
        } catch (URISyntaxException e) {
            // Preserve the option name without including private URI components.
        }
        throw new IllegalArgumentException(
                "Algolia base_url must be an HTTPS origin without user info, query or fragment");
    }

    private static Instant parseInstant(String value, String option) {
        try {
            if (value != null && value.endsWith("Z")) {
                return Instant.parse(value);
            }
        } catch (DateTimeParseException ignored) {
            // The invalid input is not needed for diagnosis.
        }
        throw new IllegalArgumentException(
                "Algolia " + option + " requires an ISO-8601 UTC timestamp");
    }

    private static int bounded(ReadonlyConfig options, Option<Integer> option, int min, int max) {
        int value = options.get(option);
        if (value < min || value > max) {
            throw new IllegalArgumentException(
                    "Algolia " + option.key() + " is outside the supported range");
        }
        return value;
    }
}
