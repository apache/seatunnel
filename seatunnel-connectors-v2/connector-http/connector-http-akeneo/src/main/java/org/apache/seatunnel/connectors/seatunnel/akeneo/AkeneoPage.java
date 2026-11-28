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

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

final class AkeneoPage {
    final List<Row> rows;
    final boolean more;
    final String nextCursor;
    final String digest;

    private AkeneoPage(List<Row> rows, boolean more, String nextCursor) {
        this.rows = rows;
        this.more = more;
        this.nextCursor = nextCursor;
        String[] digested = new String[rows.size() + 2];
        digested[0] = Boolean.toString(more);
        digested[1] = nextCursor == null ? "" : nextCursor;
        for (int index = 0; index < rows.size(); index++) {
            digested[index + 2] = rows.get(index).record;
        }
        digest = hash(digested);
    }

    static AkeneoPage read(
            AkeneoConfig config,
            AkeneoResource resource,
            String family,
            AkeneoSplit.Position position,
            JsonNode root,
            AkeneoHttpTransport transport,
            long deadline)
            throws InterruptedException {
        boolean unpaged = resource.unpaged(config);
        JsonNode items = unpaged ? root : root.path("_embedded").path("items");
        if (!items.isArray()
                || !unpaged && items.size() > config.pageSize
                || !unpaged && !root.path("_links").isObject()) {
            throw resource.failure("invalid collection response");
        }
        List<Row> rows = new ArrayList<>(items.size());
        Set<String> ids = new HashSet<>();
        for (JsonNode item : items) {
            transport.remaining(resource.optionName(), deadline);
            if (!item.isObject()) {
                throw resource.failure("collection contains a non-object record");
            }
            String id = resource.identity(item);
            if (!ids.add(id)) {
                throw resource.failure("collection page contains repeated identities");
            }
            rows.add(new Row(id, item.toString()));
        }
        JsonNode link = unpaged ? null : root.path("_links").get("next");
        if (link == null || link.isNull()) {
            return new AkeneoPage(rows, false, null);
        }
        if (position.number == config.maxPages) {
            throw resource.failure("max_pages reached with a continuation");
        }
        JsonNode href = link.get("href");
        if (href == null || !href.isTextual() || href.textValue().length() > 16384) {
            throw resource.failure("invalid continuation link");
        }
        String cursor = continuation(config, resource, family, position, href.textValue());
        if (resource == AkeneoResource.PRODUCTS && position.cursorSeen(cursor)) {
            throw resource.failure("repeated search-after cursor");
        }
        return new AkeneoPage(rows, true, cursor);
    }

    private static String continuation(
            AkeneoConfig config,
            AkeneoResource resource,
            String family,
            AkeneoSplit.Position position,
            String href) {
        String route = resource.path(config, family);
        try {
            URI link = new URI(href);
            if (link.isOpaque() || link.getRawFragment() != null || link.getRawUserInfo() != null) {
                throw invalid(resource);
            }
            if (link.isAbsolute() || link.getRawAuthority() != null) {
                String scheme = link.isAbsolute() ? link.getScheme() : "https";
                int port = link.getPort() == -1 ? 443 : link.getPort();
                if (!"https".equalsIgnoreCase(scheme)
                        || !config.host.equalsIgnoreCase(link.getHost())
                        || port != config.port) {
                    throw invalid(resource);
                }
            }
            String receivedPath = link.getPath();
            if (!(href.startsWith("?") && "".equals(receivedPath))
                    && !new URI(route).getPath().equals(receivedPath)) {
                throw invalid(resource);
            }
            Map<String, String> params = parseQuery(link.getRawQuery(), resource);
            if (!Integer.toString(config.pageSize).equals(params.remove("limit"))) {
                throw invalid(resource);
            }
            if (resource == AkeneoResource.PRODUCTS) {
                String cursor = params.remove("search_after");
                if (!"search_after".equals(params.remove("pagination_type"))
                        || !isCursor(cursor)
                        || !params.isEmpty()) {
                    throw invalid(resource);
                }
                return cursor;
            }
            String count = params.remove("with_count");
            if ((count != null && !"false".equals(count) && !"0".equals(count))
                    || !Integer.toString(position.number + 1).equals(params.remove("page"))
                    || !params.isEmpty()) {
                throw invalid(resource);
            }
            return null;
        } catch (IllegalArgumentException | URISyntaxException ex) {
            throw invalid(resource);
        }
    }

    private static Map<String, String> parseQuery(String query, AkeneoResource resource) {
        if (query == null || query.isEmpty() || query.length() > 12000) {
            throw invalid(resource);
        }
        Map<String, String> result = new HashMap<>();
        for (String parameter : query.split("&", -1)) {
            int equals = parameter.indexOf('=');
            if (equals < 1 || result.size() == 4) {
                throw invalid(resource);
            }
            String key = URLDecoder.decode(parameter.substring(0, equals), StandardCharsets.UTF_8);
            String value =
                    URLDecoder.decode(parameter.substring(equals + 1), StandardCharsets.UTF_8);
            if (result.putIfAbsent(key, value) != null || value.indexOf('�') >= 0) {
                throw invalid(resource);
            }
        }
        return result;
    }

    static boolean isCursor(String candidate) {
        return candidate != null
                && !candidate.isBlank()
                && candidate.length() <= AkeneoResource.MAX_CURSOR_CHARS
                && candidate.codePoints().noneMatch(Character::isISOControl);
    }

    private static IllegalStateException invalid(AkeneoResource resource) {
        return resource.failure("invalid continuation origin, route or pagination scope");
    }

    static boolean isHash(String value) {
        return value != null && value.matches("[A-Za-z0-9_-]{43}");
    }

    static String hash(String... components) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            for (String part : components) {
                byte[] bytes = part.getBytes(StandardCharsets.UTF_8);
                digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(bytes.length).array());
                digest.update(bytes);
            }
            return Base64.getUrlEncoder().withoutPadding().encodeToString(digest.digest());
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException("SHA-256 is unavailable", ex);
        }
    }

    static final class Row {
        final String id;
        final String record;

        private Row(String id, String record) {
            this.id = id;
            this.record = record;
        }
    }
}
