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
package org.apache.seatunnel.connectors.seatunnel.aircall;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

final class AircallPage {
    final List<Row> rows;
    final byte[] digest;
    final String fingerprint;
    final boolean more;
    final String continuation;
    final boolean saturated;

    private AircallPage(
            List<Row> rows,
            byte[] digest,
            String fingerprint,
            boolean more,
            String continuation,
            boolean saturated) {
        this.rows = rows;
        this.digest = digest;
        this.fingerprint = fingerprint;
        this.more = more;
        this.continuation = continuation;
        this.saturated = saturated;
    }

    static AircallPage saturated() {
        return new AircallPage(Collections.emptyList(), new byte[0], null, false, null, true);
    }

    static AircallPage empty(AircallResource resource, AircallSplit.Cursor cursor) {
        if (cursor.index != 0 || cursor.digest.length != 0 || cursor.page != 1) {
            throw resource.failure("previously available page is no longer present");
        }
        return new AircallPage(Collections.emptyList(), new byte[0], null, false, null, false);
    }

    static AircallPage parse(
            AircallConfig config,
            AircallResource resource,
            AircallResource.Request request,
            AircallSplit acquisition,
            AircallSplit.Cursor cursor,
            JsonNode data,
            AircallHttpTransport transport,
            long deadline)
            throws InterruptedException {
        List<JsonNode> records = resource.records(data, config);
        JsonNode meta = data.path("meta");
        boolean more = false;
        String token = null;
        if (resource.kind == AircallResource.Kind.PAGE) {
            int size = resource.pageLimit(config);
            long total = integer(meta.path("total"), resource, "meta.total");
            long received = integer(meta.path("count"), resource, "meta.count");
            long current = integer(meta.path("current_page"), resource, "meta.current_page");
            long perPage = integer(meta.path("per_page"), resource, "meta.per_page");
            long offset = (long) (cursor.page - 1) * size;
            int accessible = AircallResource.QUERY_CAP - (AircallResource.QUERY_CAP % size);
            if (received != records.size()
                    || current != cursor.page
                    || perPage != size
                    || received > 0 && offset + received > total) {
                throw resource.failure("inconsistent response pagination metadata");
            }
            if (total >= AircallResource.QUERY_CAP || total > accessible) {
                return saturated();
            }
            more = offset + size < total;
            JsonNode link = meta.get("next_page_link");
            if (link != null) {
                if ((link.isNull() && more) || (!link.isNull() && !more)) {
                    throw resource.failure("next_page_link conflicts with meta.total");
                }
                if (!link.isNull()) {
                    if (!link.isTextual()) {
                        throw resource.failure("invalid numbered continuation");
                    }
                    verifyLink(
                            config,
                            resource,
                            request,
                            link.textValue(),
                            "page",
                            Integer.toString(cursor.page + 1));
                }
            }
        } else if (resource.kind == AircallResource.Kind.CURSOR) {
            if (!meta.isObject() || !meta.has("next_cursor")) {
                throw resource.failure("missing campaign cursor metadata");
            }
            if (meta.has("count")
                    && integer(meta.get("count"), resource, "meta.count") != records.size()) {
                throw resource.failure("inconsistent campaign page count");
            }
            token = token(resource, meta.get("next_cursor"), false);
            more = token != null;
            JsonNode link = meta.get("next_page_link");
            if (link != null && !link.isNull()) {
                if (!more || !link.isTextual()) {
                    throw resource.failure("invalid campaign continuation link");
                }
                verifyLink(config, resource, request, link.textValue(), "cursor", token);
            }
        } else if (resource.kind == AircallResource.Kind.WHATSAPP) {
            JsonNode info = data.path("pageInfo");
            if (!info.isObject() || !info.has("nextToken")) {
                throw resource.failure("missing WhatsApp pagination metadata");
            }
            token = token(resource, info.get("nextToken"), true);
            more = token != null;
            if (info.hasNonNull("currentToken")) {
                long observed =
                        integer(info.get("currentToken"), resource, "pageInfo.currentToken");
                String expected = cursor.token == null ? "0" : cursor.token;
                if (!Long.toString(observed).equals(expected)) {
                    throw resource.failure("WhatsApp pagination token mismatch");
                }
            }
        }
        if (more && (cursor.page >= config.maxPages)) {
            throw resource.failure("max_pages exceeded");
        }
        if (token != null
                && (token.equals(cursor.token)
                        || cursor.recentlySeen.contains(cursorFingerprint(token)))) {
            throw resource.failure("pagination cursor repeated");
        }

        Set<String> orderedIds = new TreeSet<>();
        List<Row> rows = new ArrayList<>(records.size());
        MessageDigest body = AircallConfig.digest();
        for (JsonNode item : records) {
            transport.remaining(resource, deadline);
            String id = resource.identity(item);
            if (!orderedIds.add(id)) {
                throw resource.failure("resource page contains a repeated identity");
            }
            String encoded = item.toString();
            digestPart(body, encoded);
            Long eventTime = resource.eventTime(item);
            // On a fractional or exact boundary, the following request owns this conversation.
            if (resource != AircallResource.CONVERSATIONS
                    || (eventTime >= acquisition.from && eventTime < acquisition.until)) {
                rows.add(new Row(id, eventTime, encoded));
            }
        }
        byte[] digest = body.digest();
        if (cursor.digest.length != 0
                && (rows.size() <= cursor.index || !MessageDigest.isEqual(digest, cursor.digest))) {
            throw resource.failure("saved page changed while resuming");
        }
        MessageDigest ids = AircallConfig.digest();
        for (String id : orderedIds) {
            digestPart(ids, id);
        }
        String fingerprint =
                orderedIds.isEmpty()
                        ? null
                        : "r:" + Base64.getEncoder().encodeToString(ids.digest());
        if (fingerprint != null && cursor.recentlySeen.contains(fingerprint)) {
            throw resource.failure("pagination repeated a prior result page");
        }
        return new AircallPage(rows, digest, fingerprint, more, token, false);
    }

    private static void verifyLink(
            AircallConfig config,
            AircallResource resource,
            AircallResource.Request request,
            String link,
            String continuationKey,
            String successor) {
        try {
            if (link == null || link.length() > 8192) {
                throw resource.failure("invalid continuation URL");
            }
            URI uri = new URI(link);
            int port = uri.getPort() == -1 ? 443 : uri.getPort();
            boolean anchored = !uri.isAbsolute() && uri.getRawAuthority() == null;
            if ((!anchored
                            && (!"https".equalsIgnoreCase(uri.getScheme())
                                    || !config.host.equalsIgnoreCase(uri.getHost())
                                    || port != config.port))
                    || uri.getRawUserInfo() != null
                    || uri.getRawFragment() != null
                    || !request.path.equals(uri.getRawPath())
                    || uri.getRawQuery() == null) {
                throw resource.failure("continuation violates the configured HTTPS origin or path");
            }
            Set<String> keys = new HashSet<>();
            for (String parameter : uri.getRawQuery().split("&", -1)) {
                int equals = parameter.indexOf('=');
                if (equals < 0) {
                    throw resource.failure("invalid continuation parameter");
                }
                String name =
                        URLDecoder.decode(parameter.substring(0, equals), StandardCharsets.UTF_8);
                String value =
                        URLDecoder.decode(parameter.substring(equals + 1), StandardCharsets.UTF_8);
                String expected =
                        continuationKey.equals(name) ? successor : request.params.get(name);
                if (!keys.add(name) || !value.equals(expected)) {
                    throw resource.failure("continuation changes the query or does not advance");
                }
            }
            if (!keys.contains(continuationKey)) {
                throw resource.failure("missing continuation parameter");
            }
        } catch (IllegalArgumentException | URISyntaxException malformed) {
            throw resource.failure("invalid continuation URL");
        }
    }

    private static long integer(JsonNode value, AircallResource resource, String name) {
        if (!value.isIntegralNumber() || !value.canConvertToLong() || value.longValue() < 0) {
            throw resource.failure("invalid " + name);
        }
        return value.longValue();
    }

    private static String token(AircallResource resource, JsonNode value, boolean numeric) {
        if (value == null || value.isNull()) {
            return null;
        }
        if (numeric) {
            return Long.toString(integer(value, resource, "pageInfo.nextToken"));
        }
        if (!value.isTextual()
                || value.textValue().isBlank()
                || value.textValue().length() > AircallResource.MAX_CURSOR
                || !value.textValue().matches("[!-~]+")) {
            throw resource.failure("invalid cursor value");
        }
        return value.textValue();
    }

    static String cursorFingerprint(String token) {
        MessageDigest digest = AircallConfig.digest();
        digestPart(digest, token);
        return "c:" + Base64.getEncoder().encodeToString(digest.digest());
    }

    private static void digestPart(MessageDigest digest, String value) {
        byte[] bytes = value.getBytes(StandardCharsets.UTF_8);
        digest.update(Integer.toString(bytes.length).getBytes(StandardCharsets.US_ASCII));
        digest.update((byte) ':');
        digest.update(bytes);
    }

    static final class Row {
        final String id;
        final Long eventEpoch;
        final String record;

        private Row(String id, Long eventEpoch, String record) {
            this.id = id;
            this.eventEpoch = eventEpoch;
            this.record = record;
        }
    }
}
