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

package org.apache.seatunnel.connectors.seatunnel.confluence;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import org.apache.http.Header;
import org.apache.http.HeaderElement;
import org.apache.http.NameValuePair;
import org.apache.http.message.BasicHeaderValueParser;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

/**
 * Validates provider continuations and extracts only positions for the fixed configured endpoint.
 */
final class ConfluencePagination {
    private static final int MAX_LINK_LENGTH = 8192;
    private final ConfluenceConfig config;

    ConfluencePagination(ConfluenceConfig config) {
        this.config = config;
    }

    /**
     * Reconciles body and Link continuations. Cursor scans follow links even on empty pages; offset
     * scans validate metadata and infer another page only when the effective limit is full.
     */
    String next(JsonNode envelope, Header[] headers, String current, int rows) {
        String fromBody = bodyLink(envelope);
        String fromHeader = headerLink(headers);
        String body = fromBody == null ? null : continuation(fromBody, current);
        String header = fromHeader == null ? null : continuation(fromHeader, current);
        if (body != null
                && header != null
                && !Objects.equals(fingerprint(body), fingerprint(header))) {
            throw config.failure("conflicting pagination links");
        }
        String next = body == null ? header : body;
        if (config.resource.cursor()) {
            return next;
        }
        int start = offset(config, current);
        int effectiveLimit = size(envelope, "limit", config.pageSize);
        if (effectiveLimit < 1
                || effectiveLimit > config.pageSize
                || rows > effectiveLimit
                || size(envelope, "start", start) != start
                || size(envelope, "size", rows) != rows) {
            throw config.failure("inconsistent offset pagination response");
        }
        if (next == null && rows < effectiveLimit) {
            return null;
        }
        long upper = (long) start + effectiveLimit;
        if (upper > Integer.MAX_VALUE) {
            throw config.failure("offset exceeds supported range");
        }
        int nextStart = next == null ? (int) upper : offset(config, next);
        if (nextStart <= start || nextStart < (long) start + rows || nextStart > upper) {
            throw config.failure("nonprogressing or inconsistent next offset");
        }
        return Integer.toString(nextStart);
    }

    /**
     * Accepts bounded raw query tokens without permitting separators or malformed percent escapes.
     */
    static boolean cursorToken(String value) {
        if (value.isEmpty() || value.length() > 4096) {
            return false;
        }
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c == '%'
                    && i + 2 < value.length()
                    && Character.digit(value.charAt(i + 1), 16) >= 0
                    && Character.digit(value.charAt(i + 2), 16) >= 0) {
                i += 2;
            } else if (!((c >= 'a' && c <= 'z')
                    || (c >= 'A' && c <= 'Z')
                    || (c >= '0' && c <= '9')
                    || "_.~+=/-".indexOf(c) >= 0)) {
                return false;
            }
        }
        return true;
    }

    /*
     * Accepts only nonnegative decimal offsets within the supported integer range.
     */
    static int offset(ConfluenceConfig config, String value) {
        if (value.matches("[0-9]{1,10}")) {
            try {
                return Integer.parseInt(value);
            } catch (NumberFormatException ignored) {
                // Use the same redacted diagnostic as malformed values.
            }
        }
        throw config.failure("invalid offset in pagination state");
    }

    /**
     * Normalizes equivalent permitted percent encodings before hashing positions for cycle
     * detection.
     */
    static String fingerprint(String value) {
        StringBuilder canonical = new StringBuilder(value.length());
        for (int i = 0; i < value.length(); i++) {
            char c = value.charAt(i);
            if (c != '%') {
                canonical.append(c);
                continue;
            }
            String escape = value.substring(i + 1, i + 3);
            char decoded = (char) Integer.parseInt(escape, 16);
            if ((decoded >= 'a' && decoded <= 'z')
                    || (decoded >= 'A' && decoded <= 'Z')
                    || (decoded >= '0' && decoded <= '9')
                    || decoded == '-'
                    || decoded == '_'
                    || decoded == '.'
                    || decoded == '~'
                    || decoded == '='
                    || decoded == '/') {
                canonical.append(decoded);
            } else {
                canonical.append('%').append(escape.toUpperCase(Locale.ROOT));
            }
            i += 2;
        }
        return ConfluenceConfig.digest(canonical.toString().getBytes(StandardCharsets.US_ASCII));
    }

    private int size(JsonNode envelope, String key, int fallback) {
        JsonNode value = envelope.get(key);
        if (value == null) {
            return fallback;
        }
        if (!value.isIntegralNumber() || !value.canConvertToInt() || value.intValue() < 0) {
            throw config.failure("invalid offset pagination metadata");
        }
        return value.intValue();
    }

    private String bodyLink(JsonNode envelope) {
        JsonNode links = envelope.get("_links");
        if (links == null || links.isNull()) {
            return null;
        }
        if (!links.isObject()) {
            throw config.failure("invalid pagination links object");
        }
        JsonNode next = links.get("next");
        if (next == null || next.isNull()) {
            return null;
        }
        if (!next.isTextual()) {
            throw config.failure("invalid pagination link");
        }
        return next.textValue();
    }

    /**
     * Splits Link entries outside URI targets and quoted attributes, rejecting ambiguous next
     * links.
     */
    private String headerLink(Header[] headers) {
        String next = null;
        for (Header header : headers) {
            String value = header.getValue();
            boolean insideTarget = false;
            boolean insideQuote = false;
            boolean escaped = false;
            int start = 0;
            for (int i = 0; i <= value.length(); i++) {
                char c = i == value.length() ? ',' : value.charAt(i);
                if (c == ',' && !insideTarget && !insideQuote) {
                    String candidate = headerNext(value.substring(start, i).trim());
                    if (candidate != null) {
                        if (next != null) {
                            throw config.failure("multiple next links in HTTP headers");
                        }
                        next = candidate;
                    }
                    start = i + 1;
                } else if (escaped) {
                    escaped = false;
                } else if (insideQuote && c == '\\') {
                    escaped = true;
                } else if (!insideTarget && c == '"') {
                    insideQuote = !insideQuote;
                } else if (!insideQuote && c == '<') {
                    insideTarget = true;
                } else if (!insideQuote && c == '>') {
                    insideTarget = false;
                }
            }
            if (insideTarget || insideQuote) {
                throw config.failure("invalid HTTP Link header");
            }
        }
        return next;
    }

    private String headerNext(String value) {
        int end = value.indexOf('>');
        if (!value.startsWith("<") || end < 1) {
            throw config.failure("invalid HTTP Link header");
        }
        HeaderElement[] elements =
                BasicHeaderValueParser.parseElements("link" + value.substring(end + 1), null);
        if (elements.length != 1 || !"link".equals(elements[0].getName())) {
            throw config.failure("invalid HTTP Link attributes");
        }
        boolean hasRel = false;
        boolean next = false;
        for (NameValuePair parameter : elements[0].getParameters()) {
            if ("rel".equalsIgnoreCase(parameter.getName())) {
                if (hasRel || parameter.getValue() == null) {
                    throw config.failure("invalid HTTP Link relation");
                }
                hasRel = true;
                for (String relation : parameter.getValue().split("\\s+")) {
                    next |= "next".equalsIgnoreCase(relation);
                }
            }
        }
        return next ? value.substring(1, end) : null;
    }

    /**
     * Allows relative or absolute continuations only within the configured origin and resource. The
     * validated query is reduced to a position so subsequent requests retain fixed parameters.
     */
    private String continuation(String value, String current) {
        if (value.isEmpty()
                || value.length() > MAX_LINK_LENGTH
                || !StandardCharsets.US_ASCII.newEncoder().canEncode(value)) {
            throw config.failure("invalid pagination link");
        }
        URI next;
        try {
            URI request = config.origin.resolve(config.target(current));
            URI link = new URI(value);
            if (link.isOpaque()
                    || (link.getRawPath() != null
                            && (link.getRawPath().contains("%")
                                    || link.getRawPath().matches("(?:^|.*/)\\.{1,2}(?:/.*|$)")))) {
                throw config.failure("invalid pagination path");
            }
            next =
                    value.startsWith("?")
                            ? new URI(config.origin + config.resource.path + value)
                            : request.resolve(link);
        } catch (URISyntaxException e) {
            throw config.failure("invalid pagination link");
        }
        String path = next.getRawPath();
        boolean samePath =
                config.resource.path.equals(path)
                        || (!config.resource.cursor()
                                && config.resource.path.substring("/wiki".length()).equals(path));
        if (!"https".equalsIgnoreCase(next.getScheme())
                || next.getHost() == null
                || !config.origin.getHost().equalsIgnoreCase(next.getHost())
                || ConfluenceConfig.port(next) != ConfluenceConfig.port(config.origin)
                || next.getRawFragment() != null
                || next.getRawUserInfo() != null
                || !samePath
                || next.getRawQuery() == null) {
            throw config.failure(
                    "pagination link leaves the configured HTTPS origin or resource endpoint");
        }
        return queryPosition(next.getRawQuery());
    }

    /**
     * Rejects duplicate or unexpected query parameters instead of forwarding provider-supplied
     * options.
     */
    private String queryPosition(String query) {
        String position = null;
        Set<String> seen = new HashSet<>();
        String positionKey = config.resource.cursor() ? "cursor" : "start";
        for (String component : query.split("&", -1)) {
            int equals = component.indexOf('=');
            if (equals < 1) {
                throw config.failure("invalid pagination query");
            }
            String name = component.substring(0, equals);
            String value = component.substring(equals + 1);
            if (!seen.add(name)) {
                throw config.failure("duplicate parameter in pagination link");
            }
            if (positionKey.equals(name) && position == null) {
                position = value;
            } else if ("limit".equals(name)) {
                int limit = offset(config, value);
                if (limit < 1 || limit > config.pageSize) {
                    throw config.failure("invalid pagination limit");
                }
            } else if (!(config.resource.hasBodyFormat()
                            && "body-format".equals(name)
                            && config.bodyFormat.equals(value))
                    && !(config.resource == ConfluenceConfig.Resource.SPACE
                            && (("description-format".equals(name) && "plain".equals(value))
                                    || ("include-icon".equals(name) && "true".equals(value))))) {
                throw config.failure("unexpected parameters in pagination link");
            }
        }
        if (position == null || (config.resource.cursor() && !cursorToken(position))) {
            throw config.failure("missing or invalid next position");
        }
        return config.resource.cursor() ? position : Integer.toString(offset(config, position));
    }
}
