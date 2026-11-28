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
package org.apache.seatunnel.connectors.seatunnel.buildkite;

import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.time.DateTimeException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Parses Buildkite's RFC Link pagination within the current collection.
 *
 * <p>The caller rebuilds requests from validated page numbers and original query values. An
 * obsolete pagination credential is discarded without decoding or forwarding it.
 */
final class BuildkiteLink {
    private static final String ORIGIN = "https://" + BuildkiteConfig.API_HOST;

    private BuildkiteLink() {}

    /**
     * Returns the immediately following page or zero when the collection is terminal.
     *
     * <p>Separators inside URI references and quoted parameters remain part of their values.
     */
    static int next(
            BuildkiteResource resource,
            String path,
            Map<String, String> expected,
            int page,
            List<String> headers) {
        String target = null;
        for (String value : headers) {
            Scanner line = new Scanner(resource, value);
            while (line.hasMore()) {
                String reference = line.reference();
                String rel = null;
                boolean anchor = false;
                while (line.take(';')) {
                    String name = line.token().toLowerCase(Locale.ROOT);
                    String parameter = line.take('=') ? line.value() : null;
                    if ("rel".equals(name)) {
                        if (rel != null || parameter == null || parameter.isBlank()) {
                            throw resource.failure("invalid Link relation");
                        }
                        rel = parameter;
                    }
                    anchor |= "anchor".equals(name);
                }
                long next =
                        rel == null
                                ? 0
                                : Arrays.stream(rel.trim().split("\\s+"))
                                        .filter("next"::equalsIgnoreCase)
                                        .count();
                if (next > 0) {
                    if (target != null || anchor || next > 1) {
                        throw resource.failure("ambiguous next Link");
                    }
                    target = reference;
                }
                line.delimiter();
            }
        }
        return target == null ? 0 : scopedPage(resource, path, expected, page, target);
    }

    private static int scopedPage(
            BuildkiteResource resource,
            String path,
            Map<String, String> expected,
            int page,
            String raw) {
        try {
            URI endpoint = URI.create(ORIGIN + path);
            URI reference = URI.create(raw);
            URI resolved =
                    !reference.isAbsolute()
                                    && reference.getRawAuthority() == null
                                    && reference.getRawPath().isEmpty()
                                    && reference.getRawQuery() != null
                                    && reference.getRawFragment() == null
                            ? URI.create(ORIGIN + path + "?" + reference.getRawQuery())
                            : endpoint.resolve(reference);
            if (!"https".equalsIgnoreCase(resolved.getScheme())
                    || !BuildkiteConfig.API_HOST.equalsIgnoreCase(resolved.getHost())
                    || resolved.getPort() != -1 && resolved.getPort() != 443
                    || resolved.getRawUserInfo() != null
                    || resolved.getRawFragment() != null
                    || !path.equals(resolved.getRawPath())) {
                throw resource.failure("next Link changes origin or collection path");
            }
            String query = resolved.getRawQuery();
            if (query == null || query.isEmpty()) {
                throw resource.failure("next Link is missing a page");
            }
            int next = 0;
            Set<String> supplied = new HashSet<>();
            for (String field : query.split("&", -1)) {
                int equals = field.indexOf('=');
                if (equals <= 0) {
                    throw resource.failure("invalid next Link parameter");
                }
                String name = decoded(field.substring(0, equals));
                if (!supplied.add(name)) {
                    throw resource.failure("duplicate next Link parameter");
                }
                if ("api_key".equalsIgnoreCase(name)) {
                    continue;
                }
                String value = decoded(field.substring(equals + 1));
                if ("page".equals(name)) {
                    if (!value.matches("[0-9]{1,6}")) {
                        throw resource.failure("invalid next Link page");
                    }
                    next = Integer.parseInt(value);
                } else if (!sameExpected(name, value, expected.get(name))) {
                    throw resource.failure("next Link changes query scope");
                }
            }
            if (next != page + 1) {
                throw resource.failure("next Link does not advance by one page");
            }
            return next;
        } catch (IllegalArgumentException ex) {
            throw resource.failure("invalid next Link URI or query");
        }
    }

    private static boolean sameExpected(String name, String value, String expected) {
        if (expected == null) {
            return false;
        }
        if (expected.equals(value)) {
            return true;
        }
        if ("created_from".equals(name) || "created_to".equals(name)) {
            try {
                return BuildkiteConfig.providerTimestamp(expected)
                        .equals(BuildkiteConfig.providerTimestamp(value));
            } catch (DateTimeException ex) {
                return false;
            }
        }
        return false;
    }

    private static String decoded(String value) {
        return URLDecoder.decode(value, StandardCharsets.UTF_8);
    }

    /**
     * Reads one Link field without interpreting comma and semicolon inside protected positions.
     *
     * <p>Invalid escapes and an incomplete field fail without including raw header data.
     */
    private static final class Scanner {
        private final BuildkiteResource resource;
        private final String text;
        private int at;

        Scanner(BuildkiteResource resource, String text) {
            this.resource = resource;
            this.text = text;
        }

        boolean hasMore() {
            whitespace();
            return at < text.length();
        }

        String reference() {
            if (!take('<')) {
                throw bad();
            }
            int end = text.indexOf('>', at);
            if (end <= at) {
                throw bad();
            }
            String value = text.substring(at, end);
            at = end + 1;
            return value;
        }

        boolean take(char marker) {
            whitespace();
            if (at < text.length() && text.charAt(at) == marker) {
                ++at;
                return true;
            }
            return false;
        }

        String token() {
            whitespace();
            int first = at;
            while (at < text.length() && tokenCharacter(text.charAt(at))) {
                ++at;
            }
            if (at == first) {
                throw bad();
            }
            return text.substring(first, at);
        }

        String value() {
            if (!take('"')) {
                return token();
            }
            StringBuilder parsed = new StringBuilder();
            while (at < text.length()) {
                char ch = text.charAt(at++);
                if (ch == '"') {
                    return parsed.toString();
                }
                if (ch == '\\') {
                    if (at == text.length()) {
                        throw bad();
                    }
                    ch = text.charAt(at++);
                }
                if (ch < 32 && ch != '\t' || ch == 127) {
                    throw bad();
                }
                parsed.append(ch);
            }
            throw bad();
        }

        void delimiter() {
            if (hasMore() && (!take(',') || !hasMore())) {
                throw bad();
            }
        }

        private void whitespace() {
            while (at < text.length() && (text.charAt(at) == ' ' || text.charAt(at) == '\t')) {
                ++at;
            }
        }

        private static boolean tokenCharacter(char ch) {
            return ch >= 'a' && ch <= 'z'
                    || ch >= 'A' && ch <= 'Z'
                    || ch >= '0' && ch <= '9'
                    || "!#$%&'*+-.^_|~".indexOf(ch) >= 0
                    || ch == 96;
        }

        private BuildkiteResource.Failure bad() {
            return resource.failure("malformed Link header");
        }
    }
}
