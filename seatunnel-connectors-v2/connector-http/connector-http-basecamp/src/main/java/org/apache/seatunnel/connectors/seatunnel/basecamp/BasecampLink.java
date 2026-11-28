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
package org.apache.seatunnel.connectors.seatunnel.basecamp;

import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;

/**
 * Parses Basecamp's RFC Link pagination without letting a response change its read scope.
 *
 * <p>Opaque query data is retained as encoded. Only the original recording selectors are decoded
 * and checked before a later request.
 */
final class BasecampLink {
    private static final String ORIGIN = "https://" + BasecampConfig.API_HOST;
    private static final Set<String> CREDENTIAL_FIELDS =
            Set.of(
                    "authorization",
                    "token",
                    "access_token",
                    "refresh_token",
                    "client_secret",
                    "api_key");

    private BasecampLink() {}

    /**
     * Returns the validated next query or null when the response is terminal.
     *
     * <p>Comma separators apply only outside URI references and quoted parameters. Multiple next
     * relations are ambiguous and cannot schedule more credential-bearing requests.
     */
    static String next(
            BasecampResource resource,
            String path,
            Map<String, String> required,
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
        if (target == null) {
            return null;
        }
        return scoped(resource, path, required, target);
    }

    static String resume(
            BasecampResource resource, String path, Map<String, String> required, String saved) {
        if (!saved.startsWith("?") || !saved.equals(scoped(resource, path, required, saved))) {
            throw resource.failure("checkpoint has an invalid Link query");
        }
        return saved;
    }

    private static String scoped(
            BasecampResource resource, String path, Map<String, String> required, String raw) {
        try {
            URI endpoint = URI.create(ORIGIN + path);
            URI reference = URI.create(raw);
            boolean queryOnly =
                    !reference.isAbsolute()
                            && reference.getRawAuthority() == null
                            && reference.getRawPath().isEmpty()
                            && reference.getRawQuery() != null
                            && reference.getRawFragment() == null;
            URI resolved =
                    queryOnly
                            ? URI.create(ORIGIN + path + "?" + reference.getRawQuery())
                            : endpoint.resolve(reference);
            if (!"https".equalsIgnoreCase(resolved.getScheme())
                    || !BasecampConfig.API_HOST.equalsIgnoreCase(resolved.getHost())
                    || resolved.getPort() != -1 && resolved.getPort() != 443
                    || resolved.getRawUserInfo() != null
                    || resolved.getRawFragment() != null
                    || !path.equals(resolved.getRawPath())) {
                throw resource.failure("next Link changes account, origin, or resource path");
            }
            String original = resolved.getRawQuery();
            StringBuilder query = new StringBuilder(original == null ? "" : original);
            Set<String> supplied = new HashSet<>();
            if (original != null) {
                for (String field : original.split("&", -1)) {
                    int equals = field.indexOf('=');
                    String name = decoded(equals < 0 ? field : field.substring(0, equals));
                    if (CREDENTIAL_FIELDS.contains(name.toLowerCase(Locale.ROOT))) {
                        throw resource.failure("next Link contains a credential parameter");
                    }
                    if (required.containsKey(name)) {
                        String next = equals < 0 ? "" : decoded(field.substring(equals + 1));
                        if (!supplied.add(name) || !required.get(name).equals(next)) {
                            throw resource.failure("next Link changes recording query scope");
                        }
                    }
                }
            }
            required.forEach(
                    (name, value) -> {
                        if (!supplied.contains(name)) {
                            if (query.length() > 0) {
                                query.append('&');
                            }
                            query.append(name).append('=').append(value);
                        }
                    });
            return query.length() == 0 ? "" : "?" + query;
        } catch (IllegalArgumentException ex) {
            throw resource.failure("invalid next Link URI");
        }
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
        private final BasecampResource resource;
        private final String text;
        private int at;

        Scanner(BasecampResource resource, String text) {
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

        private BasecampResource.Failure bad() {
            return resource.failure("malformed Link header");
        }
    }
}
