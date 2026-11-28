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
package org.apache.seatunnel.connectors.seatunnel.bugsnag;

import okhttp3.HttpUrl;

import java.net.URI;
import java.net.URISyntaxException;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Interprets RFC-style Link entries without letting them change the authenticated resource scope.
 *
 * <p>Only an advertised next offset and an applicable API pagination base can enter state. The
 * transport rebuilds every request from the original endpoint and its fixed options.
 */
final class BugSnagLinks {
    private static final int MAX_HEADER_CHARS = 16384;
    private static final int MAX_OFFSET_CHARS = 2048;

    private BugSnagLinks() {}

    /**
     * Resolves the page's authoritative continuation or the documented release offset.
     *
     * <p>A valid Link governs list continuation even after empty or short pages.
     */
    static BugSnagWork next(
            BugSnagTransport.ResponsePage response,
            BugSnagWork work,
            BugSnagConfig config,
            Instant scanEnd,
            List<String> ids)
            throws BugSnagReadException {
        String target = nextTarget(response.links, work);
        if (target == null) {
            if (work.resource == BugSnagResource.RELEASES && !ids.isEmpty()) {
                // Release lists explicitly document numeric offsets in the API operation.
                return work.continueAt(Integer.toString(work.records + ids.size()), null, ids);
            }
            work.validatePage(ids);
            return null;
        }
        if (work.resource == BugSnagResource.USAGE_SUMMARY) {
            throw work.failure("a usage summary cannot have a continuation");
        }
        HttpUrl url = sameRoute(target, response.request, work);
        return query(url, work, config, scanEnd, ids);
    }

    private static HttpUrl sameRoute(String target, HttpUrl request, BugSnagWork work)
            throws BugSnagReadException {
        try {
            URI syntax = new URI(target);
            if (syntax.getRawUserInfo() != null
                    || syntax.getRawFragment() != null
                    || target.indexOf('\\') >= 0) {
                throw work.failure("invalid continuation URL");
            }
        } catch (URISyntaxException invalid) {
            throw work.failure("invalid continuation URL");
        }
        HttpUrl next = request.resolve(target);
        if (next == null
                || !request.scheme().equals(next.scheme())
                || !request.host().equals(next.host())
                || request.port() != next.port()
                || !request.encodedPath().equals(next.encodedPath())
                || !next.username().isEmpty()
                || !next.password().isEmpty()) {
            throw work.failure("continuation changed the HTTPS origin or resource path");
        }
        return next;
    }

    private static BugSnagWork query(
            HttpUrl url, BugSnagWork work, BugSnagConfig config, Instant scanEnd, List<String> ids)
            throws BugSnagReadException {
        BugSnagResource resource = work.resource;
        Set<String> names = new HashSet<>();
        String offset = null;
        String base = work.base;
        for (int i = 0; i < url.querySize(); i++) {
            String name = url.queryParameterName(i);
            String value = url.queryParameterValue(i);
            if (value == null || !names.add(name)) {
                throw work.failure("invalid or duplicate continuation parameter");
            }
            switch (name) {
                case "offset":
                    offset = value;
                    break;
                case "per_page":
                    int pageSize = resource.pageSize(config.pageSize);
                    fixed(value, Integer.toString(pageSize == 0 ? 30 : pageSize), work);
                    break;
                case "sort":
                    fixed(value, resource.sort, work);
                    break;
                case "direction":
                    fixed(value, resource.sort == null ? null : "desc", work);
                    break;
                case "full_reports":
                    fixed(value, resource == BugSnagResource.EVENTS ? "true" : null, work);
                    break;
                case "base":
                    base = base(value, work, scanEnd);
                    break;
                default:
                    throw work.failure("continuation added an unsupported query parameter");
            }
        }
        if (offset == null
                || offset.isBlank()
                || offset.length() > MAX_OFFSET_CHARS
                || offset.chars().anyMatch(c -> c < 33 || c > 126)) {
            throw work.failure("continuation has no valid offset");
        }
        if (resource == BugSnagResource.RELEASES
                && !Integer.toString(work.records + ids.size()).equals(offset)) {
            throw work.failure("release continuation did not match the next numeric offset");
        }
        return work.continueAt(offset, base, ids);
    }

    private static void fixed(String value, String expected, BugSnagWork work)
            throws BugSnagReadException {
        if (!value.equals(expected)) {
            throw work.failure("continuation changed the fixed request parameters");
        }
    }

    private static String base(String value, BugSnagWork work, Instant scanEnd)
            throws BugSnagReadException {
        if (value.length() > 48
                || (work.resource != BugSnagResource.RELEASES
                        && work.resource != BugSnagResource.ERRORS
                        && work.resource != BugSnagResource.EVENTS)) {
            throw work.failure("continuation added an unexpected pagination base");
        }
        Instant date;
        try {
            date = OffsetDateTime.parse(value).toInstant();
        } catch (DateTimeException invalid) {
            throw work.failure("continuation has an invalid pagination base");
        }
        if (work.resource == BugSnagResource.RELEASES) {
            fixed(date.toString(), scanEnd.toString(), work);
            return null;
        }
        if (work.base != null) {
            fixed(date.toString(), work.base, work);
        }
        return date.toString();
    }

    private static String nextTarget(List<String> headers, BugSnagWork work)
            throws BugSnagReadException {
        int chars = 0;
        String next = null;
        for (String line : headers) {
            chars += line.length();
            if (chars > MAX_HEADER_CHARS) {
                throw work.failure("pagination Link exceeds its size limit");
            }
            LinkParser parser = new LinkParser(line, work);
            while (parser.more()) {
                String target = parser.nextEntry();
                if (target != null) {
                    if (next != null && !next.equals(target)) {
                        throw work.failure("ambiguous next pagination links");
                    }
                    next = target;
                }
            }
        }
        return next;
    }

    /**
     * Reads an individual Link field, including quoted parameters and angle-bracket targets.
     *
     * <p>A comma inside either quoted text or a target URI is part of that value.
     */
    private static final class LinkParser {
        private final String line;
        private final BugSnagWork work;
        private int at;

        LinkParser(String line, BugSnagWork work) {
            this.line = line;
            this.work = work;
        }

        boolean more() {
            skipWhitespace();
            return at < line.length();
        }

        String nextEntry() throws BugSnagReadException {
            require('<');
            int end = line.indexOf('>', at);
            if (end <= at) {
                throw malformed();
            }
            String target = line.substring(at, end);
            at = end + 1;
            boolean next = false;
            boolean relation = false;
            boolean anchor = false;
            while (more() && line.charAt(at) == ';') {
                at++;
                skipWhitespace();
                String name = token();
                if (name.isEmpty()) {
                    throw malformed();
                }
                skipWhitespace();
                String value = "";
                if (more() && line.charAt(at) == '=') {
                    at++;
                    skipWhitespace();
                    value = parameter();
                }
                if ("rel".equalsIgnoreCase(name)) {
                    if (relation || value.isBlank()) {
                        throw malformed();
                    }
                    relation = true;
                    for (String part : value.split("[ \\t]+")) {
                        next |= "next".equalsIgnoreCase(part);
                    }
                } else if ("anchor".equalsIgnoreCase(name)) {
                    anchor = true;
                }
            }
            if (more()) {
                require(',');
                if (!more()) {
                    throw malformed();
                }
            }
            if (next && anchor) {
                throw work.failure("next pagination Link changed its anchor");
            }
            return next ? target : null;
        }

        private String parameter() throws BugSnagReadException {
            if (at < line.length() && line.charAt(at) == '"') {
                at++;
                StringBuilder value = new StringBuilder();
                while (at < line.length()) {
                    char ch = line.charAt(at++);
                    if (ch == '"') {
                        return value.toString();
                    }
                    if (ch == '\\') {
                        if (at == line.length()) {
                            throw malformed();
                        }
                        ch = line.charAt(at++);
                    }
                    value.append(ch);
                }
                throw malformed();
            }
            String value = token();
            if (value.isEmpty()) {
                throw malformed();
            }
            return value;
        }

        private String token() {
            int start = at;
            while (at < line.length() && isTokenChar(line.charAt(at))) {
                at++;
            }
            return line.substring(start, at);
        }

        private static boolean isTokenChar(char ch) {
            return ch >= 'a' && ch <= 'z'
                    || ch >= 'A' && ch <= 'Z'
                    || ch >= '0' && ch <= '9'
                    || "!#$%&'*+-.^_`|~".indexOf(ch) >= 0;
        }

        private void skipWhitespace() {
            while (at < line.length() && (line.charAt(at) == ' ' || line.charAt(at) == '\t')) {
                at++;
            }
        }

        private void require(char expected) throws BugSnagReadException {
            if (at >= line.length() || line.charAt(at++) != expected) {
                throw malformed();
            }
        }

        private BugSnagReadException malformed() {
            return work.failure("malformed pagination Link");
        }
    }
}
