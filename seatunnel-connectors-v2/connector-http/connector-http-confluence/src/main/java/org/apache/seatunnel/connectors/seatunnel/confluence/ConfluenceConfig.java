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

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Locale;

import static org.apache.seatunnel.connectors.seatunnel.confluence.ConfluenceSourceOptions.BODY_FORMAT;
import static org.apache.seatunnel.connectors.seatunnel.confluence.ConfluenceSourceOptions.CONNECT_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.confluence.ConfluenceSourceOptions.MAX_ATTEMPTS;
import static org.apache.seatunnel.connectors.seatunnel.confluence.ConfluenceSourceOptions.MAX_BYTES;
import static org.apache.seatunnel.connectors.seatunnel.confluence.ConfluenceSourceOptions.MAX_PAGES;
import static org.apache.seatunnel.connectors.seatunnel.confluence.ConfluenceSourceOptions.PAGE_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.confluence.ConfluenceSourceOptions.PAGE_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.confluence.ConfluenceSourceOptions.READ_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.confluence.ConfluenceSourceOptions.RESOURCE;
import static org.apache.seatunnel.connectors.seatunnel.confluence.ConfluenceSourceOptions.SITE_URL;
import static org.apache.seatunnel.connectors.seatunnel.confluence.ConfluenceSourceOptions.TOKEN;
import static org.apache.seatunnel.connectors.seatunnel.confluence.ConfluenceSourceOptions.USERNAME;

final class ConfluenceConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    enum Resource {
        PAGES("pages", "/wiki/api/v2/pages", "&body-format="),
        BLOG_POSTS("blog_posts", "/wiki/api/v2/blogposts", "&body-format="),
        SPACE("space", "/wiki/api/v2/spaces", "&description-format=plain&include-icon=true"),
        GROUP("group", "/wiki/rest/api/group", ""),
        AUDIT("audit", "/wiki/rest/api/audit", "");

        final String name;
        final String path;
        final String parameters;

        Resource(String name, String path, String parameters) {
            this.name = name;
            this.path = path;
            this.parameters = parameters;
        }

        boolean cursor() {
            return this != GROUP && this != AUDIT;
        }

        boolean hasBodyFormat() {
            return this == PAGES || this == BLOG_POSTS;
        }
    }

    final URI origin;
    final Resource resource;
    final String bodyFormat;
    final String binding;
    final int pageSize;
    final int maxPages;
    final int maxAttempts;
    final int connectTimeout;
    final int readTimeout;
    final int pageTimeout;
    final int maxBytes;
    private final String username;
    private final String token;

    ConfluenceConfig(ReadonlyConfig options) {
        origin = origin(required(options, SITE_URL));
        username = required(options, USERNAME);
        token = required(options, TOKEN);
        if (username.indexOf(':') >= 0
                || username.chars().anyMatch(Character::isISOControl)
                || token.chars().anyMatch(Character::isISOControl)) {
            throw invalid("invalid Basic authentication credentials");
        }
        resource = resource(required(options, RESOURCE));
        bodyFormat = options.get(BODY_FORMAT);
        if (!"storage".equals(bodyFormat) && !"atlas_doc_format".equals(bodyFormat)) {
            throw invalid("body_format must be storage or atlas_doc_format");
        }
        if (!resource.hasBodyFormat()
                && options.getOptional(BODY_FORMAT).isPresent()
                && !"storage".equals(bodyFormat)) {
            throw invalid("body_format applies to pages and blog_posts");
        }
        pageSize = range(options, PAGE_SIZE, 100);
        maxPages = range(options, MAX_PAGES, 50000);
        maxAttempts = range(options, MAX_ATTEMPTS, 10);
        connectTimeout = range(options, CONNECT_TIMEOUT, 300000);
        readTimeout = range(options, READ_TIMEOUT, 300000);
        pageTimeout = range(options, PAGE_TIMEOUT, 600000);
        maxBytes = range(options, MAX_BYTES, 33554432);
        binding =
                digest(
                        (origin + "|" + resource.name + "|" + bodyFormat + "|" + pageSize)
                                .getBytes(StandardCharsets.UTF_8));
    }

    String authorization() {
        return "Basic "
                + Base64.getEncoder()
                        .encodeToString((username + ":" + token).getBytes(StandardCharsets.UTF_8));
    }

    String target(String position) {
        String query = resource.path + "?limit=" + pageSize + resource.parameters;
        if (resource.hasBodyFormat()) {
            query += bodyFormat;
        }
        if (!resource.cursor()) {
            return query + "&start=" + position;
        }
        return position.isEmpty() ? query : query + "&cursor=" + position;
    }

    String initialPosition() {
        return resource.cursor() ? "" : "0";
    }

    static int port(URI uri) {
        return uri.getPort() == -1 ? 443 : uri.getPort();
    }

    static String digest(byte[] bytes) {
        return Base64.getEncoder().encodeToString(newDigest().digest(bytes));
    }

    static MessageDigest newDigest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 is unavailable", e);
        }
    }

    static IllegalArgumentException invalid(String reason) {
        return new IllegalArgumentException("Confluence source: " + reason);
    }

    Failure failure(String reason) {
        return new Failure("Confluence " + resource.name + ": " + reason);
    }

    private static URI origin(String value) {
        try {
            URI uri = new URI(value);
            if (!"https".equalsIgnoreCase(uri.getScheme())
                    || uri.getHost() == null
                    || uri.getRawUserInfo() != null
                    || uri.getRawQuery() != null
                    || uri.getRawFragment() != null
                    || uri.getPort() == 0
                    || uri.getPort() > 65535
                    || (!uri.getRawPath().isEmpty() && !"/".equals(uri.getRawPath()))) {
                throw invalid("site_url must be a trusted HTTPS origin");
            }
            return new URI(
                    "https",
                    null,
                    uri.getHost().toLowerCase(Locale.ROOT),
                    port(uri) == 443 ? -1 : port(uri),
                    null,
                    null,
                    null);
        } catch (URISyntaxException e) {
            throw invalid("site_url must be a trusted HTTPS origin");
        }
    }

    private static Resource resource(String name) {
        for (Resource value : Resource.values()) {
            if (value.name.equals(name)) {
                return value;
            }
        }
        throw invalid("resource must be pages, blog_posts, space, group, or audit");
    }

    private static String required(ReadonlyConfig options, Option<String> option) {
        String value = options.get(option);
        if (value == null || value.trim().isEmpty() || value.length() > 2048) {
            throw invalid(option.key() + " is missing or too long");
        }
        return value;
    }

    private static int range(ReadonlyConfig options, Option<Integer> option, int upper) {
        int value = options.get(option);
        if (value < 1 || value > upper) {
            throw invalid(option.key() + " is out of range");
        }
        return value;
    }

    static final class Failure extends IllegalStateException {
        private static final long serialVersionUID = 1L;

        private Failure(String message) {
            super(message);
        }
    }
}
