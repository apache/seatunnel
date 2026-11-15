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

package org.apache.seatunnel.connectors.seatunnel.sentry.source;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonParser;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;

import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;

import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

final class SentryPage {
    private static final ObjectMapper MAPPER =
            new ObjectMapper()
                    .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
                    .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                    .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION);
    private static final Pattern LINK = Pattern.compile("^\\s*<([^>]+)>\\s*(.*)$");
    private static final Pattern PARAMETER =
            Pattern.compile("\\s*;\\s*([A-Za-z][A-Za-z0-9_-]*)\\s*=\\s*\\\"([^\\\"]*)\\\"\\s*");
    final List<SeaTunnelRow> rows;
    final String nextCursor;

    SentryPage(byte[] body, String link, String endpoint) {
        nextCursor = nextCursor(link, endpoint);
        JsonNode root;
        try {
            root = MAPPER.readTree(body);
        } catch (Exception e) {
            throw SentryClient.failure("Invalid JSON event response (body withheld)");
        }
        if (root == null || !root.isArray()) {
            throw SentryClient.failure("Expected a JSON array of Sentry events");
        }
        rows = new ArrayList<>(root.size());
        for (JsonNode event : root) {
            if (!event.isObject()) {
                throw SentryClient.failure("Expected an event object");
            }
            rows.add(
                    new SeaTunnelRow(
                            new Object[] {
                                text(event, "eventID", true), text(event, "groupID", false),
                                        text(event, "projectID", false),
                                text(event, "dateCreated", false), text(event, "title", false),
                                        text(event, "message", false),
                                text(event, "platform", false), event.toString()
                            }));
        }
    }

    private static String text(JsonNode event, String field, boolean required) {
        JsonNode value = event.get(field);
        if (value == null || value.isNull()) {
            if (required) {
                throw SentryClient.failure("Missing event field " + field);
            }
            return null;
        }
        if (!value.isTextual() || (required && value.textValue().isEmpty())) {
            throw SentryClient.failure("Invalid event field " + field + " (value withheld)");
        }
        return value.textValue();
    }

    /** Use only the cursor, never a server-provided URL or rewritten query bounds. */
    static String nextCursor(String header, String endpoint) {
        if (header == null || header.length() > 16384) {
            throw SentryClient.failure("Missing or oversized Sentry Link header");
        }
        boolean found = false;
        String next = null;
        for (String part : header.split(",(?=\\s*<)")) {
            Matcher link = LINK.matcher(part);
            if (!link.matches()) {
                throw SentryClient.failure("Malformed Sentry Link header");
            }
            Map<String, String> parameters = new HashMap<>();
            Matcher parameter = PARAMETER.matcher(link.group(2));
            int position = 0;
            while (parameter.find()) {
                if (parameter.start() != position
                        || parameters.put(parameter.group(1), parameter.group(2)) != null) {
                    throw SentryClient.failure("Malformed Sentry Link parameters");
                }
                position = parameter.end();
            }
            if (position != link.group(2).length()) {
                throw SentryClient.failure("Malformed Sentry Link parameters");
            }
            if (!"next".equals(parameters.get("rel"))) {
                continue;
            }
            if (found) {
                throw SentryClient.failure("Duplicate next-page links");
            }
            found = true;
            String results = parameters.get("results");
            if (!"true".equals(results) && !"false".equals(results)) {
                throw SentryClient.failure("Missing or invalid next-page results flag");
            }
            URI uri;
            try {
                uri = URI.create(link.group(1));
            } catch (RuntimeException e) {
                throw SentryClient.failure("Invalid next-page URI");
            }
            URI expected = URI.create(endpoint);
            if (!expected.getScheme().equals(uri.getScheme())
                    || !expected.getRawAuthority().equals(uri.getRawAuthority())
                    || !expected.getRawPath().equals(uri.getRawPath())
                    || uri.getRawFragment() != null) {
                throw SentryClient.failure("Next-page URI changed Sentry origin or endpoint");
            }
            if ("false".equals(results)) {
                continue;
            }
            String cursor = null;
            boolean cursorFound = false;
            for (NameValuePair query : URLEncodedUtils.parse(uri, StandardCharsets.UTF_8)) {
                if ("cursor".equals(query.getName())) {
                    if (cursorFound) {
                        throw SentryClient.failure("Duplicate next-page cursor");
                    }
                    cursorFound = true;
                    cursor = query.getValue();
                }
            }
            if (cursor == null
                    || cursor.isEmpty()
                    || cursor.length() > 2048
                    || cursor.chars().anyMatch(c -> c < 33 || c > 126)
                    || (parameters.containsKey("cursor")
                            && !cursor.equals(parameters.get("cursor")))) {
                throw SentryClient.failure("Missing or invalid next-page cursor");
            }
            next = cursor;
        }
        if (!found) {
            throw SentryClient.failure("Missing next-page relation in Sentry Link header");
        }
        return next;
    }
}
