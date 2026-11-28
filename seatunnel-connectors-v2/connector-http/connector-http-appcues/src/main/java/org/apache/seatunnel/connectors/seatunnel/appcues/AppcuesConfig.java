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

package org.apache.seatunnel.connectors.seatunnel.appcues;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * Holds a validated account query, including the explicit window settings used for restoration.
 * Credentials are only needed for the HTTP authorization header and stay out of the query hash.
 */
final class AppcuesConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final String US_HOST = "api.appcues.com";
    private static final String EU_HOST = "api.eu.appcues.com";
    private static final String HEX = "0123456789ABCDEF";

    static final Option<String> USERNAME =
            Options.key("username")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Appcues API key, used as the Basic authentication username.");
    static final Option<String> PASSWORD =
            Options.key("password")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Appcues API secret, used as the Basic authentication password.");
    static final Option<String> ACCOUNT_ID =
            Options.key("account_id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Appcues account ID.");
    static final Option<String> START_DATE =
            Options.key("start_date")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Inclusive start of the local date filter as an ISO-8601 timestamp.");
    static final Option<String> END_DATE =
            Options.key("end_date")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Exclusive end of the local date filter. Defaults to the job's initial scan time.");
    static final Option<List<String>> RESOURCES =
            Options.key("resources")
                    .listType()
                    .defaultValue(AppcuesResource.names())
                    .withDescription("Ordered list of Appcues resources.");
    static final Option<String> API_URL =
            Options.key("api_url")
                    .stringType()
                    .defaultValue("https://" + US_HOST)
                    .withDescription("Appcues US or EU HTTPS origin.");
    static final Option<Integer> MAX_RECORDS =
            Options.key("max_records")
                    .intType()
                    .defaultValue(50000)
                    .withDescription(
                            "Maximum entries in a resource response before local date filtering.");
    static final Option<Integer> MAX_RESPONSE =
            Options.key("max_response_bytes")
                    .intType()
                    .defaultValue(16777216)
                    .withDescription(
                            "Independent limits on encoded and decoded HTTP response bytes.");
    static final Option<Integer> CONNECT_TIMEOUT =
            Options.key("connect_timeout_ms")
                    .intType()
                    .defaultValue(10000)
                    .withDescription("TCP connection timeout in milliseconds.");
    static final Option<Integer> READ_TIMEOUT =
            Options.key("read_timeout_ms")
                    .intType()
                    .defaultValue(10000)
                    .withDescription("TLS and read inactivity timeout in milliseconds.");
    static final Option<Integer> REQUEST_TIMEOUT =
            Options.key("request_timeout_ms")
                    .intType()
                    .defaultValue(30000)
                    .withDescription(
                            "Total timeout per resource including retry delays and HTTP requests.");
    static final Option<Integer> MAX_ATTEMPTS =
            Options.key("max_attempts")
                    .intType()
                    .defaultValue(4)
                    .withDescription(
                            "Maximum attempts per resource including the initial attempt.");

    final URI origin;
    final String authorization;
    final List<AppcuesResource> resources;
    final Instant start;
    final Instant configuredEnd;
    final int maxRecords;
    final int maxResponse;
    final int connectTimeout;
    final int readTimeout;
    final int requestTimeout;
    final int maxAttempts;
    final String fingerprint;
    private final String accountPath;

    AppcuesConfig(ReadonlyConfig input) {
        String username = credential(input.get(USERNAME), "username");
        if (username.indexOf(':') >= 0) {
            throw invalid("username must not contain a colon");
        }
        String password = credential(input.get(PASSWORD), "password");
        authorization =
                "Basic "
                        + Base64.getEncoder()
                                .encodeToString(
                                        (username + ":" + password)
                                                .getBytes(StandardCharsets.US_ASCII));
        origin = origin(input.get(API_URL));
        accountPath = "/v2/accounts/" + encodeAccount(input.get(ACCOUNT_ID));
        Set<AppcuesResource> selected = new LinkedHashSet<>();
        for (String resource : input.get(RESOURCES)) {
            if (!selected.add(AppcuesResource.named(resource))) {
                throw invalid("resources must not contain duplicates");
            }
        }
        if (selected.isEmpty()) {
            throw invalid("resources must select at least one resource");
        }
        resources = Collections.unmodifiableList(new ArrayList<>(selected));
        start = parseDate(input.get(START_DATE), "start_date");
        configuredEnd =
                input.getOptional(END_DATE).map(value -> parseDate(value, "end_date")).orElse(null);
        if (configuredEnd != null && configuredEnd.isBefore(start)) {
            throw invalid("end_date must not be earlier than start_date");
        }
        maxRecords = number(input, MAX_RECORDS, 1, 100000);
        maxResponse = number(input, MAX_RESPONSE, 1024, 33554432);
        connectTimeout = number(input, CONNECT_TIMEOUT, 1, 300000);
        readTimeout = number(input, READ_TIMEOUT, 1, 300000);
        requestTimeout = number(input, REQUEST_TIMEOUT, 1, 300000);
        maxAttempts = number(input, MAX_ATTEMPTS, 1, 10);
        String query =
                origin + "|" + accountPath + "|" + resources + "|" + start + "|" + configuredEnd;
        fingerprint = digestId(digest().digest(query.getBytes(StandardCharsets.UTF_8)));
    }

    String target(AppcuesResource resource) {
        return accountPath + "/" + resource.key();
    }

    /**
     * Captures the effective end once when the initial scan is created. Restore gets this value
     * from the checkpoint even if the current time has advanced.
     */
    Instant initialEnd() {
        Instant end = configuredEnd == null ? Instant.now() : configuredEnd;
        if (end.isBefore(start)) {
            throw invalid("start_date is after the scan end");
        }
        return end;
    }

    private static String credential(String value, String key) {
        if (value == null
                || value.isEmpty()
                || value.length() > 4096
                || value.chars().anyMatch(ch -> ch < 33 || ch > 126)) {
            throw invalid(key + " must be a nonempty Appcues credential");
        }
        return value;
    }

    private static Instant parseDate(String value, String key) {
        try {
            return Instant.parse(value);
        } catch (DateTimeParseException | NullPointerException e) {
            throw invalid(key + " must be an ISO-8601 timestamp with a time zone");
        }
    }

    private static int number(ReadonlyConfig input, Option<Integer> option, int min, int max) {
        int n = input.get(option);
        if (n < min || n > max) {
            throw invalid(option.key() + " must be between " + min + " and " + max);
        }
        return n;
    }

    private static URI origin(String value) {
        try {
            URI uri = new URI(value);
            String host = uri.getHost();
            if (!"https".equalsIgnoreCase(uri.getScheme())
                    || (!US_HOST.equalsIgnoreCase(host) && !EU_HOST.equalsIgnoreCase(host))
                    || uri.getRawUserInfo() != null
                    || uri.getRawQuery() != null
                    || uri.getRawFragment() != null
                    || (!uri.getRawPath().isEmpty() && !"/".equals(uri.getRawPath()))
                    || uri.getPort() == 0
                    || uri.getPort() > 65535) {
                throw invalid("api_url must be an Appcues US or EU HTTPS origin");
            }
            return new URI(
                    "https",
                    null,
                    US_HOST.equalsIgnoreCase(host) ? US_HOST : EU_HOST,
                    uri.getPort(),
                    null,
                    null,
                    null);
        } catch (URISyntaxException | NullPointerException e) {
            throw invalid("api_url must be an Appcues US or EU HTTPS origin");
        }
    }

    private static String encodeAccount(String id) {
        if (id == null
                || id.isEmpty()
                || id.length() > 256
                || ".".equals(id)
                || "..".equals(id)
                || id.codePoints()
                        .anyMatch(
                                c ->
                                        c == '/'
                                                || c == '\\'
                                                || c == '%'
                                                || Character.isISOControl(c)
                                                || Character.isWhitespace(c)
                                                || (c >= 0xd800 && c <= 0xdfff))) {
            throw invalid("account_id must be a single account path segment");
        }
        StringBuilder segment = new StringBuilder();
        for (byte raw : id.getBytes(StandardCharsets.UTF_8)) {
            int c = raw & 255;
            if ((c >= 'a' && c <= 'z')
                    || (c >= 'A' && c <= 'Z')
                    || (c >= '0' && c <= '9')
                    || c == '~'
                    || c == '-'
                    || c == '_'
                    || c == '.') {
                segment.append((char) c);
            } else {
                segment.append('%').append(HEX.charAt(c >>> 4)).append(HEX.charAt(c & 15));
            }
        }
        return segment.toString();
    }

    static MessageDigest digest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    static String digestId(byte[] digest) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(digest);
    }

    static IllegalArgumentException invalid(String reason) {
        return new IllegalArgumentException("Appcues source: " + reason);
    }
}
