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
package org.apache.seatunnel.connectors.seatunnel.breezyhr;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;

import java.io.Serializable;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.EnumSet;
import java.util.List;
import java.util.Objects;

/**
 * Validates the selected Breezy HR company, credentials, paging and transport settings.
 *
 * <p>Only a scope fingerprint is needed in checkpoint state.
 */
final class BreezyHrConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    static final Option<String> TOKEN =
            Options.key("token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Breezy HR personal access token or existing session token.");
    static final Option<String> COMPANY_ID =
            Options.key("company_id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The company _id returned by the Breezy HR companies API.");
    static final Option<List<String>> RESOURCES =
            Options.key("resources")
                    .listType()
                    .noDefaultValue()
                    .withDescription("Selected resources: positions, candidates and pipelines.");
    static final Option<Integer> PAGE_SIZE =
            Options.key("page_size")
                    .intType()
                    .defaultValue(50)
                    .withDescription("Positions and candidates page size from 1 to 50.");
    static final Option<Integer> MAX_PAGES =
            Options.key("max_pages_per_scope")
                    .intType()
                    .defaultValue(1000)
                    .withDescription(
                            "Maximum pages including empty termination per company positions or per position candidates.");
    static final Option<Integer> MAX_REQUESTS =
            Options.key("max_requests")
                    .intType()
                    .defaultValue(100000)
                    .withDescription("Maximum successfully processed API responses in this scan.");
    static final Option<Integer> MAX_ATTEMPTS =
            Options.key("max_attempts")
                    .intType()
                    .defaultValue(4)
                    .withDescription(
                            "Maximum attempts per page including the initial HTTPS request.");
    static final Option<Integer> TIMEOUT =
            Options.key("request_timeout_ms")
                    .intType()
                    .defaultValue(30000)
                    .withDescription(
                            "Total page acquisition budget in milliseconds, including retries.");
    static final Option<Integer> MAX_BYTES =
            Options.key("max_response_bytes")
                    .intType()
                    .defaultValue(8388608)
                    .withDescription("Maximum decoded bytes in one successful response.");
    static final Option<String> ENDPOINT =
            Options.key("endpoint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional custom HTTPS origin requiring a worker-local trust certificate.");
    static final Option<String> TRUST_CERTIFICATE =
            Options.key("trust_certificate")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Worker-local PEM certificate for the configured custom origin.");

    final String token;
    final String companyId;
    final List<BreezyHrResource> resources;
    final int pageSize;
    final int maxPages;
    final int maxRequests;
    final int attempts;
    final int timeout;
    final int maxBytes;
    final String origin;
    final String trustCertificate;
    final String scope;

    BreezyHrConfig(ReadonlyConfig options) {
        token = options.get(TOKEN);
        if (token == null
                || token.isEmpty()
                || token.length() > 4096
                || token.chars().anyMatch(value -> value < 33 || value > 126)) {
            throw invalid(
                    "token", "expected a token without a prefix, spaces or control characters");
        }
        companyId = options.get(COMPANY_ID);
        if (companyId == null || !companyId.matches("[A-Za-z0-9_-]{1,128}")) {
            throw invalid("company_id", "expected the company's API identifier");
        }
        List<String> selected = options.get(RESOURCES);
        if (selected == null
                || selected.isEmpty()
                || selected.size() > BreezyHrResource.values().length) {
            throw invalid("resources", "choose at least one supported resource");
        }
        EnumSet<BreezyHrResource> parsed = EnumSet.noneOf(BreezyHrResource.class);
        for (String name : selected) {
            if (!parsed.add(BreezyHrResource.from(name))) {
                throw invalid("resources", "choose each resource only once");
            }
        }
        resources = List.copyOf(parsed);
        pageSize = range(options, PAGE_SIZE, 1, 50);
        maxPages = range(options, MAX_PAGES, 1, 10000);
        maxRequests = range(options, MAX_REQUESTS, 1, 1000000);
        attempts = range(options, MAX_ATTEMPTS, 1, 10);
        timeout = range(options, TIMEOUT, 100, 300000);
        maxBytes = range(options, MAX_BYTES, 1024, 64 * 1024 * 1024);
        String endpoint = options.get(ENDPOINT);
        trustCertificate = options.get(TRUST_CERTIFICATE);
        if ((endpoint == null) != (trustCertificate == null)
                || (trustCertificate != null && trustCertificate.isBlank())) {
            throw invalid("endpoint", "custom HTTPS requires trust_certificate");
        }
        origin = endpoint == null ? "https://api.breezy.hr" : httpsOrigin(endpoint);
        scope =
                digest(
                        String.join(
                                "\n",
                                digest(token),
                                companyId,
                                resources.toString(),
                                origin,
                                Objects.toString(trustCertificate, ""),
                                String.valueOf(pageSize)));
    }

    static OptionRule rules() {
        return OptionRule.builder()
                .required(TOKEN, COMPANY_ID, RESOURCES)
                .optional(
                        PAGE_SIZE,
                        MAX_PAGES,
                        MAX_REQUESTS,
                        MAX_ATTEMPTS,
                        TIMEOUT,
                        MAX_BYTES,
                        ENDPOINT,
                        TRUST_CERTIFICATE)
                .build();
    }

    private static String httpsOrigin(String value) {
        try {
            URI uri = URI.create(value);
            if ("https".equalsIgnoreCase(uri.getScheme())
                    && uri.getHost() != null
                    && uri.getRawUserInfo() == null
                    && uri.getRawQuery() == null
                    && uri.getRawFragment() == null
                    && (uri.getPort() == -1 || (uri.getPort() > 0 && uri.getPort() <= 65535))
                    && ("".equals(uri.getRawPath()) || "/".equals(uri.getRawPath()))) {
                return "https://" + uri.getRawAuthority();
            }
        } catch (IllegalArgumentException ignored) {
            // The invalid option value may contain secrets and must not enter diagnostics.
        }
        throw invalid("endpoint", "expected a bare HTTPS origin");
    }

    private static int range(ReadonlyConfig settings, Option<Integer> option, int min, int max) {
        int value = settings.get(option);
        if (value < min || value > max) {
            throw invalid(option.key(), "expected a value from " + min + " to " + max);
        }
        return value;
    }

    static String digest(String text) {
        return Base64.getUrlEncoder()
                .withoutPadding()
                .encodeToString(sha256().digest(text.getBytes(StandardCharsets.UTF_8)));
    }

    static MessageDigest sha256() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException unavailable) {
            throw new IllegalStateException("SHA-256 is unavailable");
        }
    }

    static IllegalArgumentException invalid(String key, String message) {
        return new IllegalArgumentException("Breezy HR option " + key + ": " + message);
    }
}
