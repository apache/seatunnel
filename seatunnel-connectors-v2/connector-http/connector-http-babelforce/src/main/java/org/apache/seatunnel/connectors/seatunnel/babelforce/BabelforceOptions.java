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
package org.apache.seatunnel.connectors.seatunnel.babelforce;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;

import java.io.Serializable;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Instant;
import java.util.Base64;
import java.util.Locale;
import java.util.Objects;
import java.util.Set;

/**
 * Babelforce's regional report, security and fixed creation-window settings.
 *
 * <p>Only a fingerprint of the configured authentication and scope enters snapshot state.
 */
final class BabelforceOptions implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final long LATEST_SECOND = 253402300799L;
    private static final Set<String> REGIONS = Set.of("services", "us-east", "ap-southeast");

    static final Option<String> REGION =
            Options.key("region")
                    .stringType()
                    .defaultValue("services")
                    .withDescription("Regional Babelforce service host.");
    static final Option<String> AUTH_MODE =
            Options.key("auth_mode")
                    .stringType()
                    .defaultValue("access_key")
                    .withDescription(
                            "Choose the dual access key headers or an existing bearer token.");
    static final Option<String> ACCESS_KEY =
            Options.key("access_key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Access ID for dual access key authentication.");
    static final Option<String> TOKEN =
            Options.key("token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("The access token or existing OAuth bearer token.");
    static final Option<Long> FROM =
            Options.key("date_created_from")
                    .longType()
                    .defaultValue(0L)
                    .withDescription("Inclusive creation window start as a Unix second.");
    static final Option<Long> TO =
            Options.key("date_created_to")
                    .longType()
                    .noDefaultValue()
                    .withDescription("Inclusive creation window end as a Unix second.");
    static final Option<String> WINDOW_PARAMETERS =
            Options.key("window_parameters")
                    .stringType()
                    .defaultValue("date_created")
                    .withDescription(
                            "Use Manager OpenAPI date_created arguments or the Go SDK time arguments.");
    static final Option<Integer> PAGE_SIZE =
            Options.key("page_size")
                    .intType()
                    .defaultValue(102)
                    .withDescription("Maximum call records to request per page.");
    static final Option<Integer> MAX_PAGES =
            Options.key("max_pages")
                    .intType()
                    .defaultValue(10000)
                    .withDescription("Maximum pages in one fixed-window scan.");
    static final Option<Integer> MAX_ATTEMPTS =
            Options.key("max_attempts")
                    .intType()
                    .defaultValue(4)
                    .withDescription("Maximum attempts per retryable page.");
    static final Option<Integer> TIMEOUT =
            Options.key("request_timeout_ms")
                    .intType()
                    .defaultValue(30000)
                    .withDescription(
                            "Page deadline across requests, retry delays and response decoding.");
    static final Option<Integer> MAX_BYTES =
            Options.key("max_response_bytes")
                    .intType()
                    .defaultValue(8388608)
                    .withDescription("Maximum decoded bytes in one successful response.");
    static final Option<String> ENDPOINT =
            Options.key("endpoint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Custom HTTPS origin paired with worker-local PEM trust.");
    static final Option<String> CA =
            Options.key("trust_certificate")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Worker-local PEM trust anchor for a custom origin.");

    final String region;
    final boolean bearer;
    final String accessKey;
    final String token;
    final long from;
    final Long to;
    final String timePrefix;
    final int pageSize;
    final int maxPages;
    final int attempts;
    final int timeout;
    final int maxBytes;
    final String origin;
    final String certificate;
    // Only this digest identifies the configured credentials and source scope in the checkpoint.
    final String fingerprint;

    BabelforceOptions(ReadonlyConfig values) {
        region = values.get(REGION);
        if (region == null || !REGIONS.contains(region)) {
            throw invalid("region", "expected services, us-east or ap-southeast");
        }
        String mode = values.get(AUTH_MODE);
        if (!"access_key".equals(mode) && !"bearer".equals(mode)) {
            throw invalid("auth_mode", "expected access_key or bearer");
        }
        bearer = "bearer".equals(mode);
        accessKey = values.get(ACCESS_KEY);
        token = values.get(TOKEN);
        if (!header(token)) {
            throw invalid("token", "expected a nonempty provider credential");
        }
        if ((bearer && accessKey != null) || (!bearer && !header(accessKey))) {
            throw invalid("access_key", "supply an access ID only in access_key mode");
        }
        String style = values.get(WINDOW_PARAMETERS);
        if (!"date_created".equals(style) && !"time".equals(style)) {
            throw invalid("window_parameters", "expected date_created or time");
        }
        timePrefix = "date_created".equals(style) ? "dateCreated" : "time";
        from = values.get(FROM);
        to = values.get(TO);
        long last = to == null ? Instant.now().getEpochSecond() : to;
        if (from < 0 || last > LATEST_SECOND || from > last) {
            throw invalid(
                    "date_created_from/date_created_to",
                    "expected ordered Unix seconds between 1970 and 9999");
        }
        pageSize = number(values.get(PAGE_SIZE), PAGE_SIZE, 1, 102);
        maxPages = number(values.get(MAX_PAGES), MAX_PAGES, 1, 20000);
        attempts = number(values.get(MAX_ATTEMPTS), MAX_ATTEMPTS, 1, 10);
        timeout = number(values.get(TIMEOUT), TIMEOUT, 100, 300000);
        maxBytes = number(values.get(MAX_BYTES), MAX_BYTES, 1024, 64 * 1024 * 1024);
        certificate = values.get(CA);
        String override = values.get(ENDPOINT);
        if ((override == null) != (certificate == null)
                || (certificate != null && certificate.isBlank())) {
            throw invalid("endpoint", "custom HTTPS requires a worker trust certificate");
        }
        origin = override == null ? "https://" + region + ".babelforce.com" : https(override);
        fingerprint =
                hash(
                        String.join(
                                "\n",
                                region,
                                mode,
                                hash(Objects.toString(accessKey, "")),
                                hash(token),
                                Long.toString(from),
                                Objects.toString(to, ""),
                                timePrefix,
                                Integer.toString(pageSize),
                                origin,
                                Objects.toString(certificate, "")));
    }

    static OptionRule rules() {
        return OptionRule.builder()
                .required(TOKEN)
                .optional(
                        REGION,
                        AUTH_MODE,
                        ACCESS_KEY,
                        FROM,
                        TO,
                        WINDOW_PARAMETERS,
                        PAGE_SIZE,
                        MAX_PAGES,
                        MAX_ATTEMPTS,
                        TIMEOUT,
                        MAX_BYTES,
                        ENDPOINT,
                        CA)
                .build();
    }

    /**
     * Captures the current Unix second only while initializing an unassigned scan.
     *
     * <p>Later readers reuse the saved value so the provider range does not widen after recovery.
     */
    long initialEnd() {
        return to == null ? Instant.now().getEpochSecond() : to;
    }

    private static int number(int value, Option<Integer> option, int min, int max) {
        if (value < min || value > max) {
            throw invalid(option.key(), "expected a number between " + min + " and " + max);
        }
        return value;
    }

    private static boolean header(String value) {
        return value != null
                && !value.isEmpty()
                && value.length() <= 4096
                && value.chars().allMatch(ch -> ch >= 33 && ch <= 126);
    }

    private static String https(String value) {
        try {
            URI uri = URI.create(value);
            if ("https".equals(uri.getScheme())
                    && uri.getHost() != null
                    && uri.getRawUserInfo() == null
                    && uri.getRawQuery() == null
                    && uri.getRawFragment() == null
                    && (uri.getRawPath().isEmpty() || "/".equals(uri.getRawPath()))
                    && (uri.getPort() == -1 || (uri.getPort() > 0 && uri.getPort() < 65536))) {
                return "https://"
                        + uri.getHost().toLowerCase(Locale.ROOT)
                        + (uri.getPort() == -1 ? "" : ":" + uri.getPort());
            }
        } catch (IllegalArgumentException invalid) {
            // The origin could mistakenly contain a credential, so never log its value.
        }
        throw invalid("endpoint", "expected an HTTPS origin without query or credentials");
    }

    static String hash(String value) {
        try {
            return Base64.getEncoder()
                    .encodeToString(
                            MessageDigest.getInstance("SHA-256")
                                    .digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException absent) {
            throw new IllegalStateException("SHA-256 is unavailable");
        }
    }

    static IllegalArgumentException invalid(String option, String description) {
        return new IllegalArgumentException("Babelforce option " + option + ": " + description);
    }
}
