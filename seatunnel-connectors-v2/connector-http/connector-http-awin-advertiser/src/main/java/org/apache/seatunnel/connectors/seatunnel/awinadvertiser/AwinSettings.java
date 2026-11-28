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
package org.apache.seatunnel.connectors.seatunnel.awinadvertiser;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.OptionRule;

import java.io.Serializable;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.DateTimeException;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Immutable scan settings for a selected advertiser and set of report partitions.
 *
 * <p>The scope digest guards restoration against key, region, date, partition and account changes.
 */
final class AwinSettings implements Serializable {
    private static final long serialVersionUID = 1L;
    static final String LIVE_ORIGIN = "https://api.awin.com";
    private static final Set<String> REGION_CODES =
            Set.of(
                    "AT", "AU", "BE", "BR", "BU", "CA", "CH", "DE", "DK", "ES", "FI", "FR", "GB",
                    "IE", "IT", "NL", "NO", "PL", "SE", "US");

    static final Option<String> TOKEN =
            Options.key("token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Awin user-scoped personal or classic API token.");
    static final Option<List<String>> RESOURCES =
            Options.key("resources")
                    .listType()
                    .noDefaultValue()
                    .withDescription("Ordered advertiser resources to read.");
    static final Option<Long> ADVERTISER_ID =
            Options.key("advertiser_id")
                    .longType()
                    .noDefaultValue()
                    .withDescription(
                            "Advertiser program ID, required for advertiser-scoped resources.");
    static final Option<String> START =
            Options.key("start_date")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Inclusive first UTC date for windowed reports and transactions.");
    static final Option<String> END =
            Options.key("end_date")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Inclusive final UTC date. Defaults to the current UTC date.");
    static final Option<List<String>> REGIONS =
            Options.key("regions")
                    .listType()
                    .defaultValue(List.of())
                    .withDescription("Distinct Awin network regions for the creative report.");
    static final Option<Map<String, String>> RESUME_FROM =
            Options.key("resume_from")
                    .mapType()
                    .defaultValue(Map.of())
                    .withDescription("Previous UTC date per dated resource or creative region.");
    static final Option<Integer> LOOKBACK =
            Options.key("lookback_days")
                    .intType()
                    .defaultValue(0)
                    .withDescription("Days to reread before each explicit resume_from date.");
    static final Option<Integer> CAMPAIGN_DAYS =
            Options.key("campaign_window_days")
                    .intType()
                    .defaultValue(400)
                    .withDescription("Maximum inclusive dates per campaign report request.");
    static final Option<Integer> TRANSACTION_DAYS =
            Options.key("transaction_window_days")
                    .intType()
                    .defaultValue(31)
                    .withDescription("Maximum inclusive UTC dates per transaction request.");
    static final Option<Integer> REQUESTS_PER_MINUTE =
            Options.key("requests_per_minute")
                    .intType()
                    .defaultValue(20)
                    .withDescription("Per-reader legacy API attempts within a rolling minute.");
    static final Option<Integer> MAX_WINDOWS =
            Options.key("max_requests")
                    .intType()
                    .defaultValue(10000)
                    .withDescription("Maximum logical report or catalog requests per scan.");
    static final Option<Integer> MAX_ATTEMPTS =
            Options.key("max_attempts")
                    .intType()
                    .defaultValue(4)
                    .withDescription("Total attempts per retryable provider request.");
    static final Option<Integer> TIMEOUT =
            Options.key("request_timeout_ms")
                    .intType()
                    .defaultValue(90000)
                    .withDescription(
                            "Deadline per logical read covering pacing, retries and the response.");
    static final Option<Integer> MAX_BYTES =
            Options.key("max_response_bytes")
                    .intType()
                    .defaultValue(8388608)
                    .withDescription("Maximum decoded bytes for one successful response.");
    static final Option<String> ENDPOINT =
            Options.key("endpoint")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Optional custom HTTPS origin paired with a PEM trust anchor.");
    static final Option<String> TRUST_CERTIFICATE =
            Options.key("trust_certificate")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Worker-local PEM trust anchor for the configured HTTPS origin.");

    final String token;
    final Long advertiser;
    final String origin;
    final String trustCertificate;
    final LocalDate start;
    final LocalDate end;
    final int campaignDays;
    final int transactionDays;
    final int requestsPerMinute;
    final int maxRequests;
    final int attempts;
    final int timeout;
    final int maxBytes;
    final List<AwinSlice.Partition> partitions;
    // The checkpoint retains only this digest, never the configured authentication secret.
    final String scope;

    AwinSettings(ReadonlyConfig config) {
        token = config.get(TOKEN);
        if (token == null
                || token.isEmpty()
                || token.length() > 4096
                || !token.chars().allMatch(ch -> ch >= 33 && ch <= 126)) {
            throw invalid("token", "expected a non-empty Awin Bearer token");
        }
        List<String> names = config.get(RESOURCES);
        if (names == null
                || names.isEmpty()
                || names.size() > AwinResource.values().length
                || new HashSet<>(names).size() != names.size()) {
            throw invalid("resources", "select each required resource once");
        }
        List<AwinResource> resources =
                names.stream().map(AwinResource::select).collect(Collectors.toUnmodifiableList());
        advertiser = config.get(ADVERTISER_ID);
        if ((advertiser == null && resources.stream().anyMatch(r -> r != AwinResource.ACCOUNTS))
                || (advertiser != null && advertiser <= 0)) {
            throw invalid(
                    "advertiser_id", "advertiser-scoped resources require a positive program ID");
        }
        start = date(config.get(START), START.key());
        end = date(config.get(END), END.key());
        LocalDate today = LocalDate.now(ZoneOffset.UTC);
        LocalDate last = end == null ? today : end;
        if ((resources.stream().anyMatch(r -> r.dated) && start == null)
                || (start != null && start.isAfter(last))
                || last.isAfter(today)) {
            throw invalid("start_date/end_date", "use an ordered UTC date range through today");
        }
        List<String> regions = config.get(REGIONS);
        if (regions.size() > REGION_CODES.size()
                || new HashSet<>(regions).size() != regions.size()
                || regions.stream()
                        .anyMatch(region -> region == null || !REGION_CODES.contains(region))
                || (resources.contains(AwinResource.CREATIVE) && regions.isEmpty())) {
            throw invalid("regions", "creative reporting requires distinct supported Awin regions");
        }
        int lookback = range(config.get(LOOKBACK), LOOKBACK, 0, 4000);
        partitions = plan(resources, regions, config.get(RESUME_FROM), last, lookback);
        campaignDays = range(config.get(CAMPAIGN_DAYS), CAMPAIGN_DAYS, 1, 400);
        transactionDays = range(config.get(TRANSACTION_DAYS), TRANSACTION_DAYS, 1, 31);
        requestsPerMinute = range(config.get(REQUESTS_PER_MINUTE), REQUESTS_PER_MINUTE, 1, 20);
        maxRequests = range(config.get(MAX_WINDOWS), MAX_WINDOWS, 1, 100000);
        attempts = range(config.get(MAX_ATTEMPTS), MAX_ATTEMPTS, 1, 10);
        timeout = range(config.get(TIMEOUT), TIMEOUT, 100, 300000);
        maxBytes = range(config.get(MAX_BYTES), MAX_BYTES, 1024, 64 * 1024 * 1024);
        trustCertificate = config.get(TRUST_CERTIFICATE);
        String override = config.get(ENDPOINT);
        if ((override == null) != (trustCertificate == null)
                || (trustCertificate != null && trustCertificate.isBlank())) {
            throw invalid("endpoint", "custom HTTPS requires a worker trust certificate");
        }
        origin = override == null ? LIVE_ORIGIN : origin(override);
        scope =
                hash(
                        String.join(
                                "\n",
                                hash(token),
                                Objects.toString(advertiser, ""),
                                origin,
                                Objects.toString(trustCertificate, ""),
                                names.toString(),
                                regions.toString(),
                                Objects.toString(start, ""),
                                Objects.toString(end, ""),
                                new TreeMap<>(config.get(RESUME_FROM)).toString(),
                                Integer.toString(lookback),
                                Integer.toString(campaignDays),
                                Integer.toString(transactionDays)));
    }

    static OptionRule rules() {
        return OptionRule.builder()
                .required(TOKEN, RESOURCES)
                .optional(
                        ADVERTISER_ID,
                        START,
                        END,
                        REGIONS,
                        RESUME_FROM,
                        LOOKBACK,
                        CAMPAIGN_DAYS,
                        TRANSACTION_DAYS,
                        REQUESTS_PER_MINUTE,
                        MAX_WINDOWS,
                        MAX_ATTEMPTS,
                        TIMEOUT,
                        MAX_BYTES,
                        ENDPOINT,
                        TRUST_CERTIFICATE)
                .build();
    }

    /**
     * Computes each resource or regional resume position independently.
     *
     * <p>Lookback applies when an explicit seed is provided. A region without a seed reads from the
     * start date and a worker recovery later uses its exact existing split.
     */
    private List<AwinSlice.Partition> plan(
            List<AwinResource> resources,
            List<String> regions,
            Map<String, String> resume,
            LocalDate last,
            int lookback) {
        List<AwinSlice.Partition> out = new ArrayList<>();
        Set<String> allowed = new HashSet<>();
        for (AwinResource resource : resources) {
            if (resource == AwinResource.CREATIVE) {
                for (String region : regions) {
                    String key = resource.key + ":" + region;
                    allowed.add(key);
                    out.add(
                            new AwinSlice.Partition(
                                    resource, region, firstDate(resume.get(key), last, lookback)));
                }
            } else {
                if (resource.dated) {
                    allowed.add(resource.key);
                }
                out.add(
                        new AwinSlice.Partition(
                                resource,
                                null,
                                resource.dated
                                        ? firstDate(resume.get(resource.key), last, lookback)
                                        : null));
            }
        }
        if (!allowed.containsAll(resume.keySet())) {
            throw invalid("resume_from", "contains a key outside the selected dated partitions");
        }
        return List.copyOf(out);
    }

    private LocalDate firstDate(String seed, LocalDate last, int lookback) {
        LocalDate anchor = date(seed, RESUME_FROM.key());
        if (anchor == null) {
            return start;
        }
        if (anchor.isAfter(last) || anchor.isBefore(start)) {
            throw invalid("resume_from", "use dates inside the configured scan window");
        }
        LocalDate subset = anchor.minusDays(lookback);
        return subset.isBefore(start) ? start : subset;
    }

    private static LocalDate date(String value, String option) {
        if (value == null) {
            return null;
        }
        try {
            if (value.matches("[0-9]{4}-[0-9]{2}-[0-9]{2}")) {
                return LocalDate.parse(value);
            }
        } catch (DateTimeException invalid) {
            // The supplied option value is unnecessary in the error.
        }
        throw invalid(option, "expected a valid date in YYYY-MM-DD form");
    }

    private static int range(int value, Option<Integer> option, int min, int max) {
        if (value < min || value > max) {
            throw invalid(option.key(), "expected a value between " + min + " and " + max);
        }
        return value;
    }

    private static String origin(String address) {
        try {
            URI uri = URI.create(address);
            if ("https".equals(uri.getScheme())
                    && uri.getHost() != null
                    && uri.getRawUserInfo() == null
                    && uri.getRawQuery() == null
                    && uri.getRawFragment() == null
                    && (uri.getPath().isEmpty() || "/".equals(uri.getPath()))
                    && (uri.getPort() == -1 || (uri.getPort() > 0 && uri.getPort() < 65536))) {
                return "https://"
                        + uri.getHost().toLowerCase(Locale.ROOT)
                        + (uri.getPort() == -1 ? "" : ":" + uri.getPort());
            }
        } catch (IllegalArgumentException invalid) {
            // Never put the supplied origin, which may include credentials, in diagnostics.
        }
        throw invalid("endpoint", "expected an HTTPS origin without query or embedded credentials");
    }

    static String hash(String content) {
        try {
            return Base64.getEncoder()
                    .encodeToString(
                            MessageDigest.getInstance("SHA-256")
                                    .digest(content.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException missing) {
            throw new IllegalStateException("SHA-256 is unavailable");
        }
    }

    static IllegalArgumentException invalid(String key, String message) {
        return new IllegalArgumentException("AwinAdvertiser option " + key + ": " + message);
    }
}
