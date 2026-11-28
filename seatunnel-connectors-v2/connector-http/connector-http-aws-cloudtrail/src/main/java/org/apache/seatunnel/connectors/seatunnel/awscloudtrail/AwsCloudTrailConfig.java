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

package org.apache.seatunnel.connectors.seatunnel.awscloudtrail;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import java.io.Serializable;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Set;
import java.util.regex.Pattern;

/**
 * Validates CloudTrail lookup credentials, regional DNS endpoints, time bounds, and request limits.
 * A lookup identity binds the access key ID, region and parameters without including credential
 * secrets.
 */
final class AwsCloudTrailConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Set<String> REGIONS =
            Set.of(
                    "us-east-1",
                    "us-east-2",
                    "us-west-1",
                    "us-west-2",
                    "af-south-1",
                    "ap-east-1",
                    "ap-east-2",
                    "ap-south-1",
                    "ap-south-2",
                    "ap-southeast-1",
                    "ap-southeast-2",
                    "ap-southeast-3",
                    "ap-southeast-4",
                    "ap-southeast-5",
                    "ap-southeast-6",
                    "ap-southeast-7",
                    "ap-northeast-1",
                    "ap-northeast-2",
                    "ap-northeast-3",
                    "ca-central-1",
                    "ca-west-1",
                    "eu-central-1",
                    "eu-central-2",
                    "eu-west-1",
                    "eu-west-2",
                    "eu-west-3",
                    "eu-south-1",
                    "eu-south-2",
                    "eu-north-1",
                    "il-central-1",
                    "mx-central-1",
                    "me-south-1",
                    "me-central-1",
                    "sa-east-1",
                    "us-gov-east-1",
                    "us-gov-west-1");
    private static final Set<String> CHINA = Set.of("cn-north-1", "cn-northwest-1");
    private static final Set<String> FILTERS =
            Set.of(
                    "EventId",
                    "EventName",
                    "ReadOnly",
                    "Username",
                    "ResourceType",
                    "ResourceName",
                    "EventSource",
                    "AccessKeyId");
    private static final Pattern KEY = Pattern.compile("[A-Z0-9]{16,128}");
    private static final Pattern SECRET = Pattern.compile("[!-~]{1,4096}");
    private static final Pattern TOKEN = Pattern.compile("[!-~]{1,16384}");
    private static final long HISTORY_SECONDS = 90L * 86400L;
    static final String IDENTIFIER = "AwsCloudTrail";
    static final String RESOURCE = "management_events";

    static final Option<String> ACCESS_KEY =
            Options.key("access_key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("AWS access key ID.");
    static final Option<String> SECRET_KEY =
            Options.key("secret_key")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("AWS secret access key.");
    static final Option<String> SECURITY_TOKEN =
            setting("security_token", "", "AWS session token for temporary credentials.");
    static final Option<String> REGION =
            setting("region", "us-east-1", "CloudTrail control plane region.");
    static final Option<String> START_DATE =
            setting(
                    "start_date",
                    "",
                    "Inclusive UTC date or second-precision ISO offset timestamp.");
    static final Option<String> END_DATE =
            setting("end_date", "", "Inclusive UTC date or second-precision ISO offset timestamp.");
    static final Option<String> FILTER_KEY =
            setting("lookup_attribute_key", "", "Optional supported LookupEvents attribute key.");
    static final Option<String> FILTER_VALUE =
            setting("lookup_attribute_value", "", "Value for the one optional lookup filter.");
    static final Option<Integer> PAGE_SIZE =
            integer("page_size", 50, "Maximum events to request per page.");
    static final Option<Integer> MAX_PAGES =
            integer("max_pages", 2000, "Maximum completed pages for a lookup window.");
    static final Option<Integer> MAX_RESPONSE_BYTES =
            integer("max_response_bytes", 8388608, "Separate wire and decoded response limits.");
    static final Option<Integer> CONNECT_TIMEOUT =
            integer("connect_timeout_ms", 10000, "TCP connection timeout in milliseconds.");
    static final Option<Integer> READ_TIMEOUT =
            integer(
                    "read_timeout_ms",
                    15000,
                    "TLS handshake and inactivity timeout in milliseconds.");
    static final Option<Integer> REQUEST_TIMEOUT =
            integer(
                    "request_timeout_ms",
                    60000,
                    "Per-page HTTPS deadline including pacing and retries.");
    static final Option<Integer> MAX_ATTEMPTS =
            integer("max_attempts", 3, "Total attempts for retryable HTTP status responses.");

    final String access;
    final String secret;
    final String securityToken;
    final String region;
    final String host;
    final Long start;
    final Long end;
    final String attribute;
    final String value;
    final int pageSize;
    final int maxPages;
    final int maxResponse;
    final int connectTimeout;
    final int readTimeout;
    final int requestTimeout;
    final int maxAttempts;
    final String identity;

    AwsCloudTrailConfig(ReadonlyConfig input) {
        access = input.get(ACCESS_KEY);
        secret = input.get(SECRET_KEY);
        securityToken = input.get(SECURITY_TOKEN);
        if (access == null
                || !KEY.matcher(access).matches()
                || secret == null
                || !SECRET.matcher(secret).matches()
                || (!securityToken.isEmpty() && !TOKEN.matcher(securityToken).matches())) {
            throw invalid("invalid AWS access_key, secret_key or security_token");
        }
        region = input.get(REGION);
        host = hostname(region);
        start = bound(input.get(START_DATE), "start_date");
        end = bound(input.get(END_DATE), "end_date");
        if (start != null && end != null && start > end) {
            throw invalid("start_date must be at or before end_date");
        }
        attribute = input.get(FILTER_KEY);
        value = input.get(FILTER_VALUE);
        if (!(attribute.isEmpty() && value.isEmpty())
                && (!FILTERS.contains(attribute)
                        || value.isEmpty()
                        || value.length() > 4000
                        || value.codePoints()
                                        .map(
                                                c ->
                                                        c == '_' || c == ' ' || c == ','
                                                                        || c == '\n'
                                                                ? 2
                                                                : 1)
                                        .sum()
                                > 2000)) {
            throw invalid(
                    "lookup filter requires a supported key and a value within the AWS weighted length limit");
        }
        pageSize = number(input, PAGE_SIZE, 1, 50);
        maxPages = number(input, MAX_PAGES, 1, 20000);
        maxResponse = number(input, MAX_RESPONSE_BYTES, 1024, 33554432);
        connectTimeout = number(input, CONNECT_TIMEOUT, 1, 300000);
        readTimeout = number(input, READ_TIMEOUT, 1, 300000);
        requestTimeout = number(input, REQUEST_TIMEOUT, 1, 300000);
        maxAttempts = number(input, MAX_ATTEMPTS, 1, 10);
        identity =
                AwsCloudTrailJson.identity(
                        Arrays.asList(
                                access,
                                host,
                                start,
                                end,
                                attribute,
                                value,
                                pageSize,
                                maxPages,
                                maxResponse));
    }

    static String hostname(String region) {
        if (REGIONS.contains(region)) {
            return "cloudtrail." + region + ".amazonaws.com";
        }
        if (CHINA.contains(region)) {
            return "cloudtrail." + region + ".amazonaws.com.cn";
        }
        throw invalid("region is not a supported CloudTrail control plane region");
    }

    /**
     * Freezes an inclusive history interval at initial enumeration. Checkpoint restoration
     * continues using these exact values, including the implicit cutoff.
     */
    Window freeze(Instant started) {
        long now = started.getEpochSecond();
        long first = start == null ? started.truncatedTo(ChronoUnit.DAYS).getEpochSecond() : start;
        long last = end == null ? now : end;
        if (first > last || last > now) {
            throw invalid("lookup interval must be ordered and end at or before the current time");
        }
        if (first < now - HISTORY_SECONDS) {
            throw invalid("start_date is outside the 90-day CloudTrail history");
        }
        return new Window(first, last);
    }

    static boolean isRetained(Window window, Instant now) {
        return window.start >= now.getEpochSecond() - HISTORY_SECONDS;
    }

    private static Long bound(String value, String option) {
        if (value.isEmpty()) {
            return null;
        }
        try {
            if (value.length() == 10) {
                return LocalDate.parse(value).atStartOfDay().toEpochSecond(ZoneOffset.UTC);
            }
            OffsetDateTime timestamp = OffsetDateTime.parse(value);
            if (timestamp.getNano() == 0) {
                return timestamp.toEpochSecond();
            }
        } catch (DateTimeException ignored) {
            // Report the option name alone, since invalid configuration input can contain secrets.
        }
        throw invalid(
                option + " must be a UTC date or an ISO offset timestamp at second precision");
    }

    private static Option<String> setting(String key, String defaultValue, String description) {
        return Options.key(key)
                .stringType()
                .defaultValue(defaultValue)
                .withDescription(description);
    }

    private static Option<Integer> integer(String key, int defaultValue, String description) {
        return Options.key(key).intType().defaultValue(defaultValue).withDescription(description);
    }

    private static int number(ReadonlyConfig input, Option<Integer> option, int min, int max) {
        int value = input.get(option);
        if (value < min || value > max) {
            throw invalid(option.key() + " must be between " + min + " and " + max);
        }
        return value;
    }

    static IllegalArgumentException invalid(String text) {
        return new IllegalArgumentException("AWS CloudTrail source: " + text);
    }

    /**
     * Stores consecutive UTC epoch-second bounds sent unchanged with all continuation tokens. Both
     * limits are inclusive according to the LookupEvents contract.
     */
    static final class Window implements Serializable {
        private static final long serialVersionUID = 1L;
        final long start;
        final long end;

        Window(long start, long end) {
            this.start = start;
            this.end = end;
        }
    }
}
