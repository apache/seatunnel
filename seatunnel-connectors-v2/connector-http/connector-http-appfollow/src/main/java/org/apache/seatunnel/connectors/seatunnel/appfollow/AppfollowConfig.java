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

package org.apache.seatunnel.connectors.seatunnel.appfollow;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.appfollow.AppfollowData.Resource;

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/**
 * Validates one AppFollow API configuration and budgets before network work starts. The query
 * fingerprint binds restored discovery to the selected resource graph without the token.
 */
final class AppfollowConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final String HOST = "api.appfollow.io";
    static final Option<String> TOKEN =
            Options.key("token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("AppFollow API v2 token.");
    static final Option<List<String>> RESOURCES =
            Options.key("resources")
                    .listType()
                    .defaultValue(Resource.names())
                    .withDescription("AppFollow resources to emit.");
    static final Option<String> API_URL =
            Options.key("api_url")
                    .stringType()
                    .defaultValue("https://" + HOST)
                    .withDescription("HTTPS origin of the AppFollow API.");
    static final Option<Integer> MAX_ENTITIES =
            Options.key("max_entities")
                    .intType()
                    .defaultValue(10000)
                    .withDescription(
                            "Maximum discovered collections or application attachments per scan.");
    static final Option<Integer> MAX_REQUESTS =
            Options.key("max_requests")
                    .intType()
                    .defaultValue(1000)
                    .withDescription(
                            "Maximum request attempts per reader execution, including related resources and retries.");
    static final Option<Integer> MAX_RECORDS =
            Options.key("max_records")
                    .intType()
                    .defaultValue(10000)
                    .withDescription("Maximum records in one response.");
    static final Option<Integer> MAX_RESPONSE =
            Options.key("max_response_bytes")
                    .intType()
                    .defaultValue(16777216)
                    .withDescription(
                            "Independent encoded and decoded byte limits for one response.");
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
                    .withDescription("Total request deadline for one response including retries.");
    static final Option<Integer> MAX_ATTEMPTS =
            Options.key("max_attempts")
                    .intType()
                    .defaultValue(4)
                    .withDescription("Maximum attempts for one request including the first.");

    final URI origin;
    final String token;
    final Set<Resource> outputs;
    final int maxEntities;
    final int maxRequests;
    final int maxRecords;
    final int maxResponse;
    final int connectTimeout;
    final int readTimeout;
    final int requestTimeout;
    final int maxAttempts;
    final String fingerprint;

    AppfollowConfig(ReadonlyConfig config) {
        origin = origin(config.get(API_URL));
        token = config.get(TOKEN);
        if (token == null
                || token.isEmpty()
                || token.length() > 4096
                || token.chars().anyMatch(c -> c <= 32 || c >= 127)) {
            throw invalid("token must be a nonempty AppFollow v2 token");
        }
        Set<Resource> resources = EnumSet.noneOf(Resource.class);
        for (String resource : config.get(RESOURCES)) {
            if (!resources.add(Resource.named(resource))) {
                throw invalid("resources must not contain duplicates");
            }
        }
        if (resources.isEmpty()) {
            throw invalid("resources must select at least one resource");
        }
        outputs = Collections.unmodifiableSet(resources);
        maxEntities = number(config, MAX_ENTITIES, 1, 50000);
        maxRequests = number(config, MAX_REQUESTS, 1, 100000);
        maxRecords = number(config, MAX_RECORDS, 1, 100000);
        maxResponse = number(config, MAX_RESPONSE, 1024, 33554432);
        connectTimeout = number(config, CONNECT_TIMEOUT, 1, 300000);
        readTimeout = number(config, READ_TIMEOUT, 1, 300000);
        requestTimeout = number(config, REQUEST_TIMEOUT, 1, 300000);
        maxAttempts = number(config, MAX_ATTEMPTS, 1, 10);
        fingerprint =
                digestId(
                        digest().digest((origin + "|" + outputs).getBytes(StandardCharsets.UTF_8)));
    }

    boolean reads(Resource resource) {
        if (resource == Resource.APP_COLLECTIONS) {
            return outputs.contains(resource) || reads(Resource.APP_LISTS);
        }
        if (resource == Resource.APP_LISTS) {
            return outputs.contains(resource) || readsMetrics();
        }
        return outputs.contains(resource);
    }

    boolean readsMetrics() {
        return outputs.contains(Resource.STAT_REVIEWS) || outputs.contains(Resource.RATINGS);
    }

    private static int number(ReadonlyConfig config, Option<Integer> option, int min, int max) {
        int value = config.get(option);
        if (value < min || value > max) {
            throw invalid(option.key() + " must be between " + min + " and " + max);
        }
        return value;
    }

    private static URI origin(String value) {
        try {
            URI uri = new URI(value);
            if (!"https".equalsIgnoreCase(uri.getScheme())
                    || !HOST.equalsIgnoreCase(uri.getHost())
                    || uri.getRawUserInfo() != null
                    || uri.getRawQuery() != null
                    || uri.getRawFragment() != null
                    || (!uri.getRawPath().isEmpty() && !"/".equals(uri.getRawPath()))
                    || uri.getPort() == 0
                    || uri.getPort() > 65535) {
                throw invalid("api_url must be the HTTPS origin of api.appfollow.io");
            }
            return new URI("https", null, HOST, uri.getPort(), null, null, null);
        } catch (URISyntaxException | NullPointerException e) {
            throw invalid("api_url must be the HTTPS origin of api.appfollow.io");
        }
    }

    static MessageDigest digest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new IllegalStateException("SHA-256 unavailable", e);
        }
    }

    static String digestId(byte[] value) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(value);
    }

    static IllegalArgumentException invalid(String reason) {
        return new IllegalArgumentException("AppFollow source: " + reason);
    }
}
