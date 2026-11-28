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

package org.apache.seatunnel.connectors.seatunnel.apify;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.Options;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Validates the provider origin and query selection before a reader can send credentials. The
 * checkpoint fingerprint binds pagination options to the resource order.
 */
final class ApifyDatasetConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final String HTTPS = "https";
    private static final String HOST = "api.apify.com";
    private static final String HEX = "0123456789ABCDEF";

    static final Option<String> TOKEN =
            Options.key("token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Apify API bearer token.");
    static final Option<String> DATASET_ID =
            Options.key("dataset_id")
                    .stringType()
                    .noDefaultValue()
                    .withDescription(
                            "Dataset ID or named dataset reference, required for metadata and items.");
    static final Option<List<String>> RESOURCES =
            Options.key("resources")
                    .listType()
                    .defaultValue(Resource.names())
                    .withDescription("Ordered selection of dataset and item resources.");
    static final Option<String> API_URL =
            Options.key("api_url")
                    .stringType()
                    .defaultValue("https://api.apify.com")
                    .withDescription("HTTPS origin of the Apify API, with an optional port.");
    static final Option<Integer> PAGE_SIZE =
            Options.key("page_size")
                    .intType()
                    .defaultValue(50)
                    .withDescription(
                            "Maximum dataset entries or underlying dataset rows requested per page.");
    static final Option<Integer> MAX_PAGES =
            Options.key("max_pages")
                    .intType()
                    .defaultValue(10000)
                    .withDescription("Maximum completed pages per resource before failing.");
    static final Option<Integer> MAX_RESPONSE =
            Options.key("max_response_bytes")
                    .intType()
                    .defaultValue(16777216)
                    .withDescription(
                            "Independent size limits for encoded and decoded page responses.");
    static final Option<Integer> CONNECT_TIMEOUT =
            Options.key("connect_timeout_ms")
                    .intType()
                    .defaultValue(10000)
                    .withDescription("Maximum TCP connection time in milliseconds.");
    static final Option<Integer> READ_TIMEOUT =
            Options.key("read_timeout_ms")
                    .intType()
                    .defaultValue(10000)
                    .withDescription("TLS and response inactivity timeout in milliseconds.");
    static final Option<Integer> REQUEST_TIMEOUT =
            Options.key("request_timeout_ms")
                    .intType()
                    .defaultValue(30000)
                    .withDescription("Total request deadline including all retries for one page.");
    static final Option<Integer> MAX_ATTEMPTS =
            Options.key("max_attempts")
                    .intType()
                    .defaultValue(4)
                    .withDescription("Maximum HTTPS attempts per page including the first.");

    final String token;
    final String datasetId;
    final List<Resource> resources;
    final URI origin;
    final int pageSize;
    final int maxPages;
    final int maxResponse;
    final int connectTimeout;
    final int readTimeout;
    final int requestTimeout;
    final int maxAttempts;
    final String fingerprint;
    private final String datasetPath;

    ApifyDatasetConfig(ReadonlyConfig options) {
        token = options.get(TOKEN);
        if (token == null
                || token.isEmpty()
                || token.length() > 4096
                || token.chars().anyMatch(c -> c <= 32 || c >= 127)) {
            throw invalid("token must be a nonempty API bearer token");
        }
        origin = readOrigin(options.get(API_URL));
        Set<Resource> selected = new LinkedHashSet<>();
        for (String name : options.get(RESOURCES)) {
            Resource resource;
            try {
                resource = Resource.valueOf(name.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException | NullPointerException e) {
                throw invalid("resources contains an unsupported resource name");
            }
            if (!selected.add(resource)) {
                throw invalid("resources must not contain duplicates");
            }
        }
        if (selected.isEmpty()) {
            throw invalid("resources must select at least one resource");
        }
        resources = Collections.unmodifiableList(new ArrayList<>(selected));
        datasetId = options.getOptional(DATASET_ID).filter(id -> !id.isEmpty()).orElse(null);
        if (selected.stream().anyMatch(resource -> resource != Resource.DATASET_COLLECTION)
                && datasetId == null) {
            throw invalid("dataset_id is required for dataset metadata and item resources");
        }
        datasetPath = datasetId == null ? null : "/v2/datasets/" + encodeDatasetId(datasetId);
        pageSize = bounded(options, PAGE_SIZE, 1, 1000);
        maxPages = bounded(options, MAX_PAGES, 1, 100000);
        maxResponse = bounded(options, MAX_RESPONSE, 1024, 33554432);
        connectTimeout = bounded(options, CONNECT_TIMEOUT, 1, 300000);
        readTimeout = bounded(options, READ_TIMEOUT, 1, 300000);
        requestTimeout = bounded(options, REQUEST_TIMEOUT, 1, 300000);
        maxAttempts = bounded(options, MAX_ATTEMPTS, 1, 10);
        // Credentials can rotate between job executions without changing the restored query.
        String query = origin + "|" + datasetPath + "|" + resources + "|" + pageSize;
        fingerprint = digestId(digest().digest(query.getBytes(StandardCharsets.UTF_8)));
    }

    /**
     * Constructs the request for one resource with the applicable API parameters. Dataset items use
     * the provider's raw JSON export so hidden and empty items are preserved.
     */
    String target(Resource resource, long offset, int limit) {
        if (resource == Resource.DATASET) {
            return datasetPath;
        }
        String page = "?offset=" + offset + "&limit=" + limit + "&desc=false";
        if (resource == Resource.DATASET_COLLECTION) {
            return "/v2/datasets" + page;
        }
        return datasetPath + "/items" + page + "&format=json";
    }

    private static int bounded(ReadonlyConfig options, Option<Integer> option, int min, int max) {
        int value = options.get(option);
        if (value < min || value > max) {
            throw invalid(option.key() + " must be between " + min + " and " + max);
        }
        return value;
    }

    private static URI readOrigin(String value) {
        try {
            URI uri = new URI(value);
            if (!HTTPS.equalsIgnoreCase(uri.getScheme())
                    || !HOST.equalsIgnoreCase(uri.getHost())
                    || uri.getUserInfo() != null
                    || uri.getRawQuery() != null
                    || uri.getRawFragment() != null
                    || (!uri.getRawPath().isEmpty() && !"/".equals(uri.getRawPath()))
                    || uri.getPort() == 0
                    || uri.getPort() > 65535) {
                throw invalid("api_url must be the HTTPS origin of api.apify.com");
            }
            return new URI(HTTPS, null, HOST, uri.getPort(), null, null, null);
        } catch (URISyntaxException | NullPointerException e) {
            throw invalid("api_url must be the HTTPS origin of api.apify.com");
        }
    }

    private static String encodeDatasetId(String id) {
        if (id.isEmpty()
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
            throw invalid("dataset_id must be a single dataset ID or named dataset reference");
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
        return new IllegalArgumentException("Apify Dataset source: " + reason);
    }

    /**
     * Identifies each distinct output contract supported by the connector. The two item resources
     * use the same endpoint and different payload layouts.
     */
    enum Resource {
        DATASET_COLLECTION,
        DATASET,
        ITEM_COLLECTION_WEBSITE_CONTENT_CRAWLER,
        ITEM_COLLECTION;

        String key() {
            return name().toLowerCase(Locale.ROOT);
        }

        static List<String> names() {
            List<String> keys = new ArrayList<>();
            for (Resource resource : values()) {
                keys.add(resource.key());
            }
            return Collections.unmodifiableList(keys);
        }
    }
}
