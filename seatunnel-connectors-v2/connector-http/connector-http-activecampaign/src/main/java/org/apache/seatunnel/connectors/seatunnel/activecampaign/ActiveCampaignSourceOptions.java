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

package org.apache.seatunnel.connectors.seatunnel.activecampaign;

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
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;

public final class ActiveCampaignSourceOptions {
    private static final List<String> ALL =
            Collections.unmodifiableList(
                    Arrays.asList("campaigns", "contacts", "lists", "deals", "segments", "forms"));

    public static final Option<String> BASE_URL =
            Options.key("base_url")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("HTTPS API origin from the tenant's Developer settings");
    public static final Option<String> TOKEN =
            Options.key("token")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("API token sent in the Api-Token header");
    public static final Option<List<String>> RESOURCES =
            Options.key("resources")
                    .listType()
                    .defaultValue(ALL)
                    .withDescription("Resources to scan sequentially");
    public static final Option<Integer> PAGE_SIZE =
            Options.key("page_size")
                    .intType()
                    .defaultValue(20)
                    .withDescription("Requested records per page, between 1 and 100");
    public static final Option<Integer> MAX_PAGES =
            Options.key("max_pages")
                    .intType()
                    .defaultValue(10000)
                    .withDescription("Maximum number of pages per resource");
    public static final Option<Integer> REQUEST_INTERVAL_MS =
            Options.key("request_interval_ms")
                    .intType()
                    .defaultValue(250)
                    .withDescription("Minimum interval between request starts for this scan");
    public static final Option<Integer> MAX_ATTEMPTS =
            Options.key("max_attempts")
                    .intType()
                    .defaultValue(4)
                    .withDescription("Maximum attempts including the initial request");
    public static final Option<Integer> SOCKET_TIMEOUT_MS =
            Options.key("socket_timeout_ms")
                    .intType()
                    .defaultValue(15000)
                    .withDescription("Connect and read inactivity timeout per attempt");
    public static final Option<Integer> PAGE_DEADLINE_MS =
            Options.key("page_deadline_ms")
                    .intType()
                    .defaultValue(60000)
                    .withDescription("Elapsed time budget for requesting and preparing a page");
    public static final Option<Integer> MAX_RESPONSE_BYTES =
            Options.key("max_response_bytes")
                    .intType()
                    .defaultValue(8388608)
                    .withDescription("Maximum decompressed response bytes per page");

    private ActiveCampaignSourceOptions() {}

    static final class Settings implements Serializable {
        private static final long serialVersionUID = 1L;
        final String host;
        final int port;
        final String authority;
        final String token;
        final List<String> resources;
        final int pageSize;
        final int maxPages;
        final int interval;
        final int attempts;
        final int socketTimeout;
        final int deadline;
        final int maxBytes;
        private final byte[] identity;

        Settings(ReadonlyConfig config) {
            URI origin = origin(config.get(BASE_URL));
            host = origin.getHost().toLowerCase(Locale.ROOT);
            port = origin.getPort() == -1 ? 443 : origin.getPort();
            authority = port == 443 ? host : host + ":" + port;
            token = config.get(TOKEN);
            if (token == null || token.length() > 2048 || !token.matches("[!-~]+")) {
                throw invalidOption(TOKEN);
            }
            List<String> selected = config.get(RESOURCES);
            if (selected == null
                    || selected.isEmpty()
                    || selected.size() > ALL.size()
                    || !ALL.containsAll(selected)
                    || new HashSet<>(selected).size() != selected.size()) {
                throw invalidOption(RESOURCES);
            }
            resources = Collections.unmodifiableList(new ArrayList<>(selected));
            pageSize = bounded(config, PAGE_SIZE, 1, 100);
            maxPages = bounded(config, MAX_PAGES, 1, 100000);
            interval = bounded(config, REQUEST_INTERVAL_MS, 200, 60000);
            attempts = bounded(config, MAX_ATTEMPTS, 1, 8);
            socketTimeout = bounded(config, SOCKET_TIMEOUT_MS, 100, 120000);
            deadline = bounded(config, PAGE_DEADLINE_MS, 500, 600000);
            maxBytes = bounded(config, MAX_RESPONSE_BYTES, 128, 33554432);
            identity = digest(authority + "\n" + String.join(",", resources) + "\n" + pageSize);
        }

        private static URI origin(String value) {
            if (value != null && value.length() <= 512) {
                try {
                    URI uri = new URI(value);
                    if ("https".equalsIgnoreCase(uri.getScheme())
                            && uri.getHost() != null
                            && !uri.getHost().startsWith("[")
                            && uri.getRawUserInfo() == null
                            && uri.getRawQuery() == null
                            && uri.getRawFragment() == null
                            && (uri.getRawPath().isEmpty() || "/".equals(uri.getRawPath()))
                            && uri.getPort() >= -1
                            && uri.getPort() != 0
                            && uri.getPort() <= 65535) {
                        return uri;
                    }
                } catch (URISyntaxException malformed) {
                    // Configured URLs can contain credentials. Report the option name only.
                }
            }
            throw invalidOption(BASE_URL);
        }

        ActiveCampaignSplit initial() {
            return new ActiveCampaignSplit(identity);
        }

        void verify(ActiveCampaignSplit split) {
            if (split == null
                    || split.version != 1
                    || !MessageDigest.isEqual(identity, split.identity)
                    || split.resource < 0
                    || split.resource > resources.size()
                    || split.offset < 0
                    || split.offset % pageSize != 0
                    || split.offset / pageSize >= maxPages
                    || split.emitted < 0
                    || split.emitted >= pageSize
                    || (split.emitted == 0
                            ? split.pageDigest.length != 0
                            : split.pageDigest.length != 32)
                    || split.history.size() > ActiveCampaignSplit.HISTORY_LIMIT
                    || split.emptyPages < 0
                    || split.emptyPages > ActiveCampaignSplit.EMPTY_LIMIT
                    || (split.resource == resources.size()
                            && (split.offset != 0
                                    || split.emitted != 0
                                    || !split.history.isEmpty()
                                    || split.emptyPages != 0))) {
                throw new IllegalStateException(
                        "ActiveCampaign checkpoint is incompatible with the scan");
            }
        }

        private static int bounded(
                ReadonlyConfig config, Option<Integer> option, int min, int max) {
            Integer value = config.get(option);
            if (value == null || value < min || value > max) {
                throw invalidOption(option);
            }
            return value;
        }

        private static IllegalArgumentException invalidOption(Option<?> option) {
            return new IllegalArgumentException("Invalid ActiveCampaign option " + option.key());
        }
    }

    static byte[] digest(String value) {
        try {
            return MessageDigest.getInstance("SHA-256")
                    .digest(value.getBytes(StandardCharsets.UTF_8));
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 is unavailable");
        }
    }
}
