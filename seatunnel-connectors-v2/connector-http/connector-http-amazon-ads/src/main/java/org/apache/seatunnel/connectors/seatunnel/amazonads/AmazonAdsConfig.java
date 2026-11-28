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
package org.apache.seatunnel.connectors.seatunnel.amazonads;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneOffset;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.EnumSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

/**
 * Validates operator-provided origins and binds a scan to a particular Ads account and resource
 * selection.
 *
 * <p>Only non-secret scope parameters participate in checkpoint identity so credentials can rotate.
 */
final class AmazonAdsConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    final Endpoint api;
    final Endpoint oauth;
    final String clientId;
    final String clientAuthorization;
    final String refreshToken;
    final Set<AmazonAdsResource> selected;
    final List<AmazonAdsResource> profileResources;
    final List<AmazonAdsResource> adGroupResources;
    final Set<String> profiles;
    final Set<String> marketplaces;
    final Set<String> states;
    final Set<String> extraDownloadHosts;
    final LocalDate start;
    final LocalDate end;
    final int overlap;
    final int reportDays;
    final int pageSize;
    final int maxPages;
    final int maxProfiles;
    final int attempts;
    final int intervalMs;
    final int idleMs;
    final int requestMs;
    final int pollMs;
    final int reportMs;
    final int duplicateMs;
    final int downloadMs;
    final int maxResponse;
    final int maxDownload;
    final int maxReport;
    final int maxRows;
    final String scope;

    AmazonAdsConfig(ReadonlyConfig config) {
        String region = config.get(AmazonAdsSourceOptions.REGION);
        String host;
        if ("NA".equals(region)) {
            host = "advertising-api.amazon.com";
        } else if ("EU".equals(region)) {
            host = "advertising-api-eu.amazon.com";
        } else if ("FE".equals(region)) {
            host = "advertising-api-fe.amazon.com";
        } else {
            throw new IllegalArgumentException("AmazonAds region must be NA, EU or FE");
        }
        api =
                Endpoint.origin(
                        config.getOptional(AmazonAdsSourceOptions.BASE_URL)
                                .orElse("https://" + host),
                        "base_url");
        oauth =
                Endpoint.origin(
                        config.getOptional(AmazonAdsSourceOptions.OAUTH_BASE_URL)
                                .orElse("https://api.amazon.com"),
                        "oauth_base_url");
        clientId = credential(config, AmazonAdsSourceOptions.CLIENT_ID);
        if (clientId.indexOf(':') >= 0) {
            throw new IllegalArgumentException("AmazonAds client_id must not contain a colon");
        }
        String secret = credential(config, AmazonAdsSourceOptions.CLIENT_SECRET);
        clientAuthorization =
                "Basic "
                        + Base64.getEncoder()
                                .encodeToString(
                                        (clientId + ":" + secret).getBytes(StandardCharsets.UTF_8));
        refreshToken = credential(config, AmazonAdsSourceOptions.REFRESH_TOKEN);
        profiles = names(config, AmazonAdsSourceOptions.PROFILES, "[0-9]{1,40}");
        marketplaces = names(config, AmazonAdsSourceOptions.MARKETPLACE_IDS, "[a-zA-Z0-9_-]{1,64}");
        states = names(config, AmazonAdsSourceOptions.STATE_FILTER, "ENABLED|PAUSED|ARCHIVED");
        extraDownloadHosts =
                names(config, AmazonAdsSourceOptions.DOWNLOAD_HOSTS, "[a-z0-9]+(?:[.-][a-z0-9]+)+");
        EnumSet<AmazonAdsResource> chosen = EnumSet.noneOf(AmazonAdsResource.class);
        for (String name : config.get(AmazonAdsSourceOptions.RESOURCES)) {
            if (!chosen.add(AmazonAdsResource.fromLabel(name))) {
                throw new IllegalArgumentException("AmazonAds resources must be unique");
            }
        }
        selected =
                Collections.unmodifiableSet(
                        chosen.isEmpty() ? EnumSet.allOf(AmazonAdsResource.class) : chosen);
        List<AmazonAdsResource> first = new ArrayList<>();
        List<AmazonAdsResource> second = new ArrayList<>();
        for (AmazonAdsResource resource : selected) {
            if (resource.dependsOnAdGroup()) {
                second.add(resource);
            } else if (resource != AmazonAdsResource.PROFILES) {
                first.add(resource);
            }
        }
        if (!second.isEmpty() && !first.contains(AmazonAdsResource.SPONSORED_PRODUCT_AD_GROUPS)) {
            first.add(AmazonAdsResource.SPONSORED_PRODUCT_AD_GROUPS);
        }
        profileResources = List.copyOf(first);
        adGroupResources = List.copyOf(second);
        start = date(config, AmazonAdsSourceOptions.START_DATE);
        end = date(config, AmazonAdsSourceOptions.END_DATE);
        if (start != null && end != null && start.isAfter(end)) {
            throw new IllegalArgumentException("AmazonAds start_date is later than end_date");
        }
        overlap = range(config, AmazonAdsSourceOptions.LOOK_BACK_WINDOW, 0, 30);
        reportDays = range(config, AmazonAdsSourceOptions.REPORT_WINDOW_DAYS, 1, 30);
        pageSize = range(config, AmazonAdsSourceOptions.PAGE_SIZE, 1, 100);
        maxPages = range(config, AmazonAdsSourceOptions.MAX_PAGES, 1, 10000);
        maxProfiles = range(config, AmazonAdsSourceOptions.MAX_PROFILES, 1, 10000);
        attempts = range(config, AmazonAdsSourceOptions.MAX_ATTEMPTS, 1, 8);
        intervalMs = range(config, AmazonAdsSourceOptions.REQUEST_INTERVAL_MS, 10, 60000);
        idleMs = range(config, AmazonAdsSourceOptions.IO_TIMEOUT_MS, 100, 120000);
        requestMs = range(config, AmazonAdsSourceOptions.REQUEST_DEADLINE_MS, 500, 900000);
        pollMs = range(config, AmazonAdsSourceOptions.POLL_INTERVAL_MS, 100, 60000);
        reportMs = range(config, AmazonAdsSourceOptions.REPORT_TIMEOUT_MS, 1000, 14400000);
        duplicateMs = range(config, AmazonAdsSourceOptions.DUPLICATE_WAIT_MS, 100, 120000);
        downloadMs = range(config, AmazonAdsSourceOptions.DOWNLOAD_DEADLINE_MS, 500, 900000);
        maxResponse = range(config, AmazonAdsSourceOptions.MAX_RESPONSE_BYTES, 1024, 33554432);
        maxDownload = range(config, AmazonAdsSourceOptions.MAX_DOWNLOAD_BYTES, 1024, 268435456);
        maxReport = range(config, AmazonAdsSourceOptions.MAX_REPORT_BYTES, 1024, 1073741824);
        maxRows = range(config, AmazonAdsSourceOptions.MAX_REPORT_ROWS, 1, 1000000);
        scope =
                AmazonAdsRecord.hash(
                        api.authority,
                        oauth.authority,
                        clientId,
                        selected.toString(),
                        profiles.toString(),
                        marketplaces.toString(),
                        states.toString(),
                        String.valueOf(start),
                        String.valueOf(end),
                        Integer.toString(pageSize),
                        Integer.toString(reportDays),
                        Integer.toString(overlap));
    }

    boolean selectedProfile(String id, JsonNode profile) {
        return profiles.isEmpty() && marketplaces.isEmpty()
                || profiles.contains(id)
                || marketplaces.contains(
                        profile.path("accountInfo").path("marketplaceStringId").asText(""));
    }

    AmazonAdsSplit startAt(Instant now) {
        if (end != null && end.isAfter(now.atOffset(ZoneOffset.UTC).toLocalDate().plusDays(1))) {
            throw new IllegalArgumentException("AmazonAds end_date is in the future");
        }
        return new AmazonAdsSplit(scope, now);
    }

    private static LocalDate date(ReadonlyConfig config, Option<String> option) {
        try {
            String value = config.get(option);
            if (value == null) {
                return null;
            }
            if (value.matches("[0-9]{4}-[0-9]{2}-[0-9]{2}")) {
                return LocalDate.parse(value);
            }
        } catch (DateTimeParseException ignored) {
            // Report the option name without its raw value.
        }
        throw new IllegalArgumentException("AmazonAds " + option.key() + " requires YYYY-MM-DD");
    }

    private static Set<String> names(
            ReadonlyConfig config, Option<List<String>> option, String pattern) {
        Set<String> values = new LinkedHashSet<>();
        for (String value : config.get(option)) {
            if (value == null
                    || value.length() > 253
                    || !value.matches(pattern)
                    || !values.add(value)) {
                throw new IllegalArgumentException(
                        "AmazonAds requires valid unique values in " + option.key());
            }
        }
        return Collections.unmodifiableSet(values);
    }

    private static String credential(ReadonlyConfig config, Option<String> option) {
        String value = config.get(option);
        if (value == null
                || value.isEmpty()
                || value.length() > 4096
                || !value.chars().allMatch(c -> c >= 33 && c <= 126)) {
            throw new IllegalArgumentException("AmazonAds requires a valid " + option.key());
        }
        return value;
    }

    private static int range(ReadonlyConfig config, Option<Integer> option, int min, int max) {
        int v = config.get(option);
        if (v < min || v > max) {
            throw new IllegalArgumentException(
                    "AmazonAds " + option.key() + " is outside the supported range");
        }
        return v;
    }

    /**
     * Separates HTTPS authority from HTTP targets so credentials cannot follow response-supplied
     * URLs.
     *
     * <p>The transport builds distinct request objects for authorization, Ads and signed downloads.
     */
    static final class Endpoint implements Serializable {
        private static final long serialVersionUID = 1L;
        final String host;
        final String authority;
        final int port;

        Endpoint(URI uri) {
            host = uri.getHost().toLowerCase(Locale.ROOT);
            port = uri.getPort() == -1 ? 443 : uri.getPort();
            authority = host + (port == 443 ? "" : ":" + port);
        }

        static Endpoint origin(String value, String option) {
            try {
                URI uri = new URI(value);
                if (isHttps(uri)
                        && uri.getRawQuery() == null
                        && ("".equals(uri.getRawPath()) || "/".equals(uri.getRawPath()))) {
                    return new Endpoint(uri);
                }
            } catch (URISyntaxException ignored) {
                // Never include the raw credential origin.
            }
            throw new IllegalArgumentException("AmazonAds " + option + " requires an HTTPS origin");
        }

        static boolean isHttps(URI uri) {
            return "https".equalsIgnoreCase(uri.getScheme())
                    && uri.getHost() != null
                    && uri.getRawUserInfo() == null
                    && uri.getRawFragment() == null
                    && (uri.getPort() == -1 || uri.getPort() >= 1 && uri.getPort() <= 65535);
        }
    }
}
