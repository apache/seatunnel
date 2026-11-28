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
package org.apache.seatunnel.connectors.seatunnel.buildkite;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.Base64;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

import static org.apache.seatunnel.connectors.seatunnel.buildkite.BuildkiteSourceOptions.BODY_LIMIT;
import static org.apache.seatunnel.connectors.seatunnel.buildkite.BuildkiteSourceOptions.CERTIFICATE;
import static org.apache.seatunnel.connectors.seatunnel.buildkite.BuildkiteSourceOptions.CONNECT_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.buildkite.BuildkiteSourceOptions.END;
import static org.apache.seatunnel.connectors.seatunnel.buildkite.BuildkiteSourceOptions.INTERVAL;
import static org.apache.seatunnel.connectors.seatunnel.buildkite.BuildkiteSourceOptions.MAX_PAGES;
import static org.apache.seatunnel.connectors.seatunnel.buildkite.BuildkiteSourceOptions.MAX_RETRY_DELAY;
import static org.apache.seatunnel.connectors.seatunnel.buildkite.BuildkiteSourceOptions.MAX_SCAN_PAGES;
import static org.apache.seatunnel.connectors.seatunnel.buildkite.BuildkiteSourceOptions.MAX_UNPAGED;
import static org.apache.seatunnel.connectors.seatunnel.buildkite.BuildkiteSourceOptions.OPERATION_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.buildkite.BuildkiteSourceOptions.PAGE_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.buildkite.BuildkiteSourceOptions.REQUEST_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.buildkite.BuildkiteSourceOptions.RESOURCES;
import static org.apache.seatunnel.connectors.seatunnel.buildkite.BuildkiteSourceOptions.RETRIES;
import static org.apache.seatunnel.connectors.seatunnel.buildkite.BuildkiteSourceOptions.START;
import static org.apache.seatunnel.connectors.seatunnel.buildkite.BuildkiteSourceOptions.TOKEN;

/**
 * Validates credentials, resource selection, window and network limits.
 *
 * <p>Resource selection includes the parents needed for traversal. Only explicitly selected
 * resources emit records.
 */
final class BuildkiteConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    static final String API_HOST = "api.buildkite.com";
    private static final Pattern RFC3339 =
            Pattern.compile(
                    "[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}"
                            + "(?:[.][0-9]{1,9})?(?:Z|[+-][0-9]{2}:[0-9]{2})");

    final String token;
    final String certificate;
    private final Set<BuildkiteResource> selected;
    private final Map<BuildkiteResource, List<BuildkiteResource>> children;
    final List<BuildkiteResource> roots;

    // An omitted upper cutoff is captured by the coordinator before assignment.
    final Instant start;
    final Instant end;

    final int pageSize;
    final int maxPages;
    final int scanPages;
    final int unpagedItems;
    final int connectTimeout;
    final int requestTimeout;
    final int operationTimeout;
    final int interval;
    final int retries;
    final int maxRetryDelay;
    final int bodyLimit;
    final String scope;

    BuildkiteConfig(ReadonlyConfig options) {
        token = options.get(TOKEN);
        if (token == null
                || token.isEmpty()
                || token.length() > 8192
                || !token.chars().allMatch(c -> c >= 33 && c <= 126)) {
            throw invalid(TOKEN);
        }
        selected = readResources(options.get(RESOURCES));
        Set<BuildkiteResource> needed = EnumSet.copyOf(selected);
        for (BuildkiteResource resource : selected) {
            for (BuildkiteResource parent = resource.parent;
                    parent != null;
                    parent = parent.parent) {
                needed.add(parent);
            }
        }
        children = new EnumMap<>(BuildkiteResource.class);
        List<BuildkiteResource> rootCollections = new ArrayList<>();
        for (BuildkiteResource resource : needed) {
            if (resource.parent == null) {
                rootCollections.add(resource);
            } else {
                children.computeIfAbsent(resource.parent, parent -> new ArrayList<>())
                        .add(resource);
            }
        }
        children.replaceAll((parent, collections) -> List.copyOf(collections));
        roots = List.copyOf(rootCollections);
        start = configuredInstant(options, START);
        end = configuredInstant(options, END);
        if (start == null && selected.stream().anyMatch(resource -> resource.dated)) {
            throw invalid(START);
        }
        if (start != null && end != null && end.isBefore(start)) {
            throw invalid(END);
        }
        certificate = options.getOptional(CERTIFICATE).orElse(null);
        if (certificate != null) {
            parseCertificate(certificate);
        }
        pageSize = number(options, PAGE_SIZE, 1, 100);
        maxPages = number(options, MAX_PAGES, 1, 100000);
        scanPages = number(options, MAX_SCAN_PAGES, 1, 1000000);
        unpagedItems = number(options, MAX_UNPAGED, 1, 100000);
        connectTimeout = number(options, CONNECT_TIMEOUT, 100, 300000);
        requestTimeout = number(options, REQUEST_TIMEOUT, 100, 300000);
        operationTimeout = number(options, OPERATION_TIMEOUT, requestTimeout, 600000);
        interval = number(options, INTERVAL, 0, 60000);
        retries = number(options, RETRIES, 0, 8);
        maxRetryDelay = number(options, MAX_RETRY_DELAY, 250, 300000);
        bodyLimit = number(options, BODY_LIMIT, 256, 16777216);
        scope =
                hash(
                        String.join(
                                "\n",
                                API_HOST,
                                hash(token),
                                selected.toString(),
                                String.valueOf(start),
                                String.valueOf(end),
                                Integer.toString(pageSize)));
    }

    /**
     * Produces an independent root position with the frozen upper cutoff.
     *
     * <p>The checkpoint retains exactly that cutoff even when no end was configured.
     */
    BuildkiteSplit newSplit(Instant now) {
        return new BuildkiteSplit(this, end == null ? now : end);
    }

    List<BuildkiteResource> children(BuildkiteResource resource) {
        return children.getOrDefault(resource, List.of());
    }

    boolean emits(BuildkiteResource resource) {
        return selected.contains(resource);
    }

    /**
     * Reads the timestamp formats returned by the REST endpoints.
     *
     * <p>Some catalog responses use a space-separated UTC value.
     */
    static Instant providerTimestamp(String value) {
        if (value.endsWith(" UTC") && value.length() > 14 && value.charAt(10) == ' ') {
            return instant(
                    value.substring(0, 10) + "T" + value.substring(11, value.length() - 4) + "Z");
        }
        return instant(value);
    }

    private static Instant instant(String value) {
        if (!RFC3339.matcher(value).matches()) {
            throw new DateTimeException("invalid RFC 3339 timestamp");
        }
        return OffsetDateTime.parse(value).toInstant();
    }

    private static Instant configuredInstant(ReadonlyConfig options, Option<String> option) {
        String value = options.get(option);
        try {
            return value == null ? null : instant(value);
        } catch (DateTimeException ex) {
            throw invalid(option);
        }
    }

    private static Set<BuildkiteResource> readResources(List<String> names) {
        if (names.isEmpty()) {
            return EnumSet.allOf(BuildkiteResource.class);
        }
        Set<BuildkiteResource> selected = EnumSet.noneOf(BuildkiteResource.class);
        for (String name : names) {
            BuildkiteResource resource =
                    List.of(BuildkiteResource.values()).stream()
                            .filter(candidate -> candidate.name.equals(name))
                            .findFirst()
                            .orElseThrow(() -> invalid(RESOURCES));
            if (!selected.add(resource)) {
                throw invalid(RESOURCES);
            }
        }
        return selected;
    }

    private static int number(ReadonlyConfig options, Option<Integer> option, int min, int max) {
        int value = options.get(option);
        if (value < min || value > max) {
            throw invalid(option);
        }
        return value;
    }

    /**
     * Creates a connector-private trust store only if an explicit certificate is supplied.
     *
     * <p>The transport always enables HTTPS endpoint identification on each socket.
     */
    SSLSocketFactory socketFactory() {
        if (certificate == null) {
            return (SSLSocketFactory) SSLSocketFactory.getDefault();
        }
        try {
            KeyStore store = KeyStore.getInstance("PKCS12");
            store.load(null, null);
            store.setCertificateEntry("buildkite", parseCertificate(certificate));
            TrustManagerFactory manager =
                    TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            manager.init(store);
            SSLContext context = SSLContext.getInstance("TLS");
            context.init(null, manager.getTrustManagers(), null);
            return context.getSocketFactory();
        } catch (GeneralSecurityException | IOException ex) {
            throw invalid(CERTIFICATE);
        }
    }

    private static Certificate parseCertificate(String supplied) {
        if (supplied.length() > 65536) {
            throw invalid(CERTIFICATE);
        }
        try {
            String pem = supplied.trim();
            String prefix = "-----BEGIN CERTIFICATE-----";
            String suffix = "-----END CERTIFICATE-----";
            if (pem.startsWith(prefix) && pem.endsWith(suffix)) {
                pem = pem.substring(prefix.length(), pem.length() - suffix.length());
            }
            byte[] decoded = Base64.getDecoder().decode(pem.replaceAll("\\s+", ""));
            ByteArrayInputStream input = new ByteArrayInputStream(decoded);
            Certificate cert = CertificateFactory.getInstance("X.509").generateCertificate(input);
            if (input.available() != 0) {
                throw invalid(CERTIFICATE);
            }
            return cert;
        } catch (GeneralSecurityException | IllegalArgumentException ex) {
            throw invalid(CERTIFICATE);
        }
    }

    static String hash(String text) {
        try {
            byte[] bytes =
                    MessageDigest.getInstance("SHA-256")
                            .digest(text.getBytes(StandardCharsets.UTF_8));
            char[] hex = "0123456789abcdef".toCharArray();
            char[] result = new char[bytes.length * 2];
            for (int i = 0; i < bytes.length; i++) {
                result[2 * i] = hex[(bytes[i] & 255) >>> 4];
                result[2 * i + 1] = hex[bytes[i] & 15];
            }
            return new String(result);
        } catch (NoSuchAlgorithmException error) {
            throw new IllegalStateException("Buildkite SHA-256 is unavailable");
        }
    }

    private static IllegalArgumentException invalid(Option<?> option) {
        return new IllegalArgumentException("Invalid Buildkite option " + option.key());
    }
}
