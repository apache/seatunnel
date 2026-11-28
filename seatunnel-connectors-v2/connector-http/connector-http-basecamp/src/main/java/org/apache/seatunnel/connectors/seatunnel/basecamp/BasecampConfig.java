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
package org.apache.seatunnel.connectors.seatunnel.basecamp;

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
import java.util.Base64;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import static org.apache.seatunnel.connectors.seatunnel.basecamp.BasecampSourceOptions.ACCOUNT;
import static org.apache.seatunnel.connectors.seatunnel.basecamp.BasecampSourceOptions.AGENT;
import static org.apache.seatunnel.connectors.seatunnel.basecamp.BasecampSourceOptions.BODY_LIMIT;
import static org.apache.seatunnel.connectors.seatunnel.basecamp.BasecampSourceOptions.CERTIFICATE;
import static org.apache.seatunnel.connectors.seatunnel.basecamp.BasecampSourceOptions.CLIENT_ID;
import static org.apache.seatunnel.connectors.seatunnel.basecamp.BasecampSourceOptions.CLIENT_SECRET;
import static org.apache.seatunnel.connectors.seatunnel.basecamp.BasecampSourceOptions.CONNECT_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.basecamp.BasecampSourceOptions.END;
import static org.apache.seatunnel.connectors.seatunnel.basecamp.BasecampSourceOptions.INTERVAL;
import static org.apache.seatunnel.connectors.seatunnel.basecamp.BasecampSourceOptions.MAX_PAGES;
import static org.apache.seatunnel.connectors.seatunnel.basecamp.BasecampSourceOptions.MAX_PAGE_ITEMS;
import static org.apache.seatunnel.connectors.seatunnel.basecamp.BasecampSourceOptions.OPERATION_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.basecamp.BasecampSourceOptions.REFRESH;
import static org.apache.seatunnel.connectors.seatunnel.basecamp.BasecampSourceOptions.REQUEST_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.basecamp.BasecampSourceOptions.RESOURCES;
import static org.apache.seatunnel.connectors.seatunnel.basecamp.BasecampSourceOptions.RETRIES;
import static org.apache.seatunnel.connectors.seatunnel.basecamp.BasecampSourceOptions.START;
import static org.apache.seatunnel.connectors.seatunnel.basecamp.BasecampSourceOptions.TOKEN;

/**
 * Validates one account, its authentication mode and the bounded scan settings.
 *
 * <p>The fixed API and Launchpad hosts receive only their respective credentials. Scan state is
 * scoped to the account and selections without carrying authentication values.
 */
final class BasecampConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    static final String API_HOST = "3.basecampapi.com";
    static final String LAUNCHPAD_HOST = "launchpad.37signals.com";

    // Data routes always start with a validated decimal account identifier.
    final String account;
    final String userAgent;
    final String token;
    final String clientId;
    final String clientSecret;
    final String refreshToken;
    final Instant start;
    final Instant end;
    final String certificate;
    final List<BasecampResource> resources;

    // Network and page limits apply even to a response with no output rows.
    final int maxPages;
    final int maxPageItems;
    final int connectTimeout;
    final int requestTimeout;
    final int operationTimeout;
    final int interval;
    final int retries;
    final int bodyLimit;
    final String scope;

    BasecampConfig(ReadonlyConfig options) {
        account = options.get(ACCOUNT);
        if (!BasecampResource.validId(account)) {
            throw invalid(ACCOUNT);
        }
        userAgent = options.get(AGENT);
        if (userAgent == null
                || userAgent.isBlank()
                || userAgent.length() > 512
                || !userAgent.chars().allMatch(c -> c >= 32 && c <= 126)) {
            throw invalid(AGENT);
        }
        token = options.getOptional(TOKEN).orElse(null);
        if (token == null) {
            clientId = credential(CLIENT_ID, options.getOptional(CLIENT_ID).orElse(null));
            clientSecret =
                    credential(CLIENT_SECRET, options.getOptional(CLIENT_SECRET).orElse(null));
            refreshToken = credential(REFRESH, options.getOptional(REFRESH).orElse(null));
        } else {
            if (!bearer(token)
                    || options.getOptional(CLIENT_ID).isPresent()
                    || options.getOptional(CLIENT_SECRET).isPresent()
                    || options.getOptional(REFRESH).isPresent()) {
                throw invalid(TOKEN);
            }
            clientId = null;
            clientSecret = null;
            refreshToken = null;
        }
        resources = readResources(options.get(RESOURCES));
        start = date(options, START);
        end = date(options, END);
        if (start == null
                && (includes(BasecampResource.TODOS) || includes(BasecampResource.MESSAGES))) {
            throw invalid(START);
        }
        if (start != null && end != null && end.isBefore(start)) {
            throw invalid(END);
        }
        certificate = options.getOptional(CERTIFICATE).orElse(null);
        if (certificate != null) {
            parseCertificate(certificate);
        }
        maxPages = number(options, MAX_PAGES, 1, 100000);
        maxPageItems = number(options, MAX_PAGE_ITEMS, 1, 10000);
        connectTimeout = number(options, CONNECT_TIMEOUT, 100, 300000);
        requestTimeout = number(options, REQUEST_TIMEOUT, 100, 300000);
        operationTimeout = number(options, OPERATION_TIMEOUT, requestTimeout, 600000);
        interval = number(options, INTERVAL, 0, 60000);
        retries = number(options, RETRIES, 0, 8);
        bodyLimit = number(options, BODY_LIMIT, 256, 16777216);
        scope =
                hash(
                        String.join(
                                "\n",
                                account,
                                String.valueOf(clientId),
                                resources.toString(),
                                String.valueOf(start),
                                String.valueOf(end)));
    }

    /**
     * Captures the feed cutoff once before issuing any provider request.
     *
     * <p>Project and schedule discovery remain independent of the recordings date window.
     */
    BasecampSplit newSplit(Instant now) {
        Instant cutoff = end != null && end.isBefore(now) ? end : now;
        return new BasecampSplit(this, cutoff);
    }

    boolean includes(BasecampResource resource) {
        return resources.contains(resource);
    }

    boolean needsProjects() {
        return includes(BasecampResource.PROJECTS) || needsSchedules();
    }

    boolean needsSchedules() {
        return includes(BasecampResource.SCHEDULES) || includes(BasecampResource.SCHEDULE_ENTRIES);
    }

    static boolean bearer(String value) {
        return value != null
                && !value.isEmpty()
                && value.length() <= 8192
                && value.chars().allMatch(c -> c >= 33 && c <= 126);
    }

    private static Instant date(ReadonlyConfig options, Option<String> option) {
        try {
            String value = options.get(option);
            return value == null ? null : OffsetDateTime.parse(value).toInstant();
        } catch (DateTimeException ex) {
            throw invalid(option);
        }
    }

    static boolean opaqueCredential(String value) {
        return value != null
                && !value.isBlank()
                && value.length() <= 8192
                && value.codePoints()
                        .noneMatch(
                                c ->
                                        Character.isISOControl(c)
                                                || c >= Character.MIN_SURROGATE
                                                        && c <= Character.MAX_SURROGATE);
    }

    private static String credential(Option<String> option, String value) {
        if (!opaqueCredential(value)) {
            throw invalid(option);
        }
        return value;
    }

    private static List<BasecampResource> readResources(List<String> names) {
        if (names.isEmpty()) {
            return List.of(BasecampResource.values());
        }
        Set<BasecampResource> selected = EnumSet.noneOf(BasecampResource.class);
        for (String name : names) {
            try {
                if (!selected.add(BasecampResource.valueOf(name.toUpperCase(Locale.ROOT)))) {
                    throw invalid(RESOURCES);
                }
            } catch (IllegalArgumentException ex) {
                throw invalid(RESOURCES);
            }
        }
        return List.copyOf(selected);
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
            store.setCertificateEntry("basecamp", parseCertificate(certificate));
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
            throw new IllegalStateException("Basecamp SHA-256 is unavailable");
        }
    }

    private static IllegalArgumentException invalid(Option<?> option) {
        return new IllegalArgumentException("Invalid Basecamp option " + option.key());
    }
}
