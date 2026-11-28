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
package org.apache.seatunnel.connectors.seatunnel.avni;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLSocketFactory;
import javax.net.ssl.TrustManagerFactory;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Serializable;
import java.net.URI;
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
import java.time.temporal.ChronoUnit;
import java.util.Base64;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import static org.apache.seatunnel.connectors.seatunnel.avni.AvniSourceOptions.API;
import static org.apache.seatunnel.connectors.seatunnel.avni.AvniSourceOptions.BODY_LIMIT;
import static org.apache.seatunnel.connectors.seatunnel.avni.AvniSourceOptions.CERTIFICATE;
import static org.apache.seatunnel.connectors.seatunnel.avni.AvniSourceOptions.CONNECT_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.avni.AvniSourceOptions.CUSTOM;
import static org.apache.seatunnel.connectors.seatunnel.avni.AvniSourceOptions.DEFAULT_API;
import static org.apache.seatunnel.connectors.seatunnel.avni.AvniSourceOptions.DEFAULT_IDENTITY;
import static org.apache.seatunnel.connectors.seatunnel.avni.AvniSourceOptions.END;
import static org.apache.seatunnel.connectors.seatunnel.avni.AvniSourceOptions.IDENTITY;
import static org.apache.seatunnel.connectors.seatunnel.avni.AvniSourceOptions.INTERVAL;
import static org.apache.seatunnel.connectors.seatunnel.avni.AvniSourceOptions.MAX_PAGES;
import static org.apache.seatunnel.connectors.seatunnel.avni.AvniSourceOptions.OPERATION_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.avni.AvniSourceOptions.PAGE_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.avni.AvniSourceOptions.PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.avni.AvniSourceOptions.REQUEST_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.avni.AvniSourceOptions.RESOURCES;
import static org.apache.seatunnel.connectors.seatunnel.avni.AvniSourceOptions.RETRIES;
import static org.apache.seatunnel.connectors.seatunnel.avni.AvniSourceOptions.START;
import static org.apache.seatunnel.connectors.seatunnel.avni.AvniSourceOptions.USERNAME;

/**
 * Validates bounded integration options, credential scope and server trust.
 *
 * <p>Separate HTTPS origins keep the account password on the identity service and the resulting
 * token on the Avni API. The password can rotate without changing the saved account scope.
 */
final class AvniConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    // The fixed origin pair is immutable for every authentication and data attempt.
    final URI api;
    final URI identity;
    final String username;
    final String password;
    final String certificate;
    final Instant start;
    final Instant end;
    final List<AvniResource> resources;

    // Bound each TCP attempt, the combined authentication/read, and all decoded bodies
    // independently.
    final int pageSize;
    final int maxPages;
    final int connectTimeout;
    final int requestTimeout;
    final int operationTimeout;
    final int interval;
    final int retries;
    final int bodyLimit;
    final String scope;

    AvniConfig(ReadonlyConfig options) {
        boolean custom = options.get(CUSTOM);
        api = origin(options, API, DEFAULT_API, custom);
        identity = origin(options, IDENTITY, DEFAULT_IDENTITY, custom);
        if (api.equals(identity)) {
            throw invalid(IDENTITY);
        }
        username = credential(USERNAME, options.get(USERNAME));
        password = credential(PASSWORD, options.get(PASSWORD));
        start = date(options, START);
        end = date(options, END);
        if (start == null) {
            throw invalid(START);
        }
        if (end != null && end.isBefore(start)) {
            throw invalid(END);
        }
        certificate = options.getOptional(CERTIFICATE).orElse(null);
        if (certificate != null) {
            parseCertificate(certificate);
        }
        resources = readResources(options.get(RESOURCES));
        pageSize = number(options, PAGE_SIZE, 1, 1000);
        maxPages = number(options, MAX_PAGES, 1, 100000);
        requestTimeout = number(options, REQUEST_TIMEOUT, 100, 300000);
        connectTimeout = number(options, CONNECT_TIMEOUT, 100, 300000);
        operationTimeout = number(options, OPERATION_TIMEOUT, requestTimeout, 600000);
        interval = number(options, INTERVAL, 0, 60000);
        retries = number(options, RETRIES, 0, 8);
        bodyLimit = number(options, BODY_LIMIT, 256, 16777216);
        scope =
                hash(
                        String.join(
                                "\n",
                                api.toString(),
                                identity.toString(),
                                username,
                                start.toString(),
                                String.valueOf(end),
                                resources.toString(),
                                String.valueOf(pageSize)));
    }

    /**
     * Freezes the Avni safety buffer once for the entire scan and every restored page.
     *
     * <p>An explicit end can shorten the window. It cannot move the fence beyond the initial
     * ten-second buffer for source updates.
     */
    AvniSplit newSplit(Instant now) {
        Instant cutoff = now.minusSeconds(10).truncatedTo(ChronoUnit.MILLIS);
        Instant upper = end != null && end.isBefore(cutoff) ? end : cutoff;
        return new AvniSplit(scope, start, upper);
    }

    private static Instant date(ReadonlyConfig options, Option<String> option) {
        try {
            String value = options.get(option);
            if (value == null) {
                return null;
            }
            Instant date = OffsetDateTime.parse(value).toInstant();
            if (date.getNano() % 1000000 == 0) {
                return date;
            }
        } catch (DateTimeException ex) {
            // Invalid timestamp text is not needed for diagnostics.
        }
        throw invalid(option);
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
            store.setCertificateEntry("avni", parseCertificate(certificate));
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

    private static URI origin(
            ReadonlyConfig options, Option<String> option, String standard, boolean custom) {
        try {
            URI uri = URI.create(options.get(option));
            String host = uri.getHost();
            if ("https".equalsIgnoreCase(uri.getScheme())
                    && host != null
                    && !host.contains(":")
                    && !host.matches("[0-9.]+")
                    && uri.getRawUserInfo() == null
                    && uri.getRawQuery() == null
                    && uri.getRawFragment() == null
                    && (uri.getRawPath().isEmpty() || "/".equals(uri.getRawPath()))
                    && (uri.getPort() == -1 || uri.getPort() > 0 && uri.getPort() <= 65535)) {
                URI normalized =
                        new URI(
                                "https",
                                null,
                                host.toLowerCase(Locale.ROOT),
                                uri.getPort() == 443 ? -1 : uri.getPort(),
                                null,
                                null,
                                null);
                if (custom || normalized.toString().equals(standard)) {
                    return normalized;
                }
            }
        } catch (Exception ex) {
            // A URI-parser cause could contain a credential or a custom target.
        }
        throw invalid(option);
    }

    private static List<AvniResource> readResources(List<String> names) {
        if (names.isEmpty()) {
            return List.of(AvniResource.values());
        }
        Set<AvniResource> result = EnumSet.noneOf(AvniResource.class);
        for (String name : names) {
            try {
                if (!result.add(AvniResource.valueOf(name.toUpperCase(Locale.ROOT)))) {
                    throw invalid(RESOURCES);
                }
            } catch (IllegalArgumentException ex) {
                throw invalid(RESOURCES);
            }
        }
        return List.copyOf(result);
    }

    private static String credential(Option<String> key, String value) {
        if (value == null
                || value.isBlank()
                || value.length() > 4096
                || value.codePoints()
                        .anyMatch(
                                c ->
                                        Character.isISOControl(c)
                                                || c >= Character.MIN_SURROGATE
                                                        && c <= Character.MAX_SURROGATE)) {
            throw invalid(key);
        }
        return value;
    }

    private static int number(ReadonlyConfig options, Option<Integer> option, int min, int max) {
        int value = options.get(option);
        if (value < min || value > max) {
            throw invalid(option);
        }
        return value;
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
            throw new IllegalStateException("Avni SHA-256 is unavailable");
        }
    }

    private static IllegalArgumentException invalid(Option<?> option) {
        return new IllegalArgumentException("Invalid Avni option " + option.key());
    }
}
