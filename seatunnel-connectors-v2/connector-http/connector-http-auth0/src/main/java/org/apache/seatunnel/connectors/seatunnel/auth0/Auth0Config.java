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
package org.apache.seatunnel.connectors.seatunnel.auth0;

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
import java.util.Base64;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import static org.apache.seatunnel.connectors.seatunnel.auth0.Auth0SourceOptions.AUDIENCE;
import static org.apache.seatunnel.connectors.seatunnel.auth0.Auth0SourceOptions.BODY_LIMIT;
import static org.apache.seatunnel.connectors.seatunnel.auth0.Auth0SourceOptions.CERTIFICATE;
import static org.apache.seatunnel.connectors.seatunnel.auth0.Auth0SourceOptions.CLIENT_ID;
import static org.apache.seatunnel.connectors.seatunnel.auth0.Auth0SourceOptions.CLIENT_SECRET;
import static org.apache.seatunnel.connectors.seatunnel.auth0.Auth0SourceOptions.CONNECT_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.auth0.Auth0SourceOptions.CUSTOM;
import static org.apache.seatunnel.connectors.seatunnel.auth0.Auth0SourceOptions.END;
import static org.apache.seatunnel.connectors.seatunnel.auth0.Auth0SourceOptions.INTERVAL;
import static org.apache.seatunnel.connectors.seatunnel.auth0.Auth0SourceOptions.MAX_PAGES;
import static org.apache.seatunnel.connectors.seatunnel.auth0.Auth0SourceOptions.OPERATION_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.auth0.Auth0SourceOptions.PAGE_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.auth0.Auth0SourceOptions.REQUEST_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.auth0.Auth0SourceOptions.RESOURCES;
import static org.apache.seatunnel.connectors.seatunnel.auth0.Auth0SourceOptions.RETRIES;
import static org.apache.seatunnel.connectors.seatunnel.auth0.Auth0SourceOptions.START;
import static org.apache.seatunnel.connectors.seatunnel.auth0.Auth0SourceOptions.TENANT;
import static org.apache.seatunnel.connectors.seatunnel.auth0.Auth0SourceOptions.TOKEN;

/**
 * Validates source scope, credentials, request limits and private TLS settings.
 *
 * <p>Serialized split scope excludes credential values and hashes. The job credential options are
 * retained by the source only for its authenticated reads.
 */
final class Auth0Config implements Serializable {
    private static final long serialVersionUID = 1L;

    // Credentials stay in job configuration. Only the fixed TLS origin receives them.
    final URI api;
    final String token;
    final String clientId;
    final String clientSecret;
    final String audience;
    final Instant start;
    final Instant end;
    final String certificate;
    final List<Auth0Resource> resources;

    // Request attempts and a full retry sequence have independent time bounds.
    final int pageSize;
    final int maxPages;
    final int connectTimeout;
    final int requestTimeout;
    final int operationTimeout;
    final int interval;
    final int retries;
    final int bodyLimit;

    // Scope survives credential rotation but binds the tenant and logical query options.
    final String scope;

    Auth0Config(ReadonlyConfig options) {
        api = origin(options.get(TENANT), options.get(CUSTOM));
        String suppliedToken = options.getOptional(TOKEN).orElse(null);
        if (suppliedToken == null) {
            token = null;
            clientId = credential(CLIENT_ID, options.getOptional(CLIENT_ID).orElse(null));
            clientSecret =
                    credential(CLIENT_SECRET, options.getOptional(CLIENT_SECRET).orElse(null));
            audience = managementAudience(options.getOptional(AUDIENCE).orElse(null));
        } else {
            token = credential(TOKEN, suppliedToken);
            clientId = null;
            clientSecret = null;
            audience = null;
            if (options.getOptional(CLIENT_ID).isPresent()
                    || options.getOptional(CLIENT_SECRET).isPresent()
                    || options.getOptional(AUDIENCE).isPresent()) {
                throw invalid(TOKEN);
            }
        }
        resources = readResources(options.get(RESOURCES));
        start = date(options, START);
        end = date(options, END);
        if (includes(Auth0Resource.USERS) && end != null && start.isAfter(end)) {
            throw invalid(END);
        }
        certificate = options.getOptional(CERTIFICATE).orElse(null);
        if (certificate != null) {
            parseCertificate(certificate);
        }
        pageSize = number(options, PAGE_SIZE, 1, 100);
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
                                String.valueOf(audience),
                                String.valueOf(clientId),
                                String.valueOf(start),
                                String.valueOf(end),
                                resources.toString(),
                                String.valueOf(pageSize)));
    }

    /**
     * Freezes the configured user search bounds for this split.
     *
     * <p>The wall clock matters only when the caller omitted an explicit end time.
     */
    Auth0Split newSplit(Instant cutoff) {
        return new Auth0Split(this, end == null ? cutoff.toEpochMilli() : end.toEpochMilli());
    }

    boolean includes(Auth0Resource resource) {
        return resources.contains(resource);
    }

    boolean needsMembers() {
        return includes(Auth0Resource.ORGANIZATION_MEMBERS)
                || includes(Auth0Resource.ORGANIZATION_MEMBER_ROLES);
    }

    private static Instant date(ReadonlyConfig options, Option<String> option) {
        try {
            String valueWithOffset = options.get(option);
            Instant value =
                    valueWithOffset == null
                            ? null
                            : OffsetDateTime.parse(valueWithOffset).toInstant();
            if (value != null
                    && (value.getNano() % 1000000 != 0
                            || value.isBefore(Instant.parse("0001-01-01T00:00:00Z"))
                            || !value.isBefore(Instant.parse("+10000-01-01T00:00:00Z")))) {
                throw invalid(option);
            }
            return value;
        } catch (DateTimeException ex) {
            throw invalid(option);
        }
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
            store.setCertificateEntry("auth0", parseCertificate(certificate));
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

    private static URI origin(String value, boolean custom) {
        try {
            URI uri = URI.create(value);
            if ("https".equalsIgnoreCase(uri.getScheme())
                    && uri.getHost() != null
                    && !uri.getHost().contains(":")
                    && !uri.getHost().matches("[0-9.]+")
                    && (custom || standardHost(uri))
                    && uri.getRawUserInfo() == null
                    && uri.getRawQuery() == null
                    && uri.getRawFragment() == null
                    && (uri.getRawPath().isEmpty() || "/".equals(uri.getRawPath()))
                    && (uri.getPort() == -1 || uri.getPort() > 0 && uri.getPort() <= 65535)) {
                return new URI(
                        "https",
                        null,
                        uri.getHost().toLowerCase(Locale.ROOT),
                        uri.getPort() == 443 ? -1 : uri.getPort(),
                        null,
                        null,
                        null);
            }
        } catch (Exception ex) {
            // A cause created by the URI parser could print a supplied credential or target.
        }
        throw invalid(TENANT);
    }

    private static boolean standardHost(URI uri) {
        String host = uri.getHost().toLowerCase(Locale.ROOT);
        return host.endsWith(".auth0.com") && (uri.getPort() == -1 || uri.getPort() == 443);
    }

    private static String managementAudience(String supplied) {
        try {
            URI uri = URI.create(credential(AUDIENCE, supplied));
            if ("https".equalsIgnoreCase(uri.getScheme())
                    && uri.getHost() != null
                    && uri.getRawUserInfo() == null
                    && uri.getRawQuery() == null
                    && uri.getRawFragment() == null
                    && "/api/v2/".equals(uri.getRawPath())) {
                return supplied;
            }
        } catch (IllegalArgumentException ex) {
            // An invalid URI may contain sensitive material in its parser message.
        }
        throw invalid(AUDIENCE);
    }

    private static List<Auth0Resource> readResources(List<String> names) {
        if (names.isEmpty()) {
            return List.of(Auth0Resource.values());
        }
        Set<Auth0Resource> result = EnumSet.noneOf(Auth0Resource.class);
        for (String name : names) {
            try {
                if (!result.add(Auth0Resource.valueOf(name.toUpperCase(Locale.ROOT)))) {
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
                || value.isEmpty()
                || value.length() > 8192
                || !value.chars().allMatch(c -> c >= 33 && c <= 126)) {
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
            throw new IllegalStateException("Auth0 SHA-256 is unavailable");
        }
    }

    private static IllegalArgumentException invalid(Option<?> option) {
        return new IllegalArgumentException("Invalid Auth0 option " + option.key());
    }
}
