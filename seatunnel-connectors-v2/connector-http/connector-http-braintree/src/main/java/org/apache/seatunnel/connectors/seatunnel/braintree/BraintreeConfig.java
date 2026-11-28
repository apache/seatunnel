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
package org.apache.seatunnel.connectors.seatunnel.braintree;

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
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import static org.apache.seatunnel.connectors.seatunnel.braintree.BraintreeSourceOptions.ACCESS_KEY;
import static org.apache.seatunnel.connectors.seatunnel.braintree.BraintreeSourceOptions.BATCH;
import static org.apache.seatunnel.connectors.seatunnel.braintree.BraintreeSourceOptions.BYTES;
import static org.apache.seatunnel.connectors.seatunnel.braintree.BraintreeSourceOptions.CERTIFICATE;
import static org.apache.seatunnel.connectors.seatunnel.braintree.BraintreeSourceOptions.CONNECT;
import static org.apache.seatunnel.connectors.seatunnel.braintree.BraintreeSourceOptions.END;
import static org.apache.seatunnel.connectors.seatunnel.braintree.BraintreeSourceOptions.ENVIRONMENT;
import static org.apache.seatunnel.connectors.seatunnel.braintree.BraintreeSourceOptions.MERCHANT_ID;
import static org.apache.seatunnel.connectors.seatunnel.braintree.BraintreeSourceOptions.OPERATION;
import static org.apache.seatunnel.connectors.seatunnel.braintree.BraintreeSourceOptions.PAGES;
import static org.apache.seatunnel.connectors.seatunnel.braintree.BraintreeSourceOptions.REQUEST;
import static org.apache.seatunnel.connectors.seatunnel.braintree.BraintreeSourceOptions.RESOURCES;
import static org.apache.seatunnel.connectors.seatunnel.braintree.BraintreeSourceOptions.RESULT_LIMIT;
import static org.apache.seatunnel.connectors.seatunnel.braintree.BraintreeSourceOptions.RETRIES;
import static org.apache.seatunnel.connectors.seatunnel.braintree.BraintreeSourceOptions.RETRY_DELAY;
import static org.apache.seatunnel.connectors.seatunnel.braintree.BraintreeSourceOptions.SECRET_KEY;
import static org.apache.seatunnel.connectors.seatunnel.braintree.BraintreeSourceOptions.START;
import static org.apache.seatunnel.connectors.seatunnel.braintree.BraintreeSourceOptions.WINDOWS;
import static org.apache.seatunnel.connectors.seatunnel.braintree.BraintreeSourceOptions.WINDOW_DAYS;
import static org.apache.seatunnel.connectors.seatunnel.braintree.BraintreeSourceOptions.XML_DEPTH;
import static org.apache.seatunnel.connectors.seatunnel.braintree.BraintreeSourceOptions.XML_NODES;

/**
 * Validates fixed environment, connection settings and the selected merchant scope.
 *
 * <p>Credentials remain in the connection configuration. A split binds only the environment,
 * merchant, selected resources and scan semantics.
 */
final class BraintreeConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    final String host;
    final String merchant;
    final String merchantPath;
    final String authorization;
    final String certificate;
    final Instant start;
    private final Instant end;
    final List<BraintreeResource> resources;
    final int connectTimeout;
    final int requestTimeout;
    final int operationTimeout;
    final int retries;
    final int retryDelay;
    final int bytes;
    final int xmlDepth;
    final int xmlNodes;
    final int maxPages;
    final int maxWindows;
    final int windowDays;
    final int resultLimit;
    final int batchLimit;
    final String scope;

    BraintreeConfig(ReadonlyConfig options) {
        String environment = options.get(ENVIRONMENT);
        if ("production".equalsIgnoreCase(environment)) {
            host = "api.braintreegateway.com";
        } else if ("sandbox".equalsIgnoreCase(environment)) {
            host = "api.sandbox.braintreegateway.com";
        } else {
            throw invalid(ENVIRONMENT);
        }
        merchant = options.get(MERCHANT_ID);
        if (merchant == null || !merchant.matches("[A-Za-z0-9_-]{1,128}")) {
            throw invalid(MERCHANT_ID);
        }
        merchantPath = "/merchants/" + merchant;
        String access = key(ACCESS_KEY, options.get(ACCESS_KEY));
        String secret = key(SECRET_KEY, options.get(SECRET_KEY));
        if (access.indexOf(':') >= 0) {
            throw invalid(ACCESS_KEY);
        }
        authorization =
                "Basic "
                        + Base64.getEncoder()
                                .encodeToString(
                                        (access + ":" + secret)
                                                .getBytes(StandardCharsets.US_ASCII));
        resources = resources(options.get(RESOURCES));
        start = date(options, START);
        end = date(options, END);
        if (start == null && resources.stream().anyMatch(BraintreeResource::dated)) {
            throw invalid(START);
        }
        if (start != null && end != null && end.isBefore(start)) {
            throw invalid(END);
        }
        certificate = options.get(CERTIFICATE);
        if (certificate != null) {
            parseCertificate(certificate);
        }
        connectTimeout = number(options, CONNECT, 100, 300000);
        requestTimeout = number(options, REQUEST, 100, 300000);
        operationTimeout = number(options, OPERATION, requestTimeout, 600000);
        retries = number(options, RETRIES, 0, 8);
        retryDelay = number(options, RETRY_DELAY, 0, 60000);
        bytes = number(options, BYTES, 256, 33554432);
        xmlDepth = number(options, XML_DEPTH, 4, 128);
        xmlNodes = number(options, XML_NODES, 16, 1000000);
        maxPages = number(options, PAGES, 1, 100000);
        maxWindows = number(options, WINDOWS, 1, 100000);
        windowDays = number(options, WINDOW_DAYS, 1, 365);
        resultLimit = number(options, RESULT_LIMIT, 2, 50000);
        batchLimit = number(options, BATCH, 1, 1000);
        String contract =
                String.join(
                        "\n",
                        host,
                        merchant,
                        resources.toString(),
                        String.valueOf(start),
                        String.valueOf(end),
                        Integer.toString(windowDays),
                        Integer.toString(resultLimit),
                        Integer.toString(batchLimit));
        scope = hash(contract.getBytes(StandardCharsets.UTF_8));
    }

    /**
     * Freezes a time cutoff before the coordinator can assign work.
     *
     * <p>Restored splits preserve this value regardless of the worker's clock.
     */
    BraintreeSplit newSplit(Instant now) {
        Instant cutoff = end == null ? now : end;
        if (cutoff.isAfter(now) || start != null && cutoff.isBefore(start)) {
            throw invalid(END);
        }
        return new BraintreeSplit(scope, start, cutoff);
    }

    private static List<BraintreeResource> resources(List<String> selections) {
        if (selections.isEmpty()) {
            return List.of(BraintreeResource.values());
        }
        Set<BraintreeResource> seen = EnumSet.noneOf(BraintreeResource.class);
        List<BraintreeResource> selected = new ArrayList<>();
        for (String name : selections) {
            BraintreeResource resource;
            try {
                resource = BraintreeResource.valueOf(name.toUpperCase(Locale.ROOT));
            } catch (IllegalArgumentException ex) {
                throw invalid(RESOURCES);
            }
            if (!resource.name.equals(name) || !seen.add(resource)) {
                throw invalid(RESOURCES);
            }
            selected.add(resource);
        }
        return List.copyOf(selected);
    }

    private static Instant date(ReadonlyConfig options, Option<String> name) {
        String value = options.get(name);
        if (value == null) {
            return null;
        }
        try {
            OffsetDateTime parsed = OffsetDateTime.parse(value);
            if (parsed.getYear() < 1900 || parsed.getYear() > 9999) {
                throw invalid(name);
            }
            return parsed.toInstant();
        } catch (DateTimeException ex) {
            throw invalid(name);
        }
    }

    private static int number(
            ReadonlyConfig options, Option<Integer> name, int minimum, int maximum) {
        int value = options.get(name);
        if (value < minimum || value > maximum) {
            throw invalid(name);
        }
        return value;
    }

    private static String key(Option<String> name, String value) {
        if (value == null
                || value.isEmpty()
                || value.length() > 1024
                || !value.chars().allMatch(c -> c >= 33 && c <= 126)) {
            throw invalid(name);
        }
        return value;
    }

    /**
     * Creates a source-local TLS trust context if a custom certificate was supplied.
     *
     * <p>Neither process trust settings nor HTTPS hostname checks are changed.
     */
    SSLSocketFactory socketFactory() {
        if (certificate == null) {
            return (SSLSocketFactory) SSLSocketFactory.getDefault();
        }
        try {
            KeyStore store = KeyStore.getInstance("PKCS12");
            store.load(null, null);
            store.setCertificateEntry("braintree", parseCertificate(certificate));
            TrustManagerFactory trust =
                    TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            trust.init(store);
            SSLContext tls = SSLContext.getInstance("TLS");
            tls.init(null, trust.getTrustManagers(), null);
            return tls.getSocketFactory();
        } catch (GeneralSecurityException | IOException ex) {
            throw invalid(CERTIFICATE);
        }
    }

    private static Certificate parseCertificate(String encoded) {
        if (encoded.length() > 65536) {
            throw invalid(CERTIFICATE);
        }
        try {
            String base64 =
                    encoded.trim()
                            .replace("-----BEGIN CERTIFICATE-----", "")
                            .replace("-----END CERTIFICATE-----", "")
                            .replaceAll("\\s+", "");
            ByteArrayInputStream input =
                    new ByteArrayInputStream(Base64.getDecoder().decode(base64));
            Certificate cert = CertificateFactory.getInstance("X.509").generateCertificate(input);
            if (input.available() != 0) {
                throw invalid(CERTIFICATE);
            }
            return cert;
        } catch (GeneralSecurityException | IllegalArgumentException ex) {
            throw invalid(CERTIFICATE);
        }
    }

    static String hash(byte[] data) {
        try {
            return hex(MessageDigest.getInstance("SHA-256").digest(data));
        } catch (NoSuchAlgorithmException ex) {
            throw new BraintreeFailure("SHA-256 is unavailable");
        }
    }

    static String hex(byte[] digest) {
        char[] alphabet = "0123456789abcdef".toCharArray();
        char[] hex = new char[digest.length * 2];
        for (int i = 0; i < digest.length; ++i) {
            hex[2 * i] = alphabet[(digest[i] & 255) >>> 4];
            hex[2 * i + 1] = alphabet[digest[i] & 15];
        }
        return new String(hex);
    }

    private static IllegalArgumentException invalid(Option<?> name) {
        return new IllegalArgumentException("Invalid Braintree option " + name.key());
    }
}
