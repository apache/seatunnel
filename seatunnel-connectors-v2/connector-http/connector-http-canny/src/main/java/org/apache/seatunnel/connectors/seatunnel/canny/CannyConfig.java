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
package org.apache.seatunnel.connectors.seatunnel.canny;

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
import java.util.ArrayList;
import java.util.Base64;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import static org.apache.seatunnel.connectors.seatunnel.canny.CannySourceOptions.BODY_LIMIT;
import static org.apache.seatunnel.connectors.seatunnel.canny.CannySourceOptions.CERTIFICATE;
import static org.apache.seatunnel.connectors.seatunnel.canny.CannySourceOptions.CONNECT_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.canny.CannySourceOptions.INTERVAL;
import static org.apache.seatunnel.connectors.seatunnel.canny.CannySourceOptions.MAX_ITEMS;
import static org.apache.seatunnel.connectors.seatunnel.canny.CannySourceOptions.MAX_PAGES;
import static org.apache.seatunnel.connectors.seatunnel.canny.CannySourceOptions.MAX_SCAN_PAGES;
import static org.apache.seatunnel.connectors.seatunnel.canny.CannySourceOptions.OPERATION_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.canny.CannySourceOptions.PAGE_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.canny.CannySourceOptions.REQUEST_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.canny.CannySourceOptions.RESOURCES;
import static org.apache.seatunnel.connectors.seatunnel.canny.CannySourceOptions.RETRIES;
import static org.apache.seatunnel.connectors.seatunnel.canny.CannySourceOptions.RETRY_DELAY;
import static org.apache.seatunnel.connectors.seatunnel.canny.CannySourceOptions.TOKEN;

/**
 * Validates the workspace credential, paging options and board traversal.
 *
 * <p>Boards are discovered automatically when categories are selected.
 */
final class CannyConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    static final String API_HOST = "canny.io";

    final String token;
    final String certificate;
    private final Set<CannyResource> selected;
    final List<CannyResource> roots;
    final List<CannyResource> children;

    final int pageSize;
    final int maxPages;
    final int scanPages;
    final int maxItems;
    final int connectTimeout;
    final int requestTimeout;
    final int operationTimeout;
    final int interval;
    final int retries;
    final int maxRetryDelay;
    final int bodyLimit;
    final String scope;

    CannyConfig(ReadonlyConfig options) {
        token = options.get(TOKEN);
        if (token == null
                || token.isBlank()
                || token.length() > 8192
                || token.codePoints()
                        .anyMatch(
                                c ->
                                        Character.isISOControl(c)
                                                || c >= Character.MIN_SURROGATE
                                                        && c <= Character.MAX_SURROGATE)) {
            throw invalid(TOKEN);
        }
        selected = selections(options.get(RESOURCES));
        children =
                selected.contains(CannyResource.CATEGORIES)
                        ? List.of(CannyResource.CATEGORIES)
                        : List.of();
        List<CannyResource> rootCollections = new ArrayList<>();
        if (!children.isEmpty() || selected.contains(CannyResource.BOARDS)) {
            rootCollections.add(CannyResource.BOARDS);
        }
        for (CannyResource resource : selected) {
            if (resource != CannyResource.BOARDS && resource != CannyResource.CATEGORIES) {
                rootCollections.add(resource);
            }
        }
        roots = List.copyOf(rootCollections);

        certificate = options.getOptional(CERTIFICATE).orElse(null);
        if (certificate != null) {
            parseCertificate(certificate);
        }
        pageSize = number(options, PAGE_SIZE, 1, 100);
        maxPages = number(options, MAX_PAGES, 1, 100000);
        scanPages = number(options, MAX_SCAN_PAGES, 1, 1000000);
        maxItems = number(options, MAX_ITEMS, 1, 100000);
        connectTimeout = number(options, CONNECT_TIMEOUT, 100, 300000);
        requestTimeout = number(options, REQUEST_TIMEOUT, 100, 300000);
        operationTimeout = number(options, OPERATION_TIMEOUT, requestTimeout, 600000);
        interval = number(options, INTERVAL, 0, 60000);
        retries = number(options, RETRIES, 0, 8);
        maxRetryDelay = number(options, RETRY_DELAY, 250, 600000);
        bodyLimit = number(options, BODY_LIMIT, 256, 16777216);
        scope =
                hash(
                        String.join(
                                "\n",
                                API_HOST,
                                hash(token),
                                selected.toString(),
                                Integer.toString(pageSize)));
    }

    CannySplit newSplit() {
        return new CannySplit(this);
    }

    boolean emits(CannyResource resource) {
        return selected.contains(resource);
    }

    private static Set<CannyResource> selections(List<String> names) {
        if (names.isEmpty()) {
            return EnumSet.allOf(CannyResource.class);
        }
        Set<CannyResource> selected = EnumSet.noneOf(CannyResource.class);
        List<CannyResource> available = List.of(CannyResource.values());
        for (String name : names) {
            CannyResource resource =
                    available.stream()
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
            store.setCertificateEntry("canny", parseCertificate(certificate));
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
            throw new IllegalStateException("Canny SHA-256 is unavailable");
        }
    }

    private static IllegalArgumentException invalid(Option<?> option) {
        return new IllegalArgumentException("Invalid Canny option " + option.key());
    }
}
