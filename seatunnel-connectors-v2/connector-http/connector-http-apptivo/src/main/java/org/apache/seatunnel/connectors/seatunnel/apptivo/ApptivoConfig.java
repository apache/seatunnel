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
package org.apache.seatunnel.connectors.seatunnel.apptivo;

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
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

import static org.apache.seatunnel.connectors.seatunnel.apptivo.ApptivoSourceOptions.ACCESS_KEY;
import static org.apache.seatunnel.connectors.seatunnel.apptivo.ApptivoSourceOptions.API;
import static org.apache.seatunnel.connectors.seatunnel.apptivo.ApptivoSourceOptions.BODY_LIMIT;
import static org.apache.seatunnel.connectors.seatunnel.apptivo.ApptivoSourceOptions.CERTIFICATE;
import static org.apache.seatunnel.connectors.seatunnel.apptivo.ApptivoSourceOptions.FIRM;
import static org.apache.seatunnel.connectors.seatunnel.apptivo.ApptivoSourceOptions.INTERVAL;
import static org.apache.seatunnel.connectors.seatunnel.apptivo.ApptivoSourceOptions.MAX_PAGES;
import static org.apache.seatunnel.connectors.seatunnel.apptivo.ApptivoSourceOptions.OPERATION_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.apptivo.ApptivoSourceOptions.PAGE_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.apptivo.ApptivoSourceOptions.REQUEST_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.apptivo.ApptivoSourceOptions.RESOURCES;
import static org.apache.seatunnel.connectors.seatunnel.apptivo.ApptivoSourceOptions.RETRIES;
import static org.apache.seatunnel.connectors.seatunnel.apptivo.ApptivoSourceOptions.TOKEN;

/**
 * Validates source scope, credentials, request limits and private TLS settings.
 *
 * <p>Serialized split scope excludes credential values and hashes. The job credential options are
 * retained by the source only for its authenticated reads.
 */
final class ApptivoConfig implements Serializable {
    private static final long serialVersionUID = 1L;

    // Credentials are encoded as DAO query values only after the fixed TLS origin is validated.
    final URI api;
    final String token;
    final String accessKey;
    final String firm;
    final String certificate;
    final List<ApptivoResource> resources;

    // Request attempts and a full retry sequence have independent time bounds.
    final int pageSize;
    final int maxPages;
    final int requestTimeout;
    final int operationTimeout;
    final int interval;
    final int retries;
    final int bodyLimit;

    // Binds logical states to the same origin, tenant option, selected resources and page size.
    final String scope;

    ApptivoConfig(ReadonlyConfig options) {
        api = origin(options.get(API));
        token = credential(TOKEN, options.get(TOKEN));
        accessKey = credential(ACCESS_KEY, options.get(ACCESS_KEY));
        firm = options.getOptional(FIRM).orElse(null);
        if (firm != null && !firm.matches("[1-9][0-9]{0,95}")) {
            throw invalid(FIRM);
        }
        certificate = options.getOptional(CERTIFICATE).orElse(null);
        if (certificate != null) {
            parseCertificate(certificate);
        }
        resources = readResources(options.get(RESOURCES));
        pageSize = number(options, PAGE_SIZE, 1, 100);
        maxPages = number(options, MAX_PAGES, 1, 10000);
        requestTimeout = number(options, REQUEST_TIMEOUT, 100, 300000);
        operationTimeout = number(options, OPERATION_TIMEOUT, requestTimeout, 600000);
        interval = number(options, INTERVAL, 0, 60000);
        retries = number(options, RETRIES, 0, 8);
        bodyLimit = number(options, BODY_LIMIT, 256, 16777216);
        scope =
                hash(
                        String.join(
                                "\n",
                                api.toString(),
                                String.valueOf(firm),
                                resources.toString(),
                                String.valueOf(pageSize)));
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
            store.setCertificateEntry("apptivo", parseCertificate(certificate));
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

    private static URI origin(String value) {
        try {
            URI uri = URI.create(value);
            if ("https".equalsIgnoreCase(uri.getScheme())
                    && uri.getHost() != null
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
        throw invalid(API);
    }

    private static List<ApptivoResource> readResources(List<String> names) {
        if (names.isEmpty()) {
            return List.of(ApptivoResource.values());
        }
        Set<ApptivoResource> result = new LinkedHashSet<>();
        for (String name : names) {
            try {
                if (!result.add(ApptivoResource.valueOf(name.toUpperCase(Locale.ROOT)))) {
                    throw invalid(RESOURCES);
                }
            } catch (IllegalArgumentException ex) {
                throw invalid(RESOURCES);
            }
        }
        return Collections.unmodifiableList(new ArrayList<>(result));
    }

    private static String credential(Option<String> key, String value) {
        if (value == null
                || value.isEmpty()
                || value.length() > 2048
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
            throw new IllegalStateException("Apptivo SHA-256 is unavailable");
        }
    }

    private static IllegalArgumentException invalid(Option<?> option) {
        return new IllegalArgumentException("Invalid Apptivo option " + option.key());
    }
}
