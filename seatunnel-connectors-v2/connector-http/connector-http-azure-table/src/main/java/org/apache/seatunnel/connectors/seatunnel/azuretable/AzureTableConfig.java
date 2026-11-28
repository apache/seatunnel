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
package org.apache.seatunnel.connectors.seatunnel.azuretable;

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
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.StringJoiner;
import java.util.TreeMap;

import static org.apache.seatunnel.connectors.seatunnel.azuretable.AzureTableSourceOptions.ACCOUNT;
import static org.apache.seatunnel.connectors.seatunnel.azuretable.AzureTableSourceOptions.BODY_LIMIT;
import static org.apache.seatunnel.connectors.seatunnel.azuretable.AzureTableSourceOptions.CERTIFICATE;
import static org.apache.seatunnel.connectors.seatunnel.azuretable.AzureTableSourceOptions.CONNECT_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.azuretable.AzureTableSourceOptions.INTERVAL;
import static org.apache.seatunnel.connectors.seatunnel.azuretable.AzureTableSourceOptions.MAX_PAGES;
import static org.apache.seatunnel.connectors.seatunnel.azuretable.AzureTableSourceOptions.MAX_TABLES;
import static org.apache.seatunnel.connectors.seatunnel.azuretable.AzureTableSourceOptions.OPERATION_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.azuretable.AzureTableSourceOptions.PAGE_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.azuretable.AzureTableSourceOptions.PARTITION_KEYS;
import static org.apache.seatunnel.connectors.seatunnel.azuretable.AzureTableSourceOptions.REQUEST_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.azuretable.AzureTableSourceOptions.RETRIES;
import static org.apache.seatunnel.connectors.seatunnel.azuretable.AzureTableSourceOptions.ROW_KEYS;
import static org.apache.seatunnel.connectors.seatunnel.azuretable.AzureTableSourceOptions.SECRET;
import static org.apache.seatunnel.connectors.seatunnel.azuretable.AzureTableSourceOptions.SUFFIX;
import static org.apache.seatunnel.connectors.seatunnel.azuretable.AzureTableSourceOptions.TABLES;

/**
 * Validates table selection, key ranges and a fixed Azure cloud endpoint.
 *
 * <p>Credential rotation retains the logical scan scope. Private trust changes affect only this
 * connector's TLS sockets and leave hostname verification enabled.
 */
final class AzureTableConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final Set<String> AZURE_SUFFIXES =
            Set.of("core.windows.net", "core.chinacloudapi.cn", "core.usgovcloudapi.net");

    // The source derives this single HTTPS origin rather than accepting a signing target from data.
    final String account;
    final URI endpoint;
    final byte[] secret;
    final String certificate;
    final List<String> tables;
    final Map<String, Key> boundaries;

    // Discovery is bounded before any entity pages are scheduled.
    final int pageSize;
    final int maxPages;
    final int maxTables;
    final int connectTimeout;
    final int requestTimeout;
    final int operationTimeout;
    final int interval;
    final int retries;
    final int bodyLimit;
    final String scope;

    AzureTableConfig(ReadonlyConfig options) {
        account = options.get(ACCOUNT);
        if (account == null || !account.matches("[a-z0-9]{3,24}")) {
            throw invalid(ACCOUNT);
        }
        String suffix = options.get(SUFFIX);
        if (suffix == null || !AZURE_SUFFIXES.contains(suffix)) {
            throw invalid(SUFFIX);
        }
        endpoint = URI.create("https://" + account + ".table." + suffix);
        secret = readSecret(options.get(SECRET));
        certificate = options.getOptional(CERTIFICATE).orElse(null);
        if (certificate != null) {
            parseCertificate(certificate);
        }
        maxTables = number(options, MAX_TABLES, 1, 100000);
        tables = readTables(options.get(TABLES));
        boundaries = boundaries(options.get(PARTITION_KEYS), options.get(ROW_KEYS));
        pageSize = number(options, PAGE_SIZE, 1, 1000);
        maxPages = number(options, MAX_PAGES, 1, 100000);
        requestTimeout = number(options, REQUEST_TIMEOUT, 100, 300000);
        connectTimeout = number(options, CONNECT_TIMEOUT, 100, 300000);
        operationTimeout = number(options, OPERATION_TIMEOUT, requestTimeout, 600000);
        interval = number(options, INTERVAL, 0, 60000);
        retries = number(options, RETRIES, 0, 8);
        bodyLimit = number(options, BODY_LIMIT, 256, 16777216);
        StringJoiner contract = new StringJoiner("\n");
        contract.add(endpoint.toString()).add(Integer.toString(pageSize)).add(tables.toString());
        new TreeMap<>(boundaries)
                .forEach((table, key) -> contract.add(table).add(key.partition).add(key.row));
        scope = hash(contract.toString());
        if (!tables.isEmpty()) {
            validateBoundaries(tables);
        }
    }

    AzureTableSplit newSplit() {
        return new AzureTableSplit(scope, tables);
    }

    /**
     * Assures that every configured key range has an actual selected table.
     *
     * <p>During auto-discovery this check runs after the account's table listing completes.
     */
    void validateBoundaries(List<String> available) {
        Set<String> selected = new HashSet<>();
        available.forEach(table -> selected.add(normalized(table)));
        if (!selected.containsAll(boundaries.keySet())) {
            throw invalid(PARTITION_KEYS);
        }
    }

    static String normalized(String name) {
        return name.toLowerCase(Locale.ROOT);
    }

    static boolean validTable(String name) {
        return name != null
                && name.matches("[A-Za-z][A-Za-z0-9]{2,62}")
                && !"Tables".equalsIgnoreCase(name);
    }

    static boolean validKey(String value) {
        return value != null
                && value.length() <= 1024
                && value.codePoints()
                        .noneMatch(
                                c ->
                                        Character.isISOControl(c)
                                                || c == '/'
                                                || c == '\\'
                                                || c == '#'
                                                || c == '?'
                                                || c >= Character.MIN_SURROGATE
                                                        && c <= Character.MAX_SURROGATE);
    }

    private List<String> readTables(List<String> names) {
        if (names.size() > maxTables) {
            throw invalid(MAX_TABLES);
        }
        Set<String> matched = new HashSet<>();
        List<String> selected = new ArrayList<>(names.size());
        for (String name : names) {
            if (!validTable(name) || !matched.add(normalized(name))) {
                throw invalid(TABLES);
            }
            selected.add(name);
        }
        return List.copyOf(selected);
    }

    private Map<String, Key> boundaries(Map<String, String> partitions, Map<String, String> rows) {
        Map<String, String> left = readKeyMap(partitions, PARTITION_KEYS);
        Map<String, String> right = readKeyMap(rows, ROW_KEYS);
        if (!left.keySet().equals(right.keySet())) {
            throw invalid(ROW_KEYS);
        }
        Map<String, Key> result = new TreeMap<>();
        left.forEach((name, partition) -> result.put(name, new Key(partition, right.get(name))));
        return Map.copyOf(result);
    }

    private Map<String, String> readKeyMap(Map<String, String> offered, Option<?> option) {
        if (offered.size() > maxTables) {
            throw invalid(option);
        }
        Map<String, String> result = new TreeMap<>();
        offered.forEach(
                (name, value) -> {
                    if (!validTable(name)
                            || !validKey(value)
                            || result.put(normalized(name), value) != null) {
                        throw invalid(option);
                    }
                });
        return result;
    }

    private static byte[] readSecret(String supplied) {
        if (supplied != null && supplied.length() <= 512) {
            try {
                byte[] bytes = Base64.getDecoder().decode(supplied);
                if (bytes.length > 0 && bytes.length <= 256) {
                    return bytes;
                }
            } catch (IllegalArgumentException ex) {
                // A malformed key must not be included in the error or its cause.
            }
        }
        throw invalid(SECRET);
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
            store.setCertificateEntry("azuretable", parseCertificate(certificate));
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

    /**
     * Configured exclusive entity boundary for a particular table.
     *
     * <p>Keys belong in the original OData filter even during provider continuation.
     */
    static final class Key implements Serializable {
        private static final long serialVersionUID = 1L;
        final String partition;
        final String row;

        Key(String partition, String row) {
            this.partition = partition;
            this.row = row;
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
            throw new IllegalStateException("AzureTable SHA-256 is unavailable");
        }
    }

    private static IllegalArgumentException invalid(Option<?> option) {
        return new IllegalArgumentException("Invalid AzureTableStorage option " + option.key());
    }
}
