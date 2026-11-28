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
package org.apache.seatunnel.connectors.seatunnel.brex;

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
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Base64;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import java.util.TreeSet;
import java.util.regex.Pattern;

import static org.apache.seatunnel.connectors.seatunnel.brex.BrexSourceOptions.BODY_LIMIT;
import static org.apache.seatunnel.connectors.seatunnel.brex.BrexSourceOptions.CERTIFICATE;
import static org.apache.seatunnel.connectors.seatunnel.brex.BrexSourceOptions.CONNECT_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.brex.BrexSourceOptions.END;
import static org.apache.seatunnel.connectors.seatunnel.brex.BrexSourceOptions.EXPENSE_RESUME;
import static org.apache.seatunnel.connectors.seatunnel.brex.BrexSourceOptions.INTERVAL;
import static org.apache.seatunnel.connectors.seatunnel.brex.BrexSourceOptions.MAX_PAGES;
import static org.apache.seatunnel.connectors.seatunnel.brex.BrexSourceOptions.MAX_RETRY_DELAY;
import static org.apache.seatunnel.connectors.seatunnel.brex.BrexSourceOptions.OPERATION_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.brex.BrexSourceOptions.PAGE_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.brex.BrexSourceOptions.REQUEST_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.brex.BrexSourceOptions.RESOURCES;
import static org.apache.seatunnel.connectors.seatunnel.brex.BrexSourceOptions.RETRIES;
import static org.apache.seatunnel.connectors.seatunnel.brex.BrexSourceOptions.START;
import static org.apache.seatunnel.connectors.seatunnel.brex.BrexSourceOptions.TOKEN;
import static org.apache.seatunnel.connectors.seatunnel.brex.BrexSourceOptions.TRANSACTION_RESUME;
import static org.apache.seatunnel.connectors.seatunnel.brex.BrexSourceOptions.USER_STATUSES;

/**
 * Validates the Brex token, scan scope and request bounds.
 *
 * <p>Checkpoint scope includes a token fingerprint so restoring a different credential requires the
 * operator to start a new scan. The split never carries the token.
 */
final class BrexConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    static final String API_HOST = "api.brex.com";
    private static final Set<String> STATUSES =
            Set.of("INVITED", "ACTIVE", "DISABLED", "PENDING_ACTIVATION", "INACTIVE", "ARCHIVED");
    private static final Pattern RFC3339 =
            Pattern.compile(
                    "[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}"
                            + "(?:[.][0-9]{1,9})?(?:Z|[+-][0-9]{2}:[0-9]{2})");
    private static final Pattern DATE = Pattern.compile("[0-9]{4}-[0-9]{2}-[0-9]{2}");

    final String token;
    final String certificate;
    final List<BrexResource> resources;
    final List<String> userStatuses;
    final Instant end;

    // The two incremental collections use different native cursor precision and replay windows.
    final Instant transactionStart;
    final Instant expenseStart;

    final int pageSize;
    final int maxPages;
    final int connectTimeout;
    final int requestTimeout;
    final int operationTimeout;
    final int interval;
    final int retries;
    final int maxRetryDelay;
    final int bodyLimit;
    final String scope;

    BrexConfig(ReadonlyConfig options) {
        token = options.get(TOKEN);
        if (token == null
                || token.isEmpty()
                || token.length() > 8192
                || !token.chars().allMatch(c -> c >= 33 && c <= 126)) {
            throw invalid(TOKEN);
        }
        resources = readResources(options.get(RESOURCES));
        userStatuses = readStatuses(options.get(USER_STATUSES));
        if (!userStatuses.isEmpty() && !resources.contains(BrexResource.USERS)) {
            throw invalid(USER_STATUSES);
        }
        Instant start = configuredInstant(options, START);
        end = configuredInstant(options, END);
        if (start == null && resources.stream().anyMatch(BrexResource::dated)) {
            throw invalid(START);
        }
        if (start != null && end != null && end.isBefore(start)) {
            throw invalid(END);
        }
        transactionStart = transactionStart(options, start);
        expenseStart = expenseStart(options, start);
        certificate = options.getOptional(CERTIFICATE).orElse(null);
        if (certificate != null) {
            parseCertificate(certificate);
        }

        pageSize = number(options, PAGE_SIZE, 1, 1000);
        maxPages = number(options, MAX_PAGES, 1, 100000);
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
                                resources.toString(),
                                userStatuses.toString(),
                                String.valueOf(start),
                                String.valueOf(end),
                                String.valueOf(options.get(TRANSACTION_RESUME)),
                                String.valueOf(options.get(EXPENSE_RESUME)),
                                Integer.toString(pageSize)));
    }

    /**
     * Freezes one cutoff before assignment so restores reuse the exact same upper bound.
     *
     * <p>An explicitly supplied end is used as-is. Without one, the coordinator clock is used.
     */
    BrexSplit newSplit(Instant now) {
        return new BrexSplit(scope, end == null ? now : end);
    }

    int limit(BrexResource resource) {
        return Math.min(pageSize, resource.limit);
    }

    private Instant transactionStart(ReadonlyConfig options, Instant start) {
        String supplied = options.get(TRANSACTION_RESUME);
        if (!resources.contains(BrexResource.TRANSACTIONS)) {
            if (supplied != null) {
                throw invalid(TRANSACTION_RESUME);
            }
            return null;
        }
        if (supplied != null) {
            try {
                LocalDate cursor = day(supplied);
                if (end != null && cursor.isAfter(end.atOffset(ZoneOffset.UTC).toLocalDate())) {
                    throw invalid(TRANSACTION_RESUME);
                }
                Instant replay = cursor.minusDays(1).atStartOfDay().toInstant(ZoneOffset.UTC);
                return replay.isAfter(start) ? replay : start;
            } catch (DateTimeException ex) {
                throw invalid(TRANSACTION_RESUME);
            }
        }
        return start;
    }

    private Instant expenseStart(ReadonlyConfig options, Instant start) {
        Instant cursor = configuredInstant(options, EXPENSE_RESUME);
        if (!resources.contains(BrexResource.EXPENSES)) {
            if (cursor != null) {
                throw invalid(EXPENSE_RESUME);
            }
            return null;
        }
        if (cursor == null) {
            return start;
        }
        if (end != null && cursor.isAfter(end)) {
            throw invalid(EXPENSE_RESUME);
        }
        Instant replay = cursor.atOffset(ZoneOffset.UTC).minusMonths(1).toInstant();
        return replay.isAfter(start) ? replay : start;
    }

    static Instant instant(String value) {
        if (!RFC3339.matcher(value).matches()) {
            throw new DateTimeException("invalid RFC 3339 timestamp");
        }
        OffsetDateTime timestamp = OffsetDateTime.parse(value);
        int year = timestamp.withOffsetSameInstant(ZoneOffset.UTC).getYear();
        if (year < 1 || year > 9999) {
            throw new DateTimeException("invalid UTC year");
        }
        return timestamp.toInstant();
    }

    static LocalDate day(String value) {
        if (!DATE.matcher(value).matches()) {
            throw new DateTimeException("invalid ISO date");
        }
        LocalDate date = LocalDate.parse(value);
        if (date.getYear() == 0) {
            throw new DateTimeException("invalid calendar year");
        }
        return date;
    }

    private static Instant configuredInstant(ReadonlyConfig options, Option<String> option) {
        String value = options.get(option);
        try {
            return value == null ? null : instant(value);
        } catch (DateTimeException ex) {
            throw invalid(option);
        }
    }

    private static List<BrexResource> readResources(List<String> names) {
        if (names.isEmpty()) {
            return List.of(BrexResource.values());
        }
        Set<BrexResource> selected = EnumSet.noneOf(BrexResource.class);
        for (String name : names) {
            try {
                if (name == null
                        || !selected.add(BrexResource.valueOf(name.toUpperCase(Locale.ROOT)))) {
                    throw invalid(RESOURCES);
                }
            } catch (IllegalArgumentException ex) {
                throw invalid(RESOURCES);
            }
        }
        return List.copyOf(selected);
    }

    private static List<String> readStatuses(List<String> names) {
        Set<String> selected = new TreeSet<>();
        for (String name : names) {
            if (name == null || !STATUSES.contains(name) || !selected.add(name)) {
                throw invalid(USER_STATUSES);
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
            store.setCertificateEntry("brex", parseCertificate(certificate));
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
            throw new IllegalStateException("Brex SHA-256 is unavailable");
        }
    }

    private static IllegalArgumentException invalid(Option<?> option) {
        return new IllegalArgumentException("Invalid Brex option " + option.key());
    }
}
