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
package org.apache.seatunnel.connectors.seatunnel.microsoftadvertising;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

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
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDate;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.Base64;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import static org.apache.seatunnel.connectors.seatunnel.microsoftadvertising.MicrosoftAdvertisingSourceOptions.ACCOUNTS;
import static org.apache.seatunnel.connectors.seatunnel.microsoftadvertising.MicrosoftAdvertisingSourceOptions.ACCOUNT_NAMES;
import static org.apache.seatunnel.connectors.seatunnel.microsoftadvertising.MicrosoftAdvertisingSourceOptions.ACCOUNT_PAGES;
import static org.apache.seatunnel.connectors.seatunnel.microsoftadvertising.MicrosoftAdvertisingSourceOptions.BULK_FROM;
import static org.apache.seatunnel.connectors.seatunnel.microsoftadvertising.MicrosoftAdvertisingSourceOptions.CERTIFICATE;
import static org.apache.seatunnel.connectors.seatunnel.microsoftadvertising.MicrosoftAdvertisingSourceOptions.CLIENT_ID;
import static org.apache.seatunnel.connectors.seatunnel.microsoftadvertising.MicrosoftAdvertisingSourceOptions.CLIENT_SECRET;
import static org.apache.seatunnel.connectors.seatunnel.microsoftadvertising.MicrosoftAdvertisingSourceOptions.COMPRESSED_BYTES;
import static org.apache.seatunnel.connectors.seatunnel.microsoftadvertising.MicrosoftAdvertisingSourceOptions.CONNECT_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.microsoftadvertising.MicrosoftAdvertisingSourceOptions.CSV_COLUMNS;
import static org.apache.seatunnel.connectors.seatunnel.microsoftadvertising.MicrosoftAdvertisingSourceOptions.CSV_ROW;
import static org.apache.seatunnel.connectors.seatunnel.microsoftadvertising.MicrosoftAdvertisingSourceOptions.CSV_ROWS;
import static org.apache.seatunnel.connectors.seatunnel.microsoftadvertising.MicrosoftAdvertisingSourceOptions.CUSTOM_REPORTS;
import static org.apache.seatunnel.connectors.seatunnel.microsoftadvertising.MicrosoftAdvertisingSourceOptions.DEVELOPER_TOKEN;
import static org.apache.seatunnel.connectors.seatunnel.microsoftadvertising.MicrosoftAdvertisingSourceOptions.DOWNLOAD_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.microsoftadvertising.MicrosoftAdvertisingSourceOptions.END;
import static org.apache.seatunnel.connectors.seatunnel.microsoftadvertising.MicrosoftAdvertisingSourceOptions.EXPANDED_BYTES;
import static org.apache.seatunnel.connectors.seatunnel.microsoftadvertising.MicrosoftAdvertisingSourceOptions.JSON_BYTES;
import static org.apache.seatunnel.connectors.seatunnel.microsoftadvertising.MicrosoftAdvertisingSourceOptions.LOOKBACK;
import static org.apache.seatunnel.connectors.seatunnel.microsoftadvertising.MicrosoftAdvertisingSourceOptions.POLL_INTERVAL;
import static org.apache.seatunnel.connectors.seatunnel.microsoftadvertising.MicrosoftAdvertisingSourceOptions.POLL_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.microsoftadvertising.MicrosoftAdvertisingSourceOptions.REFRESH_TOKEN;
import static org.apache.seatunnel.connectors.seatunnel.microsoftadvertising.MicrosoftAdvertisingSourceOptions.REQUEST_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.microsoftadvertising.MicrosoftAdvertisingSourceOptions.RESOURCES;
import static org.apache.seatunnel.connectors.seatunnel.microsoftadvertising.MicrosoftAdvertisingSourceOptions.RESUME;
import static org.apache.seatunnel.connectors.seatunnel.microsoftadvertising.MicrosoftAdvertisingSourceOptions.RETRIES;
import static org.apache.seatunnel.connectors.seatunnel.microsoftadvertising.MicrosoftAdvertisingSourceOptions.RETRY_DELAY;
import static org.apache.seatunnel.connectors.seatunnel.microsoftadvertising.MicrosoftAdvertisingSourceOptions.START;
import static org.apache.seatunnel.connectors.seatunnel.microsoftadvertising.MicrosoftAdvertisingSourceOptions.TENANT;

/**
 * Validated credentials, scan selections and resource budgets.
 *
 * <p>Only nonsecret scan semantics contribute to checkpoint compatibility. A restored reader
 * rechecks the current OAuth user before polling an existing provider request.
 */
final class MicrosoftAdvertisingConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    static final ZoneId REPORT_ZONE = ZoneId.of("Europe/London");
    static final int SEARCH_SIZE = 1000;
    static final String TOKEN_HOST = "login.microsoftonline.com";
    static final String CUSTOMER_HOST = "clientcenter.api.bingads.microsoft.com";
    static final String CAMPAIGN_HOST = "campaign.api.bingads.microsoft.com";
    static final String REPORT_HOST = "reporting.api.bingads.microsoft.com";
    static final String BULK_HOST = "bulk.api.bingads.microsoft.com";

    final String tenant;
    final String clientId;
    final String clientSecret;
    final String refreshToken;
    final String developerToken;
    final String certificate;
    final List<AccountName> accountNames;
    final List<MicrosoftAdvertisingCatalog.Resource> resources;
    final String scope;
    final boolean emitAccounts;

    private final LocalDate start;
    private final LocalDate end;
    private final LocalDate resume;
    private final Instant bulkFrom;
    private final int lookback;
    final int connectTimeout;
    final int requestTimeout;
    final int downloadTimeout;
    final int pollInterval;
    final int pollTimeout;
    final int retries;
    final int retryDelay;
    final int jsonBytes;
    final int compressedBytes;
    final int expandedBytes;
    final int csvRow;
    final int csvColumns;
    final int csvRows;
    final int accountPages;
    final int accounts;

    MicrosoftAdvertisingConfig(ReadonlyConfig options) {
        tenant = options.get(TENANT);
        if (tenant == null
                || !tenant.matches("[A-Za-z0-9][A-Za-z0-9.-]{0,252}")
                || tenant.endsWith(".")
                || tenant.contains("..")) {
            throw invalid(TENANT);
        }
        clientId = credential(CLIENT_ID, options.get(CLIENT_ID));
        refreshToken = credential(REFRESH_TOKEN, options.get(REFRESH_TOKEN));
        clientSecret = options.get(CLIENT_SECRET);
        if (clientSecret == null || !clientSecret.isEmpty()) {
            credential(CLIENT_SECRET, clientSecret);
        }
        developerToken = options.get(DEVELOPER_TOKEN);
        if (!headerToken(developerToken)) {
            throw invalid(DEVELOPER_TOKEN);
        }
        certificate = options.get(CERTIFICATE);
        if (certificate != null) {
            parseCertificate(certificate);
        }
        accountNames = accountNames(options.get(ACCOUNT_NAMES));
        resources =
                MicrosoftAdvertisingCatalog.select(
                        options.get(RESOURCES), options.get(CUSTOM_REPORTS));
        emitAccounts = resources.stream().anyMatch(resource -> resource.name.equals("accounts"));

        start = date(options, START);
        end = date(options, END);
        resume = date(options, RESUME);
        bulkFrom = instant(options, BULK_FROM);
        if (start != null && end != null && start.isAfter(end)
                || resume != null && end != null && resume.isAfter(end)) {
            throw invalid(END);
        }
        lookback = limit(options, LOOKBACK, 0, 90);
        if (lookback > 0 && resume == null) {
            throw invalid(LOOKBACK);
        }
        connectTimeout = limit(options, CONNECT_TIMEOUT, 100, 300000);
        requestTimeout = limit(options, REQUEST_TIMEOUT, 100, 600000);
        downloadTimeout = limit(options, DOWNLOAD_TIMEOUT, 100, 1800000);
        pollInterval = limit(options, POLL_INTERVAL, 10, 60000);
        pollTimeout = limit(options, POLL_TIMEOUT, 100, 43200000);
        retries = limit(options, RETRIES, 0, 8);
        retryDelay = limit(options, RETRY_DELAY, 0, 60000);
        jsonBytes = limit(options, JSON_BYTES, 256, 33554432);
        compressedBytes = limit(options, COMPRESSED_BYTES, 256, 1073741824);
        expandedBytes = limit(options, EXPANDED_BYTES, 256, 1073741824);
        csvRow = limit(options, CSV_ROW, 64, 1048576);
        csvColumns = limit(options, CSV_COLUMNS, 1, 1024);
        csvRows = limit(options, CSV_ROWS, 1, 20000000);
        accountPages = limit(options, ACCOUNT_PAGES, 1, 100000);
        accounts = limit(options, ACCOUNTS, 1, 100000);

        StringBuilder binding = new StringBuilder();
        field(binding, tenant);
        field(binding, clientId);
        field(binding, start);
        field(binding, end);
        field(binding, resume);
        field(binding, bulkFrom);
        field(binding, lookback);
        for (AccountName predicate : accountNames) {
            field(binding, predicate.operator);
            field(binding, predicate.name);
        }
        for (MicrosoftAdvertisingCatalog.Resource resource : resources) {
            field(binding, resource.name);
            field(binding, resource.requestType);
            field(binding, resource.aggregation);
            field(binding, resource.columns);
            field(binding, resource.predefinedTime);
        }
        scope = hex(digest().digest(binding.toString().getBytes(StandardCharsets.UTF_8)));
    }

    /**
     * Freezes report dates and the configured bulk delta boundary before the first HTTP request.
     *
     * <p>Retention uses the London calendar date at the start of this scan.
     */
    MicrosoftAdvertisingSplit newSplit(Instant now) {
        LocalDate today = now.atZone(REPORT_ZONE).toLocalDate();
        LocalDate lower = start == null ? today.minusYears(1).withDayOfYear(1) : start;
        LocalDate upper = end == null ? today : end;
        if (upper.isAfter(today)
                || lower.isAfter(upper)
                || resume != null && resume.isAfter(upper)) {
            throw invalid(END);
        }
        if (resume != null && resume.minusDays(lookback).isAfter(lower)) {
            lower = resume.minusDays(lookback);
        }
        Instant delta = bulkFrom;
        if (delta == null && start != null) {
            delta = start.atStartOfDay(REPORT_ZONE).toInstant();
        }
        if (delta != null && delta.isAfter(now)) {
            throw invalid(BULK_FROM);
        }
        return new MicrosoftAdvertisingSplit(scope, today, lower, upper, delta);
    }

    static Instant supportedDelta(Instant seed, Instant now) {
        return seed != null
                        && !seed.isBefore(
                                now.minus(Duration.ofDays(30)).plus(Duration.ofMinutes(5)))
                ? seed
                : null;
    }

    private static List<AccountName> accountNames(List<JsonNode> input) {
        if (input.size() > 100) {
            throw invalid(ACCOUNT_NAMES);
        }
        Set<List<String>> seen = new HashSet<>();
        List<AccountName> result = new ArrayList<>();
        for (JsonNode entry : input) {
            JsonNode value = entry.path("name");
            String name = value.asText("");
            String operator = entry.path("operator").asText("");
            if (!entry.isObject()
                    || !value.isTextual()
                    || name.isBlank()
                    || name.length() > 512
                    || name.codePoints().anyMatch(Character::isISOControl)
                    || !Set.of("Equals", "Contains").contains(operator)
                    || !seen.add(List.of(operator, name))) {
                throw invalid(ACCOUNT_NAMES);
            }
            result.add(new AccountName(operator, name));
        }
        return List.copyOf(result);
    }

    private static void field(StringBuilder out, Object value) {
        String label = Objects.toString(value, "");
        out.append(label.length()).append(':').append(label);
    }

    static boolean headerToken(String value) {
        return value != null
                && !value.isEmpty()
                && value.length() <= 8192
                && value.chars().allMatch(c -> c >= 33 && c < 127);
    }

    static boolean opaque(String value, int limit) {
        return value != null
                && !value.isBlank()
                && value.length() <= limit
                && value.codePoints()
                        .noneMatch(
                                c ->
                                        Character.isISOControl(c)
                                                || c >= Character.MIN_SURROGATE
                                                        && c <= Character.MAX_SURROGATE);
    }

    private static String credential(Option<String> option, String value) {
        if (!opaque(value, 16384)) {
            throw invalid(option);
        }
        return value;
    }

    private static LocalDate date(ReadonlyConfig options, Option<String> option) {
        String value = options.get(option);
        try {
            if (value == null) {
                return null;
            }
            if (value.matches("[0-9]{4}-[0-9]{2}-[0-9]{2}")) {
                LocalDate date = LocalDate.parse(value);
                if (date.getYear() >= 1900) {
                    return date;
                }
            }
        } catch (DateTimeException ex) {
            throw invalid(option);
        }
        throw invalid(option);
    }

    private static Instant instant(ReadonlyConfig options, Option<String> option) {
        String value = options.get(option);
        try {
            return value == null ? null : OffsetDateTime.parse(value).toInstant();
        } catch (DateTimeException ex) {
            throw invalid(option);
        }
    }

    private static int limit(ReadonlyConfig options, Option<Integer> option, int min, int max) {
        int value = options.get(option);
        if (value < min || value > max) {
            throw invalid(option);
        }
        return value;
    }

    /**
     * Installs only an explicitly configured trust anchor in a reader-private TLS context.
     *
     * <p>Every socket still checks its requested hostname.
     */
    SSLSocketFactory socketFactory() {
        if (certificate == null) {
            return (SSLSocketFactory) SSLSocketFactory.getDefault();
        }
        try {
            KeyStore store = KeyStore.getInstance("PKCS12");
            store.load(null, null);
            store.setCertificateEntry("microsoft-advertising", parseCertificate(certificate));
            TrustManagerFactory manager =
                    TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
            manager.init(store);
            SSLContext tls = SSLContext.getInstance("TLS");
            tls.init(null, manager.getTrustManagers(), null);
            return tls.getSocketFactory();
        } catch (GeneralSecurityException | IOException ex) {
            throw invalid(CERTIFICATE);
        }
    }

    private static Certificate parseCertificate(String supplied) {
        if (supplied.length() > 65536) {
            throw invalid(CERTIFICATE);
        }
        try {
            String body =
                    supplied.trim()
                            .replace("-----BEGIN CERTIFICATE-----", "")
                            .replace("-----END CERTIFICATE-----", "")
                            .replaceAll("\\s+", "");
            ByteArrayInputStream input = new ByteArrayInputStream(Base64.getDecoder().decode(body));
            Certificate cert = CertificateFactory.getInstance("X.509").generateCertificate(input);
            if (input.available() != 0) {
                throw invalid(CERTIFICATE);
            }
            return cert;
        } catch (GeneralSecurityException | IllegalArgumentException ex) {
            throw invalid(CERTIFICATE);
        }
    }

    static MessageDigest digest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException ex) {
            throw new IllegalStateException("Microsoft Advertising requires SHA-256");
        }
    }

    static String hex(byte[] bytes) {
        char[] alphabet = "0123456789abcdef".toCharArray();
        char[] answer = new char[2 * bytes.length];
        for (int i = 0; i < bytes.length; ++i) {
            answer[2 * i] = alphabet[(bytes[i] & 255) >>> 4];
            answer[2 * i + 1] = alphabet[bytes[i] & 15];
        }
        return new String(answer);
    }

    private static IllegalArgumentException invalid(Option<?> option) {
        return new IllegalArgumentException("Invalid MicrosoftAdvertising option " + option.key());
    }

    /**
     * One SearchAccounts predicate evaluated alongside the OAuth user filter.
     *
     * <p>Multiple predicates are separate requests whose results are combined by account ID.
     */
    static final class AccountName implements Serializable {
        private static final long serialVersionUID = 1L;

        final String operator;
        final String name;

        AccountName(String operator, String name) {
            this.operator = operator;
            this.name = name;
        }
    }
}
