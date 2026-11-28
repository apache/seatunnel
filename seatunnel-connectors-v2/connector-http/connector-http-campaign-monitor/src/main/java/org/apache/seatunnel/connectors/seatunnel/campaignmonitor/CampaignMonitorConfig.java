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
package org.apache.seatunnel.connectors.seatunnel.campaignmonitor;

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
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Base64;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;

import static org.apache.seatunnel.connectors.seatunnel.campaignmonitor.CampaignMonitorSourceOptions.BODY_LIMIT;
import static org.apache.seatunnel.connectors.seatunnel.campaignmonitor.CampaignMonitorSourceOptions.CERTIFICATE;
import static org.apache.seatunnel.connectors.seatunnel.campaignmonitor.CampaignMonitorSourceOptions.CONNECT_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.campaignmonitor.CampaignMonitorSourceOptions.END;
import static org.apache.seatunnel.connectors.seatunnel.campaignmonitor.CampaignMonitorSourceOptions.INTERVAL;
import static org.apache.seatunnel.connectors.seatunnel.campaignmonitor.CampaignMonitorSourceOptions.MAX_ITEMS;
import static org.apache.seatunnel.connectors.seatunnel.campaignmonitor.CampaignMonitorSourceOptions.MAX_PAGES;
import static org.apache.seatunnel.connectors.seatunnel.campaignmonitor.CampaignMonitorSourceOptions.MAX_SCAN_PAGES;
import static org.apache.seatunnel.connectors.seatunnel.campaignmonitor.CampaignMonitorSourceOptions.OPERATION_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.campaignmonitor.CampaignMonitorSourceOptions.PAGE_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.campaignmonitor.CampaignMonitorSourceOptions.REQUEST_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.campaignmonitor.CampaignMonitorSourceOptions.RESOURCES;
import static org.apache.seatunnel.connectors.seatunnel.campaignmonitor.CampaignMonitorSourceOptions.RETRIES;
import static org.apache.seatunnel.connectors.seatunnel.campaignmonitor.CampaignMonitorSourceOptions.RETRY_DELAY;
import static org.apache.seatunnel.connectors.seatunnel.campaignmonitor.CampaignMonitorSourceOptions.START;
import static org.apache.seatunnel.connectors.seatunnel.campaignmonitor.CampaignMonitorSourceOptions.TOKEN;

/**
 * Validates the account credential, resource traversal and campaign date filters.
 *
 * <p>Client discovery is included automatically when a selected resource needs its client ID.
 */
final class CampaignMonitorConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    static final String API_HOST = "api.createsend.com";
    private static final Pattern DATE = Pattern.compile("[0-9]{4}-[0-9]{2}-[0-9]{2}");

    final String token;
    final String certificate;
    private final Set<CampaignMonitorResource> selected;
    final List<CampaignMonitorResource> roots;
    final List<CampaignMonitorResource> children;

    // These date-only boundaries are interpreted in the provider's campaign calendar.
    final LocalDate start;
    final LocalDate end;

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

    CampaignMonitorConfig(ReadonlyConfig options) {
        token = options.get(TOKEN);
        if (token == null
                || token.isEmpty()
                || token.length() > 8192
                || !token.chars().allMatch(c -> c >= 33 && c <= 126 && c != ':')) {
            throw invalid(TOKEN);
        }
        selected = selections(options.get(RESOURCES));
        List<CampaignMonitorResource> clientCollections = new ArrayList<>();
        for (CampaignMonitorResource resource : selected) {
            if (resource.scoped) {
                clientCollections.add(resource);
            }
        }
        children = List.copyOf(clientCollections);
        List<CampaignMonitorResource> rootCollections = new ArrayList<>(2);
        if (selected.contains(CampaignMonitorResource.CLIENTS) || !children.isEmpty()) {
            rootCollections.add(CampaignMonitorResource.CLIENTS);
        }
        if (selected.contains(CampaignMonitorResource.ADMINS)) {
            rootCollections.add(CampaignMonitorResource.ADMINS);
        }
        roots = List.copyOf(rootCollections);

        start = date(options, START);
        end = date(options, END);
        if (selected.contains(CampaignMonitorResource.SENT_CAMPAIGNS) && start == null) {
            throw invalid(START);
        }
        if (start != null && end != null && !end.isAfter(start)) {
            throw invalid(END);
        }

        certificate = options.getOptional(CERTIFICATE).orElse(null);
        if (certificate != null) {
            parseCertificate(certificate);
        }
        pageSize = number(options, PAGE_SIZE, 10, 1000);
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
                                String.valueOf(start),
                                String.valueOf(end),
                                Integer.toString(pageSize)));
    }

    CampaignMonitorSplit newSplit() {
        return new CampaignMonitorSplit(this);
    }

    boolean emits(CampaignMonitorResource resource) {
        return selected.contains(resource);
    }

    private static Set<CampaignMonitorResource> selections(List<String> names) {
        if (names.isEmpty()) {
            return EnumSet.allOf(CampaignMonitorResource.class);
        }
        Set<CampaignMonitorResource> selected = EnumSet.noneOf(CampaignMonitorResource.class);
        List<CampaignMonitorResource> supported = List.of(CampaignMonitorResource.values());
        for (String name : names) {
            CampaignMonitorResource resource =
                    supported.stream()
                            .filter(candidate -> candidate.name.equals(name))
                            .findFirst()
                            .orElseThrow(() -> invalid(RESOURCES));
            if (!selected.add(resource)) {
                throw invalid(RESOURCES);
            }
        }
        return selected;
    }

    private static LocalDate date(ReadonlyConfig options, Option<String> option) {
        String supplied = options.get(option);
        if (supplied == null) {
            return null;
        }
        try {
            if (!DATE.matcher(supplied).matches()) {
                throw invalid(option);
            }
            LocalDate date = LocalDate.parse(supplied);
            if (date.getYear() < 1) {
                throw invalid(option);
            }
            return date;
        } catch (DateTimeException ex) {
            throw invalid(option);
        }
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
            store.setCertificateEntry("campaignmonitor", parseCertificate(certificate));
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
            throw new IllegalStateException("CampaignMonitor SHA-256 is unavailable");
        }
    }

    private static IllegalArgumentException invalid(Option<?> option) {
        return new IllegalArgumentException("Invalid CampaignMonitor option " + option.key());
    }
}
