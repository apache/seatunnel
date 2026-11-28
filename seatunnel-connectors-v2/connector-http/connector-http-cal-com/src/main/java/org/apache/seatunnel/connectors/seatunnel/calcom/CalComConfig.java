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
package org.apache.seatunnel.connectors.seatunnel.calcom;

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
import java.util.Base64;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

import static org.apache.seatunnel.connectors.seatunnel.calcom.CalComSourceOptions.BODY_LIMIT;
import static org.apache.seatunnel.connectors.seatunnel.calcom.CalComSourceOptions.BOOKING_LIMIT;
import static org.apache.seatunnel.connectors.seatunnel.calcom.CalComSourceOptions.BOOKING_VERSION;
import static org.apache.seatunnel.connectors.seatunnel.calcom.CalComSourceOptions.CERTIFICATE;
import static org.apache.seatunnel.connectors.seatunnel.calcom.CalComSourceOptions.CONNECT_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.calcom.CalComSourceOptions.CURSOR_BOOKINGS;
import static org.apache.seatunnel.connectors.seatunnel.calcom.CalComSourceOptions.EVENT_VERSION;
import static org.apache.seatunnel.connectors.seatunnel.calcom.CalComSourceOptions.FLAT_EVENTS;
import static org.apache.seatunnel.connectors.seatunnel.calcom.CalComSourceOptions.FULL_LIST_LIMIT;
import static org.apache.seatunnel.connectors.seatunnel.calcom.CalComSourceOptions.GROUPED_EVENTS;
import static org.apache.seatunnel.connectors.seatunnel.calcom.CalComSourceOptions.INTERVAL;
import static org.apache.seatunnel.connectors.seatunnel.calcom.CalComSourceOptions.MAX_PAGES;
import static org.apache.seatunnel.connectors.seatunnel.calcom.CalComSourceOptions.MAX_RETRY_DELAY;
import static org.apache.seatunnel.connectors.seatunnel.calcom.CalComSourceOptions.OFFSET_BOOKINGS;
import static org.apache.seatunnel.connectors.seatunnel.calcom.CalComSourceOptions.OPERATION_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.calcom.CalComSourceOptions.REQUEST_TIMEOUT;
import static org.apache.seatunnel.connectors.seatunnel.calcom.CalComSourceOptions.RESOURCES;
import static org.apache.seatunnel.connectors.seatunnel.calcom.CalComSourceOptions.RETRIES;
import static org.apache.seatunnel.connectors.seatunnel.calcom.CalComSourceOptions.SCHEDULE_VERSION;
import static org.apache.seatunnel.connectors.seatunnel.calcom.CalComSourceOptions.TOKEN;

/**
 * Validates the Cal.com credential, exact API versions and batch limits.
 *
 * <p>The scan scope stores a token fingerprint instead of the credential itself.
 */
final class CalComConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    static final String API_HOST = "api.cal.com";

    final String token;
    final String certificate;
    final List<CalComResource> resources;

    // The event response shape and booking continuation protocol are fixed for a scan.
    final String eventVersion;
    final String bookingVersion;
    final int pageSize;
    final int maxItems;
    final int maxPages;

    final int connectTimeout;
    final int requestTimeout;
    final int operationTimeout;
    final int interval;
    final int retries;
    final int maxRetryDelay;
    final int bodyLimit;
    final String scope;

    CalComConfig(ReadonlyConfig options) {
        token = options.get(TOKEN);
        if (token == null
                || !token.startsWith("cal_")
                || token.length() <= 4
                || token.length() > 8192
                || !token.chars().allMatch(c -> c >= 33 && c <= 126)) {
            throw invalid(TOKEN);
        }
        resources = readResources(options.get(RESOURCES));
        eventVersion = choose(options, EVENT_VERSION, GROUPED_EVENTS, FLAT_EVENTS);
        bookingVersion = choose(options, BOOKING_VERSION, CURSOR_BOOKINGS, OFFSET_BOOKINGS);
        certificate = options.getOptional(CERTIFICATE).orElse(null);
        if (certificate != null) {
            parseCertificate(certificate);
        }
        pageSize = number(options, BOOKING_LIMIT, 1, 100);
        maxItems = number(options, FULL_LIST_LIMIT, 1, 100000);
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
                                eventVersion,
                                bookingVersion,
                                Integer.toString(pageSize)));
    }

    CalComSplit newSplit() {
        return new CalComSplit(scope);
    }

    boolean groupedEvents() {
        return GROUPED_EVENTS.equals(eventVersion);
    }

    boolean cursorBookings() {
        return CURSOR_BOOKINGS.equals(bookingVersion);
    }

    String version(CalComResource resource) {
        switch (resource) {
            case EVENT_TYPES:
                return eventVersion;
            case SCHEDULES:
                return SCHEDULE_VERSION;
            case BOOKINGS:
                return bookingVersion;
            default:
                return null;
        }
    }

    int expectedMax(CalComResource resource) {
        return resource == CalComResource.BOOKINGS ? pageSize : maxItems;
    }

    private static String choose(
            ReadonlyConfig options, Option<String> option, String a, String b) {
        String value = options.get(option);
        if (!a.equals(value) && !b.equals(value)) {
            throw invalid(option);
        }
        return value;
    }

    private static List<CalComResource> readResources(List<String> names) {
        if (names.isEmpty()) {
            return List.of(CalComResource.values());
        }
        Set<CalComResource> selected = EnumSet.noneOf(CalComResource.class);
        List<CalComResource> available = List.of(CalComResource.values());
        for (String name : names) {
            CalComResource resource =
                    available.stream()
                            .filter(candidate -> candidate.name.equals(name))
                            .findFirst()
                            .orElseThrow(() -> invalid(RESOURCES));
            if (!selected.add(resource)) {
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
            store.setCertificateEntry("calcom", parseCertificate(certificate));
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
            throw new IllegalStateException("CalCom SHA-256 is unavailable");
        }
    }

    private static IllegalArgumentException invalid(Option<?> option) {
        return new IllegalArgumentException("Invalid CalCom option " + option.key());
    }
}
