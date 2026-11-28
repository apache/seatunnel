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
package org.apache.seatunnel.connectors.seatunnel.aircall;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.DateTimeException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;

final class AircallConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    static final long DAY = 86400;
    static final long EARLIEST = 1388534400;
    static final long LATEST = 4102444800L;

    final String host;
    final int port;
    final String authority;
    final String authorization;
    final List<Group> groups;
    private final Long configuredStart;
    private final Long configuredEnd;
    private final int lookback;
    private final int callDays;
    private final int contactDays;
    private final int conversationDays;
    final int pageSize;
    final int smsLimit;
    final int maxPages;
    final int minInterval;
    final int attempts;
    final int timeout;
    final int deadline;
    final int maxBytes;
    private final byte[] identity;

    AircallConfig(ReadonlyConfig config) {
        URI origin = origin(config.get(AircallSourceOptions.BASE_URL));
        host = origin.getHost().toLowerCase(Locale.ROOT);
        port = origin.getPort() < 0 ? 443 : origin.getPort();
        authority = port == 443 ? host : host + ":" + port;
        String id = credential(config, AircallSourceOptions.USERNAME);
        if (id.indexOf(':') >= 0) {
            throw invalid(AircallSourceOptions.USERNAME);
        }
        String password = credential(config, AircallSourceOptions.PASSWORD);
        authorization =
                "Basic "
                        + Base64.getEncoder()
                                .encodeToString(
                                        (id + ":" + password).getBytes(StandardCharsets.US_ASCII));

        List<String> choices = config.get(AircallSourceOptions.RESOURCES);
        boolean intelligence = config.get(AircallSourceOptions.INCLUDE_CONVERSATION_INTELLIGENCE);
        Set<AircallResource> selected = selection(choices, intelligence);
        List<Group> plan = new ArrayList<>();
        for (AircallResource root : AircallResource.values()) {
            if (root.parent != null) {
                continue;
            }
            List<AircallResource> children = new ArrayList<>();
            for (AircallResource item : selected) {
                if (item.parent == root) {
                    children.add(item);
                }
            }
            if (selected.contains(root) || !children.isEmpty()) {
                plan.add(new Group(root, selected.contains(root), children));
            }
        }
        groups = Collections.unmodifiableList(plan);

        configuredStart = time(config, AircallSourceOptions.START_TIME);
        configuredEnd = time(config, AircallSourceOptions.END_TIME);
        if (configuredStart != null && configuredEnd != null && configuredStart >= configuredEnd) {
            throw invalid(AircallSourceOptions.END_TIME);
        }
        lookback = integer(config, AircallSourceOptions.LOOKBACK_DAYS, 0, 30);
        callDays = integer(config, AircallSourceOptions.CALL_WINDOW_DAYS, 1, 365);
        contactDays = integer(config, AircallSourceOptions.CONTACT_WINDOW_DAYS, 1, 365);
        conversationDays = integer(config, AircallSourceOptions.CONVERSATION_WINDOW_DAYS, 1, 365);
        pageSize = integer(config, AircallSourceOptions.PAGE_SIZE, 1, 50);
        smsLimit = integer(config, AircallSourceOptions.SMS_PAGE_SIZE, 1, 100);
        maxPages = integer(config, AircallSourceOptions.MAX_PAGES, 1, 10000);
        minInterval = integer(config, AircallSourceOptions.REQUEST_INTERVAL_MS, 500, 60000);
        attempts = integer(config, AircallSourceOptions.MAX_ATTEMPTS, 1, 8);
        timeout = integer(config, AircallSourceOptions.SOCKET_TIMEOUT_MS, 100, 120000);
        deadline = integer(config, AircallSourceOptions.PAGE_DEADLINE_MS, 500, 600000);
        maxBytes = integer(config, AircallSourceOptions.MAX_RESPONSE_BYTES, 128, 33554432);
        String scope =
                authority
                        + "\n"
                        + selected
                        + "\n"
                        + configuredStart
                        + "\n"
                        + configuredEnd
                        + "\n"
                        + lookback
                        + ":"
                        + callDays
                        + ":"
                        + contactDays
                        + ":"
                        + conversationDays
                        + ":"
                        + pageSize
                        + ":"
                        + smsLimit;
        identity = digest().digest(scope.getBytes(StandardCharsets.UTF_8));
    }

    private static Set<AircallResource> selection(List<String> choices, boolean intelligence) {
        if (choices == null
                || choices.size() > AircallResource.values().length
                || new HashSet<>(choices).size() != choices.size()) {
            throw invalid(AircallSourceOptions.RESOURCES);
        }
        Set<AircallResource> out = EnumSet.noneOf(AircallResource.class);
        for (AircallResource value : AircallResource.values()) {
            if (choices.isEmpty()
                    ? !value.intelligence || intelligence
                    : choices.contains(value.label())) {
                if (value.intelligence && !intelligence) {
                    throw invalid(AircallSourceOptions.INCLUDE_CONVERSATION_INTELLIGENCE);
                }
                out.add(value);
            }
        }
        if (!choices.isEmpty() && choices.size() != out.size()) {
            throw invalid(AircallSourceOptions.RESOURCES);
        }
        return out;
    }

    AircallSplit start(Instant wallClock) {
        long frozen = configuredEnd == null ? wallClock.getEpochSecond() : configuredEnd;
        if (frozen <= EARLIEST
                || frozen > LATEST
                || (configuredStart != null && configuredStart >= frozen)) {
            throw invalid(AircallSourceOptions.END_TIME);
        }
        AircallSplit state = new AircallSplit(identity, frozen);
        state.nextGroup(this);
        return state;
    }

    long first(AircallResource root, long end) {
        long nominal =
                configuredStart != null
                        ? configuredStart
                        : root == AircallResource.CALLS ? end - 365 * DAY : EARLIEST;
        return Math.max(
                EARLIEST, nominal - ((root == AircallResource.CONTACTS) ? 0 : lookback * DAY));
    }

    long window(AircallResource root) {
        switch (root) {
            case CALLS:
                return callDays * DAY;
            case CONTACTS:
                return contactDays * DAY;
            case CONVERSATIONS:
                return conversationDays * DAY;
            case NUMBERS:
            case USERS:
            case USER_AVAILABILITY:
                return 30 * DAY;
            default:
                throw new IllegalArgumentException("Aircall resource has no time window");
        }
    }

    void validate(AircallSplit saved) {
        if (saved == null
                || saved.version != 1
                || !MessageDigest.isEqual(identity, saved.identity)
                || saved.end <= EARLIEST
                || saved.end > LATEST
                || (configuredEnd != null && saved.end != configuredEnd)
                || (configuredStart != null && saved.end <= configuredStart)
                || saved.group < 0
                || saved.group > groups.size()) {
            throw new IllegalStateException("Aircall checkpoint is incompatible with this scan");
        }
        saved.validate(this);
    }

    private static URI origin(String content) {
        if (content != null && content.length() <= 512) {
            try {
                URI uri = new URI(content);
                if ("https".equalsIgnoreCase(uri.getScheme())
                        && uri.getHost() != null
                        && !uri.getHost().startsWith("[")
                        && uri.getRawUserInfo() == null
                        && uri.getRawQuery() == null
                        && uri.getRawFragment() == null
                        && (uri.getRawPath().isEmpty() || "/".equals(uri.getRawPath()))
                        && uri.getPort() != 0
                        && uri.getPort() <= 65535) {
                    return uri;
                }
            } catch (URISyntaxException ignored) {
                // The input may contain credentials, which are never printed.
            }
        }
        throw invalid(AircallSourceOptions.BASE_URL);
    }

    static MessageDigest digest() {
        try {
            return MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException unavailable) {
            throw new IllegalStateException("SHA-256 unavailable");
        }
    }

    private static String credential(ReadonlyConfig config, Option<String> option) {
        String value = config.get(option);
        if (value == null || value.length() > 2048 || !value.matches("[!-~]+")) {
            throw invalid(option);
        }
        return value;
    }

    private static Long time(ReadonlyConfig config, Option<String> option) {
        String text = config.get(option);
        if (text == null) {
            return null;
        }
        try {
            Instant value = Instant.parse(text);
            if (value.getNano() == 0
                    && value.getEpochSecond() >= EARLIEST
                    && value.getEpochSecond() <= LATEST) {
                return value.getEpochSecond();
            }
        } catch (DateTimeException ignored) {
            // No user-supplied value in validation errors.
        }
        throw invalid(option);
    }

    private static int integer(ReadonlyConfig config, Option<Integer> option, int min, int max) {
        Integer value = config.get(option);
        if (value == null || value < min || value > max) {
            throw invalid(option);
        }
        return value;
    }

    private static IllegalArgumentException invalid(Option<?> option) {
        return new IllegalArgumentException("Invalid Aircall option " + option.key());
    }

    static final class Group implements Serializable {
        private static final long serialVersionUID = 1L;
        final AircallResource root;
        final boolean exportRoot;
        final List<AircallResource> children;

        private Group(AircallResource root, boolean exportRoot, List<AircallResource> children) {
            this.root = root;
            this.exportRoot = exportRoot;
            this.children = Collections.unmodifiableList(children);
        }
    }
}
