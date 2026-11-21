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

package org.apache.seatunnel.connectors.seatunnel.mixpanel;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonPointer;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import java.io.Serializable;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.Base64;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.apache.seatunnel.connectors.seatunnel.mixpanel.MixpanelSourceOptions.ALLOW_INSECURE_HTTP;
import static org.apache.seatunnel.connectors.seatunnel.mixpanel.MixpanelSourceOptions.BASE_URL;
import static org.apache.seatunnel.connectors.seatunnel.mixpanel.MixpanelSourceOptions.CONNECT_TIMEOUT_MS;
import static org.apache.seatunnel.connectors.seatunnel.mixpanel.MixpanelSourceOptions.FIELD_MAPPING;
import static org.apache.seatunnel.connectors.seatunnel.mixpanel.MixpanelSourceOptions.FROM_DATE;
import static org.apache.seatunnel.connectors.seatunnel.mixpanel.MixpanelSourceOptions.MAX_EVENT_BYTES;
import static org.apache.seatunnel.connectors.seatunnel.mixpanel.MixpanelSourceOptions.MAX_RETRIES;
import static org.apache.seatunnel.connectors.seatunnel.mixpanel.MixpanelSourceOptions.MAX_RETRY_BACKOFF_MS;
import static org.apache.seatunnel.connectors.seatunnel.mixpanel.MixpanelSourceOptions.PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.mixpanel.MixpanelSourceOptions.PROJECT_ID;
import static org.apache.seatunnel.connectors.seatunnel.mixpanel.MixpanelSourceOptions.READ_TIMEOUT_MS;
import static org.apache.seatunnel.connectors.seatunnel.mixpanel.MixpanelSourceOptions.REGION;
import static org.apache.seatunnel.connectors.seatunnel.mixpanel.MixpanelSourceOptions.RETRY_BACKOFF_MS;
import static org.apache.seatunnel.connectors.seatunnel.mixpanel.MixpanelSourceOptions.TO_DATE;
import static org.apache.seatunnel.connectors.seatunnel.mixpanel.MixpanelSourceOptions.USERNAME;

final class MixpanelSourceConfig implements Serializable {
    final String username;
    final String password;
    final long projectId;
    final String fromDate;
    final String toDate;
    final URI endpoint;
    final int connectTimeout;
    final int readTimeout;
    final int maxRetries;
    final int backoff;
    final int maxBackoff;
    final int maxEventBytes;
    final Map<String, String> fieldMapping;

    MixpanelSourceConfig(ReadonlyConfig options) {
        username = credential(options, USERNAME);
        password = credential(options, PASSWORD);
        if (username.contains(":")) {
            throw invalid("username must not contain a colon");
        }
        projectId = options.get(PROJECT_ID);
        if (projectId <= 0) {
            throw invalid("project_id must be positive");
        }
        fromDate = options.get(FROM_DATE);
        toDate = options.get(TO_DATE);
        if (date(fromDate, "from_date").isAfter(date(toDate, "to_date"))) {
            throw invalid("from_date must not be after to_date");
        }
        String region = options.get(REGION);
        String host;
        switch (region) {
            case "US":
                host = "data.mixpanel.com";
                break;
            case "EU":
                host = "data-eu.mixpanel.com";
                break;
            case "IN":
                host = "data-in.mixpanel.com";
                break;
            default:
                throw invalid("region must be US, EU or IN");
        }
        endpoint =
                endpoint(
                        options.getOptional(BASE_URL).orElse("https://" + host),
                        options.get(ALLOW_INSECURE_HTTP));
        connectTimeout = bounded(options, CONNECT_TIMEOUT_MS, 1, 300000);
        readTimeout = bounded(options, READ_TIMEOUT_MS, 1, 600000);
        maxRetries = bounded(options, MAX_RETRIES, 0, 10);
        backoff = bounded(options, RETRY_BACKOFF_MS, 1, 60000);
        maxBackoff = bounded(options, MAX_RETRY_BACKOFF_MS, 1, 600000);
        if (backoff > maxBackoff) {
            throw invalid("retry_backoff_ms must not exceed max_retry_backoff_ms");
        }
        maxEventBytes = bounded(options, MAX_EVENT_BYTES, 1, 16777216);
        fieldMapping =
                Collections.unmodifiableMap(
                        new LinkedHashMap<>(
                                options.getOptional(FIELD_MAPPING).orElse(Collections.emptyMap())));
        for (String pointer : fieldMapping.values()) {
            try {
                JsonPointer.compile(pointer);
            } catch (RuntimeException e) {
                throw invalid("field_mapping values must be RFC 6901 JSON Pointers");
            }
        }
    }

    private static String credential(ReadonlyConfig options, Option<String> option) {
        String value = options.get(option);
        if (value == null
                || value.trim().isEmpty()
                || value.chars().anyMatch(Character::isISOControl)) {
            throw invalid(option.key() + " must be nonempty and contain no control characters");
        }
        return value;
    }

    private static LocalDate date(String value, String key) {
        try {
            if (!value.matches("[0-9]{4}-[0-9]{2}-[0-9]{2}")) {
                throw invalid(key + " must be a valid yyyy-MM-dd date");
            }
            return LocalDate.parse(value);
        } catch (DateTimeParseException e) {
            throw invalid(key + " must be a valid yyyy-MM-dd date");
        }
    }

    private static int bounded(ReadonlyConfig options, Option<Integer> option, int min, int max) {
        int value = options.get(option);
        if (value < min || value > max) {
            throw invalid(option.key() + " is outside its allowed range");
        }
        return value;
    }

    private static URI endpoint(String origin, boolean allowHttp) {
        try {
            URI uri = new URI(origin);
            if (uri.getHost() == null
                    || uri.getRawUserInfo() != null
                    || uri.getRawQuery() != null
                    || uri.getRawFragment() != null
                    || (uri.getRawPath() != null
                            && !uri.getRawPath().isEmpty()
                            && !"/".equals(uri.getRawPath()))
                    || (uri.getPort() != -1 && (uri.getPort() < 1 || uri.getPort() > 65535))
                    || (!"https".equals(uri.getScheme())
                            && !(allowHttp && "http".equals(uri.getScheme())))) {
                throw invalid(
                        "base_url must be a trusted HTTPS origin; HTTP requires allow_insecure_http for fixtures");
            }
            return new URI(
                    uri.getScheme(),
                    null,
                    uri.getHost(),
                    uri.getPort(),
                    "/api/2.0/export",
                    null,
                    null);
        } catch (URISyntaxException e) {
            throw invalid("base_url must be a valid trusted origin");
        }
    }

    String authorization() {
        return "Basic "
                + Base64.getEncoder()
                        .encodeToString(
                                (username + ":" + password).getBytes(StandardCharsets.UTF_8));
    }

    static IllegalArgumentException invalid(String message) {
        return new IllegalArgumentException("Mixpanel: " + message);
    }
}
