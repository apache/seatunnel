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

package org.apache.seatunnel.connectors.seatunnel.agilecrm;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import java.io.Serializable;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Locale;

import static org.apache.seatunnel.connectors.seatunnel.agilecrm.AgileCrmSourceOptions.ALLOW_INSECURE_HTTP;
import static org.apache.seatunnel.connectors.seatunnel.agilecrm.AgileCrmSourceOptions.BASE_URL;
import static org.apache.seatunnel.connectors.seatunnel.agilecrm.AgileCrmSourceOptions.EVENT_END;
import static org.apache.seatunnel.connectors.seatunnel.agilecrm.AgileCrmSourceOptions.EVENT_START;
import static org.apache.seatunnel.connectors.seatunnel.agilecrm.AgileCrmSourceOptions.MAX_ATTEMPTS;
import static org.apache.seatunnel.connectors.seatunnel.agilecrm.AgileCrmSourceOptions.PAGE_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.agilecrm.AgileCrmSourceOptions.PASSWORD;
import static org.apache.seatunnel.connectors.seatunnel.agilecrm.AgileCrmSourceOptions.REQUEST_TIMEOUT_MS;
import static org.apache.seatunnel.connectors.seatunnel.agilecrm.AgileCrmSourceOptions.RESOURCE;
import static org.apache.seatunnel.connectors.seatunnel.agilecrm.AgileCrmSourceOptions.USERNAME;

final class AgileCrmSettings implements Serializable {
    private static final long serialVersionUID = 1L;
    final URI origin;
    final String authorization;
    final AgileCrmResource resource;
    final int pageSize;
    final Long eventStart;
    final Long eventEnd;
    final int timeoutMillis;
    final int attempts;

    AgileCrmSettings(ReadonlyConfig options) {
        resource = AgileCrmResource.parse(options.get(RESOURCE));
        origin = validateOrigin(options.get(BASE_URL), options.get(ALLOW_INSECURE_HTTP));
        String user = options.get(USERNAME);
        String password = options.get(PASSWORD);
        if (!basicPart(user) || user.indexOf(':') != -1 || !basicPart(password)) {
            throw invalid(
                    "username and password must be nonempty Basic credentials without whitespace");
        }
        authorization =
                "Basic "
                        + Base64.getEncoder()
                                .encodeToString(
                                        (user + ":" + password).getBytes(StandardCharsets.UTF_8));
        pageSize = checked(options.get(PAGE_SIZE), 100, "page_size");
        timeoutMillis = checked(options.get(REQUEST_TIMEOUT_MS), 300000, "request_timeout_ms");
        attempts = checked(options.get(MAX_ATTEMPTS), 8, "max_attempts");

        eventStart = options.getOptional(EVENT_START).orElse(null);
        eventEnd = options.getOptional(EVENT_END).orElse(null);
        if (resource == AgileCrmResource.EVENTS) {
            if (eventStart == null
                    || eventEnd == null
                    || eventStart < 0
                    || eventEnd <= eventStart) {
                throw invalid("events requires nonnegative event_start and a larger event_end");
            }
        } else if (eventStart != null || eventEnd != null) {
            throw invalid("event_start and event_end apply only to events");
        }
    }

    String stateKey() {
        return origin + "|" + resource.name + "|" + pageSize + "|" + eventStart + "|" + eventEnd;
    }

    private static boolean basicPart(String part) {
        return part != null
                && !part.isEmpty()
                && part.length() <= 4096
                && part.chars().allMatch(ch -> ch >= 0x21 && ch <= 0x7e);
    }

    private static int checked(int input, int max, String name) {
        if (input < 1 || input > max) {
            throw invalid(name + " must be between 1 and " + max);
        }
        return input;
    }

    private static URI validateOrigin(String input, boolean allowCleartext) {
        try {
            URI value = URI.create(input);
            String scheme = value.getScheme();
            if (("https".equalsIgnoreCase(scheme)
                            || (allowCleartext && "http".equalsIgnoreCase(scheme)))
                    && value.getHost() != null
                    && value.getRawUserInfo() == null
                    && value.getRawQuery() == null
                    && value.getRawFragment() == null
                    && value.getPort() != 0
                    && value.getPort() <= 65535
                    && (value.getRawPath().isEmpty() || "/".equals(value.getRawPath()))) {
                return URI.create(
                        scheme.toLowerCase(Locale.ROOT) + "://" + value.getRawAuthority());
            }
        } catch (IllegalArgumentException ignored) {
            // Do not echo arbitrary input, which can include credentials.
        }
        throw invalid("base_url must be an HTTPS origin unless allow_insecure_http is enabled");
    }

    static IllegalArgumentException invalid(String message) {
        return new IllegalArgumentException("AgileCRM Source " + message);
    }
}
