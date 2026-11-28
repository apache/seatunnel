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

package org.apache.seatunnel.connectors.seatunnel.bunnyinc;

import org.apache.seatunnel.api.configuration.Option;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

import static org.apache.seatunnel.connectors.seatunnel.bunnyinc.BunnyIncSourceOptions.CONNECT_TIMEOUT_MS;
import static org.apache.seatunnel.connectors.seatunnel.bunnyinc.BunnyIncSourceOptions.MAX_ATTEMPTS;
import static org.apache.seatunnel.connectors.seatunnel.bunnyinc.BunnyIncSourceOptions.MAX_PAGES;
import static org.apache.seatunnel.connectors.seatunnel.bunnyinc.BunnyIncSourceOptions.MAX_RESPONSE_BYTES;
import static org.apache.seatunnel.connectors.seatunnel.bunnyinc.BunnyIncSourceOptions.PAGE_SIZE;
import static org.apache.seatunnel.connectors.seatunnel.bunnyinc.BunnyIncSourceOptions.READ_TIMEOUT_MS;
import static org.apache.seatunnel.connectors.seatunnel.bunnyinc.BunnyIncSourceOptions.REQUEST_DEADLINE_MS;
import static org.apache.seatunnel.connectors.seatunnel.bunnyinc.BunnyIncSourceOptions.RESOURCE;
import static org.apache.seatunnel.connectors.seatunnel.bunnyinc.BunnyIncSourceOptions.SUBDOMAIN;
import static org.apache.seatunnel.connectors.seatunnel.bunnyinc.BunnyIncSourceOptions.TLS_TRUST_CERTIFICATE;
import static org.apache.seatunnel.connectors.seatunnel.bunnyinc.BunnyIncSourceOptions.TOKEN;

/**
 * Validates a bounded Bunny billing read before opening a connection.
 *
 * <p>Only one tenant DNS label can form the HTTPS origin. Credentials are kept out of saved
 * progress and are represented by a one-way scope digest when a checkpoint is restored.
 */
final class BunnyIncConfig implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final int MAX_CURSOR_BYTES = 8192;
    private static final Pattern LABEL = Pattern.compile("[a-z0-9](?:[a-z0-9-]{0,61}[a-z0-9])?");
    private static final Pattern TOKEN_GRAMMAR = Pattern.compile("[\\x21-\\x7e]{1,8192}");

    final String host;
    final String token;
    final String trustCertificate;
    final BunnyIncResource resource;
    final int pageSize;
    final int maxPages;
    final int connectTimeout;
    final int readTimeout;
    final int deadlineMs;
    final int maxAttempts;
    final int maxBytes;
    final String scope;

    BunnyIncConfig(ReadonlyConfig options) {
        String tenant = options.get(SUBDOMAIN);
        String label = tenant == null ? "" : tenant.toLowerCase(Locale.ROOT);
        if (!LABEL.matcher(label).matches()) {
            throw invalid("subdomain must be one Bunny tenant DNS label");
        }
        this.host = label + ".bunny.com";
        this.token = options.get(TOKEN);
        if (token == null || !TOKEN_GRAMMAR.matcher(token).matches()) {
            throw invalid("token must be a Bunny access token without whitespace");
        }
        this.trustCertificate = options.getOptional(TLS_TRUST_CERTIFICATE).orElse(null);
        if (trustCertificate != null
                && (trustCertificate.isBlank()
                        || trustCertificate.length() > 4096
                        || trustCertificate.indexOf('\0') >= 0)) {
            throw invalid("tls_trust_certificate must be a valid engine-local certificate path");
        }
        this.resource = BunnyIncResource.parse(options.get(RESOURCE));
        this.pageSize = range(options, PAGE_SIZE, 1, 10);
        this.maxPages = range(options, MAX_PAGES, 1, 20000);
        this.connectTimeout = range(options, CONNECT_TIMEOUT_MS, 1, 120000);
        this.readTimeout = range(options, READ_TIMEOUT_MS, 1, 120000);
        this.deadlineMs = range(options, REQUEST_DEADLINE_MS, 1, 300000);
        this.maxAttempts = range(options, MAX_ATTEMPTS, 1, 10);
        this.maxBytes = range(options, MAX_RESPONSE_BYTES, 1024, 32 * 1024 * 1024);
        this.scope =
                BunnyIncJson.fingerprint(
                        List.of(host, token, resource.id, Integer.toString(pageSize)));
    }

    /**
     * Accepts opaque cursor text without interpreting or altering its contents.
     *
     * @param value cursor returned by the selected connection
     * @return whether it is a nonempty well-formed string within the memory budget
     */
    static boolean validCursor(String value) {
        return value != null
                && !value.isEmpty()
                && value.length() <= MAX_CURSOR_BYTES
                && StandardCharsets.UTF_8.newEncoder().canEncode(value)
                && value.getBytes(StandardCharsets.UTF_8).length <= MAX_CURSOR_BYTES;
    }

    private static int range(ReadonlyConfig config, Option<Integer> option, int min, int max) {
        int value = config.get(option);
        if (value < min || value > max) {
            throw invalid(option.key() + " must be within [" + min + ", " + max + "]");
        }
        return value;
    }

    static IllegalArgumentException invalid(String reason) {
        return new IllegalArgumentException("BunnyInc configuration: " + reason);
    }
}
