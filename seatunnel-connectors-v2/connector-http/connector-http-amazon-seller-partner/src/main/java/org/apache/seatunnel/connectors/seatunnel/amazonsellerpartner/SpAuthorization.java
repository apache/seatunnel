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

package org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.ObjectNode;

import okhttp3.FormBody;
import okhttp3.HttpUrl;
import okhttp3.MediaType;
import okhttp3.Request;
import okhttp3.RequestBody;

import java.io.IOException;
import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.SpTransport.Safety.CREATE_REPORT;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.SpTransport.Safety.READ;
import static org.apache.seatunnel.connectors.seatunnel.amazonsellerpartner.SpTransport.Safety.TOKEN;

/**
 * Per-reader Login with Amazon and restricted token cache.
 *
 * <p>A restricted read keeps its GET path and data elements during renewal. Rejected restricted
 * access does not fall back to a basic token.
 */
final class SpAuthorization {
    private static final String USER_AGENT = "ApacheSeaTunnel/3 (Language=Java)";
    private static final DateTimeFormatter AMZ_DATE =
            DateTimeFormatter.ofPattern("uuuuMMdd'T'HHmmss'Z'").withZone(ZoneOffset.UTC);
    private static final MediaType JSON_TYPE = MediaType.get("application/json");
    private static final int RDT_CAPACITY = 64;

    private final SpSettings settings;
    private final SpTransport transport;
    private final Map<Scope, Ticket> restricted = new LinkedHashMap<>(16, 0.75f, true);
    private Ticket lwa;

    SpAuthorization(SpSettings settings, SpTransport transport) {
        this.settings = settings;
        this.transport = transport;
    }

    JsonNode get(String call, String path, Map<String, String> query, Scope scope, long deadline)
            throws IOException, InterruptedException {
        HttpUrl.Builder endpoint = SpTransport.endpoint(settings.origin, path).newBuilder();
        query.forEach(endpoint::addQueryParameter);
        Request request = new Request.Builder().url(endpoint.build()).get().build();
        return policy(call, request, scope, READ, 200, deadline);
    }

    JsonNode createReport(String path, ObjectNode data, long deadline)
            throws IOException, InterruptedException {
        Request request =
                new Request.Builder()
                        .url(SpTransport.endpoint(settings.origin, path))
                        .post(jsonBody(data))
                        .build();
        return policy("createReport", request, null, CREATE_REPORT, 202, deadline);
    }

    private synchronized JsonNode policy(
            String operation,
            Request request,
            Scope scope,
            SpTransport.Safety safety,
            int expected,
            long deadline)
            throws IOException, InterruptedException {
        String token = scope == null ? lwa(deadline) : restricted(scope, deadline);
        SpTransport.Reply response =
                transport.exchange(operation, attach(request, token), safety, false, deadline);
        if (tokenExpired(response)) {
            if (scope == null) {
                lwa = null;
                token = lwa(deadline);
            } else {
                restricted.remove(scope);
                token = restricted(scope, deadline);
            }
            response =
                    transport.exchange(operation, attach(request, token), safety, false, deadline);
        }
        return responseJson(response, expected, operation);
    }

    private String lwa(long deadline) throws IOException, InterruptedException {
        if (lwa != null && lwa.usable()) {
            return lwa.token;
        }
        Request request =
                new Request.Builder()
                        .url(SpTransport.endpoint(settings.tokenOrigin, "auth/o2/token"))
                        .header("Accept", "application/json")
                        .header("User-Agent", USER_AGENT)
                        .post(
                                new FormBody.Builder()
                                        .add("grant_type", "refresh_token")
                                        .add("client_id", settings.clientId)
                                        .add("client_secret", settings.clientSecret)
                                        .add("refresh_token", settings.token)
                                        .build())
                        .build();
        long start = System.nanoTime();
        SpTransport.Reply result = transport.exchange("LWA", request, TOKEN, false, deadline);
        lwa = ticket(responseJson(result, 200, "LWA"), "access_token", "expires_in", "LWA", start);
        return lwa.token;
    }

    private String restricted(Scope scope, long deadline) throws IOException, InterruptedException {
        Ticket cached = restricted.get(scope);
        if (cached != null && cached.usable()) {
            return cached.token;
        }
        ObjectNode body = SpJson.JSON.createObjectNode();
        ObjectNode entry =
                body.putArray("restrictedResources")
                        .addObject()
                        .put("method", "GET")
                        .put("path", scope.path);
        if (!scope.elements.isEmpty()) {
            scope.elements.forEach(entry.putArray("dataElements")::add);
        }
        Request request =
                new Request.Builder()
                        .url(
                                SpTransport.endpoint(
                                        settings.origin, "tokens/2021-03-01/restrictedDataToken"))
                        .post(jsonBody(body))
                        .build();
        long start = System.nanoTime();
        SpTransport.Reply result =
                transport.exchange(
                        "restricted token", attach(request, lwa(deadline)), TOKEN, false, deadline);
        if (tokenExpired(result)) {
            lwa = null;
            result =
                    transport.exchange(
                            "restricted token",
                            attach(request, lwa(deadline)),
                            TOKEN,
                            false,
                            deadline);
        }
        if (result.status == 403) {
            throw new SpFailure(
                    "restricted token",
                    "HTTP 403 requires an approved restricted role for this resource");
        }
        Ticket created =
                ticket(
                        responseJson(result, 200, "restricted token"),
                        "restrictedDataToken",
                        "expiresIn",
                        "restricted token",
                        start);
        restricted.put(scope, created);
        if (restricted.size() > RDT_CAPACITY) {
            restricted.remove(restricted.entrySet().iterator().next().getKey());
        }
        return created.token;
    }

    private static boolean tokenExpired(SpTransport.Reply response) {
        if (response.status == 401) {
            return true;
        }
        if (response.status != 403 || response.body.length == 0) {
            return false;
        }
        try {
            JsonNode errors = SpJson.read(response.body, "authorization").path("errors");
            for (JsonNode error : errors) {
                String message = error.path("message").asText().toLowerCase(Locale.ROOT);
                if (message.contains("access token")
                        && (message.contains("has expired") || message.contains("is expired"))) {
                    return true;
                }
            }
        } catch (SpFailure ignored) {
            // An unknown denial is a permission error rather than a refresh signal.
        }
        return false;
    }

    private static Ticket ticket(
            JsonNode response, String tokenField, String lifetime, String operation, long issued)
            throws SpFailure {
        JsonNode ttl = response.path(lifetime);
        if (!ttl.isIntegralNumber()
                || !ttl.canConvertToInt()
                || ttl.intValue() < 1
                || ttl.intValue() > 86400) {
            throw new SpFailure(operation, "invalid token lifetime");
        }
        long life = TimeUnit.SECONDS.toNanos(ttl.intValue());
        long guard = Math.min(TimeUnit.SECONDS.toNanos(60), life / 5);
        Ticket ticket =
                new Ticket(
                        SpJson.text(response, tokenField, 8192, operation), issued + life - guard);
        if (!ticket.usable()) {
            throw new SpFailure(operation, "token expired during acquisition");
        }
        return ticket;
    }

    private static JsonNode responseJson(SpTransport.Reply response, int code, String operation)
            throws SpFailure {
        if (response.status != code) {
            throw new SpFailure(operation, "HTTP " + response.status);
        }
        return SpJson.read(response.body, operation);
    }

    private static Request attach(Request request, String token) {
        return request.newBuilder()
                .header("Accept", "application/json")
                .header("User-Agent", USER_AGENT)
                .header("x-amz-date", AMZ_DATE.format(Instant.now()))
                .header("x-amz-access-token", token)
                .build();
    }

    private static RequestBody jsonBody(JsonNode body) throws SpFailure {
        return RequestBody.create(JSON_TYPE, SpJson.write(body, "request body"));
    }

    /**
     * An exact provider GET restriction.
     *
     * <p>Scope identity includes the requested fields, preventing a cache hit from widening access
     * to another path or set of data elements.
     */
    static final class Scope {
        private final String path;
        private final List<String> elements;

        Scope(String path, List<String> elements) {
            this.path = path;
            this.elements = List.copyOf(elements);
        }

        @Override
        public boolean equals(Object other) {
            if (!(other instanceof Scope)) {
                return false;
            }
            Scope scope = (Scope) other;
            return path.equals(scope.path) && elements.equals(scope.elements);
        }

        @Override
        public int hashCode() {
            return Objects.hash(path, elements);
        }
    }

    /**
     * A transient expiring access credential.
     *
     * <p>Expiration uses the reader's monotonic clock. The value is kept out of split state.
     */
    private static final class Ticket {
        private final String token;
        private final long expires;

        private Ticket(String token, long expires) {
            this.token = token;
            this.expires = expires;
        }

        private boolean usable() {
            return System.nanoTime() - expires < 0L;
        }
    }
}
