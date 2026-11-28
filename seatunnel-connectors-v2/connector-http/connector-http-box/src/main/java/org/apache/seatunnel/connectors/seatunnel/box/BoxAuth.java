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
package org.apache.seatunnel.connectors.seatunnel.box;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import okhttp3.FormBody;
import okhttp3.Request;

import java.util.concurrent.TimeUnit;

/**
 * Per-reader static or client-credentials authentication for Box.
 *
 * <p>Client-credentials tokens are cached in memory only. A 401 on an API call invalidates the
 * cached token without requiring a persisted OAuth refresh token.
 */
final class BoxAuth {
    private final BoxConfig config;
    private String current;
    private long refreshAt;

    BoxAuth(BoxConfig config) {
        this.config = config;
    }

    /**
     * Returns the token or refreshes it against the configured Box API origin.
     *
     * <p>The grant request has no Authorization header. Only the returned access token is kept.
     */
    String token(BoxHttp transport, long deadline) throws BoxReadException, InterruptedException {
        if (!config.clientCredentials) {
            return config.token;
        }
        if (current != null && System.nanoTime() - refreshAt < 0L) {
            return current;
        }
        FormBody form =
                new FormBody.Builder()
                        .add("grant_type", "client_credentials")
                        .add("client_id", config.clientId)
                        .add("client_secret", config.secretKey)
                        .add("box_subject_type", config.subjectType)
                        .add("box_subject_id", config.subjectId)
                        .build();
        Request request =
                new Request.Builder()
                        .url(
                                transport
                                        .api
                                        .newBuilder()
                                        .addPathSegment("oauth2")
                                        .addPathSegment("token")
                                        .build())
                        .header("Accept", "application/json")
                        .post(form)
                        .build();
        JsonNode response = transport.grant(request, deadline);
        String token = response.path("access_token").textValue();
        JsonNode expires = response.path("expires_in");
        String type = response.path("token_type").asText("bearer");
        long seconds =
                expires.isIntegralNumber() && expires.canConvertToLong() ? expires.longValue() : -1;
        if (token == null
                || token.isEmpty()
                || token.length() > 2048
                || token.chars().anyMatch(c -> c < 33 || c > 126)
                || !"bearer".equalsIgnoreCase(type)
                || seconds < 1
                || seconds > 86400) {
            throw new BoxReadException("token grant", "invalid Box access token response");
        }
        current = token;
        refreshAt =
                System.nanoTime()
                        + TimeUnit.SECONDS.toNanos(
                                Math.max(1, seconds - Math.min(30, seconds / 4)));
        return current;
    }

    void invalidate() {
        current = null;
    }
}
