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

package org.apache.seatunnel.connectors.seatunnel.firebase.client;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.type.TypeReference;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.org.apache.commons.lang3.StringUtils;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.common.utils.SeaTunnelException;
import org.apache.seatunnel.connectors.seatunnel.firebase.config.FirebaseSourceOptions;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import lombok.extern.slf4j.Slf4j;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.URI;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.ReentrantLock;

@Slf4j
public class FirebaseHttpClient {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final int MAX_ERROR_BODY_BYTES = 4096;
    private final String baseUrl;
    private final String path;
    private final int timeoutMs;
    private final Map<String, String> extraQueryParams;
    private static final List<String> FIREBASE_SCOPES =
            Arrays.asList(
                    "https://www.googleapis.com/auth/firebase.database",
                    "https://www.googleapis.com/auth/userinfo.email");
    private final GoogleCredentials credentials;
    private final String databaseSecret;
    private final ReentrantLock tokenLock = new ReentrantLock();

    public FirebaseHttpClient(ReadonlyConfig config) {
        this.baseUrl = config.get(FirebaseSourceOptions.URL).replaceAll("/+$", "");
        this.path = config.get(FirebaseSourceOptions.PATH).replaceAll("^/+|/+$", "");
        int toms = config.get(FirebaseSourceOptions.TIMEOUT_MS);
        if (toms <= 0) {
            throw new SeaTunnelException("timeout_ms must be greater than 0");
        }
        this.timeoutMs = toms;
        this.extraQueryParams =
                config.getOptional(FirebaseSourceOptions.QUERY_PARAMS)
                        .orElse(Collections.emptyMap());

        this.credentials = initGoogleCredentials(config);
        this.databaseSecret =
                config.getOptional(FirebaseSourceOptions.DATABASE_SECRET).orElse(null);
        if (this.credentials != null) {
            log.info("Initialized FirebaseHttpClient with Google OAuth2 Service Account.");
        } else if (StringUtils.isNotEmpty(this.databaseSecret)) {
            log.info("Initialized FirebaseHttpClient with Legacy Database Secret.");
        } else {
            log.info("Initialized FirebaseHttpClient without authentication (Public Read).");
        }
    }

    /**
     * Executes a shallow scan on the configured node path to retrieve top-level keys. Endpoint: GET
     * /<path>.json?shallow=true
     */
    public List<String> fetchShallowKeys() {
        String endpointUrl = buildUrl(this.path, "shallow=true", false);
        String jsonResponse = executeGet(endpointUrl);
        return parseShallowKeys(jsonResponse);
    }

    /** Parses the JSON payload returned by a shallow scan query. */
    List<String> parseShallowKeys(String jsonResponse) {
        if (jsonResponse == null || jsonResponse.trim().equals("null")) {
            return Collections.emptyList();
        }
        String trimmed = jsonResponse.trim();
        if (!trimmed.startsWith("{")) {
            return Collections.emptyList();
        }

        try {
            Map<String, Boolean> keysMap =
                    OBJECT_MAPPER.readValue(trimmed, new TypeReference<Map<String, Boolean>>() {});
            return new ArrayList<>(keysMap.keySet());
        } catch (Exception e) {
            throw new SeaTunnelException("Failed to parse shallow keys from Firebase response", e);
        }
    }

    /**
     * Fetches the raw JSON payload for a given sub-path or individual node key. Endpoint: GET
     * /<path>/<nodeKey>.json
     */
    public String fetchNodeData(String nodeKey) {
        String targetPath =
                nodeKey == null || nodeKey.isEmpty() ? this.path : this.path + "/" + nodeKey;

        String endpointUrl = buildUrl(targetPath, null, true);
        return executeGet(endpointUrl);
    }

    /** Constructs a full REST URL with .json extension and query string parameters. */
    String buildUrl(String subPath, String extraQueryParam, boolean includeExtraParams) {
        StringBuilder urlBuilder = new StringBuilder(baseUrl);
        String cleanSubPath = subPath == null ? "" : subPath.replaceAll("^/+|/+$", "");
        urlBuilder.append("/").append(cleanSubPath);

        urlBuilder.append(".json");

        List<String> queryParts = new ArrayList<>();
        if (extraQueryParam != null && !extraQueryParam.isEmpty()) {
            queryParts.add(extraQueryParam);
        }

        if (databaseSecret != null && !databaseSecret.isEmpty()) {
            queryParts.add("auth=" + encodeUriComponent(databaseSecret));
        }

        if (includeExtraParams) {
            for (Map.Entry<String, String> entry : extraQueryParams.entrySet()) {
                String key = entry.getKey();
                String value = entry.getValue();

                if ("orderBy".equals(key)
                        || "equalTo".equals(key)
                        || "startAt".equals(key)
                        || "endAt".equals(key)) {
                    if (!value.startsWith("\"")) {
                        value = "\"" + value + "\"";
                    }
                }

                queryParts.add(encodeUriComponent(key) + "=" + encodeUriComponent(value));
            }
        }

        if (!queryParts.isEmpty()) {
            urlBuilder.append("?").append(String.join("&", queryParts));
        }
        return urlBuilder.toString();
    }

    /** Sends an HTTP GET request and handles status code verification. */
    private String executeGet(String urlStr) {
        HttpURLConnection connection = null;
        try {
            URL url;
            try {
                url = URI.create(urlStr).toURL();
            } catch (IllegalArgumentException e) {
                throw new SeaTunnelException(
                        "Invalid Firebase REST URI constructed. Check parameter formatting.");
            }
            connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            connection.setConnectTimeout(timeoutMs);
            connection.setReadTimeout(timeoutMs);
            connection.setRequestProperty("Accept", "application/json");
            connection.setInstanceFollowRedirects(true);

            if (credentials != null) {
                String token = getAccessToken();
                connection.setRequestProperty("Authorization", "Bearer " + token);
            }

            int responseCode = connection.getResponseCode();
            if (responseCode >= 200 && responseCode < 300) {
                try (InputStream inputStream = connection.getInputStream();
                        BufferedReader reader =
                                new BufferedReader(
                                        new InputStreamReader(
                                                inputStream, StandardCharsets.UTF_8))) {
                    StringBuilder responseBuilder = new StringBuilder();
                    String line;
                    while ((line = reader.readLine()) != null) {
                        responseBuilder.append(line);
                    }
                    return responseBuilder.toString();
                }
            } else {
                String rawErrorBody = readErrorStream(connection);
                throw new SeaTunnelException(
                        String.format(
                                "Firebase HTTP request failed with status code %d. Response body: %s",
                                responseCode, rawErrorBody.isEmpty() ? "N/A" : rawErrorBody));
            }
        } catch (IOException e) {
            throw new SeaTunnelException("Failed to execute HTTP request to Firebase endpoint", e);
        } finally {
            if (connection != null) {
                connection.disconnect();
            }
        }
    }

    private String encodeUriComponent(String value) {
        try {
            return URLEncoder.encode(value, StandardCharsets.UTF_8.name())
                    .replaceAll("\\+", "%20"); // Handle space encoding for URIs
        } catch (UnsupportedEncodingException e) {
            throw new RuntimeException(e);
        }
    }

    private String readErrorStream(HttpURLConnection connection) {
        InputStream errorStream = connection.getErrorStream();
        if (errorStream == null) {
            return "";
        }
        try (BufferedReader reader =
                new BufferedReader(new InputStreamReader(errorStream, StandardCharsets.UTF_8))) {
            StringBuilder builder = new StringBuilder();
            char[] buffer = new char[1024];
            int bytesRead;
            int totalRead = 0;
            while ((bytesRead =
                            reader.read(
                                    buffer,
                                    0,
                                    Math.min(buffer.length, MAX_ERROR_BODY_BYTES - totalRead)))
                    != -1) {
                builder.append(buffer, 0, bytesRead);
                totalRead += bytesRead;
                if (totalRead >= MAX_ERROR_BODY_BYTES) {
                    break;
                }
            }
            return builder.toString().trim();
        } catch (IOException e) {
            log.warn("Failed to read error stream from Firebase HTTP connection", e);
            return "";
        }
    }

    /** Retrieves or refreshes OAuth 2.0 access token safely. */
    private synchronized String getAccessToken() throws IOException {
        tokenLock.lock();
        try {
            AccessToken token = credentials.getAccessToken();
            if (token == null
                    || token.getExpirationTime() == null
                    || token.getExpirationTime().getTime() <= System.currentTimeMillis() + 60000) {
                credentials.refresh();
                token = credentials.getAccessToken();
            }
            return token.getTokenValue();
        } finally {
            tokenLock.unlock();
        }
    }

    private GoogleCredentials initGoogleCredentials(ReadonlyConfig config) {
        try {
            if (config.getOptional(FirebaseSourceOptions.CREDENTIALS).isPresent()) {
                String base64Credentials = config.get(FirebaseSourceOptions.CREDENTIALS);
                byte[] decodedBytes = Base64.getDecoder().decode(base64Credentials);
                try (InputStream inputStream = new ByteArrayInputStream(decodedBytes)) {
                    return GoogleCredentials.fromStream(inputStream).createScoped(FIREBASE_SCOPES);
                }
            } else if (config.getOptional(FirebaseSourceOptions.SERVICE_ACCOUNT_PATH).isPresent()) {
                String path = config.get(FirebaseSourceOptions.SERVICE_ACCOUNT_PATH);
                try (InputStream inputStream = new FileInputStream(path)) {
                    return GoogleCredentials.fromStream(inputStream).createScoped(FIREBASE_SCOPES);
                }
            }
        } catch (IOException e) {
            throw new SeaTunnelException(
                    "Failed to initialize Google Service Account credentials", e);
        }
        return null;
    }
}
