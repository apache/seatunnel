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

package org.apache.seatunnel.connectors.seatunnel.sonarqube;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonParser;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.AbstractSingleSplitReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;

import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

/**
 * Fetches and validates one complete project-measures response before emission. Inherited polling
 * holds the checkpoint lock throughout I/O and output; restore replays the read.
 */
final class SonarQubeSourceReader extends AbstractSingleSplitReader<SeaTunnelRow> {
    private static final int TIMEOUT_MS = 30_000;
    private static final int MAX_RESPONSE_BYTES = 2 * 1024 * 1024;
    private static final ObjectMapper MAPPER =
            new ObjectMapper()
                    .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
                    .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION);

    /*
     * One request is constructed before open so cancellation can abort it before dispatch as well as during I/O.
     */
    private final HttpGet request;
    private final String projectKey;
    private final Set<String> keys;
    private final String tokenEnv;
    private final SingleSplitReaderContext context;
    /*
     * Sticky cancellation suppresses emission and terminal signaling after request abortion.
     */
    private volatile boolean cancelled;
    /*
     * Created by open and closed by close in the engine lifecycle; publication is not coordinated with concurrent open/close.
     */
    private CloseableHttpClient client;

    SonarQubeSourceReader(
            URI endpoint,
            String projectKey,
            Set<String> keys,
            String tokenEnv,
            SingleSplitReaderContext context) {
        request = new HttpGet(endpoint);
        this.projectKey = projectKey;
        this.keys = keys;
        this.tokenEnv = tokenEnv;
        this.context = context;
    }

    /*
     * Resolves the optional bearer token on the worker, validates it without echoing its value,
     * and creates the owned client with inactivity timeouts and no redirects or retries.
     */
    @Override
    public void open() throws IOException {
        if (cancelled) {
            return;
        }
        request.setHeader("Accept", "application/json");
        if (tokenEnv != null) {
            String token = System.getenv(tokenEnv);
            if (token == null || !token.matches("[A-Za-z0-9._~+/=-]+")) {
                throw new IOException(
                        "SonarQube bearer token environment variable is missing or invalid");
            }
            request.setHeader("Authorization", "Bearer " + token);
        }
        client =
                HttpClients.custom()
                        .useSystemProperties()
                        .disableRedirectHandling()
                        .disableAutomaticRetries()
                        .disableCookieManagement()
                        .setDefaultRequestConfig(
                                RequestConfig.custom()
                                        .setConnectTimeout(TIMEOUT_MS)
                                        .setSocketTimeout(TIMEOUT_MS)
                                        .build())
                        .build();
    }

    /*
     * Permanently marks cancellation and aborts the one request, including cancellation before open.
     */
    @Override
    public void cancel() {
        cancelled = true;
        request.abort();
    }

    /*
     * Cancels first, closes the reader-owned client and clears the stored Authorization header.
     */
    @Override
    public void close() throws IOException {
        cancel();
        if (client != null) {
            client.close();
        }
        request.removeHeaders("Authorization");
    }

    /*
     * Reads at most 2 MiB under the inherited checkpoint lock, validates all measures before collection,
     * and signals end only if not cancelled. Cancellation-induced I/O failures terminate polling quietly.
     */
    @Override
    public void internalPollNext(Collector<SeaTunnelRow> output) throws IOException {
        if (cancelled) {
            return;
        }
        final List<SeaTunnelRow> rows;
        try (CloseableHttpResponse response = client.execute(request)) {
            int code = response.getStatusLine().getStatusCode();
            if (code != 200 || response.getEntity() == null) {
                throw new IOException("SonarQube project measures request returned HTTP " + code);
            }
            try (InputStream body = response.getEntity().getContent()) {
                byte[] payload = body.readNBytes(MAX_RESPONSE_BYTES + 1);
                if (payload.length > MAX_RESPONSE_BYTES) {
                    request.abort();
                    throw new IOException("SonarQube project measures response exceeds 2 MiB");
                }
                rows = parseMeasures(payload, projectKey, keys);
            }
        } catch (IOException error) {
            if (cancelled) {
                return;
            }
            throw error;
        }
        for (SeaTunnelRow row : rows) {
            if (cancelled) {
                return;
            }
            output.collect(row);
        }
        if (!cancelled) {
            context.signalNoMoreElement();
        }
    }

    /*
     * Requires the requested project, valid typed measures and unique requested metric keys.
     * Absent metrics are allowed; absent overall value/bestValue stay null while period JSON is retained.
     * No row is emitted until the entire response passes validation.
     */
    static List<SeaTunnelRow> parseMeasures(
            byte[] payload, String expectedProject, Set<String> expectedMetrics)
            throws IOException {
        final JsonNode root;
        try {
            root = MAPPER.readTree(payload);
        } catch (IOException e) {
            throw new IOException("SonarQube project measures response is not valid JSON");
        }
        if (root == null || !root.isObject()) {
            throw new IOException("SonarQube project measures response must be an object");
        }
        JsonNode component = root.path("component");
        JsonNode project = component.path("key");
        JsonNode name = component.path("name");
        JsonNode measures = component.path("measures");
        if (!project.isTextual()
                || !expectedProject.equals(project.textValue())
                || !name.isTextual()
                || !"TRK".equals(component.path("qualifier").textValue())
                || !measures.isArray()) {
            throw new IOException(
                    "SonarQube response did not identify the requested project and measures");
        }
        Set<String> seen = new HashSet<>();
        List<SeaTunnelRow> rows = new ArrayList<>();
        for (JsonNode measure : measures) {
            JsonNode metric = measure.path("metric");
            JsonNode value = measure.path("value");
            JsonNode best = measure.path("bestValue");
            if (!metric.isTextual()
                    || !expectedMetrics.contains(metric.textValue())
                    || !seen.add(metric.textValue())
                    || !isNullable(value, value.isTextual())
                    || !isNullable(best, best.isBoolean())) {
                throw new IOException(
                        "SonarQube response contains an invalid, unrequested or duplicated measure");
            }
            rows.add(
                    new SeaTunnelRow(
                            new Object[] {
                                expectedProject,
                                name.textValue(),
                                metric.textValue(),
                                value.textValue(),
                                best.isBoolean() ? best.booleanValue() : null,
                                measure.toString()
                            }));
        }
        return rows;
    }

    private static boolean isNullable(JsonNode node, boolean validType) {
        return node.isMissingNode() || node.isNull() || validType;
    }
}
