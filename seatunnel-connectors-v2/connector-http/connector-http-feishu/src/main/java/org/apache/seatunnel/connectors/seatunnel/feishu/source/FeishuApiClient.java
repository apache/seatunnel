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

package org.apache.seatunnel.connectors.seatunnel.feishu.source;

import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonFactory;
import org.apache.seatunnel.shade.com.fasterxml.jackson.core.JsonFactoryBuilder;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.node.JsonNodeFactory;

import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.http.exception.HttpConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.http.exception.HttpConnectorException;

import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.utils.URIBuilder;
import org.apache.http.config.ConnectionConfig;
import org.apache.http.config.MessageConstraints;
import org.apache.http.conn.ConnectTimeoutException;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.DefaultManagedHttpClientConnection;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.net.URISyntaxException;
import java.time.Duration;
import java.time.Instant;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Owns bounded tenant authentication and Bitable record requests for one reader worker. Token
 * refresh and retries share the page deadline; close aborts active I/O without progress locks. Only
 * validated pages cross to polling, and external transport/parser details stay out of errors.
 */
final class FeishuApiClient implements AutoCloseable {

    // Provider codes permit one token refresh or bounded retry, never silent empty-page success.
    private static final int TOKEN_EXPIRED_CODE = 99991663;
    private static final int RATE_LIMIT_CODE = 99991400;
    // Separate finite budgets apply before authentication or record JSON is materialized.
    private static final int MAX_TOKEN_RESPONSE_BYTES = 64 * 1024;
    private static final int MAX_PAGE_RESPONSE_BYTES = 16 * 1024 * 1024;
    static final int MAX_CURSOR_LENGTH = 4096;
    // Settings only: response-local copies own parser symbols, with global interning disabled.
    private static final ObjectMapper MAPPER =
            new ObjectMapper(
                            new JsonFactoryBuilder()
                                    .disable(JsonFactory.Feature.INTERN_FIELD_NAMES)
                                    .build())
                    .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS)
                    .enable(DeserializationFeature.FAIL_ON_READING_DUP_TREE_KEY)
                    .enable(DeserializationFeature.USE_BIG_DECIMAL_FOR_FLOATS)
                    .setNodeFactory(JsonNodeFactory.withExactBigDecimals(true));

    private final FeishuSourceOptions config;
    private final CloseableHttpClient client;
    // Publish before dispatch so an independent close can abort the current exchange.
    private final AtomicReference<HttpPost> activeRequest = new AtomicReference<>();
    private volatile boolean closed;
    // Only the I/O worker accesses authentication state.
    private String tenantToken;
    private long refreshAfterNanos;

    FeishuApiClient(FeishuSourceOptions config) {
        this.config = config;
        // The default connection factory attaches wire and header logging to credential-bearing
        // I/O.
        PoolingHttpClientConnectionManager connections =
                new PoolingHttpClientConnectionManager(
                        (route, connectionConfig) ->
                                new DefaultManagedHttpClientConnection(
                                        "feishu-source",
                                        connectionConfig.getBufferSize(),
                                        connectionConfig.getFragmentSizeHint(),
                                        null,
                                        null,
                                        connectionConfig.getMessageConstraints(),
                                        null,
                                        null,
                                        null,
                                        null));
        connections.setMaxTotal(1);
        connections.setDefaultMaxPerRoute(1);
        connections.setDefaultConnectionConfig(
                ConnectionConfig.custom()
                        .setMessageConstraints(
                                MessageConstraints.custom()
                                        .setMaxHeaderCount(100)
                                        .setMaxLineLength(16 * 1024)
                                        .build())
                        .build());
        // The full execution chain logs the request line at DEBUG. This direct chain
        // keeps socket/TLS handling and cancellation without logging the path or query,
        // or implicitly performing redirects, authentication challenges or retries.
        client = HttpClients.createMinimal(connections);
    }

    /**
     * Shares one absolute page deadline across authentication, retries and at most one
     * expired-token refresh. Full record validation finishes before a page can reach the polling
     * thread.
     */
    Page fetchPage(String cursor, long deadline) throws InterruptedException {
        URI uri = recordsUri(cursor);
        ensureTenantToken(deadline);
        Response response = exchangeWithRetries(uri, "{}", false, deadline);
        if (response.status == 401 || response.code() == TOKEN_EXPIRED_CODE) {
            tenantToken = null;
            ensureTenantToken(deadline);
            response = exchangeWithRetries(uri, "{}", false, deadline);
        }
        checkSuccess(response, "record search");
        Page page = decodePage(response.body);
        remainingMillis(deadline);
        return page;
    }

    /**
     * Makes cancellation sticky and aborts the published exchange before closing owned transport.
     * No collector or checkpoint-progress monitor is needed for this independent cleanup path.
     */
    @Override
    public void close() throws IOException {
        closed = true;
        HttpPost request = activeRequest.get();
        if (request != null) {
            request.abort();
        }
        client.close();
    }

    /**
     * Marks connector-owned diagnostics without attaching external exceptions whose text or causes
     * can carry credentials, opaque cursors or response excerpts.
     */
    static HttpConnectorException failure(String message) {
        return new HttpConnectorException(
                HttpConnectorErrorCode.REQUEST_FAILED, "Feishu source: " + message);
    }

    /**
     * Reuses only a worker-owned token that is still inside its refresh window. Refresh consumes
     * the same page budget and does not publish authentication credentials into reader progress.
     */
    private void ensureTenantToken(long deadline) throws InterruptedException {
        if (tenantToken != null && System.nanoTime() - refreshAfterNanos < 0L) {
            return;
        }
        String credentials =
                MAPPER.createObjectNode()
                        .put("app_id", config.appId)
                        .put("app_secret", config.clientSecret)
                        .toString();
        long issuedAt = System.nanoTime();
        Response response = exchangeWithRetries(config.tokenUri, credentials, true, deadline);
        checkSuccess(response, "tenant authentication");
        JsonNode expire = response.body.path("expire");
        JsonNode token = response.body.path("tenant_access_token");
        if (!expire.isIntegralNumber()
                || !expire.canConvertToInt()
                || expire.intValue() <= 0
                || expire.intValue() > 7200
                || !token.isTextual()
                || !token.textValue().matches("[A-Za-z0-9._~+/=-]{1,8192}")) {
            throw failure("tenant authentication returned a malformed token or expiry");
        }
        long lifetimeMs = TimeUnit.SECONDS.toMillis(expire.intValue());
        refreshAfterNanos =
                issuedAt
                        + TimeUnit.MILLISECONDS.toNanos(
                                lifetimeMs - Math.min(30_000L, lifetimeMs / 10L));
        tenantToken = token.textValue();
    }

    /**
     * Encodes an opaque continuation as a query value on the already validated table endpoint;
     * provider cursor text cannot replace the configured origin or record-search path.
     */
    private URI recordsUri(String cursor) {
        URIBuilder builder =
                new URIBuilder(config.recordsUri)
                        .addParameter("page_size", Integer.toString(config.pageSize));
        if (cursor != null) {
            builder.addParameter("page_token", cursor);
        }
        try {
            return builder.build();
        } catch (URISyntaxException e) {
            throw failure("cannot encode the Bitable page request");
        }
    }

    /**
     * Retries only rate-limit and server failures within the configured count and shared deadline.
     * A provider delay that cannot fit fails rather than retrying early or extending the page
     * budget.
     */
    private Response exchangeWithRetries(URI uri, String body, boolean login, long deadline)
            throws InterruptedException {
        for (int retry = 0; ; retry++) {
            Response response = exchange(uri, body, login, deadline);
            if (!(response.status == 429
                    || (response.status >= 500 && response.status <= 599)
                    || response.code() == RATE_LIMIT_CODE)) {
                return response;
            }
            if (retry >= config.maxRetries) {
                throw failure(
                        (login ? "tenant authentication" : "record search")
                                + " retries exhausted after "
                                + response.statusAndCode());
            }
            long fallbackMs = 250L << retry;
            long delayMs = response.retryDelayMs >= 0 ? response.retryDelayMs : fallbackMs;
            if (delayMs >= remainingMillis(deadline)) {
                throw failure("page deadline would expire before the requested retry delay");
            }
            Thread.sleep(delayMs);
            if (!login) {
                ensureTenantToken(deadline);
            }
        }
    }

    /**
     * Publishes an abortable request before connecting and applies the remaining page budget.
     * Response ownership bounds reads and cleanup while transport diagnostics remain sanitized.
     */
    private Response exchange(URI uri, String body, boolean login, long deadline) {
        int timeoutMs = remainingMillis(deadline);
        HttpPost request = new HttpPost(uri);
        request.setConfig(
                RequestConfig.custom()
                        .setConnectTimeout(timeoutMs)
                        .setConnectionRequestTimeout(timeoutMs)
                        .setSocketTimeout(timeoutMs)
                        .build());
        request.setHeader("Accept", "application/json");
        request.setHeader("Accept-Encoding", "identity");
        request.setEntity(new StringEntity(body, ContentType.APPLICATION_JSON));
        if (!login) {
            request.setHeader("Authorization", "Bearer " + tenantToken);
        }
        activeRequest.set(request);
        try {
            if (closed) {
                throw failure("request was cancelled");
            }
            try (CloseableHttpResponse response = client.execute(request)) {
                int status = response.getStatusLine().getStatusCode();
                if (status >= 300 && status < 400) {
                    throw failure("refused credential-bearing redirect with HTTP " + status);
                }
                long retryDelayMs =
                        Math.max(
                                retryAfterMillis(response.getFirstHeader("Retry-After")),
                                secondsMillis(response.getFirstHeader("x-ogw-ratelimit-reset")));
                JsonNode payload = null;
                if ((status >= 200 && status < 300) || status == 400) {
                    byte[] bytes =
                            readBody(
                                    response.getEntity(),
                                    login ? MAX_TOKEN_RESPONSE_BYTES : MAX_PAGE_RESPONSE_BYTES,
                                    deadline);
                    payload = parse(bytes, status != 400);
                }
                return new Response(status, retryDelayMs, payload);
            }
        } catch (SocketTimeoutException | ConnectTimeoutException e) {
            throw failure("page deadline expired");
        } catch (IOException e) {
            if (System.nanoTime() - deadline >= 0) {
                throw failure("page deadline expired");
            }
            throw failure(
                    login
                            ? "tenant authentication transport failed"
                            : "record search transport failed");
        } finally {
            request.releaseConnection();
            activeRequest.compareAndSet(request, null);
        }
    }

    /**
     * Bounds body growth before parsing and checks the page clock between reads. The response owns
     * the stream so failure can close the connection without draining an oversized or stalled body.
     */
    private static byte[] readBody(HttpEntity entity, int sizeLimit, long deadline)
            throws IOException {
        if (entity == null) {
            return new byte[0];
        }
        if (entity.getContentLength() > sizeLimit) {
            throw failure("response exceeds the configured protocol size limit");
        }
        ByteArrayOutputStream body = new ByteArrayOutputStream();
        // The enclosing response owns the stream. On failure, closing the response aborts the
        // connection instead of trying to drain an oversized or stalled body.
        InputStream input = entity.getContent();
        byte[] buffer = new byte[8192];
        while (true) {
            remainingMillis(deadline);
            int read = input.read(buffer);
            if (read < 0) {
                return body.toByteArray();
            }
            if (body.size() + read > sizeLimit) {
                throw failure("response exceeds the configured protocol size limit");
            }
            body.write(buffer, 0, read);
        }
    }

    /**
     * Accepts only a strict JSON object envelope from already bounded bytes. Parser excerpts are
     * withheld because authentication and record responses can contain private data.
     */
    private static JsonNode parse(byte[] bytes, boolean required) {
        try {
            // Retain native byte/BOM validation while releasing symbol roots with this response.
            JsonNode body = MAPPER.copy().readTree(bytes);
            if (body != null && body.isObject()) {
                return body;
            }
        } catch (IOException e) {
            // JSON parsing exceptions may contain the request/response payload.
        }
        if (required) {
            throw failure("invalid JSON response envelope");
        }
        return null;
    }

    /**
     * Requires both HTTP success and provider code zero; a successful HTTP exchange alone cannot
     * turn a permission or authentication error into an empty table.
     */
    private static void checkSuccess(Response response, String operation) {
        if (response.status < 200 || response.status >= 300 || response.code() != 0) {
            throw failure(operation + " failed with " + response.statusAndCode());
        }
    }

    /**
     * Validates continuation and all records before any row is emitted. Optional missing/null
     * values remain nullable columns while the complete record JSON preserves their original
     * distinction.
     */
    private Page decodePage(JsonNode envelope) {
        JsonNode data = envelope.path("data");
        JsonNode hasMore = data.path("has_more");
        JsonNode items = data.path("items");
        if (!data.isObject()
                || !hasMore.isBoolean()
                || !(items.isArray() || items.isMissingNode() || items.isNull())
                || items.size() > config.pageSize) {
            throw failure("malformed Bitable page metadata or items");
        }
        JsonNode pageToken = data.path("page_token");
        String nextCursor = null;
        if (hasMore.booleanValue()) {
            if (!pageToken.isTextual()
                    || pageToken.textValue().trim().isEmpty()
                    || pageToken.textValue().length() > MAX_CURSOR_LENGTH
                    || pageToken.textValue().chars().anyMatch(Character::isISOControl)) {
                throw failure("has_more=true requires a valid Bitable page_token");
            }
            nextCursor = pageToken.textValue();
        } else if (!(pageToken.isMissingNode() || pageToken.isNull() || pageToken.isTextual())) {
            throw failure("malformed Bitable page_token");
        }

        List<SeaTunnelRow> rows = new ArrayList<>(items.size());
        for (JsonNode item : items) {
            JsonNode recordId = item.path("record_id");
            JsonNode id = item.path("id");
            JsonNode fields = item.path("fields");
            if (!item.isObject()
                    || !recordId.isTextual()
                    || recordId.textValue().trim().isEmpty()
                    || !(id.isTextual() || id.isMissingNode() || id.isNull())
                    || !(fields.isObject() || fields.isMissingNode() || fields.isNull())) {
                throw failure("Bitable item requires record_id and valid optional id and fields");
            }
            rows.add(
                    new SeaTunnelRow(
                            new Object[] {
                                recordId.textValue(),
                                id.textValue(),
                                fields.isObject() ? fields.toString() : null,
                                item.toString()
                            }));
        }
        return new Page(rows, nextCursor);
    }

    /**
     * Accepts numeric seconds or an HTTP date without shortening a valid provider delay. Overflow
     * becomes an over-budget wait, which the shared page deadline rejects.
     */
    private static long retryAfterMillis(Header header) {
        long seconds = secondsMillis(header);
        if (seconds >= 0 || header == null) {
            return seconds;
        }
        try {
            Instant retryAt =
                    ZonedDateTime.parse(header.getValue(), DateTimeFormatter.RFC_1123_DATE_TIME)
                            .toInstant();
            return Math.max(0L, Duration.between(Instant.now(), retryAt).toMillis());
        } catch (DateTimeParseException e) {
            return -1L;
        } catch (ArithmeticException e) {
            return Long.MAX_VALUE;
        }
    }

    /**
     * Distinguishes an absent/invalid delay from a valid zero, and saturates oversized numeric
     * values so retry admission cannot wrap them into an immediate request.
     */
    private static long secondsMillis(Header header) {
        if (header == null || !header.getValue().matches("[0-9]+")) {
            return -1L;
        }
        try {
            return TimeUnit.SECONDS.toMillis(Long.parseLong(header.getValue()));
        } catch (NumberFormatException e) {
            return Long.MAX_VALUE;
        }
    }

    /**
     * Uses the monotonic page clock for authentication, I/O and retry admission; expiration fails
     * instead of handing a nonpositive timeout to the HTTP client.
     */
    private static int remainingMillis(long deadline) {
        long remainingNanos = deadline - System.nanoTime();
        if (remainingNanos <= 0) {
            throw failure("page deadline expired");
        }
        return (int) Math.max(1L, TimeUnit.NANOSECONDS.toMillis(remainingNanos));
    }

    /**
     * Validated worker-to-poller handoff. Rows and continuation are transient; only the reader
     * commits accepted-prefix and cursor progress under its checkpoint boundary.
     */
    static final class Page {
        final List<SeaTunnelRow> rows;
        final String nextCursor;

        Page(List<SeaTunnelRow> rows, String nextCursor) {
            this.rows = rows;
            this.nextCursor = nextCursor;
        }
    }

    /**
     * Keeps one bounded exchange result private to request handling. Numeric status/code drive
     * retries and safe diagnostics while arbitrary provider message/body text stays internal.
     */
    private static final class Response {
        private final int status;
        private final long retryDelayMs;
        private final JsonNode body;

        private Response(int status, long retryDelayMs, JsonNode body) {
            this.status = status;
            this.retryDelayMs = retryDelayMs;
            this.body = body;
        }

        private int code() {
            JsonNode code = body == null ? null : body.get("code");
            return code != null && code.isIntegralNumber() && code.canConvertToInt()
                    ? code.intValue()
                    : Integer.MIN_VALUE;
        }

        private String statusAndCode() {
            return code() == Integer.MIN_VALUE
                    ? "HTTP " + status + " with missing or invalid API code"
                    : "HTTP " + status + ", API code " + code();
        }
    }
}
