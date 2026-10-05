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

package org.apache.seatunnel.transform.embedding;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.common.utils.VectorUtils;
import org.apache.seatunnel.transform.nlpmodel.ModelInvocationErrorType;
import org.apache.seatunnel.transform.nlpmodel.ModelInvocationException;
import org.apache.seatunnel.transform.nlpmodel.embedding.EmbeddingTransform;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;
import okhttp3.mockwebserver.SocketPolicy;

import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

@Timeout(60)
class AmazonEmbeddingInvocationTest {

    @Test
    void shouldReadBatchSizeBeforeConstructingClient() throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            Map<String, Object> overrides = new HashMap<>();
            overrides.put("single_vectorized_input_number", "not-a-number");
            // Invalid endpoint parsing must not precede the existing configuration failure.
            overrides.put("api_path", "not a URI");
            EmbeddingTransform transform = transform(server, 3, null, overrides);
            try {
                IllegalArgumentException failure =
                        assertThrows(
                                IllegalArgumentException.class, transform::getProducedCatalogTable);
                assertTrue(failure.getMessage().contains("not-a-number"));
                assertEquals(0, server.getRequestCount());
            } finally {
                transform.close();
            }
        }
    }

    @Test
    void shouldApplyConfiguredRetriesThroughTransform() throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            // HTTP 424 is retried by SeaTunnel's adapter, not the SDK's default HTTP retry policy.
            server.enqueue(modelError());
            server.enqueue(modelError());
            server.enqueue(success());
            EmbeddingTransform transform = transform(server, 3, 5000);
            try {
                transform.getProducedCatalogTable();
                assertVector(transform.map(row()));
                assertEquals(3, server.getRequestCount());
                String payload = request(server).getBody().readUtf8();
                assertEquals(payload, request(server).getBody().readUtf8());
                assertEquals(payload, request(server).getBody().readUtf8());
            } finally {
                transform.close();
            }
        }
    }

    @Test
    void shouldKeepSingleSeaTunnelAttemptByDefault() throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            server.enqueue(modelError());
            server.enqueue(success());
            EmbeddingTransform transform = transform(server, null, null);
            try {
                transform.getProducedCatalogTable();
                ModelInvocationException failure = failure(transform);
                assertEquals(
                        ModelInvocationErrorType.TEMPORARY_REMOTE_ERROR, failure.getErrorType());
                assertTrue(failure.isRetryable());
                assertEquals(1, server.getRequestCount());
            } finally {
                transform.close();
            }
        }
    }

    @Test
    void shouldStopAfterConfiguredAttempts() throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            server.enqueue(modelError());
            server.enqueue(modelError());
            server.enqueue(success());
            EmbeddingTransform transform = transform(server, 2, 5000);
            try {
                transform.getProducedCatalogTable();
                ModelInvocationException failure = failure(transform);
                assertEquals(
                        ModelInvocationErrorType.TEMPORARY_REMOTE_ERROR, failure.getErrorType());
                assertEquals(2, server.getRequestCount());
            } finally {
                transform.close();
            }
        }
    }

    @Test
    void shouldNotRetryAuthenticationFailure() throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            server.enqueue(
                    error(
                            403,
                            "AccessDeniedException",
                            "api_key=private-api secret_key=private-secret"));
            server.enqueue(success());
            EmbeddingTransform transform = transform(server, 3, 5000);
            try {
                transform.getProducedCatalogTable();
                ModelInvocationException failure = failure(transform);
                assertEquals(ModelInvocationErrorType.AUTHENTICATION_ERROR, failure.getErrorType());
                assertFalse(failure.isRetryable());
                assertFalse(failure.getMessage().contains("private-api"));
                assertFalse(failure.getMessage().contains("private-secret"));
                assertEquals(1, server.getRequestCount());
            } finally {
                transform.close();
            }
        }
    }

    @Test
    void shouldNotRetryMissingVector() throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            server.enqueue(new MockResponse().setBody("{}"));
            server.enqueue(success());
            EmbeddingTransform transform = transform(server, 3, 5000);
            try {
                transform.getProducedCatalogTable();
                ModelInvocationException failure = failure(transform);
                assertEquals(
                        ModelInvocationErrorType.RESPONSE_COUNT_MISMATCH, failure.getErrorType());
                assertFalse(failure.isRetryable());
                assertEquals(1, server.getRequestCount());
            } finally {
                transform.close();
            }
        }
    }

    @Test
    void shouldNotRetryMalformedResponse() throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            server.enqueue(new MockResponse().setBody("not-json"));
            server.enqueue(success());
            EmbeddingTransform transform = transform(server, 3, 5000);
            try {
                transform.getProducedCatalogTable();
                ModelInvocationException failure = failure(transform);
                assertEquals(ModelInvocationErrorType.RESPONSE_PARSE_ERROR, failure.getErrorType());
                assertFalse(failure.isRetryable());
                assertEquals(1, server.getRequestCount());
            } finally {
                transform.close();
            }
        }
    }

    @Test
    void shouldPreserveExternalInterruptionWithoutAnotherHttpRequest() throws Exception {
        try (MockWebServer server = new MockWebServer()) {
            Thread caller = Thread.currentThread();
            AtomicBoolean interruptFirstRequest = new AtomicBoolean(true);
            server.setDispatcher(
                    new Dispatcher() {
                        @Override
                        public MockResponse dispatch(RecordedRequest request) {
                            if (interruptFirstRequest.compareAndSet(true, false)) {
                                caller.interrupt();
                            }
                            return new MockResponse().setSocketPolicy(SocketPolicy.NO_RESPONSE);
                        }
                    });
            EmbeddingTransform transform = transform(server, 3, 5000);
            try {
                transform.getProducedCatalogTable();
                failure(transform);
                assertTrue(
                        Thread.currentThread().isInterrupted(),
                        "External interrupt was cleared; observed HTTP requests: "
                                + server.getRequestCount());
                assertEquals(1, server.getRequestCount());
            } finally {
                // Clear only the test-owned interrupt before client/server teardown.
                Thread.interrupted();
                transform.close();
            }
        }
    }

    private static EmbeddingTransform transform(
            MockWebServer server, Integer attempts, Integer timeout) {
        return transform(server, attempts, timeout, Collections.emptyMap());
    }

    private static EmbeddingTransform transform(
            MockWebServer server,
            Integer attempts,
            Integer timeout,
            Map<String, Object> overrides) {
        Map<String, Object> options = new HashMap<>();
        options.put("model_provider", "AMAZON");
        options.put("model", "amazon.titan-embed-text-v2:0");
        options.put("aws_region", "us-east-1");
        options.put("api_key", "test-access-key");
        options.put("secret_key", "test-secret-key");
        options.put("api_path", server.url("/").toString());
        options.put("dimension", 2);
        options.put("vectorization_fields", Collections.singletonMap("vector", "text"));
        options.put("model_retry_backoff_ms", 0);
        options.put("model_retry_max_backoff_ms", 0);
        if (attempts != null) {
            options.put("model_retry_max_attempts", attempts);
        }
        if (timeout != null) {
            options.put("model_request_timeout_ms", timeout);
        }
        options.putAll(overrides);
        CatalogTable table =
                CatalogTable.of(
                        TableIdentifier.of("catalog", "database", "documents"),
                        TableSchema.builder()
                                .column(
                                        PhysicalColumn.of(
                                                "text",
                                                BasicType.STRING_TYPE,
                                                1024L,
                                                true,
                                                null,
                                                ""))
                                .build(),
                        Collections.emptyMap(),
                        Collections.emptyList(),
                        "");
        return new EmbeddingTransform(ReadonlyConfig.fromMap(options), table);
    }

    private static SeaTunnelRow row() {
        return new SeaTunnelRow(new Object[] {"test document"});
    }

    private static void assertVector(SeaTunnelRow row) {
        assertEquals("test document", row.getField(0));
        assertEquals(
                VectorUtils.toByteBuffer(new Float[] {0.25F, 0.5F}), (ByteBuffer) row.getField(1));
    }

    private static ModelInvocationException failure(EmbeddingTransform transform) {
        RuntimeException failure = assertThrows(RuntimeException.class, () -> transform.map(row()));
        assertEquals("Failed to data vectorization", failure.getMessage());
        assertTrue(failure.getCause() instanceof ModelInvocationException);
        return (ModelInvocationException) failure.getCause();
    }

    private static MockResponse success() {
        return new MockResponse().setBody("{\"embedding\":[0.25,0.5]}");
    }

    private static MockResponse modelError() {
        return error(424, "ModelErrorException", "Temporary model failure");
    }

    private static MockResponse error(int status, String type, String message) {
        return new MockResponse()
                .setResponseCode(status)
                .setHeader("Content-Type", "application/json")
                .setHeader("x-amzn-errortype", type)
                .setBody("{\"message\":\"" + message + "\"}");
    }

    private static RecordedRequest request(MockWebServer server) throws InterruptedException {
        RecordedRequest request = server.takeRequest(5, TimeUnit.SECONDS);
        assertNotNull(request);
        return request;
    }
}
