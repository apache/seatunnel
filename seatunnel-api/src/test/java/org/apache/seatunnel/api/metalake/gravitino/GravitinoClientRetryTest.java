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

package org.apache.seatunnel.api.metalake.gravitino;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.impl.client.CloseableHttpClient;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class GravitinoClientRetryTest {

    private static final String TEST_URL = "http://localhost:8090/api/test/tables/test_table";

    @Mock private CloseableHttpClient mockHttpClient;

    @Mock private CloseableHttpResponse mockResponse;

    @Mock private HttpEntity mockEntity;

    @Test
    void testRetrySuccessAfterFailure() throws Exception {
        // Setup: first two calls fail, third succeeds
        setupMockResponse("{\"table\":{\"name\":\"test_table\"}}");
        when(mockHttpClient.execute(any()))
                .thenThrow(new IOException("Connection timeout"))
                .thenThrow(new IOException("Connection reset"))
                .thenReturn(mockResponse);
        // Execute
        try (GravitinoClient client = new GravitinoClient(mockHttpClient)) {
            JsonNode result = client.getTableSchema(TEST_URL);
            // Verify success
            Assertions.assertNotNull(result);
            Assertions.assertEquals("test_table", result.get("name").asText());
        }
        // Verify exactly 3 attempts were made
        verify(mockHttpClient, times(3)).execute(any());
    }

    @Test
    void testRetryExhaustedThrowsIOException() throws IOException {
        // Setup: all calls fail with retryable exception
        when(mockHttpClient.execute(any())).thenThrow(new IOException("Connection timeout"));
        // Execute
        try (GravitinoClient client = new GravitinoClient(mockHttpClient)) {
            IOException exception =
                    Assertions.assertThrows(
                            IOException.class, () -> client.getTableSchema(TEST_URL));
            // Verify exception message contains URL and retry count
            Assertions.assertTrue(
                    exception.getMessage().contains(TEST_URL),
                    "Exception message should contain URL");
            Assertions.assertTrue(
                    exception.getMessage().contains("3 attempts"),
                    "Exception message should contain retry count");
            Assertions.assertTrue(
                    exception.getCause() instanceof IOException,
                    "Exception should have original IOException as cause");
        }
        // Verify exactly 3 attempts were made (MAX_RETRY_ATTEMPTS)
        verify(mockHttpClient, times(3)).execute(any());
    }

    @Test
    void testNonRetryableExceptionFailsImmediately() throws IOException {
        // Setup: non-retryable exception (DNS/UnknownHost)
        when(mockHttpClient.execute(any()))
                .thenThrow(new IOException("UnknownHost: invalid-host.com"));
        // Execute
        try (GravitinoClient client = new GravitinoClient(mockHttpClient)) {
            IOException exception =
                    Assertions.assertThrows(
                            IOException.class, () -> client.getTableSchema(TEST_URL));
            // Verify it's a DNS-related failure
            Assertions.assertTrue(
                    exception.getMessage().contains("UnknownHost")
                            || exception.getCause().getMessage().contains("UnknownHost"),
                    "Exception should indicate DNS failure");
        }
        // Verify only 1 attempt was made (no retries for non-retryable exceptions)
        verify(mockHttpClient, times(1)).execute(any());
    }

    @Test
    void test404ErrorNotRetried() throws IOException {
        // Setup: 404 error (non-retryable)
        when(mockHttpClient.execute(any())).thenThrow(new IOException("404 Not Found"));
        // Execute
        try (GravitinoClient client = new GravitinoClient(mockHttpClient)) {
            Assertions.assertThrows(IOException.class, () -> client.getTableSchema(TEST_URL));
        }
        // Verify only 1 attempt was made
        verify(mockHttpClient, times(1)).execute(any());
    }

    @Test
    void test401ErrorNotRetried() throws IOException {
        // Setup: 401 error (non-retryable)
        when(mockHttpClient.execute(any())).thenThrow(new IOException("401 Unauthorized"));
        // Execute
        try (GravitinoClient client = new GravitinoClient(mockHttpClient)) {
            Assertions.assertThrows(IOException.class, () -> client.getTableSchema(TEST_URL));
        }
        // Verify only 1 attempt was made
        verify(mockHttpClient, times(1)).execute(any());
    }

    @Test
    void testSSLErrorNotRetried() throws IOException {
        // Setup: SSL error (non-retryable)
        when(mockHttpClient.execute(any())).thenThrow(new IOException("SSL handshake failed"));
        // Execute
        try (GravitinoClient client = new GravitinoClient(mockHttpClient)) {
            Assertions.assertThrows(IOException.class, () -> client.getTableSchema(TEST_URL));
        }
        // Verify only 1 attempt was made
        verify(mockHttpClient, times(1)).execute(any());
    }

    @Test
    void test503ErrorIsRetried() throws Exception {
        // Setup: 503 error (retryable - server error)
        setupMockResponse("{\"table\":{\"name\":\"test_table\"}}");
        when(mockHttpClient.execute(any()))
                .thenThrow(new IOException("503 Service Unavailable"))
                .thenReturn(mockResponse);
        // Execute
        try (GravitinoClient client = new GravitinoClient(mockHttpClient)) {
            JsonNode result = client.getTableSchema(TEST_URL);
            // Verify success after retry
            Assertions.assertNotNull(result);
            Assertions.assertEquals("test_table", result.get("name").asText());
        }
        // Verify 2 attempts were made (initial + 1 retry)
        verify(mockHttpClient, times(2)).execute(any());
    }

    @Test
    void testConnectionTimeoutIsRetried() throws Exception {
        // Setup: connection timeout (retryable)
        setupMockResponse("{\"table\":{\"name\":\"test_table\"}}");
        when(mockHttpClient.execute(any()))
                .thenThrow(new IOException("Read timed out"))
                .thenReturn(mockResponse);
        // Execute
        try (GravitinoClient client = new GravitinoClient(mockHttpClient)) {
            JsonNode result = client.getTableSchema(TEST_URL);
            // Verify success after retry
            Assertions.assertNotNull(result);
        }
        // Verify 2 attempts were made
        verify(mockHttpClient, times(2)).execute(any());
    }

    @Test
    void testRetryWithMultipleFailuresBeforeSuccess() throws Exception {
        // Setup: fail twice, then succeed
        setupMockResponse("{\"table\":{\"name\":\"test_table\"}}");
        when(mockHttpClient.execute(any()))
                .thenThrow(new IOException("Connection reset"))
                .thenThrow(new IOException("Read timed out"))
                .thenReturn(mockResponse);
        // Execute
        try (GravitinoClient client = new GravitinoClient(mockHttpClient)) {
            JsonNode result = client.getTableSchema(TEST_URL);
            // Verify success
            Assertions.assertNotNull(result);
        }
        // Verify 3 attempts were made
        verify(mockHttpClient, times(3)).execute(any());
    }

    @Test
    void testNullMessageExceptionIsRetried() throws IOException {
        // Setup: exception with null message (default to retryable)
        when(mockHttpClient.execute(any())).thenThrow(new IOException((String) null));
        // Execute
        try (GravitinoClient client = new GravitinoClient(mockHttpClient)) {
            Assertions.assertThrows(IOException.class, () -> client.getTableSchema(TEST_URL));
        }
        // Verify 3 attempts were made (null message defaults to retryable)
        verify(mockHttpClient, times(3)).execute(any());
    }

    @Test
    void testEmptyMessageExceptionIsRetried() throws IOException {
        // Setup: exception with empty message (default to retryable)
        when(mockHttpClient.execute(any())).thenThrow(new IOException(""));
        // Execute
        try (GravitinoClient client = new GravitinoClient(mockHttpClient)) {
            Assertions.assertThrows(IOException.class, () -> client.getTableSchema(TEST_URL));
        }
        // Verify 3 attempts were made
        verify(mockHttpClient, times(3)).execute(any());
    }

    /** Helper method to setup mock response with JSON content. */
    private void setupMockResponse(String jsonContent) throws IOException {
        when(mockResponse.getEntity()).thenReturn(mockEntity);
        when(mockEntity.getContent()).thenReturn(new ByteArrayInputStream(jsonContent.getBytes()));
        when(mockEntity.isStreaming()).thenReturn(false);
    }
}
