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

import org.apache.seatunnel.api.table.catalog.TablePath;

import org.apache.http.HttpEntity;
import org.apache.http.StatusLine;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.impl.client.CloseableHttpClient;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.io.ByteArrayInputStream;
import java.io.IOException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class GravitinoClientTest {

    private static final String TEST_URL = "http://localhost:8090/api/test/tables/test_table";

    @Mock private CloseableHttpClient mockHttpClient;

    @Mock private CloseableHttpResponse mockResponse;

    @Mock private HttpEntity mockEntity;

    @Mock private StatusLine mockStatusLine;

    // ========== TablePath Parsing Tests ==========

    @Test
    void testGetTableSchemaPathWithFullUrl() {
        String url = "http://localhost:8090/catalogs/postgres/schemas/public/tables/users";
        try (GravitinoClient client = new GravitinoClient()) {
            TablePath tablePath = client.getTableSchemaPath(url);
            Assertions.assertNotNull(tablePath);
            Assertions.assertEquals("postgres", tablePath.getDatabaseName());
            Assertions.assertEquals("public", tablePath.getSchemaName());
            Assertions.assertEquals("users", tablePath.getTableName());
        }
    }

    @Test
    void testIOExceptionRetrySuccessAfterFailure() throws Exception {
        // Setup: first two calls fail with IOException, third succeeds
        setupMockResponse(200, "{\"table\":{\"name\":\"test_table\"}}");
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
    void testIOExceptionRetryExhaustedThrowsException() throws IOException {
        // Setup: all calls fail with IOException
        when(mockHttpClient.execute(any())).thenThrow(new IOException("Connection timeout"));
        // Execute
        try (GravitinoClient client = new GravitinoClient(mockHttpClient)) {
            Exception exception =
                    Assertions.assertThrows(Exception.class, () -> client.getTableSchema(TEST_URL));
            // Verify exception message contains URL and retry count
            Assertions.assertTrue(
                    exception.getMessage().contains(TEST_URL),
                    "Exception message should contain URL");
            Assertions.assertTrue(
                    exception.getMessage().contains("3 attempts"),
                    "Exception message should contain retry count");
        }
        // Verify exactly 3 attempts were made (MAX_RETRY_ATTEMPTS)
        verify(mockHttpClient, times(3)).execute(any());
    }

    @Test
    void testIOExceptionRetryWithSingleFailureThenSuccess() throws Exception {
        // Setup: first call fails, second succeeds
        setupMockResponse(200, "{\"table\":{\"name\":\"test_table\"}}");
        when(mockHttpClient.execute(any()))
                .thenThrow(new IOException("Read timed out"))
                .thenReturn(mockResponse);
        // Execute
        try (GravitinoClient client = new GravitinoClient(mockHttpClient)) {
            JsonNode result = client.getTableSchema(TEST_URL);
            Assertions.assertNotNull(result);
            Assertions.assertEquals("test_table", result.get("name").asText());
        }
        // Verify 2 attempts were made
        verify(mockHttpClient, times(2)).execute(any());
    }

    @Test
    void testRetryableStatus503SuccessAfterRetry() throws Exception {
        // Setup: first call returns 503, second succeeds
        setupMockResponse(200, "{\"table\":{\"name\":\"test_table\"}}");
        when(mockHttpClient.execute(any())).thenReturn(mockResponse).thenReturn(mockResponse);
        // Configure first response with 503, second with 200
        setupMockResponseStatusLine(503);
        when(mockHttpClient.execute(any()))
                .thenReturn(mockResponse)
                .thenAnswer(
                        invocation -> {
                            setupMockResponse(200, "{\"table\":{\"name\":\"test_table\"}}");
                            return mockResponse;
                        });
        // Re-setup with proper sequence
        resetMocks();
        CloseableHttpResponse response503 = createMockResponse(503, null);
        CloseableHttpResponse response200 =
                createMockResponse(200, "{\"table\":{\"name\":\"test_table\"}}");
        when(mockHttpClient.execute(any())).thenReturn(response503).thenReturn(response200);
        try (GravitinoClient client = new GravitinoClient(mockHttpClient)) {
            JsonNode result = client.getTableSchema(TEST_URL);
            Assertions.assertNotNull(result);
            Assertions.assertEquals("test_table", result.get("name").asText());
        }
        verify(mockHttpClient, times(2)).execute(any());
    }

    @Test
    void testRetryableStatus500IsRetried() throws Exception {
        // Setup: first returns 500, second succeeds
        CloseableHttpResponse response500 = createMockResponse(500, null);
        CloseableHttpResponse response200 =
                createMockResponse(200, "{\"table\":{\"name\":\"test_table\"}}");

        when(mockHttpClient.execute(any())).thenReturn(response500).thenReturn(response200);

        try (GravitinoClient client = new GravitinoClient(mockHttpClient)) {
            JsonNode result = client.getTableSchema(TEST_URL);
            Assertions.assertNotNull(result);
            Assertions.assertEquals("test_table", result.get("name").asText());
        }
        verify(mockHttpClient, times(2)).execute(any());
    }

    @Test
    void testNonRetryableStatus404FailsImmediately() throws IOException {
        // Setup: 404 Not Found (non-retryable)
        CloseableHttpResponse response404 = createMockResponse(404, null);
        when(mockHttpClient.execute(any())).thenReturn(response404);
        try (GravitinoClient client = new GravitinoClient(mockHttpClient)) {
            Exception exception =
                    Assertions.assertThrows(Exception.class, () -> client.getTableSchema(TEST_URL));
            Assertions.assertTrue(exception.getMessage().contains("404"));
        }
        // Verify only 1 attempt was made
        verify(mockHttpClient, times(1)).execute(any());
    }

    @Test
    void testMixedFailuresBeforeSuccess() throws Exception {
        // Setup: IOException, then 503, then success
        CloseableHttpResponse response503 = createMockResponse(503, null);
        CloseableHttpResponse response200 =
                createMockResponse(200, "{\"table\":{\"name\":\"test_table\"}}");
        when(mockHttpClient.execute(any()))
                .thenThrow(new IOException("Connection reset"))
                .thenReturn(response503)
                .thenReturn(response200);
        try (GravitinoClient client = new GravitinoClient(mockHttpClient)) {
            JsonNode result = client.getTableSchema(TEST_URL);
            Assertions.assertNotNull(result);
        }
        // Verify 3 attempts were made
        verify(mockHttpClient, times(3)).execute(any());
    }

    /** Helper method to setup mock response with JSON content. */
    private void setupMockResponse(int statusCode, String jsonContent) throws IOException {
        when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
        when(mockStatusLine.getStatusCode()).thenReturn(statusCode);
        if (jsonContent != null) {
            when(mockResponse.getEntity()).thenReturn(mockEntity);
            when(mockEntity.getContent())
                    .thenReturn(new ByteArrayInputStream(jsonContent.getBytes()));
            when(mockEntity.isStreaming()).thenReturn(false);
        }
    }

    /** Helper method to setup mock status line. */
    private void setupMockResponseStatusLine(int statusCode) {
        when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
        when(mockStatusLine.getStatusCode()).thenReturn(statusCode);
    }

    /** Reset mock configurations. */
    private void resetMocks() {
        org.mockito.Mockito.reset(mockHttpClient, mockResponse, mockEntity, mockStatusLine);
    }

    /**
     * Create a mock HTTP response with specified status code and optional JSON content.
     *
     * @param statusCode HTTP status code
     * @param jsonContent JSON content (null for error responses without body)
     * @return mock CloseableHttpResponse
     * @throws IOException if setting up mock content fails
     */
    private CloseableHttpResponse createMockResponse(int statusCode, String jsonContent)
            throws IOException {
        CloseableHttpResponse response = org.mockito.Mockito.mock(CloseableHttpResponse.class);
        StatusLine statusLine = org.mockito.Mockito.mock(StatusLine.class);

        when(response.getStatusLine()).thenReturn(statusLine);
        when(statusLine.getStatusCode()).thenReturn(statusCode);

        if (jsonContent != null) {
            HttpEntity entity = org.mockito.Mockito.mock(HttpEntity.class);
            when(response.getEntity()).thenReturn(entity);
            when(entity.getContent()).thenReturn(new ByteArrayInputStream(jsonContent.getBytes()));
        } else {
            when(response.getEntity()).thenReturn(null);
        }
        return response;
    }
}
