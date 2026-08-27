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

package org.apache.seatunnel.connectors.seatunnel.http.sink;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.http.client.HttpClientProvider;
import org.apache.seatunnel.connectors.seatunnel.http.client.HttpResponse;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpParameter;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class HttpSinkBatchWriterTest {

    private static final String TEST_URL = "http://example.com/test";
    private static final int BATCH_SIZE = 3;
    private static final int REQUEST_INTERVAL_MS = 0;

    @Mock private HttpClientProvider httpClientProvider;

    @Captor private ArgumentCaptor<String> requestBodyCaptor;

    private HttpParameter httpParameter;
    private SeaTunnelRowType rowType;
    private TestableHttpSinkWriter sinkWriter;

    @BeforeEach
    public void setUp() throws Exception {
        // Setting HTTP Parameters
        httpParameter = new HttpParameter();
        httpParameter.setUrl(TEST_URL);
        Map<String, String> headers = new HashMap<>();
        headers.put("Content-Type", "application/json");
        httpParameter.setHeaders(headers);

        // Simulate HTTP response
        HttpResponse mockResponse = Mockito.mock(HttpResponse.class);
        when(mockResponse.getCode()).thenReturn(HttpResponse.STATUS_OK);
        when(httpClientProvider.doPost(anyString(), any(), anyString())).thenReturn(mockResponse);

        // Creating Row Types
        String[] fieldNames = new String[] {"id", "name", "age"};
        SeaTunnelDataType<?>[] dataTypes =
                new SeaTunnelDataType<?>[] {
                    BasicType.INT_TYPE, BasicType.STRING_TYPE, BasicType.INT_TYPE
                };
        rowType = new SeaTunnelRowType(fieldNames, dataTypes);
    }

    @Test
    public void testDefaultParameterValues() throws Exception {
        // No parameters are set, use default values
        // default：arrayMode = false, batchSize = 1, requestIntervalMs = 0
        HttpParameter defaultHttpParameter = new HttpParameter();
        defaultHttpParameter.setUrl(TEST_URL);
        Map<String, String> headers = new HashMap<>();
        headers.put("Content-Type", "application/json");
        defaultHttpParameter.setHeaders(headers);

        // Verify the default parameter value
        assertFalse(defaultHttpParameter.isArrayMode());
        assertEquals(1, defaultHttpParameter.getBatchSize());
        assertEquals(0, defaultHttpParameter.getRequestIntervalMs());

        sinkWriter = new TestableHttpSinkWriter(rowType, defaultHttpParameter);

        // Write 3 records
        for (int i = 0; i < 3; i++) {
            SeaTunnelRow row = createTestRow(i + 1, "user" + (i + 1), 20 + i);
            sinkWriter.write(row);
        }

        // In the default object mode, there should be 3 HTTP requests, each record is sent
        // separately
        verify(httpClientProvider, times(3))
                .doPost(eq(TEST_URL), any(), requestBodyCaptor.capture());

        // Verify request format (single object)
        for (String requestBody : requestBodyCaptor.getAllValues()) {
            assertTrue(requestBody.startsWith("{"));
            assertTrue(requestBody.endsWith("}"));
        }
    }

    @Test
    public void testObjectModeIgnoresBatchSize() throws Exception {
        // Use object mode (default) to ignore batch size
        httpParameter.setArrayMode(false);
        httpParameter.setBatchSize(BATCH_SIZE);
        httpParameter.setRequestIntervalMs(REQUEST_INTERVAL_MS);
        sinkWriter = new TestableHttpSinkWriter(rowType, httpParameter);

        // Write 3 records (equal to batch size)
        for (int i = 0; i < BATCH_SIZE; i++) {
            SeaTunnelRow row = createTestRow(i + 1, "user" + (i + 1), 20 + i);
            sinkWriter.write(row);
        }

        // In object mode, there should be 3 HTTP requests, each record sent separately
        verify(httpClientProvider, times(3))
                .doPost(eq(TEST_URL), any(), requestBodyCaptor.capture());

        // Validation request format (single object)
        for (String requestBody : requestBodyCaptor.getAllValues()) {
            assertTrue(requestBody.startsWith("{"));
            assertTrue(requestBody.endsWith("}"));
        }
    }

    @Test
    public void testArrayModeWithBatch() throws Exception {
        // Use array mode to turn on batch processing
        httpParameter.setArrayMode(true);
        httpParameter.setBatchSize(BATCH_SIZE);
        httpParameter.setRequestIntervalMs(REQUEST_INTERVAL_MS);
        sinkWriter = new TestableHttpSinkWriter(rowType, httpParameter);

        // Write 5 records (over batch size)
        for (int i = 0; i < 5; i++) {
            SeaTunnelRow row = createTestRow(i + 1, "user" + (i + 1), 20 + i);
            sinkWriter.write(row);
        }

        // There should only be 1 HTTP request (the first batch of 3), the remaining 2 have not yet
        // met the batch size
        verify(httpClientProvider, times(1))
                .doPost(eq(TEST_URL), any(), requestBodyCaptor.capture());

        // Validation request format (array)
        String requestBody = requestBodyCaptor.getValue();
        assertTrue(requestBody.startsWith("["));
        assertTrue(requestBody.endsWith("]"));

        // Close SinkWriter, should send another request (for the remaining 2 records)
        sinkWriter.close();
        verify(httpClientProvider, times(2))
                .doPost(eq(TEST_URL), any(), requestBodyCaptor.capture());

        // Validating the content of the second request
        requestBody = requestBodyCaptor.getValue();
        assertTrue(requestBody.startsWith("["));
        assertTrue(requestBody.endsWith("]"));
    }

    private SeaTunnelRow createTestRow(int id, String name, int age) {
        return new SeaTunnelRow(new Object[] {id, name, age});
    }

    private class TestableHttpSinkWriter extends HttpSinkWriter {
        public TestableHttpSinkWriter(
                SeaTunnelRowType seaTunnelRowType, HttpParameter httpParameter) {
            super(seaTunnelRowType, httpParameter);
        }

        @Override
        protected HttpClientProvider createHttpClient(HttpParameter httpParameter) {
            return httpClientProvider;
        }
    }
}
