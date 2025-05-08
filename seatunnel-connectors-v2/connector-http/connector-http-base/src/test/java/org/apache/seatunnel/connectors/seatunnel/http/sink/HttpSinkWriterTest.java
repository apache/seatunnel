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

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
public class HttpSinkWriterTest {

    private static final String TEST_URL = "http://example.com/test";
    private static final int BATCH_SIZE = 3;
    private static final int REQUEST_INTERVAL_MS = 0;
    private static final String FORMAT = "json";

    @Mock private HttpClientProvider httpClientProvider;

    @Captor private ArgumentCaptor<String> requestBodyCaptor;

    private HttpParameter httpParameter;
    private SeaTunnelRowType rowType;
    private TestableHttpSinkWriter sinkWriter;

    @BeforeEach
    public void setUp() throws Exception {
        // 设置 HTTP 参数
        httpParameter = new HttpParameter();
        httpParameter.setUrl(TEST_URL);
        Map<String, String> headers = new HashMap<>();
        headers.put("Content-Type", "application/json");
        httpParameter.setHeaders(headers);

        // 模拟HTTP响应
        HttpResponse mockResponse = Mockito.mock(HttpResponse.class);
        when(mockResponse.getCode()).thenReturn(HttpResponse.STATUS_OK);
        when(httpClientProvider.doPost(anyString(), any(), anyString())).thenReturn(mockResponse);

        // 创建行类型
        String[] fieldNames = new String[] {"id", "name", "age"};
        SeaTunnelDataType<?>[] dataTypes =
                new SeaTunnelDataType<?>[] {
                    BasicType.INT_TYPE, BasicType.STRING_TYPE, BasicType.INT_TYPE
                };
        rowType = new SeaTunnelRowType(fieldNames, dataTypes);
    }

    @Test
    public void testObjectModeIgnoresBatchSize() throws Exception {
        // 使用对象模式（默认），忽略批处理大小
        sinkWriter =
                new TestableHttpSinkWriter(
                        rowType, httpParameter, false, BATCH_SIZE, REQUEST_INTERVAL_MS, FORMAT);

        // 写入3条记录（等于批处理大小）
        for (int i = 0; i < BATCH_SIZE; i++) {
            SeaTunnelRow row = createTestRow(i + 1, "user" + (i + 1), 20 + i);
            sinkWriter.write(row);
        }

        // 在对象模式下，应该有3次HTTP请求，每条记录单独发送
        verify(httpClientProvider, times(3))
                .doPost(eq(TEST_URL), any(), requestBodyCaptor.capture());

        // 验证请求体格式（单个对象）
        for (String requestBody : requestBodyCaptor.getAllValues()) {
            assertTrue(requestBody.startsWith("{"));
            assertTrue(requestBody.endsWith("}"));
        }
    }

    @Test
    public void testArrayModeWithBatch() throws Exception {
        // 使用数组模式，开启批处理
        sinkWriter =
                new TestableHttpSinkWriter(
                        rowType, httpParameter, true, BATCH_SIZE, REQUEST_INTERVAL_MS, FORMAT);

        // 写入5条记录（超过批处理大小）
        for (int i = 0; i < 5; i++) {
            SeaTunnelRow row = createTestRow(i + 1, "user" + (i + 1), 20 + i);
            sinkWriter.write(row);
        }

        // 应该只有1次HTTP请求（第一批3条），剩余2条还未满足批处理大小
        verify(httpClientProvider, times(1))
                .doPost(eq(TEST_URL), any(), requestBodyCaptor.capture());

        // 验证请求体格式（数组）
        String requestBody = requestBodyCaptor.getValue();
        assertTrue(requestBody.startsWith("["));
        assertTrue(requestBody.endsWith("]"));

        // 关闭SinkWriter，应该再发送一次请求（剩余的2条记录）
        sinkWriter.close();
        verify(httpClientProvider, times(2))
                .doPost(eq(TEST_URL), any(), requestBodyCaptor.capture());

        // 验证第二次请求的内容
        requestBody = requestBodyCaptor.getValue();
        assertTrue(requestBody.startsWith("["));
        assertTrue(requestBody.endsWith("]"));
    }

    private SeaTunnelRow createTestRow(int id, String name, int age) {
        return new SeaTunnelRow(new Object[] {id, name, age});
    }

    private class TestableHttpSinkWriter extends HttpSinkWriter {
        public TestableHttpSinkWriter(
                SeaTunnelRowType seaTunnelRowType,
                HttpParameter httpParameter,
                boolean arrayMode,
                int batchSize,
                int requestIntervalMs,
                String format) {
            super(seaTunnelRowType, httpParameter, arrayMode, batchSize, requestIntervalMs, format);
        }

        @Override
        protected HttpClientProvider createHttpClient(HttpParameter httpParameter) {
            return httpClientProvider;
        }
    }
}
