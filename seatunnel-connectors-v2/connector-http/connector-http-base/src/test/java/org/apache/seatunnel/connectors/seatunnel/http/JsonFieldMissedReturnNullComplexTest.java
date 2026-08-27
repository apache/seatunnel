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

package org.apache.seatunnel.connectors.seatunnel.http;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.http.client.HttpClientProvider;
import org.apache.seatunnel.connectors.seatunnel.http.client.HttpResponse;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpParameter;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpRequestMethod;
import org.apache.seatunnel.connectors.seatunnel.http.config.JsonField;
import org.apache.seatunnel.connectors.seatunnel.http.source.HttpSourceReader;
import org.apache.seatunnel.connectors.seatunnel.http.source.SimpleTextDeserializationSchema;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class JsonFieldMissedReturnNullComplexTest {

    private HttpParameter httpParameter;
    private JsonField jsonField;
    private SimpleTextDeserializationSchema deserializationSchema;

    @Mock private SingleSplitReaderContext context;

    @Mock private Collector<SeaTunnelRow> collector;

    @Mock private HttpClientProvider httpClientProvider;

    @Mock private HttpResponse httpResponse;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);

        httpParameter = new HttpParameter();
        httpParameter.setUrl("http://test-url.com");
        httpParameter.setMethod(HttpRequestMethod.GET);

        Map<String, String> fields = new HashMap<>();
        fields.put("key1_1", "$.result.rows[*].key1.key1_1");
        fields.put("key2_1", "$.result.rows[*].key2.key2_1");
        jsonField = JsonField.builder().fields(fields).build();

        // Create the schema with two string fields
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"key1_1", "key2_1"},
                        new SeaTunnelDataType[] {BasicType.STRING_TYPE, BasicType.STRING_TYPE});
        deserializationSchema = new SimpleTextDeserializationSchema(rowType);

        // Setup mocks
        when(httpResponse.getCode()).thenReturn(200);
        when(collector.getCheckpointLock()).thenReturn(new Object());
    }

    @Test
    public void testJsonFieldMissedReturnNull() throws Exception {
        // Test data with missing fields  Array with common parent path
        String testJsonData =
                "{\n"
                        + "    \"result\": {\n"
                        + "        \"rows\": [\n"
                        + "            {\n"
                        + "                \"rowNumber\": 1,\n"
                        + "                \"key1\": {\n"
                        + "                    \"key1_1\": \"value11\"\n"
                        + "                },\n"
                        + "                \"key2\": {\n"
                        + "                    \"key2_1\": 100\n"
                        + "                }\n"
                        + "            },\n"
                        + "            {\n"
                        + "                \"rowNumber\": 2,\n"
                        + "                \"key1\": {\n"
                        + "                },\n"
                        + "                \"key2\": {\n"
                        + "                    \"key2_1\": 200\n"
                        + "                }\n"
                        + "            },\n"
                        + "            {\n"
                        + "                \"rowNumber\": 3,\n"
                        + "                \"key1\": {\n"
                        + "                    \"key1_1\": \"value33\"\n"
                        + "                },\n"
                        + "                \"key2\": {\n"
                        + "                }\n"
                        + "            },\n"
                        + "            {\n"
                        + "                \"rowNumber\": 4,\n"
                        + "                \"key1\": {\n"
                        + "                    \"key1_1\": \"value44\"\n"
                        + "                }\n"
                        + "            },\n"
                        + "            {\n"
                        + "                \"rowNumber\": 5,\n"
                        + "                \"key2\": {\n"
                        + "                    \"key2_1\": 500\n"
                        + "                }\n"
                        + "            },\n"
                        + "            {\n"
                        + "                \"rowNumber\": 6,\n"
                        + "                \"key1\": null,\n"
                        + "                \"key2\": {\n"
                        + "                    \"key2_1\": 600\n"
                        + "                }\n"
                        + "            },\n"
                        + "            {\n"
                        + "                \"rowNumber\": 7,\n"
                        + "                \"key1\": {\n"
                        + "                    \"key1_1\": \"value77\"\n"
                        + "                },\n"
                        + "                \"key2\": null\n"
                        + "            }\n"
                        + "        ]\n"
                        + "    }\n"
                        + "}";

        // Set json_filed_missed_return_null to true
        httpParameter.setJsonFiledMissedReturnNull(true);

        // Setup HTTP response
        when(httpResponse.getContent()).thenReturn(testJsonData);
        when(httpClientProvider.execute(
                        anyString(), anyString(), any(), any(), any(), any(Boolean.class)))
                .thenReturn(httpResponse);

        // Create HttpSourceReader
        HttpSourceReader sourceReader =
                new HttpSourceReader(
                        httpParameter, context, deserializationSchema, jsonField, null);

        // Use reflection to inject our mocked HTTP client
        sourceReader.open(); // This creates the real HTTP client
        sourceReader.setHttpClient(httpClientProvider);

        //        Field httpClientField = HttpSourceReader.class.getDeclaredField("httpClient");
        //        httpClientField.setAccessible(true);
        //        httpClientField.set(sourceReader, httpClientProvider);

        // Capture the rows collected
        ArgumentCaptor<SeaTunnelRow> rowCaptor = ArgumentCaptor.forClass(SeaTunnelRow.class);

        // Call the method that processes data
        sourceReader.pollNext(collector);

        // Verify collector.collect was called 3 times (once for each JSON object)
        verify(collector, times(1)).collect(rowCaptor.capture());

        // Get the captured rows
        try {
            String result = (rowCaptor.getValue().getFields())[0].toString();
            ObjectMapper objectMapper = new ObjectMapper();
            List list = objectMapper.readValue(result, List.class);

            // Check the first row (has both fields)
            Assertions.assertEquals("value11", ((Map) list.get(0)).get("key1_1"));
            Assertions.assertEquals("100", ((Map) list.get(0)).get("key2_1"));

            // Check the second row (missing key1)
            Assertions.assertNull(
                    ((Map) list.get(1)).get("key1_1"), "Field key1 should be a JSON null");
            Assertions.assertEquals("200", ((Map) list.get(1)).get("key2_1"));

            Assertions.assertNull(
                    ((Map) list.get(2)).get("key2_1"), "Field key1 should be a JSON null");
            Assertions.assertEquals("value33", ((Map) list.get(2)).get("key1_1"));

            Assertions.assertNull(
                    ((Map) list.get(3)).get("key2_1"), "Field key1 should be a JSON null");
            Assertions.assertEquals("value44", ((Map) list.get(3)).get("key1_1"));

            Assertions.assertNull(
                    ((Map) list.get(4)).get("key1_1"), "Field key1 should be a JSON null");
            Assertions.assertEquals("500", ((Map) list.get(4)).get("key2_1"));

            Assertions.assertNull(
                    ((Map) list.get(5)).get("key1_1"), "Field key1 should be a JSON null");
            Assertions.assertEquals("600", ((Map) list.get(5)).get("key2_1"));

            Assertions.assertNull(
                    ((Map) list.get(6)).get("key2_1"), "Field key1 should be a JSON null");
            Assertions.assertEquals("value77", ((Map) list.get(6)).get("key1_1"));

        } catch (Exception e) {
            throw new RuntimeException(
                    "set JsonFiledMissedReturnNull is True  Unit Test is failed!", e);
        }
    }
}
