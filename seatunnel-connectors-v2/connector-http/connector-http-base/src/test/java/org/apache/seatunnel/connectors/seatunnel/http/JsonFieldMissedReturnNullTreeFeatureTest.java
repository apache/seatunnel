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

public class JsonFieldMissedReturnNullTreeFeatureTest {

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
        fields.put("author", "$.store['book'][*].author");
        fields.put("isbn", "$.store['book'][*].isbn");
        jsonField = JsonField.builder().fields(fields).build();

        // Create the schema with two string fields
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"author", "isbn"},
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
                        + "    \"store\": {\n"
                        + "        \"book\": [\n"
                        + "            {\n"
                        + "                \"category\": \"reference\",\n"
                        + "                \"author\": \"Nigel Rees\",\n"
                        + "                \"title\": \"Sayings of the Century\",\n"
                        + "                \"price\": 8.95\n"
                        + "            },\n"
                        + "            {\n"
                        + "                \"category\": \"fiction\",\n"
                        + "                \"author\": \"Evelyn Waugh\",\n"
                        + "                \"title\": \"Sword of Honour\",\n"
                        + "                \"price\": 12.99\n"
                        + "            },\n"
                        + "            {\n"
                        + "                \"category\": \"fiction\",\n"
                        + "                \"author\": \"Herman Melville\",\n"
                        + "                \"title\": \"Moby Dick\",\n"
                        + "                \"isbn\": \"0-553-21311-3\",\n"
                        + "                \"price\": 8.99\n"
                        + "            },\n"
                        + "            {\n"
                        + "                \"category\": \"fiction\",\n"
                        + "                \"author\": \"J. R. R. Tolkien\",\n"
                        + "                \"title\": \"The Lord of the Rings\",\n"
                        + "                \"isbn\": \"0-395-19395-8\",\n"
                        + "                \"price\": 22.99\n"
                        + "            }\n"
                        + "        ],\n"
                        + "        \"bicycle\": {\n"
                        + "            \"color\": \"red\",\n"
                        + "            \"price\": 19.95\n"
                        + "        }\n"
                        + "    },\n"
                        + "    \"expensive\": 10\n"
                        + "}";

        // Set json_filed_missed_return_null to true
        httpParameter.setJsonFiledMissedReturnNull(false);

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
            Assertions.assertEquals("Nigel Rees", ((Map) list.get(0)).get("author"));
            Assertions.assertNull(
                    ((Map) list.get(0)).get("isbn"), "Field key1 should be a JSON null");

            Assertions.assertEquals("Evelyn Waugh", ((Map) list.get(1)).get("author"));
            Assertions.assertNull(
                    ((Map) list.get(1)).get("isbn"), "Field key1 should be a JSON null");

            Assertions.assertEquals("Herman Melville", ((Map) list.get(2)).get("author"));
            Assertions.assertEquals("0-553-21311-3", ((Map) list.get(2)).get("isbn"));

            Assertions.assertEquals("J. R. R. Tolkien", ((Map) list.get(3)).get("author"));
            Assertions.assertEquals("0-395-19395-8", ((Map) list.get(3)).get("isbn"));

        } catch (Exception e) {
            throw new RuntimeException(
                    "set JsonFiledMissedReturnNull is True  Unit Test is failed!", e);
        }
    }
}
