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

package org.apache.seatunnel.connectors.seatunnel.airtable.sink;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.http.client.HttpClientProvider;
import org.apache.seatunnel.connectors.seatunnel.http.client.HttpResponse;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpParameter;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AirtableSinkWriterTest {

    @Mock private HttpClientProvider httpClient;

    private SeaTunnelRowType rowType;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        rowType =
                new SeaTunnelRowType(
                        new String[] {"Name", "Age"},
                        new SeaTunnelDataType[] {BasicType.STRING_TYPE, BasicType.INT_TYPE});
    }

    private AirtableSinkWriter createWriter(int batchSize, boolean typecast) throws Exception {
        HttpParameter param = new HttpParameter();
        param.setUrl("https://api.airtable.com/v0/appXXX/tblYYY");
        Map<String, String> headers = new HashMap<>();
        headers.put("Authorization", "Bearer test_token");
        headers.put("Content-Type", "application/json");
        param.setHeaders(headers);

        AirtableSinkWriter writer =
                new AirtableSinkWriter(rowType, param, batchSize, typecast, 0, 0, 3);

        Field field = AirtableSinkWriter.class.getDeclaredField("httpClient");
        field.setAccessible(true);
        field.set(writer, httpClient);
        return writer;
    }

    @Test
    public void testBatchWriteBodyFormat() throws Exception {
        when(httpClient.doPost(anyString(), any(), anyString()))
                .thenReturn(new HttpResponse(200, "{}"));

        AirtableSinkWriter writer = createWriter(2, false);
        writer.write(new SeaTunnelRow(new Object[] {"Alice", 30}));
        writer.write(new SeaTunnelRow(new Object[] {"Bob", 25}));

        ArgumentCaptor<String> bodyCaptor = ArgumentCaptor.forClass(String.class);
        verify(httpClient, times(1)).doPost(anyString(), any(), bodyCaptor.capture());

        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(bodyCaptor.getValue());
        Assertions.assertTrue(root.has("records"));
        Assertions.assertFalse(root.has("typecast"));

        JsonNode records = root.get("records");
        Assertions.assertEquals(2, records.size());
        Assertions.assertTrue(records.get(0).has("fields"));
        Assertions.assertEquals("Alice", records.get(0).get("fields").get("Name").asText());
    }

    @Test
    public void testThrowsAfterMaxRetries() throws Exception {
        when(httpClient.doPost(anyString(), any(), anyString()))
                .thenReturn(new HttpResponse(429, "{\"error\":{\"type\":\"RATE_LIMIT\"}}"));

        AirtableSinkWriter writer = createWriter(1, false);

        Assertions.assertThrows(
                IOException.class,
                () -> writer.write(new SeaTunnelRow(new Object[] {"Alice", 30})));
        // 1 initial + 3 retries = 4 calls
        verify(httpClient, times(4)).doPost(anyString(), any(), anyString());
    }
}
