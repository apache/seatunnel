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
package org.apache.seatunnel.connectors.seatunnel.mem0.sink;

import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.JsonNode;
import org.apache.seatunnel.shade.com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.seatunnel.api.table.type.ArrayType;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.http.client.HttpClientProvider;
import org.apache.seatunnel.connectors.seatunnel.http.client.HttpResponse;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpParameter;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

class Mem0SinkWriterTest {
    @Mock private HttpClientProvider httpClient;
    private SeaTunnelRowType rowType;

    @BeforeEach
    void setUp() {
        MockitoAnnotations.openMocks(this);
        rowType =
                new SeaTunnelRowType(
                        new String[] {"messages", "user_id", "agent_id"},
                        new SeaTunnelDataType[] {
                            ArrayType.of(BasicType.STRING_TYPE),
                            BasicType.STRING_TYPE,
                            BasicType.STRING_TYPE
                        });
    }

    private Mem0SinkWriter writer() throws Exception {
        HttpParameter parameter = new HttpParameter();
        parameter.setUrl("https://api.mem0.ai/v3/memories/add/");
        Map<String, String> headers = new HashMap<>();
        headers.put("Authorization", "Token secret");
        headers.put("Content-Type", "application/json");
        headers.put("Accept", "application/json");
        parameter.setHeaders(headers);
        Mem0SinkWriter writer =
                new Mem0SinkWriter(
                        rowType, parameter, "messages", "user_id", null, null, null, null);
        Field field = Mem0SinkWriter.class.getDeclaredField("httpClient");
        field.setAccessible(true);
        field.set(writer, httpClient);
        return writer;
    }

    @Test
    void sendsV3AddPayloadAndRequiresEventId() throws Exception {
        when(httpClient.doPost(anyString(), any(), anyString()))
                .thenReturn(new HttpResponse(202, "{\"event_id\":\"evt-1\"}"));

        writer().write(new SeaTunnelRow(new Object[] {new String[] {"hello"}, "u-1", null}));

        org.mockito.ArgumentCaptor<String> body = org.mockito.ArgumentCaptor.forClass(String.class);
        verify(httpClient).doPost(anyString(), any(), body.capture());
        JsonNode json = new ObjectMapper().readTree(body.getValue());
        assertEquals("hello", json.get("messages").get(0).asText());
        assertEquals("u-1", json.get("user_id").asText());
    }

    @Test
    void rejectsAcceptedResponseWithoutEventId() throws Exception {
        when(httpClient.doPost(anyString(), any(), anyString()))
                .thenReturn(new HttpResponse(202, "{}"));

        assertThrows(
                java.io.IOException.class,
                () ->
                        writer().write(
                                        new SeaTunnelRow(
                                                new Object[] {
                                                    new String[] {"hello"}, "u-1", null
                                                })));
    }

    @Test
    void retriesRateLimitAndServerResponses() throws Exception {
        when(httpClient.doPost(anyString(), any(), anyString()))
                .thenReturn(new HttpResponse(429, "{}"))
                .thenReturn(new HttpResponse(503, "{}"))
                .thenReturn(new HttpResponse(202, "{\"event_id\":\"evt-3\"}"));
        HttpParameter parameter = parameter();
        parameter.setRetry(2);
        Mem0SinkWriter writer =
                new Mem0SinkWriter(
                        rowType, parameter, "messages", "user_id", null, null, null, null);
        injectClient(writer);

        writer.write(new SeaTunnelRow(new Object[] {new String[] {"hello"}, "u-1", null}));

        verify(httpClient, org.mockito.Mockito.times(3)).doPost(anyString(), any(), anyString());
    }

    @Test
    void onlyTransientStatusesAreRetryable() {
        org.junit.jupiter.api.Assertions.assertTrue(Mem0SinkWriter.isRetryableStatus(408));
        org.junit.jupiter.api.Assertions.assertTrue(Mem0SinkWriter.isRetryableStatus(429));
        org.junit.jupiter.api.Assertions.assertTrue(Mem0SinkWriter.isRetryableStatus(500));
        org.junit.jupiter.api.Assertions.assertFalse(Mem0SinkWriter.isRetryableStatus(400));
        org.junit.jupiter.api.Assertions.assertFalse(Mem0SinkWriter.isRetryableStatus(401));
    }

    @Test
    void acceptsAnotherScopeWhenConfiguredUserIdIsNull() throws Exception {
        when(httpClient.doPost(anyString(), any(), anyString()))
                .thenReturn(new HttpResponse(202, "{\"event_id\":\"evt-2\"}"));
        Mem0SinkWriter writer =
                new Mem0SinkWriter(
                        rowType, parameter(), "messages", "user_id", "agent_id", null, null, null);
        injectClient(writer);
        writer.write(new SeaTunnelRow(new Object[] {new String[] {"hello"}, null, "a-1"}));
    }

    private HttpParameter parameter() {
        HttpParameter parameter = new HttpParameter();
        parameter.setUrl("https://api.mem0.ai/v3/memories/add/");
        Map<String, String> headers = new HashMap<>();
        headers.put("Authorization", "Token secret");
        headers.put("Content-Type", "application/json");
        headers.put("Accept", "application/json");
        parameter.setHeaders(headers);
        return parameter;
    }

    private void injectClient(Mem0SinkWriter writer) throws Exception {
        Field field = Mem0SinkWriter.class.getDeclaredField("httpClient");
        field.setAccessible(true);
        field.set(writer, httpClient);
    }
}
