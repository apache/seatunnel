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

package org.apache.seatunnel.connectors.seatunnel.zendesk.sink;

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
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ZendeskSinkWriterTest {

    @Mock private HttpClientProvider httpClient;

    private SeaTunnelRowType rowType;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        rowType =
                new SeaTunnelRowType(
                        new String[] {"subject", "status"},
                        new SeaTunnelDataType[] {BasicType.STRING_TYPE, BasicType.STRING_TYPE});
    }

    private ZendeskSinkWriter createWriter(String url) throws Exception {
        return createWriter(url, null, 1);
    }

    private ZendeskSinkWriter createWriter(String url, String resourceKeyOverride, int parallelism)
            throws Exception {
        HttpParameter param = new HttpParameter();
        param.setUrl(url);
        Map<String, String> headers = new HashMap<>();
        headers.put("Authorization", "Basic dGVzdEBleGFtcGxlLmNvbS90b2tlbjpzZWNyZXQ=");
        headers.put("Content-Type", "application/json");
        param.setHeaders(headers);

        ZendeskSinkWriter writer =
                new ZendeskSinkWriter(rowType, param, resourceKeyOverride, 0, 0, 3, parallelism);

        Field field = ZendeskSinkWriter.class.getDeclaredField("httpClient");
        field.setAccessible(true);
        field.set(writer, httpClient);
        return writer;
    }

    @Test
    public void testInferResourceKeyTickets() {
        Assertions.assertEquals(
                "ticket",
                ZendeskSinkWriter.inferResourceKey("https://example.zendesk.com/api/v2/tickets"));
    }

    @Test
    public void testInferResourceKeyTicketsWithJsonSuffix() {
        Assertions.assertEquals(
                "ticket",
                ZendeskSinkWriter.inferResourceKey(
                        "https://example.zendesk.com/api/v2/tickets.json"));
    }

    @Test
    public void testInferResourceKeyUsers() {
        Assertions.assertEquals(
                "user",
                ZendeskSinkWriter.inferResourceKey("https://example.zendesk.com/api/v2/users"));
    }

    @Test
    public void testInferResourceKeyUsersCreateOrUpdate() {
        Assertions.assertEquals(
                "user",
                ZendeskSinkWriter.inferResourceKey(
                        "https://example.zendesk.com/api/v2/users/create_or_update"));
    }

    @Test
    public void testInferResourceKeyOrganizations() {
        Assertions.assertEquals(
                "organization",
                ZendeskSinkWriter.inferResourceKey(
                        "https://example.zendesk.com/api/v2/organizations"));
    }

    @Test
    public void testInferResourceKeyTrailingSlash() {
        Assertions.assertEquals(
                "ticket",
                ZendeskSinkWriter.inferResourceKey("https://example.zendesk.com/api/v2/tickets/"));
    }

    @Test
    public void testInferResourceKeyWithQueryParams() {
        Assertions.assertEquals(
                "ticket",
                ZendeskSinkWriter.inferResourceKey(
                        "https://example.zendesk.com/api/v2/tickets?page=1"));
    }

    @Test
    public void testInferResourceKeyReturnsNullForUnrecognizedUrl() {
        Assertions.assertNull(ZendeskSinkWriter.inferResourceKey("https://example.com/other"));
    }

    @Test
    public void testInferResourceKeyReturnsNullForNull() {
        Assertions.assertNull(ZendeskSinkWriter.inferResourceKey(null));
    }

    @Test
    public void testInferResourceKeyCategories() {
        Assertions.assertEquals(
                "category",
                ZendeskSinkWriter.inferResourceKey(
                        "https://example.zendesk.com/api/v2/categories"));
    }

    @Test
    public void testInferResourceKeyAddresses() {
        Assertions.assertEquals(
                "address",
                ZendeskSinkWriter.inferResourceKey("https://example.zendesk.com/api/v2/addresses"));
    }

    @Test
    public void testResourceKeyOverrideBypassesInference() throws Exception {
        when(httpClient.doPost(anyString(), any(), anyString()))
                .thenReturn(new HttpResponse(201, "{}"));

        ZendeskSinkWriter writer =
                createWriter(
                        "https://example.zendesk.com/api/v2/custom_endpoint", "my_resource", 1);
        writer.write(new SeaTunnelRow(new Object[] {"Test", "open"}));

        ArgumentCaptor<String> bodyCaptor = ArgumentCaptor.forClass(String.class);
        verify(httpClient, times(1)).doPost(anyString(), any(), bodyCaptor.capture());

        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(bodyCaptor.getValue());
        Assertions.assertTrue(root.has("my_resource"));
        Assertions.assertEquals("Test", root.get("my_resource").get("subject").asText());
    }

    @Test
    public void testTicketBodyFormat() throws Exception {
        when(httpClient.doPost(anyString(), any(), anyString()))
                .thenReturn(new HttpResponse(201, "{}"));

        ZendeskSinkWriter writer = createWriter("https://example.zendesk.com/api/v2/tickets");
        writer.write(new SeaTunnelRow(new Object[] {"Help me", "open"}));

        ArgumentCaptor<String> bodyCaptor = ArgumentCaptor.forClass(String.class);
        verify(httpClient, times(1)).doPost(anyString(), any(), bodyCaptor.capture());

        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(bodyCaptor.getValue());
        Assertions.assertTrue(root.has("ticket"));
        Assertions.assertEquals("Help me", root.get("ticket").get("subject").asText());
        Assertions.assertEquals("open", root.get("ticket").get("status").asText());
    }

    @Test
    public void testUserBodyFormat() throws Exception {
        SeaTunnelRowType userRowType =
                new SeaTunnelRowType(
                        new String[] {"name", "email"},
                        new SeaTunnelDataType[] {BasicType.STRING_TYPE, BasicType.STRING_TYPE});

        HttpParameter param = new HttpParameter();
        param.setUrl("https://example.zendesk.com/api/v2/users/create_or_update");
        Map<String, String> headers = new HashMap<>();
        headers.put("Authorization", "Basic dGVzdA==");
        param.setHeaders(headers);

        ZendeskSinkWriter writer = new ZendeskSinkWriter(userRowType, param, null, 0, 0, 3, 1);

        Field field = ZendeskSinkWriter.class.getDeclaredField("httpClient");
        field.setAccessible(true);
        field.set(writer, httpClient);

        when(httpClient.doPost(anyString(), any(), anyString()))
                .thenReturn(new HttpResponse(201, "{}"));

        writer.write(new SeaTunnelRow(new Object[] {"John", "john@example.com"}));

        ArgumentCaptor<String> bodyCaptor = ArgumentCaptor.forClass(String.class);
        verify(httpClient, times(1)).doPost(anyString(), any(), bodyCaptor.capture());

        ObjectMapper mapper = new ObjectMapper();
        JsonNode root = mapper.readTree(bodyCaptor.getValue());
        Assertions.assertTrue(root.has("user"));
        Assertions.assertEquals("John", root.get("user").get("name").asText());
    }

    @Test
    public void testSuccessOn201Response() throws Exception {
        when(httpClient.doPost(anyString(), any(), anyString()))
                .thenReturn(new HttpResponse(201, "{}"));

        ZendeskSinkWriter writer = createWriter("https://example.zendesk.com/api/v2/tickets");
        writer.write(new SeaTunnelRow(new Object[] {"Test", "open"}));

        verify(httpClient, times(1)).doPost(anyString(), any(), anyString());
    }

    @Test
    public void testSuccessOn200Response() throws Exception {
        when(httpClient.doPost(anyString(), any(), anyString()))
                .thenReturn(new HttpResponse(200, "{}"));

        ZendeskSinkWriter writer = createWriter("https://example.zendesk.com/api/v2/tickets");
        writer.write(new SeaTunnelRow(new Object[] {"Test", "open"}));

        verify(httpClient, times(1)).doPost(anyString(), any(), anyString());
    }

    @Test
    public void testThrowsOnValidationError() throws Exception {
        when(httpClient.doPost(anyString(), any(), anyString()))
                .thenReturn(new HttpResponse(422, "{\"error\":\"RecordInvalid\"}"));

        ZendeskSinkWriter writer = createWriter("https://example.zendesk.com/api/v2/tickets");

        IOException exception =
                Assertions.assertThrows(
                        IOException.class,
                        () -> writer.write(new SeaTunnelRow(new Object[] {"Test", "open"})));
        Assertions.assertTrue(exception.getMessage().contains("422"));
        Assertions.assertTrue(
                exception.getMessage().contains("example.zendesk.com"),
                "error message should contain the request URL");
    }

    @Test
    public void testThrowsAfterMaxRetries() throws Exception {
        when(httpClient.doPost(anyString(), any(), anyString()))
                .thenReturn(new HttpResponse(429, "{\"error\":\"rate_limit\"}"));

        ZendeskSinkWriter writer = createWriter("https://example.zendesk.com/api/v2/tickets");

        Assertions.assertThrows(
                IOException.class,
                () -> writer.write(new SeaTunnelRow(new Object[] {"Test", "open"})));
        // 1 initial + 3 retries = 4 calls
        verify(httpClient, times(4)).doPost(anyString(), any(), anyString());
    }

    @Test
    public void testBackoffIsZeroWhenDisabled() {
        HttpParameter param = new HttpParameter();
        param.setUrl("https://example.zendesk.com/api/v2/tickets");
        ZendeskSinkWriter writer = new ZendeskSinkWriter(rowType, param, null, 0, 0, 3, 1);

        Assertions.assertEquals(0L, writer.calculateBackoffMillis(1));
        Assertions.assertEquals(0L, writer.calculateBackoffMillis(5));
    }

    @Test
    public void testBackoffIsNotDeterministic() {
        HttpParameter param = new HttpParameter();
        param.setUrl("https://example.zendesk.com/api/v2/tickets");
        ZendeskSinkWriter writer = new ZendeskSinkWriter(rowType, param, null, 0, 1000, 3, 1);

        Set<Long> observed = new HashSet<>();
        for (int i = 0; i < 200; i++) {
            observed.add(writer.calculateBackoffMillis(3));
        }

        Assertions.assertTrue(
                observed.size() > 1,
                "backoff must vary between calls, but observed only " + observed);
    }

    @Test
    public void testBackoffRespectsMaximum() {
        HttpParameter param = new HttpParameter();
        param.setUrl("https://example.zendesk.com/api/v2/tickets");
        ZendeskSinkWriter writer = new ZendeskSinkWriter(rowType, param, null, 0, 60000, 30, 1);

        for (int i = 0; i < 100; i++) {
            Assertions.assertTrue(
                    writer.calculateBackoffMillis(20) <= 300000L,
                    "jittered backoff must never exceed MAX_BACKOFF_MILLIS");
        }
    }

    @Test
    public void testBackoffHasMinimumFloor() {
        HttpParameter param = new HttpParameter();
        param.setUrl("https://example.zendesk.com/api/v2/tickets");
        ZendeskSinkWriter writer = new ZendeskSinkWriter(rowType, param, null, 0, 1000, 3, 1);

        for (int i = 0; i < 100; i++) {
            long backoff = writer.calculateBackoffMillis(1);
            Assertions.assertTrue(
                    backoff >= 500L,
                    "equal jitter backoff must be at least half the base, got " + backoff);
        }
    }

    @Test
    public void testBlankResourceKeyThrows() {
        HttpParameter param = new HttpParameter();
        param.setUrl("https://example.zendesk.com/api/v2/tickets");

        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> new ZendeskSinkWriter(rowType, param, "", 0, 0, 3, 1));

        Assertions.assertThrows(
                IllegalArgumentException.class,
                () -> new ZendeskSinkWriter(rowType, param, "   ", 0, 0, 3, 1));
    }

    @Test
    public void testParallelismScalesRequestInterval() throws Exception {
        HttpParameter param = new HttpParameter();
        param.setUrl("https://example.zendesk.com/api/v2/tickets");
        Map<String, String> headers = new HashMap<>();
        param.setHeaders(headers);

        // parallelism=4, requestIntervalMs=100 → effective interval should be 400
        ZendeskSinkWriter writer = new ZendeskSinkWriter(rowType, param, null, 100, 0, 3, 4);

        Field intervalField = ZendeskSinkWriter.class.getDeclaredField("requestIntervalMs");
        intervalField.setAccessible(true);
        int effectiveInterval = (int) intervalField.get(writer);
        Assertions.assertEquals(400, effectiveInterval);
    }
}
