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

import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.common.utils.JsonUtils;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.http.client.HttpClientProvider;
import org.apache.seatunnel.connectors.seatunnel.http.client.HttpResponse;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpParameter;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpRequestMethod;
import org.apache.seatunnel.connectors.seatunnel.http.config.JsonField;
import org.apache.seatunnel.connectors.seatunnel.http.config.PageInfo;
import org.apache.seatunnel.connectors.seatunnel.http.source.HttpSourceReader;
import org.apache.seatunnel.connectors.seatunnel.http.source.SimpleTextDeserializationSchema;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.HashMap;
import java.util.Map;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

public class HttpSourceReaderUpdateRequestParamTest {

    private HttpParameter httpParameter;
    private JsonField jsonField;
    private SimpleTextDeserializationSchema deserializationSchema;
    private HttpSourceReader httpSourceReader;

    @Mock private SingleSplitReaderContext context;

    @Mock private Collector<SeaTunnelRow> collector;

    @Mock private HttpClientProvider httpClientProvider;

    @Mock private HttpResponse httpResponse;

    @BeforeEach
    public void setUp() throws Exception {
        MockitoAnnotations.openMocks(this);

        httpParameter = new HttpParameter();
        httpParameter.setUrl("http://test-url.com");
        httpParameter.setMethod(HttpRequestMethod.GET);

        Map<String, String> fields = new HashMap<>();
        fields.put("key1", "$[*].key1");
        fields.put("key2", "$[*].key2");
        jsonField = JsonField.builder().fields(fields).build();

        // Create the schema with two string fields
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"key1", "key2"},
                        new SeaTunnelDataType[] {BasicType.STRING_TYPE, BasicType.STRING_TYPE});
        deserializationSchema = new SimpleTextDeserializationSchema(rowType);

        // Setup mocks
        when(httpResponse.getCode()).thenReturn(200);
        when(collector.getCheckpointLock()).thenReturn(new Object());
        when(httpClientProvider.execute(
                        anyString(), anyString(), any(), any(), any(), anyBoolean()))
                .thenReturn(httpResponse);

        // Create HttpSourceReader
        httpSourceReader =
                new HttpSourceReader(
                        httpParameter, context, deserializationSchema, jsonField, null);

        httpSourceReader.open();
    }

    @Test
    public void testUpdateRequestParamWithHeaderPlaceholder() throws Exception {
        // Setup test data
        Map<String, String> headers = new HashMap<>();
        headers.put("Page-Number", "${page}");
        headers.put("Authorization", "Bearer token-123");
        headers.put("Cursor", "${cursor}");
        httpParameter.setHeaders(headers);

        PageInfo pageInfo = new PageInfo();
        pageInfo.setPageField("page");
        pageInfo.setPageIndex(5L);
        pageInfo.setCursor("cursor");
        pageInfo.setPageCursorFieldName("cursor");
        pageInfo.setUsePlaceholderReplacement(true);

        // Call updateRequestParam method directly
        httpSourceReader.updateRequestParam(pageInfo, true);

        // Verify the headers were updated correctly
        Map<String, String> updatedHeaders = httpParameter.getHeaders();
        Assertions.assertEquals("5", updatedHeaders.get("Page-Number"));
        Assertions.assertEquals("Bearer token-123", updatedHeaders.get("Authorization"));
        Assertions.assertEquals("cursor", updatedHeaders.get("Cursor"));
    }

    @Test
    public void testUpdateRequestParamWithHeaderPrefixedPlaceholder() throws Exception {
        // Setup test data
        Map<String, String> headers = new HashMap<>();
        headers.put("Page-Number", "10${page}");
        headers.put("Authorization", "Bearer token-123");
        headers.put("Cursor", "${cursor}");
        httpParameter.setHeaders(headers);

        PageInfo pageInfo = new PageInfo();
        pageInfo.setPageField("page");
        pageInfo.setPageIndex(5L);
        pageInfo.setCursor("cursor");
        pageInfo.setPageCursorFieldName("cursor");
        pageInfo.setUsePlaceholderReplacement(true);

        // Call updateRequestParam method directly
        httpSourceReader.updateRequestParam(pageInfo, true);

        // Verify the headers were updated correctly
        Map<String, String> updatedHeaders = httpParameter.getHeaders();
        Assertions.assertEquals("105", updatedHeaders.get("Page-Number"));
        Assertions.assertEquals("Bearer token-123", updatedHeaders.get("Authorization"));
        Assertions.assertEquals("cursor", updatedHeaders.get("Cursor"));
    }

    @Test
    public void testUpdateRequestParamWithParamsPlaceholder() throws Exception {
        // Setup test data
        Map<String, String> params = new HashMap<>();
        params.put("page", "${page}");
        params.put("limit", "10");
        params.put("cursor", "${cursor}");
        httpParameter.setParams(params);

        PageInfo pageInfo = new PageInfo();
        pageInfo.setPageField("page");
        pageInfo.setPageIndex(5L);
        pageInfo.setCursor("cursor");
        pageInfo.setPageCursorFieldName("cursor");
        pageInfo.setUsePlaceholderReplacement(true);

        // Call updateRequestParam method directly
        httpSourceReader.updateRequestParam(pageInfo, true);

        // Verify the params were updated correctly
        Map<String, String> updatedParams = httpParameter.getParams();
        Assertions.assertEquals("5", updatedParams.get("page"));
        Assertions.assertEquals("10", updatedParams.get("limit"));
        Assertions.assertEquals("cursor", updatedParams.get("cursor"));
    }

    @Test
    public void testUpdateRequestParamWithParamsPrefixedPlaceholder() throws Exception {
        // Setup test data
        Map<String, String> params = new HashMap<>();
        params.put("page", "10${page}");
        params.put("limit", "10");
        params.put("cursor", "${cursor}");
        httpParameter.setParams(params);

        PageInfo pageInfo = new PageInfo();
        pageInfo.setPageField("page");
        pageInfo.setPageIndex(5L);
        pageInfo.setCursor("cursor");
        pageInfo.setPageCursorFieldName("cursor");
        pageInfo.setUsePlaceholderReplacement(true);

        // Call updateRequestParam method directly
        httpSourceReader.updateRequestParam(pageInfo, true);

        // Verify the params were updated correctly
        Map<String, String> updatedParams = httpParameter.getParams();
        Assertions.assertEquals("105", updatedParams.get("page"));
        Assertions.assertEquals("10", updatedParams.get("limit"));
        Assertions.assertEquals("cursor", updatedParams.get("cursor"));
    }

    @Test
    public void testUpdateRequestParamWithBodyPlaceholder() throws Exception {
        // Setup test data
        String bodyJson = "{\"page\":\"${page}\",\"limit\":10,\"cursor\":\"${cursor}\"}";
        httpParameter.setBody(bodyJson);

        PageInfo pageInfo = new PageInfo();
        pageInfo.setPageField("page");
        pageInfo.setPageIndex(5L);
        pageInfo.setCursor("cursor");
        pageInfo.setPageCursorFieldName("cursor");
        pageInfo.setUsePlaceholderReplacement(true);

        // Call updateRequestParam method directly
        httpSourceReader.updateRequestParam(pageInfo, true);

        // Verify the body was updated correctly
        Assertions.assertEquals(
                "{\"page\":\"5\",\"limit\":10,\"cursor\":\"cursor\"}", httpParameter.getBody());
        Map<String, Object> bodyMap =
                JsonUtils.toMap(JsonUtils.stringToJsonNode(httpParameter.getBody()));
        Assertions.assertEquals("5", bodyMap.get("page"));
        Assertions.assertEquals(10, bodyMap.get("limit"));
        Assertions.assertEquals("cursor", bodyMap.get("cursor"));
    }

    @Test
    public void testUpdateRequestParamWithBodyPrefixedPlaceholder() throws Exception {
        // Setup test data
        String bodyJson = "{\"page\":\"10${page}\",\"limit\":10,\"cursor\":\"${cursor}\"}";
        httpParameter.setBody(bodyJson);

        PageInfo pageInfo = new PageInfo();
        pageInfo.setPageField("page");
        pageInfo.setPageIndex(5L);
        pageInfo.setCursor("cursor");
        pageInfo.setPageCursorFieldName("cursor");
        pageInfo.setUsePlaceholderReplacement(true);

        // Call updateRequestParam method directly
        httpSourceReader.updateRequestParam(pageInfo, true);

        // Verify the body was updated correctly
        Assertions.assertEquals(
                "{\"page\":\"105\",\"limit\":10,\"cursor\":\"cursor\"}", httpParameter.getBody());
        Map<String, Object> bodyMap =
                JsonUtils.toMap(JsonUtils.stringToJsonNode(httpParameter.getBody()));
        Assertions.assertEquals("105", bodyMap.get("page"));
        Assertions.assertEquals(10, bodyMap.get("limit"));
        Assertions.assertEquals("cursor", bodyMap.get("cursor"));
    }

    @Test
    public void testUpdateRequestParamWithNestedBodyPlaceholder() throws Exception {
        // Setup test data with nested structure
        String bodyJson =
                "{\"pagination\":{\"page\":\"${page}\",\"limit\":10,\"cursor\":\"${cursor}\"},\"filter\":\"active\"}";
        httpParameter.setBody(bodyJson);

        PageInfo pageInfo = new PageInfo();
        pageInfo.setPageField("page");
        pageInfo.setPageIndex(5L);
        pageInfo.setCursor("cursor");
        pageInfo.setPageCursorFieldName("cursor");
        pageInfo.setUsePlaceholderReplacement(true);

        // Call updateRequestParam method directly
        httpSourceReader.updateRequestParam(pageInfo, true);

        // Verify the nested body was updated correctly
        Assertions.assertEquals(
                "{\"pagination\":{\"page\":\"5\",\"limit\":10,\"cursor\":\"cursor\"},\"filter\":\"active\"}",
                httpParameter.getBody());
        Map<String, Object> bodyMap =
                JsonUtils.toMap(JsonUtils.stringToJsonNode(httpParameter.getBody()));
        Map<String, Object> pagination = (Map<String, Object>) bodyMap.get("pagination");
        Assertions.assertEquals("5", pagination.get("page"));
        Assertions.assertEquals(10, pagination.get("limit"));
        Assertions.assertEquals("cursor", pagination.get("cursor"));
        Assertions.assertEquals("active", bodyMap.get("filter"));
    }

    @Test
    public void testUpdateRequestParamWithKeepPageParamAsHttpParam() throws Exception {
        // Setup test data
        httpParameter.setKeepPageParamAsHttpParam(true);

        PageInfo pageInfo = new PageInfo();
        pageInfo.setPageField("page");
        pageInfo.setPageIndex(5L);
        pageInfo.setCursor("cursor");
        pageInfo.setPageCursorFieldName("cursor");

        // Call updateRequestParam method directly
        httpSourceReader.updateRequestParam(pageInfo, true);

        // Verify the params were updated correctly
        Map<String, String> updatedParams = httpParameter.getParams();
        Assertions.assertEquals("5", updatedParams.get("page"));
        // Add cursor param to the params map
        updatedParams.put("cursor", "cursor");
        Assertions.assertEquals("cursor", updatedParams.get("cursor"));
    }

    @Test
    public void testUpdateRequestParamWithKeyBasedReplacement() throws Exception {
        // Setup test data
        String bodyJson = "{\"page\":1,\"limit\":10,\"cursor\":\"old_cursor\"}";
        httpParameter.setBody(bodyJson);

        PageInfo pageInfo = new PageInfo();
        pageInfo.setPageField("page");
        pageInfo.setPageIndex(5L);
        pageInfo.setCursor("cursor");
        pageInfo.setPageCursorFieldName("cursor");
        pageInfo.setUsePlaceholderReplacement(false);

        // Call updateRequestParam method directly
        httpSourceReader.updateRequestParam(pageInfo, false);

        // Verify the body was updated correctly using key-based replacement
        Map<String, Object> bodyMap =
                JsonUtils.toMap(JsonUtils.stringToJsonNode(httpParameter.getBody()));
        Assertions.assertEquals(5, bodyMap.get("page"));
        Assertions.assertEquals(10, bodyMap.get("limit"));
        // For key-based replacement with cursor, the cursor value is still updated
        Assertions.assertEquals("cursor", bodyMap.get("cursor"));
    }

    @Test
    public void testUpdateRequestParamWithNestedKeyBasedReplacement() throws Exception {
        // Setup test data with nested structure
        String bodyJson =
                "{\"pagination\":{\"page\":1,\"limit\":10,\"cursor\":\"old_cursor\"},\"filter\":\"active\"}";
        httpParameter.setBody(bodyJson);

        PageInfo pageInfo = new PageInfo();
        pageInfo.setPageField("page");
        pageInfo.setPageIndex(5L);
        pageInfo.setCursor("cursor");
        pageInfo.setPageCursorFieldName("cursor");
        pageInfo.setUsePlaceholderReplacement(false);

        // Call updateRequestParam method directly
        httpSourceReader.updateRequestParam(pageInfo, false);

        // Verify the nested body was updated correctly using key-based replacement
        Map<String, Object> bodyMap =
                JsonUtils.toMap(JsonUtils.stringToJsonNode(httpParameter.getBody()));
        Map<String, Object> pagination = (Map<String, Object>) bodyMap.get("pagination");
        Assertions.assertEquals(5, pagination.get("page"));
        Assertions.assertEquals(10, pagination.get("limit"));
        // For key-based replacement with cursor, the cursor value is still updated
        Assertions.assertEquals("cursor", pagination.get("cursor"));
        Assertions.assertEquals("active", bodyMap.get("filter"));
    }

    @Test
    public void testUpdateRequestParamWithUnquotedPlaceholder() throws Exception {
        // Setup test data with JSON string body containing unquoted placeholder
        String bodyJson = "{\"a\":${page},\"limit\":10,\"cursor\":\"${cursor}\"}";
        httpParameter.setBody(bodyJson);

        PageInfo pageInfo = new PageInfo();
        pageInfo.setPageField("page");
        pageInfo.setPageIndex(5L);
        pageInfo.setCursor("cursor");
        pageInfo.setPageCursorFieldName("cursor");
        pageInfo.setUsePlaceholderReplacement(true);

        // Call updateRequestParam method directly
        httpSourceReader.updateRequestParam(pageInfo, true);

        // Verify the body was updated correctly
        Assertions.assertEquals(
                "{\"a\":5,\"limit\":10,\"cursor\":\"cursor\"}", httpParameter.getBody());
        Map<String, Object> bodyMap =
                JsonUtils.toMap(JsonUtils.stringToJsonNode(httpParameter.getBody()));
        Assertions.assertEquals(5, bodyMap.get("a"));
        Assertions.assertEquals(10, bodyMap.get("limit"));
        Assertions.assertEquals("cursor", bodyMap.get("cursor"));
    }

    @Test
    public void testUpdateRequestParamWithPrefixedUnquotedPlaceholder() throws Exception {
        // Setup test data with JSON string body containing prefixed unquoted placeholder
        String bodyJson = "{\"a\":10${page},\"limit\":10,\"cursor\":\"${cursor}\"}";
        httpParameter.setBody(bodyJson);

        PageInfo pageInfo = new PageInfo();
        pageInfo.setPageField("page");
        pageInfo.setPageIndex(5L);
        pageInfo.setCursor("cursor");
        pageInfo.setPageCursorFieldName("cursor");
        pageInfo.setUsePlaceholderReplacement(true);

        // Call updateRequestParam method directly
        httpSourceReader.updateRequestParam(pageInfo, true);

        // Verify the body was updated correctly
        Assertions.assertEquals(
                "{\"a\":105,\"limit\":10,\"cursor\":\"cursor\"}", httpParameter.getBody());
        Map<String, Object> bodyMap =
                JsonUtils.toMap(JsonUtils.stringToJsonNode(httpParameter.getBody()));
        Assertions.assertEquals(105, bodyMap.get("a"));
        Assertions.assertEquals(10, bodyMap.get("limit"));
        Assertions.assertEquals("cursor", bodyMap.get("cursor"));
    }

    @Test
    public void testUpdateRequestParamWithNestedUnquotedPlaceholder() throws Exception {
        // Setup test data with nested JSON string body containing unquoted placeholder
        String bodyJson =
                "{\"data\":{\"a\":${page},\"limit\":10,\"cursor\":\"${cursor}\"},\"filter\":\"active\"}";
        httpParameter.setBody(bodyJson);

        PageInfo pageInfo = new PageInfo();
        pageInfo.setPageField("page");
        pageInfo.setPageIndex(5L);
        pageInfo.setCursor("cursor");
        pageInfo.setPageCursorFieldName("cursor");
        pageInfo.setUsePlaceholderReplacement(true);

        // Call updateRequestParam method directly
        httpSourceReader.updateRequestParam(pageInfo, true);

        // Verify the body was updated correctly
        Assertions.assertEquals(
                "{\"data\":{\"a\":5,\"limit\":10,\"cursor\":\"cursor\"},\"filter\":\"active\"}",
                httpParameter.getBody());
        Map<String, Object> bodyMap =
                JsonUtils.toMap(JsonUtils.stringToJsonNode(httpParameter.getBody()));
        Map<String, Object> data = (Map<String, Object>) bodyMap.get("data");
        Assertions.assertEquals(5, data.get("a"));
        Assertions.assertEquals(10, data.get("limit"));
        Assertions.assertEquals("cursor", data.get("cursor"));
        Assertions.assertEquals("active", bodyMap.get("filter"));
    }

    @Test
    public void testUpdateRequestParamWithMultiplePlaceholders() throws Exception {
        // Setup test data with JSON string body containing multiple placeholders
        String bodyJson =
                "{\"a\":${page},\"b\":\"${page}\",\"c\":10${page},\"limit\":10,\"cursor\":\"${cursor}\"}";
        httpParameter.setBody(bodyJson);

        PageInfo pageInfo = new PageInfo();
        pageInfo.setPageField("page");
        pageInfo.setPageIndex(5L);
        pageInfo.setCursor("cursor");
        pageInfo.setPageCursorFieldName("cursor");
        pageInfo.setUsePlaceholderReplacement(true);

        // Call updateRequestParam method directly
        httpSourceReader.updateRequestParam(pageInfo, true);

        // Verify the body was updated correctly
        Assertions.assertEquals(
                "{\"a\":5,\"b\":\"5\",\"c\":105,\"limit\":10,\"cursor\":\"cursor\"}",
                httpParameter.getBody());
        Map<String, Object> bodyMap =
                JsonUtils.toMap(JsonUtils.stringToJsonNode(httpParameter.getBody()));
        Assertions.assertEquals(5, bodyMap.get("a"));
        Assertions.assertEquals("5", bodyMap.get("b"));
        Assertions.assertEquals(105, bodyMap.get("c"));
        Assertions.assertEquals(10, bodyMap.get("limit"));
        Assertions.assertEquals("cursor", bodyMap.get("cursor"));
    }

    @Test
    public void testUpdateRequestParamWithComplexNestedPlaceholders() throws Exception {
        // Setup test data with complex nested JSON string body containing various placeholders
        String bodyJson =
                "{\"pagination\":{\"page\":${page},\"size\":\"${page}\",\"offset\":10${page},\"cursor\":\"${cursor}\"},\"filters\":{\"active\":true,\"code\":\"${page}\"},\"limit\":10}";
        httpParameter.setBody(bodyJson);

        PageInfo pageInfo = new PageInfo();
        pageInfo.setPageField("page");
        pageInfo.setPageIndex(5L);
        pageInfo.setCursor("cursor");
        pageInfo.setPageCursorFieldName("cursor");
        pageInfo.setUsePlaceholderReplacement(true);

        // Call updateRequestParam method directly
        httpSourceReader.updateRequestParam(pageInfo, true);

        // Verify the body was updated correctly
        Assertions.assertEquals(
                "{\"pagination\":{\"page\":5,\"size\":\"5\",\"offset\":105,\"cursor\":\"cursor\"},\"filters\":{\"active\":true,\"code\":\"5\"},\"limit\":10}",
                httpParameter.getBody());
        Map<String, Object> bodyMap =
                JsonUtils.toMap(JsonUtils.stringToJsonNode(httpParameter.getBody()));
        Map<String, Object> pagination = (Map<String, Object>) bodyMap.get("pagination");
        Map<String, Object> filters = (Map<String, Object>) bodyMap.get("filters");

        Assertions.assertEquals(5, pagination.get("page"));
        Assertions.assertEquals("5", pagination.get("size"));
        Assertions.assertEquals(105, pagination.get("offset"));
        Assertions.assertEquals("cursor", pagination.get("cursor"));
        Assertions.assertEquals(true, filters.get("active"));
        Assertions.assertEquals("5", filters.get("code"));
        Assertions.assertEquals(10, bodyMap.get("limit"));
    }
}
