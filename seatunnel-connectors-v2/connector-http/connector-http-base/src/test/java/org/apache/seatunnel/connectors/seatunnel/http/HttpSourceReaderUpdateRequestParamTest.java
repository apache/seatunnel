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

import java.lang.reflect.Method;
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
        httpParameter.setHeaders(headers);

        PageInfo pageInfo = new PageInfo();
        pageInfo.setPageField("page");
        pageInfo.setPageIndex(5L);
        pageInfo.setUsePlaceholderReplacement(true);

        // Call updateRequestParam method using reflection
        Method updateRequestParamMethod =
                HttpSourceReader.class.getDeclaredMethod(
                        "updateRequestParam", PageInfo.class, boolean.class);
        updateRequestParamMethod.setAccessible(true);
        updateRequestParamMethod.invoke(httpSourceReader, pageInfo, true);

        // Verify the headers were updated correctly
        Map<String, String> updatedHeaders = httpParameter.getHeaders();
        Assertions.assertEquals("5", updatedHeaders.get("Page-Number"));
        Assertions.assertEquals("Bearer token-123", updatedHeaders.get("Authorization"));
    }

    @Test
    public void testUpdateRequestParamWithHeaderPrefixedPlaceholder() throws Exception {
        // Setup test data
        Map<String, String> headers = new HashMap<>();
        headers.put("Page-Number", "10${page}");
        headers.put("Authorization", "Bearer token-123");
        httpParameter.setHeaders(headers);

        PageInfo pageInfo = new PageInfo();
        pageInfo.setPageField("page");
        pageInfo.setPageIndex(5L);
        pageInfo.setUsePlaceholderReplacement(true);

        // Call updateRequestParam method using reflection
        Method updateRequestParamMethod =
                HttpSourceReader.class.getDeclaredMethod(
                        "updateRequestParam", PageInfo.class, boolean.class);
        updateRequestParamMethod.setAccessible(true);
        updateRequestParamMethod.invoke(httpSourceReader, pageInfo, true);

        // Verify the headers were updated correctly
        Map<String, String> updatedHeaders = httpParameter.getHeaders();
        Assertions.assertEquals("105", updatedHeaders.get("Page-Number"));
        Assertions.assertEquals("Bearer token-123", updatedHeaders.get("Authorization"));
    }

    @Test
    public void testUpdateRequestParamWithParamsPlaceholder() throws Exception {
        // Setup test data
        Map<String, String> params = new HashMap<>();
        params.put("page", "${page}");
        params.put("limit", "10");
        httpParameter.setParams(params);

        PageInfo pageInfo = new PageInfo();
        pageInfo.setPageField("page");
        pageInfo.setPageIndex(5L);
        pageInfo.setUsePlaceholderReplacement(true);

        // Call updateRequestParam method using reflection
        Method updateRequestParamMethod =
                HttpSourceReader.class.getDeclaredMethod(
                        "updateRequestParam", PageInfo.class, boolean.class);
        updateRequestParamMethod.setAccessible(true);
        updateRequestParamMethod.invoke(httpSourceReader, pageInfo, true);

        // Verify the params were updated correctly
        Map<String, String> updatedParams = httpParameter.getParams();
        Assertions.assertEquals("5", updatedParams.get("page"));
        Assertions.assertEquals("10", updatedParams.get("limit"));
    }

    @Test
    public void testUpdateRequestParamWithParamsPrefixedPlaceholder() throws Exception {
        // Setup test data
        Map<String, String> params = new HashMap<>();
        params.put("page", "10${page}");
        params.put("limit", "10");
        httpParameter.setParams(params);

        PageInfo pageInfo = new PageInfo();
        pageInfo.setPageField("page");
        pageInfo.setPageIndex(5L);
        pageInfo.setUsePlaceholderReplacement(true);

        // Call updateRequestParam method using reflection
        Method updateRequestParamMethod =
                HttpSourceReader.class.getDeclaredMethod(
                        "updateRequestParam", PageInfo.class, boolean.class);
        updateRequestParamMethod.setAccessible(true);
        updateRequestParamMethod.invoke(httpSourceReader, pageInfo, true);

        // Verify the params were updated correctly
        Map<String, String> updatedParams = httpParameter.getParams();
        Assertions.assertEquals("105", updatedParams.get("page"));
        Assertions.assertEquals("10", updatedParams.get("limit"));
    }

    @Test
    public void testUpdateRequestParamWithBodyPlaceholder() throws Exception {
        // Setup test data
        Map<String, Object> body = new HashMap<>();
        body.put("page", "${page}");
        body.put("limit", 10);
        httpParameter.setBody(body);

        PageInfo pageInfo = new PageInfo();
        pageInfo.setPageField("page");
        pageInfo.setPageIndex(5L);
        pageInfo.setUsePlaceholderReplacement(true);

        // Call updateRequestParam method using reflection
        Method updateRequestParamMethod =
                HttpSourceReader.class.getDeclaredMethod(
                        "updateRequestParam", PageInfo.class, boolean.class);
        updateRequestParamMethod.setAccessible(true);
        updateRequestParamMethod.invoke(httpSourceReader, pageInfo, true);

        // Verify the body was updated correctly
        Map<String, Object> updatedBody = httpParameter.getBody();
        Assertions.assertEquals(5, updatedBody.get("page"));
        Assertions.assertEquals(10, updatedBody.get("limit"));
    }

    @Test
    public void testUpdateRequestParamWithBodyPrefixedPlaceholder() throws Exception {
        // Setup test data
        Map<String, Object> body = new HashMap<>();
        body.put("page", "10${page}");
        body.put("limit", 10);
        httpParameter.setBody(body);

        PageInfo pageInfo = new PageInfo();
        pageInfo.setPageField("page");
        pageInfo.setPageIndex(5L);
        pageInfo.setUsePlaceholderReplacement(true);

        // Call updateRequestParam method using reflection
        Method updateRequestParamMethod =
                HttpSourceReader.class.getDeclaredMethod(
                        "updateRequestParam", PageInfo.class, boolean.class);
        updateRequestParamMethod.setAccessible(true);
        updateRequestParamMethod.invoke(httpSourceReader, pageInfo, true);

        // Verify the body was updated correctly
        Map<String, Object> updatedBody = httpParameter.getBody();
        Assertions.assertEquals("105", updatedBody.get("page"));
        Assertions.assertEquals(10, updatedBody.get("limit"));
    }

    @Test
    public void testUpdateRequestParamWithNestedBodyPlaceholder() throws Exception {
        // Setup test data with nested structure
        Map<String, Object> pagination = new HashMap<>();
        pagination.put("page", "${page}");
        pagination.put("limit", 10);

        Map<String, Object> body = new HashMap<>();
        body.put("pagination", pagination);
        body.put("filter", "active");
        httpParameter.setBody(body);

        PageInfo pageInfo = new PageInfo();
        pageInfo.setPageField("page");
        pageInfo.setPageIndex(5L);
        pageInfo.setUsePlaceholderReplacement(true);

        // Call updateRequestParam method using reflection
        Method updateRequestParamMethod =
                HttpSourceReader.class.getDeclaredMethod(
                        "updateRequestParam", PageInfo.class, boolean.class);
        updateRequestParamMethod.setAccessible(true);
        updateRequestParamMethod.invoke(httpSourceReader, pageInfo, true);

        // Verify the nested body was updated correctly
        Map<String, Object> updatedBody = httpParameter.getBody();
        Map<String, Object> updatedPagination = (Map<String, Object>) updatedBody.get("pagination");
        Assertions.assertEquals(5, updatedPagination.get("page"));
        Assertions.assertEquals(10, updatedPagination.get("limit"));
        Assertions.assertEquals("active", updatedBody.get("filter"));
    }

    @Test
    public void testUpdateRequestParamWithKeepPageParamAsHttpParam() throws Exception {
        // Setup test data
        httpParameter.setKeepPageParamAsHttpParam(true);

        PageInfo pageInfo = new PageInfo();
        pageInfo.setPageField("page");
        pageInfo.setPageIndex(5L);

        // Call updateRequestParam method using reflection
        Method updateRequestParamMethod =
                HttpSourceReader.class.getDeclaredMethod(
                        "updateRequestParam", PageInfo.class, boolean.class);
        updateRequestParamMethod.setAccessible(true);
        updateRequestParamMethod.invoke(httpSourceReader, pageInfo, true);

        // Verify the params were updated correctly
        Map<String, String> updatedParams = httpParameter.getParams();
        Assertions.assertEquals("5", updatedParams.get("page"));
    }

    @Test
    public void testUpdateRequestParamWithKeyBasedReplacement() throws Exception {
        // Setup test data
        Map<String, Object> body = new HashMap<>();
        body.put("page", 1);
        body.put("limit", 10);
        httpParameter.setBody(body);

        PageInfo pageInfo = new PageInfo();
        pageInfo.setPageField("page");
        pageInfo.setPageIndex(5L);
        pageInfo.setUsePlaceholderReplacement(false);

        // Call updateRequestParam method using reflection
        Method updateRequestParamMethod =
                HttpSourceReader.class.getDeclaredMethod(
                        "updateRequestParam", PageInfo.class, boolean.class);
        updateRequestParamMethod.setAccessible(true);
        updateRequestParamMethod.invoke(httpSourceReader, pageInfo, false);

        // Verify the body was updated correctly using key-based replacement
        Map<String, Object> updatedBody = httpParameter.getBody();
        Assertions.assertEquals(5L, updatedBody.get("page"));
        Assertions.assertEquals(10, updatedBody.get("limit"));
    }

    @Test
    public void testUpdateRequestParamWithNestedKeyBasedReplacement() throws Exception {
        // Setup test data with nested structure
        Map<String, Object> pagination = new HashMap<>();
        pagination.put("page", 1);
        pagination.put("limit", 10);

        Map<String, Object> body = new HashMap<>();
        body.put("pagination", pagination);
        body.put("filter", "active");
        httpParameter.setBody(body);

        PageInfo pageInfo = new PageInfo();
        pageInfo.setPageField("page");
        pageInfo.setPageIndex(5L);
        pageInfo.setUsePlaceholderReplacement(false);

        // Call updateRequestParam method using reflection
        Method updateRequestParamMethod =
                HttpSourceReader.class.getDeclaredMethod(
                        "updateRequestParam", PageInfo.class, boolean.class);
        updateRequestParamMethod.setAccessible(true);
        updateRequestParamMethod.invoke(httpSourceReader, pageInfo, false);

        // Verify the nested body was updated correctly using key-based replacement
        Map<String, Object> updatedBody = httpParameter.getBody();
        Map<String, Object> updatedPagination = (Map<String, Object>) updatedBody.get("pagination");
        Assertions.assertEquals(5L, updatedPagination.get("page"));
        Assertions.assertEquals(10, updatedPagination.get("limit"));
        Assertions.assertEquals("active", updatedBody.get("filter"));
    }
}
