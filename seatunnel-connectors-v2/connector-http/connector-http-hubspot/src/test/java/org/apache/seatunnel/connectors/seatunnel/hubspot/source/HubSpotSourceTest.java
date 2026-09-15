/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.hubspot.source;

import org.apache.seatunnel.api.common.JobContext;
import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.common.constants.JobMode;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpPaginationType;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpParameter;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.http.config.PageInfo;
import org.apache.seatunnel.connectors.seatunnel.http.source.HttpSourceReader;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.lang.reflect.Field;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/** Regression tests for the HubSpot source runtime wiring. */
public class HubSpotSourceTest {

    /**
     * Verifies that the reader receives the HubSpot-specific request parameter together with the
     * cursor pagination state from the shared HTTP source path.
     */
    @Test
    public void testCreateReaderUsesHubSpotRuntimeParameterAndPageInfo() throws Exception {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("access_token", "test_secret_token");
        configMap.put("object_type", "companies");

        HubSpotSource source = new HubSpotSource(ReadonlyConfig.fromMap(configMap));
        SourceReader.Context context = Mockito.mock(SourceReader.Context.class);
        Mockito.when(context.getBoundedness()).thenReturn(Boundedness.BOUNDED);

        HttpSourceReader reader =
                (HttpSourceReader) source.createReader(new SingleSplitReaderContext(context));

        HttpParameter httpParameter = (HttpParameter) getField(reader, "httpParameter");
        Assertions.assertEquals(
                "Bearer test_secret_token", httpParameter.getHeaders().get("Authorization"));
        Assertions.assertEquals(
                "https://api.hubapi.com/crm/v3/objects/companies", httpParameter.getUrl());
        Assertions.assertTrue(httpParameter.isKeepPageParamAsHttpParam());

        @SuppressWarnings("unchecked")
        Optional<PageInfo> pageInfoOptional =
                (Optional<PageInfo>) getField(reader, "pageInfoOptional");
        Assertions.assertTrue(pageInfoOptional.isPresent());
        Assertions.assertEquals(
                HttpPaginationType.CURSOR.getCode(), pageInfoOptional.get().getPageType());
        Assertions.assertEquals("after", pageInfoOptional.get().getPageCursorFieldName());
        Assertions.assertEquals(
                "$.paging.next.after", pageInfoOptional.get().getPageCursorResponseField());
        Assertions.assertEquals(
                HubSpotSourceParameter.DEFAULT_CONTENT_FIELD, getField(reader, "contentJson"));
    }

    @Test
    public void testCreateReaderUsesCursorPagingKeysFromPageingConfig() throws Exception {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("access_token", "test_secret_token");

        Map<String, Object> pageing = new HashMap<>();
        pageing.put(HttpSourceOptions.PAGE_TYPE.key(), HttpPaginationType.CURSOR.getCode());
        pageing.put(HttpSourceOptions.PAGE_CURSOR_FIELD_NAME.key(), "next_after");
        pageing.put(HttpSourceOptions.PAGE_CURSOR_RESPONSE_FIELD.key(), "$.paging.token.after");
        pageing.put(HttpSourceOptions.USE_PLACEHOLDER_REPLACEMENT.key(), true);
        configMap.put(HttpSourceOptions.PAGEING.key(), pageing);

        HubSpotSource source = new HubSpotSource(ReadonlyConfig.fromMap(configMap));
        SourceReader.Context context = Mockito.mock(SourceReader.Context.class);
        Mockito.when(context.getBoundedness()).thenReturn(Boundedness.BOUNDED);

        HttpSourceReader reader =
                (HttpSourceReader) source.createReader(new SingleSplitReaderContext(context));

        @SuppressWarnings("unchecked")
        Optional<PageInfo> pageInfoOptional =
                (Optional<PageInfo>) getField(reader, "pageInfoOptional");
        Assertions.assertTrue(pageInfoOptional.isPresent());
        Assertions.assertEquals("next_after", pageInfoOptional.get().getPageCursorFieldName());
        Assertions.assertEquals(
                "$.paging.token.after", pageInfoOptional.get().getPageCursorResponseField());
        Assertions.assertTrue(pageInfoOptional.get().isUsePlaceholderReplacement());
    }

    @Test
    public void testHubSpotSourceSupportsStreamingJsonMode() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("access_token", "test_secret_token");

        HubSpotSource source = new HubSpotSource(ReadonlyConfig.fromMap(configMap));
        source.setJobContext(new JobContext().setJobMode(JobMode.STREAMING));

        Assertions.assertEquals(Boundedness.UNBOUNDED, source.getBoundedness());
    }

    @Test
    public void testCreateReaderSupportsBinaryModeWithoutPagingDefaults() throws Exception {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put("access_token", "test_secret_token");
        configMap.put("format", "BINARY");
        configMap.put("url", "https://api.hubapi.com/files/v3/files/123/download");

        HubSpotSource source = new HubSpotSource(ReadonlyConfig.fromMap(configMap));
        SourceReader.Context context = Mockito.mock(SourceReader.Context.class);
        Mockito.when(context.getBoundedness()).thenReturn(Boundedness.BOUNDED);

        HttpSourceReader reader =
                (HttpSourceReader) source.createReader(new SingleSplitReaderContext(context));

        @SuppressWarnings("unchecked")
        Optional<PageInfo> pageInfoOptional =
                (Optional<PageInfo>) getField(reader, "pageInfoOptional");
        Assertions.assertFalse(pageInfoOptional.isPresent());
        Assertions.assertEquals(Boolean.TRUE, getField(reader, "binaryMode"));
    }

    /**
     * Reads a private field so the test can assert the exact runtime object passed into the real
     * HTTP source reader.
     */
    private static Object getField(Object target, String fieldName) throws Exception {
        Field field = HttpSourceReader.class.getDeclaredField(fieldName);
        field.setAccessible(true);
        return field.get(target);
    }
}
