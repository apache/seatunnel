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

import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpPaginationType;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpParameter;
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

        HubSpotSource source =
                new HubSpotSource(
                        org.apache.seatunnel.api.configuration.ReadonlyConfig.fromMap(configMap));
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
