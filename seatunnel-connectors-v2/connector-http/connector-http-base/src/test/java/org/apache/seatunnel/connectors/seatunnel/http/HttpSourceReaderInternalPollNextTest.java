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
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpPaginationType;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpParameter;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpRequestMethod;
import org.apache.seatunnel.connectors.seatunnel.http.config.JsonField;
import org.apache.seatunnel.connectors.seatunnel.http.config.PageInfo;
import org.apache.seatunnel.connectors.seatunnel.http.source.HttpSourceReader;
import org.apache.seatunnel.connectors.seatunnel.http.source.SimpleTextDeserializationSchema;

import org.junit.jupiter.api.AfterEach;
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

public class HttpSourceReaderInternalPollNextTest {
    private HttpParameter httpParameter;
    private JsonField jsonField;
    private SimpleTextDeserializationSchema deserializationSchema;
    private HttpSourceReader httpSourceReader;
    private AutoCloseable mock;
    @Mock private SingleSplitReaderContext context;
    @Mock private Collector<SeaTunnelRow> collector;
    @Mock private HttpClientProvider httpClientProvider;
    @Mock private HttpResponse httpResponse;

    @BeforeEach
    public void setup() throws Exception {
        mock = MockitoAnnotations.openMocks(this);
        when(httpResponse.getCode()).thenReturn(200);
        when(collector.getCheckpointLock()).thenReturn(new Object());
        when(httpClientProvider.execute(
                        anyString(), anyString(), any(), any(), any(), anyBoolean()))
                .thenAnswer(
                        invocation -> {
                            String requestBody = invocation.getArgument(4);
                            if (requestBody != null && requestBody.contains("\"page\":\"1\"")) {
                                when(httpResponse.getContent())
                                        .thenReturn("[{\"key1\":\"v1\",\"key2\":\"v2\"}]");
                            } else {
                                when(httpResponse.getContent()).thenReturn("[]");
                            }
                            when(httpResponse.getCode()).thenReturn(200);
                            return httpResponse;
                        });

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
        collector =
                new Collector<SeaTunnelRow>() {
                    @Override
                    public void collect(SeaTunnelRow record) {}

                    @Override
                    public Object getCheckpointLock() {
                        return null;
                    }
                };
    }

    @Test
    public void testPageNumberPlaceHolderRequestBodyUpdate() throws Exception {
        String bodyJson = "{\"page\":\"${page}\",\"limit\":10}";
        httpParameter.setBody(bodyJson);

        PageInfo pageInfo = new PageInfo();
        pageInfo.setPageField("page");
        pageInfo.setPageIndex(1L);
        pageInfo.setBatchSize(1);
        pageInfo.setPageType(HttpPaginationType.PAGE_NUMBER.getCode());
        pageInfo.setUsePlaceholderReplacement(true);
        pageInfo.setTotalPageSize(2L);

        httpSourceReader =
                new HttpSourceReader(
                        httpParameter, context, deserializationSchema, jsonField, null, pageInfo);
        httpSourceReader.open();

        httpSourceReader.internalPollNext(collector);

        // Verify the body was updated correctly
        Assertions.assertEquals("{\"page\":\"2\",\"limit\":10}", httpParameter.getBody());
        Map<String, Object> bodyMap =
                JsonUtils.toMap(JsonUtils.stringToJsonNode(httpParameter.getBody()));
        Assertions.assertEquals("2", bodyMap.get("page"));
        Assertions.assertEquals(10, bodyMap.get("limit"));
        httpSourceReader.close();
    }

    @AfterEach
    public void tearDown() throws Exception {
        mock.close();
    }
}
