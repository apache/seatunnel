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

package org.apache.seatunnel.connectors.seatunnel.airtable.source;

import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.http.client.HttpClientProvider;
import org.apache.seatunnel.connectors.seatunnel.http.client.HttpResponse;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpParameter;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpRequestMethod;
import org.apache.seatunnel.connectors.seatunnel.http.source.SimpleTextDeserializationSchema;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class AirtableSourceReaderTest {

    @Mock private SingleSplitReaderContext context;
    @Mock private HttpClientProvider httpClient;

    private HttpParameter parameter;
    private SimpleTextDeserializationSchema schema;

    @BeforeEach
    public void setUp() {
        MockitoAnnotations.openMocks(this);
        parameter = new HttpParameter();
        parameter.setUrl("https://api.airtable.com/v0/appBase/table/listRecords");
        parameter.setMethod(HttpRequestMethod.POST);

        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"content"}, new SeaTunnelDataType[] {BasicType.STRING_TYPE});
        schema = new SimpleTextDeserializationSchema(rowType);
    }

    private AirtableSourceReader createReader(int rateLimitMaxRetries) {
        AirtableSourceReader reader =
                new AirtableSourceReader(
                        parameter, context, schema, null, null, null, 0, 0, rateLimitMaxRetries);
        reader.setHttpClient(httpClient);
        return reader;
    }

    @Test
    public void testRetryOn429ThenSuccess() throws Exception {
        when(httpClient.execute(anyString(), anyString(), any(), any(), any(), anyBoolean()))
                .thenReturn(new HttpResponse(429, "{\"error\":{\"type\":\"RATE_LIMIT\"}}"))
                .thenReturn(
                        new HttpResponse(
                                200,
                                "{\"records\":[{\"id\":\"rec1\",\"fields\":{\"Name\":\"Alice\"}}]}"));

        AirtableSourceReader reader = createReader(2);
        HttpResponse response = reader.executeRequest();

        Assertions.assertEquals(200, response.getCode());
        verify(httpClient, times(2))
                .execute(anyString(), anyString(), any(), any(), any(), anyBoolean());
    }

    @Test
    public void testStopRetryAfterMaxRetries() throws Exception {
        when(httpClient.execute(anyString(), anyString(), any(), any(), any(), anyBoolean()))
                .thenReturn(new HttpResponse(429, "{\"error\":{\"type\":\"RATE_LIMIT\"}}"));

        AirtableSourceReader reader = createReader(1);
        HttpResponse response = reader.executeRequest();

        Assertions.assertEquals(429, response.getCode());
        // 1 initial + 1 retry = 2 calls
        verify(httpClient, times(2))
                .execute(anyString(), anyString(), any(), any(), any(), anyBoolean());
    }
}
