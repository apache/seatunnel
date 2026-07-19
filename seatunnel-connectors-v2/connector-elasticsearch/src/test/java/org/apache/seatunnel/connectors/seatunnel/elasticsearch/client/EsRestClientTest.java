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

package org.apache.seatunnel.connectors.seatunnel.elasticsearch.client;

import org.apache.seatunnel.connectors.seatunnel.elasticsearch.dto.BulkResponse;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.exception.ElasticsearchConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.elasticsearch.exception.ElasticsearchConnectorException;

import org.apache.http.StatusLine;

import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.lang.reflect.Constructor;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class EsRestClientTest {

    @Mock private RestClient mockRestClient;

    // ---- bulk: non-200 path ----

    @Test
    void testBulkIncludesEsErrorBodyOnNon200Response() throws Exception {
        Response mockResponse = mock(Response.class);
        StatusLine mockStatusLine = mock(StatusLine.class);
        String esErrorJson = "{\"error\":{\"root_cause\":[{\"type\":\"rate_limit_exceeded\"}]}}";

        when(mockRestClient.performRequest(any(Request.class))).thenReturn(mockResponse);
        when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
        when(mockStatusLine.getStatusCode()).thenReturn(429);
        when(mockResponse.getEntity())
                .thenReturn(
                        new org.apache.http.entity.StringEntity(
                                esErrorJson, org.apache.http.entity.ContentType.APPLICATION_JSON));

        EsRestClient client = createEsRestClient(mockRestClient);

        ElasticsearchConnectorException exception =
                Assertions.assertThrows(
                        ElasticsearchConnectorException.class,
                        () -> client.bulk("{\"index\":{}}\n{}\n"));

        Assertions.assertEquals(
                ElasticsearchConnectorErrorCode.BULK_RESPONSE_ERROR,
                exception.getSeaTunnelErrorCode());
        Assertions.assertTrue(
                exception.getMessage().contains(esErrorJson),
                "Error message should contain ES response body for debugging");
    }

    // ---- bulk: 200 success path ----

    @Test
    void testBulkSucceedsOn200Response() throws Exception {
        Response mockResponse = mock(Response.class);
        StatusLine mockStatusLine = mock(StatusLine.class);

        when(mockRestClient.performRequest(any(Request.class))).thenReturn(mockResponse);
        when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
        when(mockStatusLine.getStatusCode()).thenReturn(200);
        when(mockResponse.getEntity())
                .thenReturn(
                        new org.apache.http.entity.StringEntity(
                                "{\"took\":1,\"errors\":false,\"items\":[]}",
                                org.apache.http.entity.ContentType.APPLICATION_JSON));

        EsRestClient client = createEsRestClient(mockRestClient);

        BulkResponse result = client.bulk("{\"index\":{}}\n{}\n");
        Assertions.assertFalse(result.isErrors());
    }

    // ---- createIndex: 200 success path (void method, must still consume entity) ----

    @Test
    void testCreateIndexConsumesEntityOn200Response() throws Exception {
        Response mockResponse = mock(Response.class);
        StatusLine mockStatusLine = mock(StatusLine.class);

        when(mockRestClient.performRequest(any(Request.class))).thenReturn(mockResponse);
        when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
        when(mockStatusLine.getStatusCode()).thenReturn(200);
        when(mockResponse.getEntity())
                .thenReturn(
                        new org.apache.http.entity.StringEntity(
                                "{\"acknowledged\":true}",
                                org.apache.http.entity.ContentType.APPLICATION_JSON));

        EsRestClient client = createEsRestClient(mockRestClient);

        // Should not throw — entity is consumed internally
        Assertions.assertDoesNotThrow(() -> client.createIndex("test_index"));
    }

    // ---- createIndex: non-200 path includes ES error body ----

    @Test
    void testCreateIndexIncludesEsErrorBodyOnNon200Response() throws Exception {
        Response mockResponse = mock(Response.class);
        StatusLine mockStatusLine = mock(StatusLine.class);
        String esErrorJson = "{\"error\":{\"type\":\"resource_already_exists_exception\"}}";

        when(mockRestClient.performRequest(any(Request.class))).thenReturn(mockResponse);
        when(mockResponse.getStatusLine()).thenReturn(mockStatusLine);
        when(mockStatusLine.getStatusCode()).thenReturn(400);
        when(mockResponse.getEntity())
                .thenReturn(
                        new org.apache.http.entity.StringEntity(
                                esErrorJson, org.apache.http.entity.ContentType.APPLICATION_JSON));

        EsRestClient client = createEsRestClient(mockRestClient);

        ElasticsearchConnectorException exception =
                Assertions.assertThrows(
                        ElasticsearchConnectorException.class,
                        () -> client.createIndex("test_index"));

        Assertions.assertTrue(
                exception.getMessage().contains(esErrorJson),
                "Error message should contain ES response body for debugging");
    }

    private EsRestClient createEsRestClient(RestClient restClient) throws Exception {
        Constructor<EsRestClient> constructor =
                EsRestClient.class.getDeclaredConstructor(RestClient.class);
        constructor.setAccessible(true);
        return constructor.newInstance(restClient);
    }
}
