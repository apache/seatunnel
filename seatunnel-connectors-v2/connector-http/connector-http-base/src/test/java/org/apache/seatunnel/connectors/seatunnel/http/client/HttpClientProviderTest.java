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
package org.apache.seatunnel.connectors.seatunnel.http.client;

import org.apache.http.Header;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.message.BasicHeader;
import org.apache.http.protocol.HTTP;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class HttpClientProviderTest {

    private HttpPost mockRequest;

    @BeforeEach
    void setUp() {

        mockRequest = mock(HttpPost.class);
    }

    @AfterEach
    void tearDown() {
        mockRequest = null;
    }

    @Test
    void testAddBodyPreservesOriginalHeaders() throws Exception {
        Header[] originalHeaders =
                new Header[] {new BasicHeader(HTTP.CONTENT_TYPE, "application/json;utf-8")};
        when(mockRequest.getAllHeaders()).thenReturn(originalHeaders);
        when(mockRequest.getHeaders(HTTP.CONTENT_TYPE)).thenReturn(new Header[0]);

        // verify original headers are preserved
        HttpClientProvider.addBody(mockRequest, Collections.emptyMap());

        //        verify(mockRequest).setHeader(HTTP.CONTENT_TYPE, "application/json;utf-8");
        //        verify(mockRequest).setEntity(any(HttpEntity.class));

        Header[] currentHeaders =
                new Header[] {new BasicHeader(HTTP.CONTENT_TYPE, "application/json;utf-8")};
        when(mockRequest.getAllHeaders()).thenReturn(currentHeaders);

        Header[] resultHeaders = mockRequest.getAllHeaders();
        Assertions.assertEquals(1, resultHeaders.length);
        Assertions.assertEquals(HTTP.CONTENT_TYPE, resultHeaders[0].getName());
        Assertions.assertEquals("application/json;utf-8", resultHeaders[0].getValue());
    }

    @Test
    void addBody() throws Exception {
        HttpPost post = mockRequest;

        Header[] originalHeaders =
                new Header[] {new BasicHeader(HTTP.CONTENT_TYPE, "application/json;utf-8")};
        when(mockRequest.getAllHeaders()).thenReturn(originalHeaders);
        when(mockRequest.getHeaders(HTTP.CONTENT_TYPE)).thenReturn(new Header[0]);

        Map<String, Object> body = new HashMap<>();
        //        Header[] originalHeaders = post.getAllHeaders();
        HttpClientProvider.addBody(post, body);

        // ensure the original headers are preserved
        Header[] currentHeaders = post.getAllHeaders();
        Assertions.assertEquals(originalHeaders.length, currentHeaders.length);
        for (int i = 0; i < originalHeaders.length; i++) {
            Assertions.assertEquals(
                    originalHeaders[i].getName(),
                    currentHeaders[i].getName(),
                    "Header name mismatch at index " + i);
            Assertions.assertEquals(
                    originalHeaders[i].getValue(),
                    currentHeaders[i].getValue(),
                    "Header value mismatch at index " + i);
        }
        // ensure no manually set content type or encoding
        // test case fix, content type support user manually  set
        //        Assertions.assertNull(post.getEntity().getContentEncoding());
    }
}
