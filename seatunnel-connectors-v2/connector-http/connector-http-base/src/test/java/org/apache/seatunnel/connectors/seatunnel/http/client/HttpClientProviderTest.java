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

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.when;

class HttpClientProviderTest {

    private HttpPost mockRequest;

    @BeforeEach
    void setUp() {

        mockRequest = new HttpPost("http://localhost:8080");
    }

    @AfterEach
    void tearDown() {
        mockRequest = null;
    }

    @Test
    void testAddDefaultJsonContentTypeWhenNotPresent() throws Exception {
        Map<String, Object> body = new HashMap<>();
        body.put("key", "value");

        HttpClientProvider.addBody(mockRequest, body);

        // case 1: user not define content-type, use default content type
        assertNotNull(mockRequest.getFirstHeader("Content-Type"));
        Assertions.assertEquals(
                "application/json", mockRequest.getFirstHeader("Content-Type").getValue());
    }

    @Test
    void testPreserveExistingContentType() throws Exception {
        mockRequest.addHeader(new BasicHeader("Content-Type", "text/plain"));

        Map<String, Object> body = new HashMap<>();
        body.put("key", "value");

        HttpClientProvider.addBody(mockRequest, body);

        // case 2: if user define content-type, set it
        assertNotNull(mockRequest.getFirstHeader("Content-Type"));
        Assertions.assertEquals(
                "text/plain", mockRequest.getFirstHeader("Content-Type").getValue());
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
