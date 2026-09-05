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

import org.apache.seatunnel.connectors.seatunnel.http.config.HttpParameter;

import org.apache.http.Header;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.message.BasicHeader;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpServer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.junit.jupiter.api.Assertions.assertNotNull;

class HttpClientProviderTest {

    private HttpClientProvider httpClientProvider;
    private HttpServer server;

    @AfterEach
    void tearDown() throws Exception {
        if (httpClientProvider != null) {
            httpClientProvider.close();
        }
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    void testAddDefaultJsonContentTypeWhenNotPresent() throws Exception {
        HttpPost mockRequest = new HttpPost("http://localhost:8080");
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
        HttpPost mockRequest = new HttpPost("http://localhost:8080");
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
        HttpPost post = new HttpPost("http://localhost:8080");
        Map<String, Object> body = new HashMap<>();
        Header[] originalHeaders = post.getAllHeaders();
        HttpClientProvider.addBody(post, body);

        // ensure the original headers are preserved
        Header[] currentHeaders = post.getAllHeaders();
        Assertions.assertEquals(0, originalHeaders.length);
        Assertions.assertEquals(1, currentHeaders.length);
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
        Assertions.assertNull(post.getEntity().getContentEncoding());
    }

    @Test
    void executePreservesNestedJsonBody() throws Exception {
        AtomicReference<String> receivedBody = new AtomicReference<>();
        AtomicReference<String> receivedQuery = new AtomicReference<>();
        List<String> receivedContentTypes = Collections.synchronizedList(new ArrayList<>());
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext(
                "/request",
                exchange -> {
                    receivedBody.set(readRequestBody(exchange.getRequestBody()));
                    receivedQuery.set(exchange.getRequestURI().getQuery());
                    receivedContentTypes.clear();
                    receivedContentTypes.addAll(exchange.getRequestHeaders().get("Content-Type"));
                    exchange.sendResponseHeaders(200, -1);
                    exchange.close();
                });
        server.start();

        HttpParameter parameter = new HttpParameter();
        parameter.setConnectTimeoutMs(1_000);
        parameter.setSocketTimeoutMs(1_000);
        httpClientProvider = new HttpClientProvider(parameter);
        String body = "{\"pageNo\":1,\"data\":{\"type\":1}}";

        HttpResponse response =
                httpClientProvider.execute(
                        "http://127.0.0.1:" + server.getAddress().getPort() + "/request",
                        "POST",
                        Collections.singletonMap("Content-Type", "application/json"),
                        Collections.singletonMap("traceId", "123"),
                        body,
                        false);

        Assertions.assertEquals(200, response.getCode());
        Assertions.assertEquals(body, receivedBody.get());
        Assertions.assertEquals("traceId=123", receivedQuery.get());
        // The caller-supplied Content-Type must not be duplicated. Otherwise the request
        // would carry two Content-Type headers, which RFC 7230 §3.2.2 forbids for
        // non-list headers and yields implementation-defined behaviour on the receiver.
        Assertions.assertEquals(
                Collections.singletonList("application/json"), receivedContentTypes);
    }

    @Test
    void executeConvertsBodyToFormWhenContentTypeIsFormUrlencoded() throws Exception {
        AtomicReference<String> receivedBody = new AtomicReference<>();
        AtomicReference<String> receivedContentType = new AtomicReference<>();
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext(
                "/formBody",
                exchange -> {
                    receivedBody.set(readRequestBody(exchange.getRequestBody()));
                    receivedContentType.set(exchange.getRequestHeaders().getFirst("Content-Type"));
                    exchange.sendResponseHeaders(200, -1);
                    exchange.close();
                });
        server.start();

        HttpParameter parameter = new HttpParameter();
        parameter.setConnectTimeoutMs(1_000);
        parameter.setSocketTimeoutMs(1_000);
        httpClientProvider = new HttpClientProvider(parameter);
        String body = "{id=1}";

        HttpResponse response =
                httpClientProvider.execute(
                        "http://127.0.0.1:" + server.getAddress().getPort() + "/formBody",
                        "POST",
                        Collections.singletonMap(
                                "Content-Type", "application/x-www-form-urlencoded"),
                        Collections.emptyMap(),
                        body,
                        false);

        Assertions.assertEquals(200, response.getCode());
        // Body must be form-encoded (legacy behaviour for keepParamsAsForm=false + form CT)
        Assertions.assertEquals("id=1", receivedBody.get());
        Assertions.assertEquals("application/x-www-form-urlencoded", receivedContentType.get());
    }

    private String readRequestBody(InputStream inputStream) throws IOException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        byte[] buffer = new byte[256];
        int bytesRead;
        while ((bytesRead = inputStream.read(buffer)) != -1) {
            outputStream.write(buffer, 0, bytesRead);
        }
        return new String(outputStream.toByteArray(), StandardCharsets.UTF_8);
    }
}
