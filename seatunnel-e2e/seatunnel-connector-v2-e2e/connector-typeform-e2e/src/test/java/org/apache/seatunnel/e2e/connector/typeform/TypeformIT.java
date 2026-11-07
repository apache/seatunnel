/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.e2e.connector.typeform;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.Testcontainers;
import org.testcontainers.containers.Container;

import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

public class TypeformIT extends TestSuiteBase implements TestResource {

    private static final String TOKEN = "placeholder-typeform-token-12345";
    private static final String PAGE_2_CURSOR = "tok-r2";
    private static final String PAGE_3_CURSOR = "tok-r3";

    private static HttpServer httpServer;
    private static int serverPort;
    private static final AtomicInteger requestCount = new AtomicInteger(0);

    private static String item(String id, String answers) {
        return "{\"landing_id\":\"l-"
                + id
                + "\",\"token\":\"tok-"
                + id
                + "\",\"response_id\":\""
                + id
                + "\",\"landed_at\":\"2026-09-14T22:33:59Z\",\"submitted_at\":\"2026-09-14T22:38:22Z\","
                + "\"answers\":"
                + answers
                + ",\"hidden\":{\"utm_source\":\"newsletter\"}}";
    }

    private static String page(String... items) {
        return "{\"total_items\":3,\"page_count\":2,\"items\":[" + String.join(",", items) + "]}";
    }

    @BeforeAll
    @Override
    public void startUp() throws IOException {
        httpServer = HttpServer.create(new InetSocketAddress(0), 0);
        serverPort = httpServer.getAddress().getPort();
        Testcontainers.exposeHostPorts(serverPort);

        httpServer.createContext(
                "/forms/abc123/responses",
                exchange -> {
                    int currentReq = requestCount.incrementAndGet();
                    String cursor = queryParam(exchange.getRequestURI(), "before");
                    String auth = exchange.getRequestHeaders().getFirst("Authorization");
                    String responseBody;
                    int statusCode;

                    // The first request gets a 429 to exercise the retry. After that the page is
                    // chosen by the before cursor, so the job only reads page 2 if the connector
                    // sends back the token of the last response on page 1.
                    if (!("Bearer " + TOKEN).equals(auth)) {
                        statusCode = 401;
                        responseBody = "{\"code\":\"AUTHENTICATION_FAILED\"}";
                    } else if (currentReq == 1) {
                        statusCode = 429;
                        responseBody = "{\"code\":\"RATE_LIMITED\"}";
                    } else if (cursor == null) {
                        statusCode = 200;
                        responseBody =
                                page(
                                        item(
                                                "r1",
                                                "[{\"field\":{\"id\":\"f1\",\"type\":\"short_text\"},\"type\":\"text\",\"text\":\"Lian\"}]"),
                                        item("r2", "[]"));
                    } else if (PAGE_2_CURSOR.equals(cursor)) {
                        statusCode = 200;
                        responseBody =
                                page(
                                        item(
                                                "r3",
                                                "[{\"field\":{\"id\":\"f2\",\"type\":\"yes_no\"},\"type\":\"boolean\",\"boolean\":true}]"));
                    } else if (PAGE_3_CURSOR.equals(cursor)) {
                        statusCode = 200;
                        responseBody = page();
                    } else {
                        statusCode = 400;
                        responseBody = "{\"code\":\"VALIDATION_ERROR\"}";
                    }

                    byte[] bytes = responseBody.getBytes(StandardCharsets.UTF_8);
                    exchange.getResponseHeaders().set("Content-Type", "application/json");
                    exchange.sendResponseHeaders(statusCode, bytes.length);
                    try (OutputStream os = exchange.getResponseBody()) {
                        os.write(bytes);
                    }
                });

        httpServer.start();
    }

    private static String queryParam(URI uri, String name) {
        String query = uri.getRawQuery();
        if (query == null) {
            return null;
        }
        for (String pair : query.split("&")) {
            int eq = pair.indexOf('=');
            if (eq > 0 && pair.substring(0, eq).equals(name)) {
                try {
                    return URLDecoder.decode(pair.substring(eq + 1), "UTF-8");
                } catch (UnsupportedEncodingException e) {
                    throw new IllegalStateException(e);
                }
            }
        }
        return null;
    }

    @AfterAll
    @Override
    public void tearDown() {
        if (httpServer != null) {
            httpServer.stop(0);
        }
    }

    @BeforeEach
    public void resetCounter() {
        requestCount.set(0);
    }

    @TestTemplate
    public void testTypeformSourceE2E(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob(
                        "/typeform_source_to_assert.conf",
                        Collections.singletonList("typeform.test.port=" + serverPort));
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
    }
}
