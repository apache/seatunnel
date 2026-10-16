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

package org.apache.seatunnel.e2e.connector.intercom;

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
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;

public class IntercomIT extends TestSuiteBase implements TestResource {

    private static HttpServer httpServer;
    private static int serverPort;
    private static final AtomicInteger requestCount = new AtomicInteger(0);

    @BeforeAll
    @Override
    public void startUp() throws IOException {
        httpServer = HttpServer.create(new InetSocketAddress(0), 0);
        serverPort = httpServer.getAddress().getPort();
        Testcontainers.exposeHostPorts(serverPort);

        httpServer.createContext(
                "/contacts",
                exchange -> {
                    int currentReq = requestCount.incrementAndGet();
                    String responseBody;
                    int statusCode;

                    if (currentReq == 1) {
                        statusCode = 429;
                        responseBody = "{\"type\": \"error.list\", \"errors\": []}";
                    } else if (currentReq == 2) {
                        statusCode = 200;
                        responseBody =
                                "{\"type\": \"list\", \"data\": ["
                                        + "{\"type\": \"contact\", \"id\": \"c1\", \"role\": \"user\", \"email\": \"a@example.com\", \"name\": \"Alice\", \"phone\": null, \"external_id\": null, \"created_at\": 1571672154, \"updated_at\": 1571672200},"
                                        + "{\"type\": \"contact\", \"id\": \"c2\", \"role\": \"lead\", \"email\": \"b@example.com\", \"name\": null, \"phone\": \"+1123456789\", \"external_id\": \"ext-2\", \"created_at\": 1571672155, \"updated_at\": 1571672201}"
                                        + "], \"pages\": {\"type\": \"pages\", \"next\": {\"page\": 2, \"starting_after\": \"WzE1NzE2NzIxNTUwMDAsImMyIiwyXQ==\"}}}";
                    } else {
                        statusCode = 200;
                        responseBody =
                                "{\"type\": \"list\", \"data\": ["
                                        + "{\"type\": \"contact\", \"id\": \"c3\", \"role\": \"user\", \"email\": \"c@example.com\", \"name\": \"Carol\", \"phone\": null, \"external_id\": null, \"created_at\": 1571672156, \"updated_at\": 1571672202}"
                                        + "], \"pages\": {\"type\": \"pages\"}}";
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
    public void testIntercomSourceE2E(TestContainer container)
            throws IOException, InterruptedException {
        Container.ExecResult execResult =
                container.executeJob(
                        "/intercom_source_to_assert.conf",
                        Collections.singletonList("intercom.test.port=" + serverPort));
        Assertions.assertEquals(0, execResult.getExitCode(), execResult.getStderr());
    }
}
