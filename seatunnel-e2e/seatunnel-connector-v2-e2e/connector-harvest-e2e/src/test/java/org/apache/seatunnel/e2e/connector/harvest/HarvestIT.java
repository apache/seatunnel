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

package org.apache.seatunnel.e2e.connector.harvest;

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

/**
 * Exercises the registered source through the actual job engine with deterministic API responses.
 */
public class HarvestIT extends TestSuiteBase implements TestResource {
    private HttpServer server;
    private int port;
    private final AtomicInteger requests = new AtomicInteger();

    @BeforeAll
    @Override
    public void startUp() throws IOException {
        server = HttpServer.create(new InetSocketAddress(0), 0);
        port = server.getAddress().getPort();
        Testcontainers.exposeHostPorts(port);
        server.createContext(
                "/v2/time_entries",
                exchange -> {
                    int status = 200;
                    String body;
                    String query = exchange.getRequestURI().getQuery();
                    int count = requests.incrementAndGet();
                    if (!"Bearer harvest-test-token"
                                    .equals(exchange.getRequestHeaders().getFirst("Authorization"))
                            || !"12345"
                                    .equals(
                                            exchange.getRequestHeaders()
                                                    .getFirst("Harvest-Account-Id"))
                            || !"GET".equals(exchange.getRequestMethod())
                            || query == null
                            || !query.contains("per_page=2")
                            || !query.contains("from=2026-11-01")) {
                        status = 400;
                        body = "{\"error\":\"unexpected request\"}";
                    } else if (count == 1) {
                        status = 429;
                        exchange.getResponseHeaders().set("Retry-After", "0");
                        body = "{}";
                    } else if (query.contains("page=1&")) {
                        body =
                                "{\"page\":1,\"next_page\":2,\"time_entries\":["
                                        + entry(1)
                                        + ","
                                        + entry(2)
                                        + "]}";
                    } else if (query.contains("page=2&")) {
                        body =
                                "{\"page\":2,\"next_page\":null,\"time_entries\":["
                                        + entry(3)
                                        + "]}";
                    } else {
                        status = 400;
                        body = "{\"error\":\"unexpected page\"}";
                    }
                    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
                    exchange.getResponseHeaders().set("Content-Type", "application/json");
                    exchange.sendResponseHeaders(status, bytes.length);
                    try (OutputStream out = exchange.getResponseBody()) {
                        out.write(bytes);
                    }
                });
        server.start();
    }

    private static String entry(int id) {
        return "{\"id\":" + id + ",\"hours\":1.25,\"notes\":null,\"spent_date\":\"2026-11-23\"}";
    }

    @BeforeEach
    public void resetRequests() {
        requests.set(0);
    }

    @AfterAll
    @Override
    public void tearDown() {
        if (server != null) {
            server.stop(0);
        }
    }

    @TestTemplate
    public void readsTimeEntries(TestContainer container) throws IOException, InterruptedException {
        Container.ExecResult result =
                container.executeJob(
                        "/harvest_source_to_assert.conf",
                        Collections.singletonList("harvest.test.port=" + port));
        Assertions.assertEquals(0, result.getExitCode(), result.getStderr());
        Assertions.assertEquals(3, requests.get());
    }
}
