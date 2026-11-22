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

package org.apache.seatunnel.e2e.connector.mailchimp;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.EngineType;
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
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Pattern;

class MailchimpIT extends TestSuiteBase implements TestResource {
    private HttpServer server;
    private final AtomicInteger requests = new AtomicInteger();
    private final AtomicReference<String> invalidRequest = new AtomicReference<>();

    @BeforeAll
    @Override
    public void startUp() throws IOException {
        server = HttpServer.create(new InetSocketAddress(0), 0);
        Testcontainers.exposeHostPorts(server.getAddress().getPort());
        server.createContext(
                "/3.0/lists/audience1/members",
                exchange -> {
                    int request = requests.incrementAndGet();
                    String auth =
                            "Basic "
                                    + Base64.getEncoder()
                                            .encodeToString(
                                                    "seatunnel:fixturekey-us1"
                                                            .getBytes(StandardCharsets.UTF_8));
                    String expected = request <= 3 ? "count=2&offset=0" : "count=1&offset=2";
                    int code = 200;
                    String body;
                    if (!"GET".equals(exchange.getRequestMethod())
                            || !auth.equals(exchange.getRequestHeaders().getFirst("Authorization"))
                            || !expected.equals(exchange.getRequestURI().getRawQuery())
                            || !"/3.0/lists/audience1/members"
                                    .equals(exchange.getRequestURI().getPath())) {
                        invalidRequest.set(
                                "Unexpected request method, authentication or pagination");
                        code = 400;
                        body = "{}";
                    } else if (request == 1 || request == 2) {
                        code = request == 1 ? 429 : 503;
                        exchange.getResponseHeaders().set("Retry-After", "0");
                        body = "{}";
                    } else {
                        String resource =
                                request == 3
                                        ? "/mailchimp-members-page-1.json"
                                        : "/mailchimp-members-page-2.json";
                        try (InputStream input = getClass().getResourceAsStream(resource)) {
                            if (input == null) {
                                throw new IOException("Missing Mailchimp fixture");
                            }
                            body = new String(input.readAllBytes(), StandardCharsets.UTF_8);
                        }
                    }
                    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
                    exchange.getResponseHeaders().set("Content-Type", "application/json");
                    if (code == 429 || code == 503) {
                        exchange.sendResponseHeaders(code, -1);
                        exchange.close();
                        return;
                    }
                    exchange.sendResponseHeaders(code, bytes.length);
                    try (OutputStream out = exchange.getResponseBody()) {
                        out.write(bytes);
                    }
                });
        server.start();
    }

    @BeforeEach
    void reset() {
        requests.set(0);
        invalidRequest.set(null);
    }

    @AfterAll
    @Override
    public void tearDown() {
        if (server != null) {
            server.stop(0);
        }
    }

    @TestTemplate
    void listMembersToAssert(TestContainer container) throws Exception {
        Container.ExecResult result =
                container.executeJob(
                        "/mailchimp_source_to_assert.conf",
                        Collections.singletonList(
                                "mailchimp.test.port=" + server.getAddress().getPort()));
        Assertions.assertEquals(0, result.getExitCode(), result.getStderr());
        if (container.identifier().getEngineType() == EngineType.SEATUNNEL) {
            String clientLogs = result.getStdout() + "\n" + result.getStderr();
            Assertions.assertTrue(
                    Pattern.compile("Job \\([0-9]+\\) end with state FINISHED")
                            .matcher(clientLogs)
                            .find(),
                    clientLogs);
            Assertions.assertFalse(
                    Pattern.compile("Job \\([0-9]+\\) end with state FAILED|jobStatus=FAILED")
                            .matcher(clientLogs)
                            .find(),
                    clientLogs);
        }
        Assertions.assertNull(invalidRequest.get());
        Assertions.assertEquals(
                4, requests.get(), "Retries stay at offset zero, followed by the final page");
    }
}
