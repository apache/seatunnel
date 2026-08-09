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

package org.apache.seatunnel.e2e.connector.firebase;

import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.containers.Container;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.util.Collections;
import java.util.List;

public class FirebaseSourceIT extends TestSuiteBase {

    private static HttpServer server;
    private static int serverPort;

    @BeforeAll
    public static void startBuiltInHttpServer() throws IOException {
        // Create lightweight Java HTTP server
        server = HttpServer.create(new InetSocketAddress(0), 0);
        serverPort = server.getAddress().getPort();
        // Expose localhost port to Testcontainers
        org.testcontainers.Testcontainers.exposeHostPorts(serverPort);
        server.createContext(
                "/users.json",
                new HttpHandler() {
                    @Override
                    public void handle(HttpExchange exchange) throws IOException {
                        String jsonResponse =
                                "{\n"
                                        + "  \"user_1\": {\"name\": \"Test User One\", \"role\": \"Engineer\"},\n"
                                        + "  \"user_2\": {\"name\": \"Test User Two\", \"role\": \"Developer\"}\n"
                                        + "}";

                        byte[] responseBytes = jsonResponse.getBytes();
                        exchange.getResponseHeaders().set("Content-Type", "application/json");
                        exchange.sendResponseHeaders(200, responseBytes.length);

                        OutputStream os = exchange.getResponseBody();
                        os.write(responseBytes);
                        os.close();
                    }
                });
        server.setExecutor(null); // default executor
        server.start();
    }

    @AfterAll
    public static void stopHttpServer() {
        if (server != null) {
            server.stop(0);
        }
    }

    @TestTemplate
    public void testFirebaseSourceToAssert(TestContainer container)
            throws IOException, InterruptedException {

        List<String> variables = Collections.singletonList("FIREBASE_MOCK_PORT=" + serverPort);
        Container.ExecResult execResult =
                container.executeJob("/firebase_source_to_assert.conf", variables);

        if (execResult.getExitCode() != 0) {
            System.err.println("Job Stdout: " + execResult.getStdout());
            System.err.println("Job Stderr: " + execResult.getStderr());
        }

        Assertions.assertEquals(0, execResult.getExitCode());
    }

    public static int getServerPort() {
        return serverPort;
    }
}
