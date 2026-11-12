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

package org.apache.seatunnel.connectors.doris.rest;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.doris.config.DorisSourceConfig;
import org.apache.seatunnel.connectors.doris.exception.DorisConnectorException;

import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpRequestBase;
import org.apache.http.entity.StringEntity;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;

public class RestServiceTest {

    private static final Logger LOG = LoggerFactory.getLogger(RestServiceTest.class);

    private static final String QUERY_PLAN_PATH = "/api/test_db/test_table/_query_plan";
    private static final int READ_TIMEOUT_MS = 1000;
    private static final int SERVER_DELAY_MS = 10 * 1000;
    private static final int ELAPSED_LIMIT_MS = 3 * READ_TIMEOUT_MS;

    private HttpServer server;
    private DorisSourceConfig config;

    /**
     * Starts a local FE stub that delays every response far beyond the configured read timeout, so
     * a request without an effective read timeout blocks for the full delay, while one with an
     * effective read timeout fails fast with a SocketTimeoutException.
     */
    @BeforeEach
    void setUp() throws IOException {
        server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext(
                QUERY_PLAN_PATH,
                exchange -> {
                    try {
                        Thread.sleep(SERVER_DELAY_MS);
                        byte[] body = "{}".getBytes(StandardCharsets.UTF_8);
                        exchange.sendResponseHeaders(200, body.length);
                        try (OutputStream out = exchange.getResponseBody()) {
                            out.write(body);
                        }
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    } catch (IOException ignored) {
                        // the client may time out and disconnect before the response is sent
                    }
                });
        server.setExecutor(
                Executors.newCachedThreadPool(
                        runnable -> {
                            Thread thread = new Thread(runnable);
                            thread.setDaemon(true);
                            return thread;
                        }));
        server.start();

        Map<String, Object> options = new HashMap<>();
        options.put("fenodes", "127.0.0.1:" + server.getAddress().getPort());
        options.put("username", "root");
        options.put("password", "");
        options.put("database", "test_db");
        options.put("table", "test_table");
        options.put("doris.request.connect.timeout.ms", READ_TIMEOUT_MS);
        options.put("doris.request.read.timeout.ms", READ_TIMEOUT_MS);
        options.put("doris.request.retries", 1);
        config = DorisSourceConfig.of(ReadonlyConfig.fromMap(options));
    }

    @AfterEach
    void tearDown() {
        if (server != null) {
            server.stop(0);
        }
    }

    @Test
    @Timeout(30)
    void testGetRequestHonorsReadTimeout() {
        HttpGet request = new HttpGet(queryPlanUrl());
        assertReadTimeoutFailsFast(request);
    }

    @Test
    @Timeout(30)
    void testPostRequestHonorsReadTimeout() throws Exception {
        HttpPost request = new HttpPost(queryPlanUrl());
        request.setEntity(new StringEntity("{\"sql\":\"select 1\"}", StandardCharsets.UTF_8));
        assertReadTimeoutFailsFast(request);
    }

    private String queryPlanUrl() {
        return "http://127.0.0.1:" + server.getAddress().getPort() + QUERY_PLAN_PATH;
    }

    private void assertReadTimeoutFailsFast(HttpRequestBase request) {
        long start = System.currentTimeMillis();
        DorisConnectorException exception =
                Assertions.assertThrows(
                        DorisConnectorException.class,
                        () -> RestService.send(config, request, LOG));
        long elapsed = System.currentTimeMillis() - start;
        Assertions.assertTrue(
                exception.getCause() instanceof SocketTimeoutException,
                "expected a SocketTimeoutException cause but got: " + exception.getCause());
        Assertions.assertTrue(
                elapsed < ELAPSED_LIMIT_MS,
                "request should fail after about the "
                        + READ_TIMEOUT_MS
                        + "ms read timeout but took "
                        + elapsed
                        + "ms");
    }
}
