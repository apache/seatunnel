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

package org.apache.seatunnel.e2e.connector.websocket;

import org.apache.seatunnel.e2e.common.TestResource;
import org.apache.seatunnel.e2e.common.TestSuiteBase;
import org.apache.seatunnel.e2e.common.container.TestContainer;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.TestTemplate;
import org.testcontainers.Testcontainers;
import org.testcontainers.containers.Container;

import lombok.extern.slf4j.Slf4j;
import okhttp3.Response;
import okhttp3.WebSocket;
import okhttp3.WebSocketListener;
import okhttp3.mockwebserver.Dispatcher;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import okhttp3.mockwebserver.RecordedRequest;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/**
 * Runs a WebSocket server in the host JVM and lets the job under test read from it through the
 * {@code host.testcontainers.internal} tunnel.
 */
@Slf4j
public class WebSocketSourceIT extends TestSuiteBase implements TestResource {

    private static final List<String> PUSHED_MESSAGES =
            Arrays.asList(
                    "{\"id\":101,\"name\":\"Apache\",\"score\":98.5}",
                    "{\"id\":102,\"name\":\"SeaTunnel\",\"score\":100.0}");

    private MockWebServer webSocketServer;
    private int serverPort;

    @BeforeAll
    @Override
    public void startUp() throws Exception {
        WebSocketListener listener =
                new WebSocketListener() {
                    @Override
                    public void onOpen(WebSocket webSocket, Response response) {
                        for (String message : PUSHED_MESSAGES) {
                            webSocket.send(message);
                        }
                    }

                    @Override
                    public void onClosing(WebSocket webSocket, int code, String reason) {
                        // echo the close frame, otherwise the connection stays half closed and
                        // the server cannot shut down
                        webSocket.close(code, reason);
                    }
                };
        webSocketServer = new MockWebServer();
        // every engine under test opens its own connection, so the upgrade response cannot be
        // enqueued once, it has to be produced for each incoming request
        webSocketServer.setDispatcher(
                new Dispatcher() {
                    @Override
                    public MockResponse dispatch(RecordedRequest request) {
                        return new MockResponse().withWebSocketUpgrade(listener);
                    }
                });
        webSocketServer.start();
        serverPort = webSocketServer.getPort();
        Testcontainers.exposeHostPorts(serverPort);
        log.info("WebSocket test server started on host port [{}]", serverPort);
    }

    @TestTemplate
    public void testWebSocketSourceToAssertSink(TestContainer container) throws Exception {
        String url = String.format("ws://host.testcontainers.internal:%d/", serverPort);
        List<String> variables = Collections.singletonList("URL=" + url);

        Container.ExecResult execResult =
                container.executeJob("/websocket_to_assert.conf", variables);

        Assertions.assertEquals(
                0,
                execResult.getExitCode(),
                "SeaTunnel job failed to execute. Error output: " + execResult.getStderr());
    }

    @AfterAll
    @Override
    public void tearDown() throws Exception {
        if (webSocketServer != null) {
            webSocketServer.shutdown();
            webSocketServer = null;
        }
    }
}
