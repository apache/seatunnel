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

package org.apache.seatunnel.connectors.doris.sink.writer;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.TablePath;
import org.apache.seatunnel.connectors.doris.config.DorisSinkConfig;
import org.apache.seatunnel.connectors.doris.exception.DorisConnectorException;
import org.apache.seatunnel.connectors.doris.util.HttpUtil;

import org.apache.http.Header;
import org.apache.http.ProtocolVersion;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicStatusLine;
import org.apache.http.protocol.HttpContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Executors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class DorisStreamLoadTest {

    @Test
    void testAbortPreCommitAddsRedirectDiagnostics() throws Exception {
        CloseableHttpClient httpClient = mock(CloseableHttpClient.class);
        CloseableHttpResponse response = mock(CloseableHttpResponse.class);
        Header location =
                new BasicHeader("Location", "http://be1:8040/api/test_db/test_table/_stream_load");
        when(response.getStatusLine())
                .thenReturn(
                        new BasicStatusLine(
                                new ProtocolVersion("HTTP", 1, 1), 307, "Temporary Redirect"));
        when(response.getFirstHeader("Location")).thenReturn(location);
        when(httpClient.execute(any(HttpUriRequest.class), any(HttpContext.class)))
                .thenReturn(response);

        DorisStreamLoad streamLoad =
                new DorisStreamLoad(
                        "fe1:8030",
                        TablePath.of("test_db", "test_table"),
                        createSinkConfig(true, true),
                        new LabelGenerator("test_job", true),
                        httpClient);

        DorisConnectorException exception =
                Assertions.assertThrows(
                        DorisConnectorException.class,
                        () -> streamLoad.abortPreCommit("test_job", 1L));

        Assertions.assertTrue(exception.getMessage().contains("307 Temporary Redirect"));
        Assertions.assertTrue(
                exception
                        .getMessage()
                        .contains("Location=http://be1:8040/api/test_db/test_table/_stream_load"));
        Assertions.assertTrue(exception.getMessage().contains("direct_to_be=true"));
        Assertions.assertTrue(exception.getMessage().contains("2pc=true"));
        Assertions.assertTrue(exception.getMessage().contains("stage=pre-commit"));
    }

    @Test
    void testAbortPreCommitAddsRedirectDiagnosticsAfterFollowUpConnectionFailure()
            throws Exception {
        int unreachablePort = reserveUnusedPort();
        String location =
                String.format(
                        "http://127.0.0.1:%s/api/test_db/test_table/_stream_load", unreachablePort);
        HttpServer redirectServer = createRedirectServer(location);
        try (CloseableHttpClient httpClient = new HttpUtil().getHttpClient()) {
            DorisStreamLoad streamLoad =
                    new DorisStreamLoad(
                            "127.0.0.1:" + redirectServer.getAddress().getPort(),
                            TablePath.of("test_db", "test_table"),
                            createSinkConfig(true, true),
                            new LabelGenerator("test_job", true),
                            httpClient);

            DorisConnectorException exception =
                    Assertions.assertThrows(
                            DorisConnectorException.class,
                            () -> streamLoad.abortPreCommit("test_job", 1L));

            Assertions.assertTrue(exception.getMessage().contains("redirect follow-up failed"));
            Assertions.assertTrue(exception.getMessage().contains("Location=" + location));
            Assertions.assertTrue(exception.getMessage().contains("stage=pre-commit"));
        } finally {
            redirectServer.stop(0);
        }
    }

    private DorisSinkConfig createSinkConfig(boolean directToBe, boolean enable2PC) {
        Map<String, Object> options = new HashMap<>();
        options.put("fenodes", "fe1:8030");
        options.put("benodes", "be1:8040");
        options.put("direct_to_be", directToBe);
        options.put("sink.enable-2pc", enable2PC);
        options.put("username", "root");
        options.put("password", "");
        options.put("database", "test_db");
        options.put("table", "test_table");
        options.put("sink.label-prefix", "test_job");
        options.put("doris.config", createStreamLoadProperties());
        return DorisSinkConfig.of(ReadonlyConfig.fromMap(options));
    }

    private Map<String, String> createStreamLoadProperties() {
        Map<String, String> properties = new HashMap<>();
        properties.put("format", "json");
        properties.put("read_json_by_line", "true");
        return properties;
    }

    private HttpServer createRedirectServer(String location) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext(
                "/api/test_db/test_table/_stream_load",
                exchange -> writeRedirectResponse(exchange, location));
        server.setExecutor(Executors.newCachedThreadPool());
        server.start();
        return server;
    }

    private void writeRedirectResponse(HttpExchange exchange, String location) throws IOException {
        exchange.getResponseHeaders().add("Location", location);
        exchange.sendResponseHeaders(307, -1);
        try (OutputStream outputStream = exchange.getResponseBody()) {
            outputStream.flush();
        }
    }

    private int reserveUnusedPort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
