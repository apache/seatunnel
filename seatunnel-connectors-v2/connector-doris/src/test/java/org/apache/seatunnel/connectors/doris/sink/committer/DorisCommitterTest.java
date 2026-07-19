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

package org.apache.seatunnel.connectors.doris.sink.committer;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.doris.config.DorisSinkConfig;
import org.apache.seatunnel.connectors.doris.exception.DorisConnectorException;
import org.apache.seatunnel.connectors.doris.util.HttpUtil;

import org.apache.http.Header;
import org.apache.http.ProtocolVersion;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.client.methods.HttpUriRequest;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicHeader;
import org.apache.http.message.BasicStatusLine;
import org.apache.http.protocol.HttpContext;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DorisCommitterTest {

    @Test
    void testCommitAddsRedirectDiagnostics() throws IOException {
        CloseableHttpClient httpClient = mock(CloseableHttpClient.class);
        CloseableHttpResponse response = mock(CloseableHttpResponse.class);
        Header location =
                new BasicHeader("Location", "http://be1:8040/api/test_db/_stream_load_2pc");
        when(response.getStatusLine())
                .thenReturn(
                        new BasicStatusLine(
                                new ProtocolVersion("HTTP", 1, 1), 307, "Temporary Redirect"));
        when(response.getFirstHeader("Location")).thenReturn(location);
        when(httpClient.execute(any(HttpUriRequest.class), any(HttpContext.class)))
                .thenReturn(response);

        DorisCommitter committer = new DorisCommitter(createSinkConfig(true, true), httpClient);

        DorisConnectorException exception =
                Assertions.assertThrows(
                        DorisConnectorException.class,
                        () ->
                                committer.commit(
                                        Collections.singletonList(
                                                new DorisCommitInfo("fe1:8030", "test_db", 11L))));

        Assertions.assertTrue(exception.getMessage().contains("307 Temporary Redirect"));
        Assertions.assertTrue(
                exception
                        .getMessage()
                        .contains("Location=http://be1:8040/api/test_db/_stream_load_2pc"));
        Assertions.assertTrue(exception.getMessage().contains("direct_to_be=true"));
        Assertions.assertTrue(exception.getMessage().contains("2pc=true"));
        Assertions.assertTrue(exception.getMessage().contains("stage=commit"));
    }

    @Test
    void testCommitAddsRedirectDiagnosticsAfterFollowUpConnectionFailure() throws Exception {
        int unreachablePort = reserveUnusedPort();
        String location =
                String.format("http://127.0.0.1:%s/api/test_db/_stream_load_2pc", unreachablePort);
        HttpServer redirectServer = createRedirectServer(location);
        DorisCommitInfo commitInfo =
                new DorisCommitInfo(
                        "127.0.0.1:" + redirectServer.getAddress().getPort(), "test_db", 11L);
        try (CloseableHttpClient httpClient = new HttpUtil().getHttpClient()) {
            DorisCommitter committer =
                    new DorisCommitter(createRuntimeConfig(commitInfo.getHostPort()), httpClient);

            DorisConnectorException exception =
                    Assertions.assertThrows(
                            DorisConnectorException.class,
                            () -> committer.commit(Collections.singletonList(commitInfo)));

            Assertions.assertTrue(exception.getMessage().contains("redirect follow-up failed"));
            Assertions.assertTrue(exception.getMessage().contains("Location=" + location));
            Assertions.assertTrue(exception.getMessage().contains("stage=commit"));
        } finally {
            redirectServer.stop(0);
        }
    }

    @Test
    void testAbortStopsAfterFirstSuccessfulResponse() throws IOException {
        CloseableHttpClient httpClient = mock(CloseableHttpClient.class);
        CloseableHttpResponse response = successResponse();
        when(httpClient.execute(any(HttpUriRequest.class), any(HttpContext.class)))
                .thenReturn(response);

        DorisCommitter committer = new DorisCommitter(createSinkConfig(true, true), httpClient);
        committer.abort(Collections.singletonList(new DorisCommitInfo("fe1:8030", "test_db", 11L)));

        verify(httpClient, times(1)).execute(any(HttpUriRequest.class), any(HttpContext.class));
    }

    @Test
    void testCommitRetriesNextFrontendInsteadOfUsingRawFenodesString() throws IOException {
        CloseableHttpClient httpClient = mock(CloseableHttpClient.class);
        CloseableHttpResponse successResponse = successResponse();
        when(httpClient.execute(any(HttpUriRequest.class), any(HttpContext.class)))
                .thenThrow(new IOException("first fe failed"))
                .thenReturn(successResponse);

        DorisCommitter committer =
                new DorisCommitter(createMultiFrontendConfig(true, true), httpClient);
        committer.commit(
                Collections.singletonList(new DorisCommitInfo("fe1:8030", "test_db", 12L)));

        ArgumentCaptor<HttpPut> requestCaptor = ArgumentCaptor.forClass(HttpPut.class);
        verify(httpClient, times(2)).execute(requestCaptor.capture(), any(HttpContext.class));

        Assertions.assertEquals(
                "http://fe1:8030/api/test_db/_stream_load_2pc",
                requestCaptor.getAllValues().get(0).getURI().toString());
        Assertions.assertEquals(
                "http://fe2:8030/api/test_db/_stream_load_2pc",
                requestCaptor.getAllValues().get(1).getURI().toString());
    }

    @Test
    void testAbortRetriesNextFrontendOnIOException() throws IOException {
        CloseableHttpClient httpClient = mock(CloseableHttpClient.class);
        CloseableHttpResponse successResponse = successResponse();
        when(httpClient.execute(any(HttpUriRequest.class), any(HttpContext.class)))
                .thenThrow(new IOException("first fe failed"))
                .thenReturn(successResponse);

        DorisCommitter committer =
                new DorisCommitter(createMultiFrontendConfig(true, true), httpClient);
        committer.abort(Collections.singletonList(new DorisCommitInfo("fe1:8030", "test_db", 12L)));

        ArgumentCaptor<HttpPut> requestCaptor = ArgumentCaptor.forClass(HttpPut.class);
        verify(httpClient, times(2)).execute(requestCaptor.capture(), any(HttpContext.class));

        Assertions.assertEquals(
                "http://fe1:8030/api/test_db/_stream_load_2pc",
                requestCaptor.getAllValues().get(0).getURI().toString());
        Assertions.assertEquals(
                "http://fe2:8030/api/test_db/_stream_load_2pc",
                requestCaptor.getAllValues().get(1).getURI().toString());
    }

    @Test
    void testCommitBackoffSkipsFinalSleep() throws IOException {
        CloseableHttpClient httpClient = mock(CloseableHttpClient.class);
        when(httpClient.execute(any(HttpUriRequest.class), any(HttpContext.class)))
                .thenThrow(new IOException("attempt-1"))
                .thenThrow(new IOException("attempt-2"))
                .thenThrow(new IOException("attempt-3"));

        List<Integer> sleepRetries = new ArrayList<>();
        DorisCommitter committer =
                new DorisCommitter(createSinkConfig(true, true, 2), httpClient, sleepRetries::add);

        Assertions.assertThrows(
                IOException.class,
                () ->
                        committer.commit(
                                Collections.singletonList(
                                        new DorisCommitInfo("fe1:8030", "test_db", 21L))));
        Assertions.assertEquals(Arrays.asList(1, 2), sleepRetries);
    }

    @Test
    void testAbortBackoffSkipsFinalSleep() throws IOException {
        CloseableHttpClient httpClient = mock(CloseableHttpClient.class);
        when(httpClient.execute(any(HttpUriRequest.class), any(HttpContext.class)))
                .thenThrow(new IOException("attempt-1"))
                .thenThrow(new IOException("attempt-2"))
                .thenThrow(new IOException("attempt-3"));

        List<Integer> sleepRetries = new ArrayList<>();
        DorisCommitter committer =
                new DorisCommitter(createSinkConfig(true, true, 2), httpClient, sleepRetries::add);

        Assertions.assertThrows(
                IOException.class,
                () ->
                        committer.abort(
                                Collections.singletonList(
                                        new DorisCommitInfo("fe1:8030", "test_db", 22L))));
        Assertions.assertEquals(Arrays.asList(1, 2), sleepRetries);
    }

    @Test
    void testCommitClosesFailedResponseBeforeRetry() throws IOException {
        CloseableHttpClient httpClient = mock(CloseableHttpClient.class);
        CloseableHttpResponse failedResponse =
                responseWithStatus(500, "Internal Server Error", null);
        CloseableHttpResponse successResponse = successResponse();
        when(httpClient.execute(any(HttpUriRequest.class), any(HttpContext.class)))
                .thenReturn(failedResponse)
                .thenReturn(successResponse);

        DorisCommitter committer = new DorisCommitter(createSinkConfig(true, true), httpClient);
        committer.commit(
                Collections.singletonList(new DorisCommitInfo("fe1:8030", "test_db", 23L)));

        verify(failedResponse, times(1)).close();
        verify(successResponse, times(1)).close();
    }

    @Test
    void testAbortClosesFailedResponseBeforeRetry() throws IOException {
        CloseableHttpClient httpClient = mock(CloseableHttpClient.class);
        CloseableHttpResponse failedResponse =
                responseWithStatus(500, "Internal Server Error", null);
        CloseableHttpResponse successResponse = successResponse();
        when(httpClient.execute(any(HttpUriRequest.class), any(HttpContext.class)))
                .thenReturn(failedResponse)
                .thenReturn(successResponse);

        DorisCommitter committer = new DorisCommitter(createSinkConfig(true, true), httpClient);
        committer.abort(Collections.singletonList(new DorisCommitInfo("fe1:8030", "test_db", 24L)));

        verify(failedResponse, times(1)).close();
        verify(successResponse, times(1)).close();
    }

    private DorisSinkConfig createSinkConfig(boolean directToBe, boolean enable2PC) {
        return createSinkConfig(directToBe, enable2PC, 3);
    }

    private DorisSinkConfig createSinkConfig(
            boolean directToBe, boolean enable2PC, int maxRetries) {
        Map<String, Object> options = new HashMap<>();
        options.put("fenodes", "fe1:8030");
        options.put("benodes", "be1:8040");
        options.put("direct_to_be", directToBe);
        options.put("sink.enable-2pc", enable2PC);
        options.put("sink.max-retries", maxRetries);
        options.put("username", "root");
        options.put("password", "");
        options.put("database", "test_db");
        options.put("table", "test_table");
        options.put("sink.label-prefix", "test_job");
        options.put("doris.config", createStreamLoadProperties());
        return DorisSinkConfig.of(ReadonlyConfig.fromMap(options));
    }

    private DorisSinkConfig createMultiFrontendConfig(boolean directToBe, boolean enable2PC) {
        Map<String, Object> options = new HashMap<>();
        options.put("fenodes", "fe1:8030,fe2:8030");
        options.put("benodes", "be1:8040");
        options.put("direct_to_be", directToBe);
        options.put("sink.enable-2pc", enable2PC);
        options.put("sink.max-retries", 3);
        options.put("username", "root");
        options.put("password", "");
        options.put("database", "test_db");
        options.put("table", "test_table");
        options.put("sink.label-prefix", "test_job");
        options.put("doris.config", createStreamLoadProperties());
        return DorisSinkConfig.of(ReadonlyConfig.fromMap(options));
    }

    private DorisSinkConfig createRuntimeConfig(String frontend) {
        Map<String, Object> options = new HashMap<>();
        options.put("fenodes", frontend);
        options.put("benodes", "be1:8040");
        options.put("direct_to_be", true);
        options.put("sink.enable-2pc", true);
        options.put("sink.max-retries", 0);
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

    private CloseableHttpResponse successResponse() throws IOException {
        return responseWithStatus(
                200, "OK", new StringEntity("{\"status\":\"Success\",\"msg\":\"\"}"));
    }

    private CloseableHttpResponse responseWithStatus(
            int statusCode, String reasonPhrase, StringEntity entity) throws IOException {
        CloseableHttpResponse response = mock(CloseableHttpResponse.class);
        when(response.getStatusLine())
                .thenReturn(
                        new BasicStatusLine(
                                new ProtocolVersion("HTTP", 1, 1), statusCode, reasonPhrase));
        when(response.getEntity()).thenReturn(entity);
        return response;
    }

    private HttpServer createRedirectServer(String location) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext(
                "/api/test_db/_stream_load_2pc",
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
