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

package org.apache.seatunnel.connectors.doris.util;

import org.apache.seatunnel.connectors.doris.sink.HttpPutBuilder;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.InputStreamEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import com.sun.net.httpserver.HttpServer;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicReference;

public class HttpUtilTest {

    private static final String LOAD_PATH = "/api/test_db/test_table/_stream_load";

    /**
     * A busy FE may return the 307 redirect later than the default 3s waitForContinue timeout of
     * HttpRequestExecutor. The stream load client must keep waiting for the redirect instead of
     * sending the non-repeatable request body to FE: once the body is consumed, RedirectExec
     * refuses to follow the redirect and returns the raw 307 response to the caller.
     */
    @Test
    @Timeout(30)
    void testStreamLoadClientWaitsForSlowFeRedirect() throws Exception {
        String payload = "{\"k1\":1}";
        AtomicReference<String> bodyReceivedByBe = new AtomicReference<>();
        HttpServer beServer = createBeServer(bodyReceivedByBe);
        String location =
                String.format("http://127.0.0.1:%s%s", beServer.getAddress().getPort(), LOAD_PATH);
        try (SlowRedirectFeServer feServer = new SlowRedirectFeServer(4000, location)) {
            HttpPut put =
                    new HttpPutBuilder()
                            .setUrl(
                                    String.format(
                                            "http://127.0.0.1:%s%s", feServer.getPort(), LOAD_PATH))
                            .addCommonHeader()
                            .setEntity(
                                    new InputStreamEntity(
                                            new ByteArrayInputStream(
                                                    payload.getBytes(StandardCharsets.UTF_8))))
                            .build();
            try (CloseableHttpClient httpClient = new HttpUtil().getHttpClient();
                    CloseableHttpResponse response = httpClient.execute(put)) {
                Assertions.assertEquals(200, response.getStatusLine().getStatusCode());
                EntityUtils.consume(response.getEntity());
            }
            Assertions.assertEquals(payload, bodyReceivedByBe.get());
        } finally {
            beServer.stop(0);
        }
    }

    private HttpServer createBeServer(AtomicReference<String> bodyHolder) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext(
                LOAD_PATH,
                exchange -> {
                    bodyHolder.set(
                            new String(readAll(exchange.getRequestBody()), StandardCharsets.UTF_8));
                    byte[] result = "{\"Status\":\"Success\"}".getBytes(StandardCharsets.UTF_8);
                    exchange.sendResponseHeaders(200, result.length);
                    try (OutputStream outputStream = exchange.getResponseBody()) {
                        outputStream.write(result);
                    }
                });
        server.setExecutor(Executors.newCachedThreadPool());
        server.start();
        return server;
    }

    private static byte[] readAll(InputStream inputStream) throws IOException {
        ByteArrayOutputStream buffer = new ByteArrayOutputStream();
        byte[] chunk = new byte[1024];
        int read;
        while ((read = inputStream.read(chunk)) != -1) {
            buffer.write(chunk, 0, read);
        }
        return buffer.toByteArray();
    }

    /**
     * Raw-socket FE stub which reads the request headers and then delays the 307 redirect without
     * sending "100 Continue". A com.sun HttpServer cannot simulate this: it automatically replies
     * "100 Continue" before the handler runs, so the client would never wait.
     */
    private static final class SlowRedirectFeServer implements AutoCloseable {
        private final ServerSocket serverSocket;

        SlowRedirectFeServer(long redirectDelayMs, String location) throws IOException {
            this.serverSocket = new ServerSocket(0, 1, InetAddress.getByName("127.0.0.1"));
            Thread thread =
                    new Thread(
                            () -> {
                                try (Socket socket = serverSocket.accept()) {
                                    readRequestHeaders(socket.getInputStream());
                                    Thread.sleep(redirectDelayMs);
                                    String response =
                                            "HTTP/1.1 307 Temporary Redirect\r\n"
                                                    + "Location: "
                                                    + location
                                                    + "\r\n"
                                                    + "Content-Length: 0\r\n"
                                                    + "Connection: close\r\n"
                                                    + "\r\n";
                                    OutputStream outputStream = socket.getOutputStream();
                                    outputStream.write(response.getBytes(StandardCharsets.UTF_8));
                                    outputStream.flush();
                                } catch (Exception ignored) {
                                    // client-side assertions fail the test
                                }
                            },
                            "slow-redirect-fe");
            thread.setDaemon(true);
            thread.start();
        }

        int getPort() {
            return serverSocket.getLocalPort();
        }

        private static void readRequestHeaders(InputStream inputStream) throws IOException {
            int state = 0;
            int b;
            while (state < 4 && (b = inputStream.read()) != -1) {
                if (b == '\r' && (state == 0 || state == 2)) {
                    state++;
                } else if (b == '\n' && (state == 1 || state == 3)) {
                    state++;
                } else {
                    state = 0;
                }
            }
        }

        @Override
        public void close() throws IOException {
            serverSocket.close();
        }
    }
}
