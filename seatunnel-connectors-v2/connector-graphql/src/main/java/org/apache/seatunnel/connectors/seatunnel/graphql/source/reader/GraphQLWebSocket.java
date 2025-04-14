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

package org.apache.seatunnel.connectors.seatunnel.graphql.source.reader;

import org.apache.seatunnel.connectors.seatunnel.graphql.config.GraphQLSourceParameter;
import org.apache.seatunnel.connectors.seatunnel.http.config.HttpParameter;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import com.google.gson.Gson;
import lombok.extern.slf4j.Slf4j;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.WebSocket;
import okhttp3.WebSocketListener;

import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

@Slf4j
public class GraphQLWebSocket {

    private LinkedBlockingQueue<String> buffer;
    private WebSocket webSocket;
    private OkHttpClient client;
    private GraphQLSourceParameter graphQLSourceParameter;
    private HttpParameter httpParameter;

    private int MAX_RETRIES;
    private int RETRY_DELAY_MS;
    private int retryCount = 0;

    private final Gson gson = new Gson();

    public GraphQLWebSocket(
            LinkedBlockingQueue<String> buffer, GraphQLSourceParameter graphQLSourceParameter) {
        this.buffer = buffer;
        this.graphQLSourceParameter = graphQLSourceParameter;
        this.httpParameter = graphQLSourceParameter.getHttpParameter();

        MAX_RETRIES = graphQLSourceParameter.getMaxRetries();
        RETRY_DELAY_MS = graphQLSourceParameter.getRetryDelayMs();
        this.client = new OkHttpClient.Builder().readTimeout(0, TimeUnit.MILLISECONDS).build();
    }

    public void start() {
        connect();
    }

    public void close() {
        webSocket.close(1000, null);
    }

    private void connect() {
        Request.Builder requestBuilder = new Request.Builder().url(httpParameter.getUrl());

        Map<String, String> headers = httpParameter.getHeaders();
        if (headers != null && !headers.isEmpty()) {
            for (Map.Entry<String, String> entry : headers.entrySet()) {
                requestBuilder.addHeader(entry.getKey(), entry.getValue());
            }
        }

        Request request = requestBuilder.build();
        webSocket = client.newWebSocket(request, new GraphQLWebSocketListener());
    }

    private class GraphQLWebSocketListener extends WebSocketListener {
        @Override
        public void onClosed(@NotNull WebSocket webSocket, int code, @NotNull String reason) {}

        @Override
        public void onClosing(@NotNull WebSocket webSocket, int code, @NotNull String reason) {
            webSocket.close(1000, null);
            scheduleReconnect();
        }

        @Override
        public void onFailure(
                @NotNull WebSocket webSocket, @NotNull Throwable t, @Nullable Response response) {
            log.error("WebSocket connection failed: " + t.getMessage());
            t.printStackTrace();
            scheduleReconnect();
        }

        @Override
        public void onMessage(@NotNull WebSocket webSocket, @NotNull String text) {
            try {
                buffer.put(text);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void onOpen(@NotNull WebSocket webSocket, @NotNull Response response) {
            retryCount = 0;

            Map<String, Object> body = httpParameter.getBody();

            String json = gson.toJson(body);
            webSocket.send(json);
        }

        private void scheduleReconnect() {
            if (retryCount < MAX_RETRIES) {
                retryCount++;
                log.info(
                        "Retrying connection in "
                                + RETRY_DELAY_MS
                                + "ms (Attempt "
                                + retryCount
                                + ")");
                new Thread(
                                () -> {
                                    try {
                                        Thread.sleep(RETRY_DELAY_MS);
                                        connect();
                                    } catch (InterruptedException e) {
                                        e.printStackTrace();
                                    }
                                })
                        .start();
            } else {
                log.info("Max retries reached. Giving up.");
            }
        }
    }
}
