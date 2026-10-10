/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.connectors.seatunnel.websocket.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.websocket.config.WebSocketSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.websocket.config.WebSocketSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.websocket.exception.WebSocketConnectorException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.WebSocket;
import okio.ByteString;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * A failure recorded while a connection was dying (e.g. a rejected open-message send) must not kill
 * the task after the automatic reconnect has established a healthy replacement connection, see <a
 * href="https://github.com/apache/seatunnel/issues/12713">#12713</a>.
 */
class WebSocketSourceClientFatalErrorTest {

    private static Response upgradeResponse() {
        return new Response.Builder()
                .request(new Request.Builder().url("https://seatunnel.invalid/").build())
                .protocol(Protocol.HTTP_1_1)
                .code(101)
                .message("Switching Protocols")
                .build();
    }

    /** A socket that behaves like one that is already closing: every send is rejected. */
    private static WebSocket refusingSocket() {
        return new StubWebSocket(false);
    }

    /** A socket that behaves like the healthy replacement connection: every send succeeds. */
    private static WebSocket acceptingSocket() {
        return new StubWebSocket(true);
    }

    @Test
    void shouldClearFatalErrorRecordedByPreviousConnectionWhenReconnectSucceeds() {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(WebSocketSourceOptions.URL.key(), "ws://seatunnel.invalid/");
        configMap.put(WebSocketSourceOptions.OPEN_MESSAGES.key(), Collections.singletonList("x"));
        WebSocketSourceClient client =
                new WebSocketSourceClient(
                        new WebSocketSourceConfig(ReadonlyConfig.fromMap(configMap)));
        WebSocketSourceClient.SourceWebSocketListener listener =
                client.new SourceWebSocketListener();

        // the first connection dies right after the handshake: sending the open messages fails
        // and the failure is recorded so that the reader fails the task on its next poll
        listener.onOpen(refusingSocket(), upgradeResponse());
        Assertions.assertThrows(
                WebSocketConnectorException.class,
                client::rethrowFatalIfNeeded,
                "the failure of the dead connection should be reported while it is current");

        // the automatic reconnect establishes a healthy replacement connection and re-sends the
        // open messages successfully: the error of the dead connection is now stale and must not
        // fail the recovered task
        listener.onOpen(acceptingSocket(), upgradeResponse());
        Assertions.assertDoesNotThrow(
                client::rethrowFatalIfNeeded,
                "a stale error recorded by the replaced connection must not kill the recovered task");
    }

    private static final class StubWebSocket implements WebSocket {

        private final boolean acceptsSends;

        private StubWebSocket(boolean acceptsSends) {
            this.acceptsSends = acceptsSends;
        }

        @Override
        public Request request() {
            return new Request.Builder().url("https://seatunnel.invalid/").build();
        }

        @Override
        public long queueSize() {
            return 0L;
        }

        @Override
        public boolean send(String text) {
            return acceptsSends;
        }

        @Override
        public boolean send(ByteString bytes) {
            return acceptsSends;
        }

        @Override
        public boolean close(int code, String reason) {
            return true;
        }

        @Override
        public void cancel() {}
    }
}
