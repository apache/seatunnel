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
import org.apache.seatunnel.connectors.seatunnel.websocket.exception.WebSocketConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.websocket.exception.WebSocketConnectorException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import okhttp3.Protocol;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.WebSocket;
import okio.ByteString;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * A connection that rejects the open-message send while it is dying must not fail the task while a
 * reconnect can still replace it, see <a
 * href="https://github.com/apache/seatunnel/issues/12713">#12713</a>. The reader rethrows a
 * terminal error within {@code poll_timeout_ms} (1000ms by default), while the reconnect only fires
 * after {@code reconnect_interval_ms} (3000ms by default), so recording the send failure as fatal
 * would kill the task before the recovery the PR promises. Only an exhausted reconnect budget (or
 * reconnect disabled) may turn it into the task's terminal error.
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

    /**
     * The scenario of #12713: the send of the open message is rejected because the connection is
     * dying. The rejected send must not become a terminal error on its own, otherwise the reader
     * fails the task on its next poll, about two seconds before the scheduled reconnect runs.
     */
    @Test
    void shouldNotFailTheTaskWhenAnOpenMessageSendIsRejected() {
        WebSocketSourceClient client = new WebSocketSourceClient(config(true));
        WebSocketSourceClient.SourceWebSocketListener listener =
                client.new SourceWebSocketListener();

        listener.onOpen(refusingSocket(), upgradeResponse());

        Assertions.assertDoesNotThrow(
                client::rethrowFatalIfNeeded,
                "a rejected open-message send must not fail the task while a reconnect can still"
                        + " replace the dying connection");
    }

    /**
     * Cancellation of the dying socket leads to the regular onFailure -> onDisconnected path. Once
     * no reconnect is allowed to follow, the task must fail with an error that names the rejected
     * send instead of only the cancellation the client triggered itself.
     */
    @Test
    void shouldFailNamingTheSendFailureWhenNoReconnectMayFollow() {
        WebSocketSourceClient client = new WebSocketSourceClient(config(false));
        WebSocketSourceClient.SourceWebSocketListener listener =
                client.new SourceWebSocketListener();

        WebSocket dyingSocket = refusingSocket();
        listener.onOpen(dyingSocket, upgradeResponse());
        listener.onFailure(dyingSocket, new IOException("canceled"), null);

        WebSocketConnectorException exception =
                Assertions.assertThrows(
                        WebSocketConnectorException.class, client::rethrowFatalIfNeeded);
        Assertions.assertEquals(
                WebSocketConnectorErrorCode.SEND_MESSAGE_FAILED, exception.getSeaTunnelErrorCode());
        Assertions.assertTrue(
                exception.getMessage().contains("Failed to send open message"),
                "the terminal error must name the rejected send: " + exception.getMessage());
        Assertions.assertNotNull(exception.getCause(), "the transport error must be kept as cause");
    }

    /**
     * A re-established connection invalidates any failure recorded for the dead one: after a
     * successful reconnect the recovered task must not be killed by a stale error.
     */
    @Test
    void shouldNotFailTheRecoveredTaskAfterASuccessfulReconnect() {
        WebSocketSourceClient client = new WebSocketSourceClient(config(false));
        WebSocketSourceClient.SourceWebSocketListener listener =
                client.new SourceWebSocketListener();

        listener.onOpen(refusingSocket(), upgradeResponse());
        // the dying socket reports the failure the cancellation provoked
        listener.onFailure(refusingSocket(), new IOException("canceled"), null);
        // the automatic reconnect establishes a healthy replacement connection
        listener.onOpen(acceptingSocket(), upgradeResponse());

        Assertions.assertDoesNotThrow(
                client::rethrowFatalIfNeeded,
                "a stale error recorded by the replaced connection must not kill the recovered"
                        + " task");
    }

    /**
     * Repeated open-message send rejections must count against {@code max_reconnect_times} instead
     * of resetting the reconnect counter on every handshake and retrying forever, see <a
     * href="https://github.com/apache/seatunnel/issues/13452">#13452</a>.
     */
    @Test
    void shouldExhaustReconnectBudgetWhenOpenMessageSendKeepsFailing() {
        AtomicInteger scheduledReconnects = new AtomicInteger();
        WebSocketSourceClient client = clientWithReconnectCounter(2, scheduledReconnects);

        // initial attempt + 2 retries = 3 total attempts; only the first two may schedule a retry
        for (int attempt = 0; attempt < 2; attempt++) {
            WebSocketSourceClient.SourceWebSocketListener listener =
                    client.new SourceWebSocketListener();
            WebSocket dyingSocket = refusingSocket();
            listener.onOpen(dyingSocket, upgradeResponse());
            listener.onFailure(dyingSocket, new IOException("canceled " + attempt), null);

            Assertions.assertEquals(attempt + 1, scheduledReconnects.get());
            Assertions.assertDoesNotThrow(
                    client::rethrowFatalIfNeeded,
                    "a rejected open-message send must still allow retries while budget remains");
        }

        IOException finalCause = new IOException("canceled final");
        WebSocketSourceClient.SourceWebSocketListener finalListener =
                client.new SourceWebSocketListener();
        WebSocket finalSocket = refusingSocket();
        finalListener.onOpen(finalSocket, upgradeResponse());
        finalListener.onFailure(finalSocket, finalCause, null);

        Assertions.assertEquals(
                2,
                scheduledReconnects.get(),
                "no further reconnect may be scheduled once max_reconnect_times is exhausted");
        WebSocketConnectorException exception =
                Assertions.assertThrows(
                        WebSocketConnectorException.class, client::rethrowFatalIfNeeded);
        Assertions.assertEquals(
                WebSocketConnectorErrorCode.SEND_MESSAGE_FAILED, exception.getSeaTunnelErrorCode());
        Assertions.assertTrue(
                exception.getMessage().contains("Failed to send open message"),
                "the terminal error must name the rejected send: " + exception.getMessage());
        Assertions.assertSame(finalCause, exception.getCause());
    }

    /**
     * When an earlier attempt rejected an open message and the final attempt fails before {@code
     * onOpen()}, the terminal error must report the final connection failure rather than the stale
     * send failure from the earlier attempt, see <a
     * href="https://github.com/apache/seatunnel/issues/13452">#13452</a>.
     */
    @Test
    void shouldReportConnectionFailureWhenFinalAttemptFailsBeforeOnOpen() {
        AtomicInteger scheduledReconnects = new AtomicInteger();
        WebSocketSourceClient client = clientWithReconnectCounter(2, scheduledReconnects);

        // attempt 0: handshake succeeds, open-message send fails -> schedules retry 1/2
        WebSocketSourceClient.SourceWebSocketListener firstListener =
                client.new SourceWebSocketListener();
        WebSocket dyingSocket = refusingSocket();
        firstListener.onOpen(dyingSocket, upgradeResponse());
        firstListener.onFailure(dyingSocket, new IOException("canceled"), null);
        Assertions.assertEquals(1, scheduledReconnects.get());
        Assertions.assertDoesNotThrow(client::rethrowFatalIfNeeded);

        // attempt 1: fails before handshake completes -> schedules retry 2/2
        WebSocketSourceClient.SourceWebSocketListener secondListener =
                client.new SourceWebSocketListener();
        secondListener.onFailure(refusingSocket(), new IOException("connection reset"), null);
        Assertions.assertEquals(2, scheduledReconnects.get());
        Assertions.assertDoesNotThrow(client::rethrowFatalIfNeeded);

        // attempt 2 (final): fails before handshake completes -> budget exhausted
        IOException finalHandshakeError = new IOException("connection refused");
        WebSocketSourceClient.SourceWebSocketListener finalListener =
                client.new SourceWebSocketListener();
        finalListener.onFailure(refusingSocket(), finalHandshakeError, null);

        Assertions.assertEquals(2, scheduledReconnects.get());
        WebSocketConnectorException exception =
                Assertions.assertThrows(
                        WebSocketConnectorException.class, client::rethrowFatalIfNeeded);
        Assertions.assertEquals(
                WebSocketConnectorErrorCode.CONNECT_FAILED, exception.getSeaTunnelErrorCode());
        Assertions.assertFalse(
                exception.getMessage().contains("Failed to send open message"),
                "the terminal error must not attribute an earlier open-message failure to a later"
                        + " pre-handshake failure: "
                        + exception.getMessage());
        Assertions.assertTrue(
                exception.getMessage().contains("after [2] reconnect attempts"),
                "the terminal error must report the exhausted reconnect attempts: "
                        + exception.getMessage());
        Assertions.assertSame(finalHandshakeError, exception.getCause());
    }

    /**
     * Once a reconnect succeeds in sending all open messages, the reconnect budget must be reset so
     * a later drop of the healthy connection gets a fresh budget.
     */
    @Test
    void shouldResetReconnectBudgetAfterOpenMessagesSucceed() {
        AtomicInteger scheduledReconnects = new AtomicInteger();
        WebSocketSourceClient client = clientWithReconnectCounter(1, scheduledReconnects);

        // attempt 0 fails open-message send -> uses the single retry slot (1/1)
        WebSocketSourceClient.SourceWebSocketListener firstListener =
                client.new SourceWebSocketListener();
        WebSocket dyingSocket = refusingSocket();
        firstListener.onOpen(dyingSocket, upgradeResponse());
        firstListener.onFailure(dyingSocket, new IOException("canceled"), null);
        Assertions.assertEquals(1, scheduledReconnects.get());

        // attempt 1 succeeds and sends all open messages -> resets budget back to 0
        WebSocketSourceClient.SourceWebSocketListener healthyListener =
                client.new SourceWebSocketListener();
        WebSocket healthySocket = acceptingSocket();
        healthyListener.onOpen(healthySocket, upgradeResponse());

        // a later drop of the healthy connection must be allowed to reconnect again
        healthyListener.onClosed(healthySocket, 1006, "server restarted");
        Assertions.assertEquals(2, scheduledReconnects.get());
        Assertions.assertDoesNotThrow(client::rethrowFatalIfNeeded);
    }

    private static WebSocketSourceClient clientWithReconnectCounter(
            int maxReconnectTimes, AtomicInteger scheduledReconnects) {
        return new WebSocketSourceClient(config(true, maxReconnectTimes)) {
            @Override
            void scheduleReconnect() {
                scheduledReconnects.incrementAndGet();
            }
        };
    }

    private static WebSocketSourceConfig config(boolean enableReconnect) {
        return config(enableReconnect, WebSocketSourceOptions.MAX_RECONNECT_TIMES.defaultValue());
    }

    private static WebSocketSourceConfig config(boolean enableReconnect, int maxReconnectTimes) {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(WebSocketSourceOptions.URL.key(), "ws://seatunnel.invalid/");
        configMap.put(WebSocketSourceOptions.OPEN_MESSAGES.key(), Collections.singletonList("x"));
        configMap.put(WebSocketSourceOptions.ENABLE_RECONNECT.key(), enableReconnect);
        configMap.put(WebSocketSourceOptions.MAX_RECONNECT_TIMES.key(), maxReconnectTimes);
        return new WebSocketSourceConfig(ReadonlyConfig.fromMap(configMap));
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
