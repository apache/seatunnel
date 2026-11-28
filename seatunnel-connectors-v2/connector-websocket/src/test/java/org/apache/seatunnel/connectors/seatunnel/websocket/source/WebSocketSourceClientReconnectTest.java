package org.apache.seatunnel.connectors.seatunnel.websocket.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.connectors.seatunnel.websocket.config.WebSocketSourceConfig;
import org.apache.seatunnel.connectors.seatunnel.websocket.config.WebSocketSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.websocket.exception.WebSocketConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.websocket.exception.WebSocketConnectorException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import okhttp3.WebSocket;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ScheduledExecutorService;

class WebSocketSourceClientReconnectTest {

    private static WebSocketSourceClient clientWith(int maxReconnectTimes) throws Exception {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(WebSocketSourceOptions.URL.key(), "ws://localhost:1/");
        configMap.put(
                WebSocketSourceOptions.OPEN_MESSAGES.key(), Collections.singletonList("subscribe"));
        configMap.put(WebSocketSourceOptions.ENABLE_RECONNECT.key(), true);
        configMap.put(WebSocketSourceOptions.MAX_RECONNECT_TIMES.key(), maxReconnectTimes);
        WebSocketSourceConfig config = new WebSocketSourceConfig(ReadonlyConfig.fromMap(configMap));
        WebSocketSourceClient client = new WebSocketSourceClient(config);
        Field scheduler = WebSocketSourceClient.class.getDeclaredField("reconnectScheduler");
        scheduler.setAccessible(true);
        scheduler.set(client, Mockito.mock(ScheduledExecutorService.class));
        return client;
    }

    private static WebSocket rejectingSocket() {
        WebSocket webSocket = Mockito.mock(WebSocket.class);
        Mockito.when(webSocket.send(Mockito.anyString())).thenReturn(false);
        return webSocket;
    }

    @Test
    void sendFailuresStopAfterExactlyMaxReconnectTimesAndFailTheSource() throws Exception {
        int maxReconnectTimes = 2;
        WebSocketSourceClient client = clientWith(maxReconnectTimes);
        WebSocketSourceClient.SourceWebSocketListener listener =
                client.new SourceWebSocketListener();
        WebSocket webSocket = rejectingSocket();

        // every attempt completes the handshake but the open-message send is rejected. While the
        // reconnect budget is not yet exhausted the source must keep retrying, not fail.
        for (int attempt = 1; attempt <= maxReconnectTimes; attempt++) {
            listener.onOpen(webSocket, null);
            listener.onFailure(
                    webSocket, new IOException("open-message rejected, attempt " + attempt), null);
            Assertions.assertDoesNotThrow(
                    client::rethrowFatalIfNeeded,
                    "source must keep retrying until max_reconnect_times is reached");
        }

        // the attempt after the budget is exhausted must fail the source with a terminal error
        listener.onOpen(webSocket, null);
        listener.onFailure(
                webSocket, new IOException("open-message rejected, final attempt"), null);
        WebSocketConnectorException terminal =
                Assertions.assertThrows(
                        WebSocketConnectorException.class, client::rethrowFatalIfNeeded);
        Assertions.assertEquals(
                WebSocketConnectorErrorCode.SEND_MESSAGE_FAILED, terminal.getSeaTunnelErrorCode());
    }

    @Test
    void finalAttemptFailingBeforeOpenReportsItsOwnError() throws Exception {
        WebSocketSourceClient client = clientWith(1);
        WebSocketSourceClient.SourceWebSocketListener listener =
                client.new SourceWebSocketListener();
        WebSocket webSocket = rejectingSocket();

        listener.onOpen(webSocket, null);
        listener.onFailure(webSocket, new IOException("send-triggered drop"), null);

        IOException finalCause = new IOException("handshake failed on final attempt");
        listener.onFailure(webSocket, finalCause, null);

        WebSocketConnectorException terminal =
                Assertions.assertThrows(
                        WebSocketConnectorException.class, client::rethrowFatalIfNeeded);
        Assertions.assertEquals(
                WebSocketConnectorErrorCode.CONNECT_FAILED, terminal.getSeaTunnelErrorCode());
        Assertions.assertSame(finalCause, terminal.getCause());
    }
}
