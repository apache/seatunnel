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

package org.apache.seatunnel.connectors.seatunnel.socket.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.configuration.util.ConfigValidator;
import org.apache.seatunnel.api.configuration.util.OptionRule;
import org.apache.seatunnel.api.configuration.util.OptionValidationException;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.socket.config.SocketCommonOptions;
import org.apache.seatunnel.connectors.seatunnel.socket.config.SocketConfig;
import org.apache.seatunnel.connectors.seatunnel.socket.config.SocketSourceOptions;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.MockedConstruction;
import org.mockito.Mockito;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SocketSourceReaderTest {

    @Test
    void shouldKeepSocketOpenAcrossPollNextCalls() throws Exception {
        try (ServerSocket serverSocket = new ServerSocket(0)) {
            SocketSourceReader reader = createReader(serverSocket.getLocalPort());
            reader.open();
            try (Socket socket = serverSocket.accept();
                    BufferedWriter writer =
                            new BufferedWriter(
                                    new OutputStreamWriter(
                                            socket.getOutputStream(), StandardCharsets.UTF_8))) {
                TestCollector collector = new TestCollector();

                writeLine(writer, "first");
                reader.pollNext(collector);
                writeLine(writer, "second");
                reader.pollNext(collector);

                Assertions.assertEquals("first", collector.rows.get(0).getField(0));
                Assertions.assertEquals("second", collector.rows.get(1).getField(0));
            } finally {
                reader.close();
            }
        }
    }

    private static SocketSourceReader createReader(int port) {
        return createReader(port, new HashMap<>());
    }

    private static SocketSourceReader createReader(int port, Map<String, Object> extraOptions) {
        Map<String, Object> options = new HashMap<>();
        options.put(SocketCommonOptions.HOST.key(), "127.0.0.1");
        options.put(SocketCommonOptions.PORT.key(), port);
        options.putAll(extraOptions);
        SourceReader.Context context = mock(SourceReader.Context.class);
        when(context.getBoundedness()).thenReturn(Boundedness.BOUNDED);
        return new SocketSourceReader(
                new SocketConfig(ReadonlyConfig.fromMap(options)),
                new SingleSplitReaderContext(context));
    }

    @Test
    void shouldApplyConfiguredConnectAndReadTimeouts() {
        Map<String, Object> defaults = new HashMap<>();
        defaults.put(SocketCommonOptions.HOST.key(), "127.0.0.1");
        defaults.put(SocketCommonOptions.PORT.key(), 1);
        SocketConfig defaultConfig = new SocketConfig(ReadonlyConfig.fromMap(defaults));
        // connect must stay bounded by default; reads stay unbounded to preserve streaming
        Assertions.assertEquals(
                SocketCommonOptions.DEFAULT_CONNECT_TIMEOUT_MS,
                defaultConfig.getConnectTimeoutMs());
        Assertions.assertEquals(
                SocketSourceOptions.DEFAULT_READ_TIMEOUT_MS, defaultConfig.getReadTimeoutMs());

        Map<String, Object> overrides = new HashMap<>(defaults);
        overrides.put(SocketCommonOptions.CONNECT_TIMEOUT_MS.key(), 1234);
        overrides.put(SocketSourceOptions.READ_TIMEOUT_MS.key(), 4321);
        SocketConfig overridden = new SocketConfig(ReadonlyConfig.fromMap(overrides));
        Assertions.assertEquals(1234, overridden.getConnectTimeoutMs());
        Assertions.assertEquals(4321, overridden.getReadTimeoutMs());
    }

    /**
     * Deterministic replacement for a "wait for an unreachable host" test.
     *
     * <p>Wall-clock assertions on real network addresses are not reliable here: on some platforms a
     * saturated accept backlog or a filtered route fails immediately with a reset, so the test
     * would pass even without the fix. Asserting the arguments handed to Socket.connect is exact
     * and fails as soon as the timeout stops being propagated.
     */
    @Test
    void shouldPassConfiguredTimeoutToConnect() throws Exception {
        Map<String, Object> options = new HashMap<>();
        options.put(SocketCommonOptions.HOST.key(), "127.0.0.1");
        options.put(SocketCommonOptions.PORT.key(), 12345);
        options.put(SocketCommonOptions.CONNECT_TIMEOUT_MS.key(), 4321);
        SourceReader.Context context = mock(SourceReader.Context.class);
        when(context.getBoundedness()).thenReturn(Boundedness.BOUNDED);

        try (MockedConstruction<Socket> mocked =
                Mockito.mockConstruction(
                        Socket.class,
                        (socket, ctx) ->
                                Mockito.when(socket.getInputStream())
                                        .thenReturn(new ByteArrayInputStream(new byte[0])))) {
            SocketSourceReader reader =
                    new SocketSourceReader(
                            new SocketConfig(ReadonlyConfig.fromMap(options)),
                            new SingleSplitReaderContext(context));
            reader.open();

            Socket created = mocked.constructed().get(0);
            Mockito.verify(created).connect(Mockito.any(InetSocketAddress.class), Mockito.eq(4321));
            Mockito.verify(created).setSoTimeout(0);
        }
    }

    @Test
    void shouldCloseSocketWhenConnectFails() throws Exception {
        Map<String, Object> options = new HashMap<>();
        options.put(SocketCommonOptions.HOST.key(), "127.0.0.1");
        options.put(SocketCommonOptions.PORT.key(), 12345);
        SourceReader.Context context = mock(SourceReader.Context.class);
        when(context.getBoundedness()).thenReturn(Boundedness.BOUNDED);

        try (MockedConstruction<Socket> mocked =
                Mockito.mockConstruction(
                        Socket.class,
                        (socket, ctx) ->
                                Mockito.doThrow(new ConnectException("refused"))
                                        .when(socket)
                                        .connect(
                                                Mockito.any(InetSocketAddress.class),
                                                Mockito.anyInt()))) {
            SocketSourceReader reader =
                    new SocketSourceReader(
                            new SocketConfig(ReadonlyConfig.fromMap(options)),
                            new SingleSplitReaderContext(context));
            Assertions.assertThrows(ConnectException.class, reader::open);
            Mockito.verify(mocked.constructed().get(0)).close();
        }
    }

    @Test
    void shouldRejectNegativeConnectTimeout() {
        Map<String, Object> options = new HashMap<>();
        options.put(SocketCommonOptions.HOST.key(), "localhost");
        options.put(SocketCommonOptions.PORT.key(), 1);
        options.put(SocketCommonOptions.CONNECT_TIMEOUT_MS.key(), -1);
        SocketConfig config = new SocketConfig(ReadonlyConfig.fromMap(options));
        Assertions.assertEquals(-1, config.getConnectTimeoutMs());

        OptionRule rule = new SocketSourceFactory().optionRule();
        Assertions.assertThrows(
                OptionValidationException.class,
                () -> ConfigValidator.of(ReadonlyConfig.fromMap(options)).validate(rule));
    }

    private static void writeLine(BufferedWriter writer, String value) throws IOException {
        writer.write(value);
        writer.newLine();
        writer.flush();
    }

    private static class TestCollector implements Collector<SeaTunnelRow> {
        private final Object lock = new Object();
        private final List<SeaTunnelRow> rows = new ArrayList<>();

        @Override
        public void collect(SeaTunnelRow row) {
            rows.add(row);
        }

        @Override
        public Object getCheckpointLock() {
            return lock;
        }
    }
}
