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

package org.apache.seatunnel.connectors.seatunnel.edgesocket.source;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.source.SourceReader;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.config.EdgeSocketCommonOptions;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.config.EdgeSocketSourceOptions;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.exception.EdgeSocketConnectorErrorCode;
import org.apache.seatunnel.connectors.seatunnel.edgesocket.exception.EdgeSocketConnectorException;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

class EdgeSocketSourceReaderTest {

    @Test
    void shouldAckBatchAfterCheckpointComplete() throws Exception {
        int port = allocateFreePort();
        EdgeSocketSourceReader reader = createReader(port, 5);
        try {
            reader.open();
            try (Socket socket = new Socket("127.0.0.1", port);
                    BufferedWriter writer =
                            new BufferedWriter(
                                    new OutputStreamWriter(
                                            socket.getOutputStream(), StandardCharsets.UTF_8));
                    BufferedReader bufferedReader =
                            new BufferedReader(
                                    new InputStreamReader(
                                            socket.getInputStream(), StandardCharsets.UTF_8))) {
                socket.setSoTimeout(3000);
                writeLine(writer, "__AUTH__:edge-test-token");
                Assertions.assertEquals("ACK", readLine(bufferedReader));

                writeLine(writer, "__BATCH__:1:message-1");
                Assertions.assertEquals("RECEIVED", readLine(bufferedReader));

                writeLine(writer, "__COMMIT__:1");
                Assertions.assertEquals("PENDING", readLine(bufferedReader));

                TestCollector collector = new TestCollector();
                reader.pollNext(collector);
                Assertions.assertEquals(1, collector.rows.size());

                reader.snapshotState(1L);
                reader.notifyCheckpointComplete(1L);

                writeLine(writer, "__COMMIT__:1");
                String ackReply = readLine(bufferedReader);
                Assertions.assertTrue(ackReply.startsWith("ACK:"));
                Assertions.assertTrue(Long.parseLong(ackReply.substring("ACK:".length())) >= 1L);
            }
        } finally {
            reader.close();
        }
    }

    @Test
    void shouldFailWhenRetryBudgetExhausted() throws Exception {
        int port = allocateFreePort();
        try (ServerSocket blocked = new ServerSocket(port)) {
            EdgeSocketSourceReader reader = createReader(port, 0);
            try {
                reader.open();
                Thread.sleep(300);
                EdgeSocketConnectorException exception =
                        Assertions.assertThrows(
                                EdgeSocketConnectorException.class,
                                () -> reader.pollNext(new TestCollector()));
                Assertions.assertEquals(
                        EdgeSocketConnectorErrorCode.SOURCE_REOPEN_EXHAUSTED,
                        exception.getSeaTunnelErrorCode());
            } finally {
                reader.close();
            }
        }
    }

    private EdgeSocketSourceReader createReader(int port, int maxRetries) {
        Map<String, Object> configMap = new HashMap<>();
        configMap.put(EdgeSocketCommonOptions.PORT.key(), port);
        configMap.put(EdgeSocketSourceOptions.AUTH_TOKEN.key(), "edge-test-token");
        configMap.put(EdgeSocketSourceOptions.MAX_RETRIES.key(), maxRetries);
        configMap.put(EdgeSocketSourceOptions.RECONNECT_INTERVAL_MS.key(), 50);
        configMap.put(EdgeSocketSourceOptions.ACCEPT_TIMEOUT_MS.key(), 100);
        configMap.put(EdgeSocketSourceOptions.LOCAL_QUEUE_CAPACITY.key(), 8);
        ReadonlyConfig config = ReadonlyConfig.fromMap(configMap);
        SourceReader.Context context = Mockito.mock(SourceReader.Context.class);
        Mockito.when(context.getBoundedness()).thenReturn(Boundedness.UNBOUNDED);
        SingleSplitReaderContext readerContext = new SingleSplitReaderContext(context);
        SeaTunnelRowType rowType =
                new SeaTunnelRowType(
                        new String[] {"value"}, new SeaTunnelDataType<?>[] {BasicType.STRING_TYPE});
        return new EdgeSocketSourceReader(
                new org.apache.seatunnel.connectors.seatunnel.edgesocket.config.EdgeSocketConfig(
                        config),
                readerContext,
                new EdgeSocketTextDeserializationSchema(rowType));
    }

    private static int allocateFreePort() throws IOException {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }

    private static void writeLine(BufferedWriter writer, String value) throws IOException {
        writer.write(value);
        writer.newLine();
        writer.flush();
    }

    private static String readLine(BufferedReader reader) throws IOException {
        String line = reader.readLine();
        if (line == null) {
            throw new IOException("Read EOF");
        }
        return line.trim();
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
