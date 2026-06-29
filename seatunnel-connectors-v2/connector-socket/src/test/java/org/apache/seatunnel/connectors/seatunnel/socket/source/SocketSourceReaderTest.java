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

import org.apache.seatunnel.api.source.Boundedness;
import org.apache.seatunnel.api.source.Collector;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.connectors.seatunnel.common.source.SingleSplitReaderContext;
import org.apache.seatunnel.connectors.seatunnel.socket.config.SocketConfig;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

import java.io.OutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

class SocketSourceReaderTest {

    /**
     * Regression test for #10528: the socket/reader must be owned by open()/close() and survive
     * across multiple pollNext() calls. With the old per-pollNext try-with-resources, the
     * underlying socket was closed after the first pollNext(), so the second call failed with
     * "Socket is closed". This test opens the reader once and calls pollNext() twice, asserting the
     * second call still reads data.
     *
     * <p>BOUNDED boundedness is used so that each pollNext() returns after a single read() chunk
     * (under UNBOUNDED the read loop keeps going until EOF inside one pollNext() call, which would
     * prevent observing the per-call reuse). A CountDownLatch keeps the two server writes separate
     * so the first read() sees only the first line.
     */
    @Test
    @Timeout(30)
    void pollNextReusesReaderAcrossCalls() throws Exception {
        CountDownLatch firstPollConsumed = new CountDownLatch(1);
        AtomicReference<Exception> serverError = new AtomicReference<>();

        try (ServerSocket serverSocket = new ServerSocket(0)) {
            serverSocket.setSoTimeout(15_000);
            int port = serverSocket.getLocalPort();

            Thread serverThread =
                    new Thread(
                            () -> {
                                try (Socket client = serverSocket.accept();
                                        OutputStream out = client.getOutputStream()) {
                                    out.write("line1\n".getBytes(StandardCharsets.UTF_8));
                                    out.flush();
                                    // Wait until the reader has finished its first pollNext() so
                                    // the
                                    // second line is not available during the first read().
                                    if (!firstPollConsumed.await(15, TimeUnit.SECONDS)) {
                                        return;
                                    }
                                    out.write("line2\n".getBytes(StandardCharsets.UTF_8));
                                    out.flush();
                                    // Keep the connection open briefly so close() (not the server)
                                    // owns teardown.
                                    Thread.sleep(500);
                                } catch (Exception e) {
                                    serverError.set(e);
                                }
                            });
            serverThread.setDaemon(true);
            serverThread.start();

            SocketConfig config = mock(SocketConfig.class);
            when(config.getHost()).thenReturn("127.0.0.1");
            when(config.getPort()).thenReturn(port);

            SingleSplitReaderContext context = mock(SingleSplitReaderContext.class);
            when(context.getBoundedness()).thenReturn(Boundedness.BOUNDED);

            SocketSourceReader reader = new SocketSourceReader(config, context);
            reader.open();
            try {
                CollectingCollector collector = new CollectingCollector();

                // First read: consumes "line1".
                reader.pollNext(collector);
                Assertions.assertEquals(
                        1, collector.rows.size(), "first pollNext() should read line1");
                Assertions.assertEquals("line1", collector.rows.get(0).getField(0));

                // Let the server send the second line now that the first read is done.
                firstPollConsumed.countDown();

                // Second read: with the buggy code this threw "Socket is closed"; the fix keeps the
                // reader/socket open so it still reads "line2".
                reader.pollNext(collector);
                Assertions.assertEquals(
                        2,
                        collector.rows.size(),
                        "second pollNext() must still read data (reader/socket reused)");
                Assertions.assertEquals("line2", collector.rows.get(1).getField(0));
            } finally {
                reader.close();
            }

            Assertions.assertNull(
                    serverError.get(), () -> "server thread failed: " + serverError.get());
        }
    }

    private static class CollectingCollector implements Collector<SeaTunnelRow> {
        private final List<SeaTunnelRow> rows = new ArrayList<>();
        private final Object lock = new Object();

        @Override
        public void collect(SeaTunnelRow record) {
            rows.add(record);
        }

        @Override
        public Object getCheckpointLock() {
            return lock;
        }
    }
}
