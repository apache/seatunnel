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

package org.apache.seatunnel.connectors.seatunnel.socket.sink;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.sink.SupportMultiTableSink;
import org.apache.seatunnel.api.sink.SupportMultiTableSinkWriter;
import org.apache.seatunnel.api.table.type.BasicType;
import org.apache.seatunnel.api.table.type.SeaTunnelDataType;
import org.apache.seatunnel.api.table.type.SeaTunnelRowType;
import org.apache.seatunnel.connectors.seatunnel.socket.config.SocketCommonOptions;
import org.apache.seatunnel.connectors.seatunnel.socket.config.SocketConfig;
import org.apache.seatunnel.connectors.seatunnel.socket.config.SocketSinkOptions;

import org.junit.jupiter.api.Test;

import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Contract tests verifying SocketSink and SocketSinkWriter correctly implement multi-table sink
 * interfaces. Prevents ClassCastException when the framework casts writers to
 * SupportMultiTableSinkWriter in multi-table scenarios.
 */
public class SocketSinkWriterContractTest {

    /** Verifies SupportMultiTableSink at class level - no network needed. */
    @Test
    public void testSocketSinkImplementsMultiTableSinkInterface() {
        assertTrue(
                SupportMultiTableSink.class.isAssignableFrom(SocketSink.class),
                "SocketSink must implement SupportMultiTableSink");
    }

    /** Verifies SupportMultiTableSinkWriter at class level - no network needed. */
    @Test
    public void testSocketSinkWriterImplementsMultiTableSinkWriterInterface() {
        assertTrue(
                SupportMultiTableSinkWriter.class.isAssignableFrom(SocketSinkWriter.class),
                "SocketSinkWriter must implement SupportMultiTableSinkWriter");
    }

    /**
     * Verifies the framework cast succeeds at runtime by spinning up a temporary ServerSocket so
     * the writer can connect and be instantiated without errors. This replicates exactly what
     * MultiTableSinkWriter.initResourceManager() does.
     */
    @Test
    public void testFrameworkCastSucceedsAtRuntime() throws Exception {
        CountDownLatch serverReady = new CountDownLatch(1);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        SocketSinkWriter writer = null;

        try (ServerSocket serverSocket = new ServerSocket(0)) {
            int port = serverSocket.getLocalPort();

            executor.submit(
                    () -> {
                        serverReady.countDown();
                        try (Socket client = serverSocket.accept()) {
                            Thread.sleep(2000);
                        } catch (Exception ignored) {
                        }
                    });

            serverReady.await();

            Map<String, Object> configMap = new HashMap<>();
            configMap.put(SocketCommonOptions.HOST.key(), "localhost");
            configMap.put(SocketCommonOptions.PORT.key(), port);
            configMap.put(SocketSinkOptions.MAX_RETRIES.key(), 0);

            ReadonlyConfig readonlyConfig = ReadonlyConfig.fromMap(configMap);
            SocketConfig config = new SocketConfig(readonlyConfig);

            SeaTunnelRowType rowType =
                    new SeaTunnelRowType(
                            new String[] {"id", "name"},
                            new SeaTunnelDataType[] {BasicType.INT_TYPE, BasicType.STRING_TYPE});

            writer = new SocketSinkWriter(config, rowType);

            // Replicates the exact cast in MultiTableSinkWriter.initResourceManager()
            SupportMultiTableSinkWriter<?> cast = (SupportMultiTableSinkWriter<?>) writer;
            assertNotNull(cast, "Cast to SupportMultiTableSinkWriter must not be null");
            assertTrue(
                    cast instanceof SupportMultiTableSinkWriter,
                    "Runtime cast to SupportMultiTableSinkWriter must succeed");
        } finally {
            executor.shutdownNow();
            if (writer != null) {
                writer.close();
            }
        }
    }
}
