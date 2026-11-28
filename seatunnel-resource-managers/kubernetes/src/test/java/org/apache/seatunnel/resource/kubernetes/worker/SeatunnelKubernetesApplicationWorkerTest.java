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

package org.apache.seatunnel.resource.kubernetes.worker;

import org.junit.jupiter.api.Test;

import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

class SeatunnelKubernetesApplicationWorkerTest {

    @Test
    void waitsForDelayedMasterServiceBeforeJoining() throws Exception {
        int port = unusedPort();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try {
            Future<Boolean> accepted =
                    executor.submit(
                            () -> {
                                TimeUnit.MILLISECONDS.sleep(250);
                                try (ServerSocket server = new ServerSocket(port)) {
                                    server.setSoTimeout(3000);
                                    try (Socket connection = server.accept()) {
                                        return connection.isConnected();
                                    }
                                }
                            });
            SeatunnelKubernetesApplicationWorker.awaitMasterService("127.0.0.1:" + port, 3000);
            assertTrue(accepted.get(4, TimeUnit.SECONDS));
        } finally {
            executor.shutdownNow();
        }
    }

    @Test
    void failsWithinStartupDeadlineWhenMasterServiceNeverStarts() throws Exception {
        String address = "127.0.0.1:" + unusedPort();
        long started = System.nanoTime();
        IllegalStateException failure =
                assertThrows(
                        IllegalStateException.class,
                        () ->
                                SeatunnelKubernetesApplicationWorker.awaitMasterService(
                                        address, 150));
        long elapsedMillis = TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - started);
        assertTrue(elapsedMillis >= 100, "The worker should allow its configured startup window");
        assertTrue(elapsedMillis < 3000, "The worker must not wait indefinitely");
        assertTrue(failure.getMessage().contains(address));
        assertTrue(failure.getMessage().contains("150 ms"));
    }

    @Test
    void interruptionStopsWaitingAndPreservesInterruptStatus() throws Exception {
        String address = "127.0.0.1:" + unusedPort();
        Thread.currentThread().interrupt();
        try {
            assertThrows(
                    IllegalStateException.class,
                    () -> SeatunnelKubernetesApplicationWorker.awaitMasterService(address, 3000));
            assertTrue(Thread.currentThread().isInterrupted());
        } finally {
            Thread.interrupted();
        }
    }

    private static int unusedPort() throws Exception {
        try (ServerSocket socket = new ServerSocket(0)) {
            return socket.getLocalPort();
        }
    }
}
