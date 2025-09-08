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

package org.apache.seatunnel.engine.server.utils;

import org.apache.seatunnel.engine.common.utils.concurrent.CompletableFuture;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;

public class PeekBlockingQueueTest {

    private PeekBlockingQueue<String> queue;

    @BeforeEach
    void setUp() {
        queue = new PeekBlockingQueue<>();
    }

    @Test
    public void testBasic() throws InterruptedException {
        queue.put("1");
        queue.put("2");
        queue.put("3");
        Assertions.assertEquals(3, queue.size());
        Assertions.assertEquals("1", queue.peekBlocking());
        Assertions.assertEquals("1", queue.take());
        Assertions.assertEquals(2, queue.size());
        Assertions.assertEquals("2", queue.peekBlocking());
        Assertions.assertEquals("2", queue.take());
        Assertions.assertEquals(1, queue.size());
        Assertions.assertEquals("3", queue.peekBlocking());
        Assertions.assertEquals("3", queue.take());
        Assertions.assertEquals(0, queue.size());
    }

    @Test
    public void testPeekBlocking() throws InterruptedException {
        // Test if peekBlocking successfully peek the element
        CompletableFuture<Void> peekFuture =
                CompletableFuture.runAsync(
                        () -> {
                            await().atMost(5, TimeUnit.SECONDS)
                                    .untilAsserted(
                                            () ->
                                                    Assertions.assertEquals(
                                                            "1", queue.peekBlocking()));
                            try {
                                Assertions.assertEquals("1", queue.take());
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                        });
        Thread.sleep(1000);
        queue.put("1");
        peekFuture.join();
    }

    @Test
    public void testMultiPeekBlocking() throws InterruptedException, ExecutionException {
        // Test if peekBlocking successfully peek the element
        CompletableFuture<Void> peekFuture =
                CompletableFuture.runAsync(
                        () -> {
                            await().atMost(5, TimeUnit.SECONDS)
                                    .untilAsserted(
                                            () ->
                                                    Assertions.assertEquals(
                                                            "1", queue.peekBlocking()));
                            try {
                                Assertions.assertEquals("1", queue.take());
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                        });
        CompletableFuture<Void> secondPeekFuture =
                CompletableFuture.runAsync(
                        () -> {
                            await().atMost(5, TimeUnit.SECONDS)
                                    .untilAsserted(
                                            () ->
                                                    Assertions.assertEquals(
                                                            "2", queue.peekBlocking()));
                            try {
                                Assertions.assertEquals("2", queue.take());
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                        });
        Thread.sleep(1000);
        queue.put("1");
        queue.put("2");

        CompletableFuture.allOf(peekFuture, secondPeekFuture).join();
    }
}
