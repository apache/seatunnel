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

package org.apache.seatunnel.engine.common.utils.concurrent;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

public class CompletableFutureTest {

    @Test
    void testCompletableFuture() {
        CompletableFuture<Integer> future = new CompletableFuture<>();
        future.complete(1);
        Assertions.assertEquals(1, future.join());
        future = new CompletableFuture<>();
        future.completeExceptionally(new RuntimeException());
        Assertions.assertThrows(RuntimeException.class, future::join);
    }

    @Test
    void testCompletedNormally() {
        CompletableFuture<Integer> future = new CompletableFuture<>();
        future.complete(1);
        Assertions.assertTrue(future.isDone());
        Assertions.assertFalse(future.isCompletedExceptionally());
        Assertions.assertFalse(future.isCancelled());
    }

    @Test
    void testAsyncMethodWithOwnExecutor() {
        AtomicInteger value = new AtomicInteger(0);
        Assertions.assertFalse(getThreads().contains("SeaTunnel-CompletableFuture-Thread-0"));
        CompletableFuture.runAsync(value::getAndIncrement).join();
        Assertions.assertTrue(getThreads().contains("SeaTunnel-CompletableFuture-Thread-0"));
        Assertions.assertEquals(1, value.get());
        CompletableFuture.allOf(
                        CompletableFuture.supplyAsync(
                                () -> {
                                    value.getAndIncrement();
                                    try {
                                        Thread.sleep(1000);
                                    } catch (InterruptedException e) {
                                        throw new RuntimeException(e);
                                    }
                                    return null;
                                }),
                        CompletableFuture.supplyAsync(value::getAndIncrement))
                .join();
        Assertions.assertTrue(getThreads().contains("SeaTunnel-CompletableFuture-Thread-1"));
        Assertions.assertEquals(3, value.get());
        CompletableFuture.allOf(
                        getWhenCompleteAsync(value),
                        getWhenCompleteAsync(value),
                        getWhenCompleteAsync(value))
                .join();
        Assertions.assertTrue(getThreads().contains("SeaTunnel-CompletableFuture-Thread-2"));
        Assertions.assertEquals(6, value.get());
        CompletableFuture.allOf(
                        getThenApplyAsync(value),
                        getThenApplyAsync(value),
                        getThenApplyAsync(value),
                        getThenApplyAsync(value))
                .join();
        Assertions.assertTrue(getThreads().contains("SeaTunnel-CompletableFuture-Thread-3"));
        Assertions.assertEquals(10, value.get());
    }

    private static CompletableFuture<Object> getWhenCompleteAsync(AtomicInteger value) {
        return CompletableFuture.completedFuture(null)
                .whenCompleteAsync(
                        (aVoid, throwable) -> {
                            value.getAndIncrement();
                            try {
                                Thread.sleep(1000);
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                        });
    }

    private static CompletableFuture<Object> getThenApplyAsync(AtomicInteger value) {
        return CompletableFuture.completedFuture(null)
                .thenApplyAsync(
                        aVoid -> {
                            value.getAndIncrement();
                            try {
                                Thread.sleep(1000);
                            } catch (InterruptedException e) {
                                throw new RuntimeException(e);
                            }
                            return null;
                        });
    }

    private static Set<String> getThreads() {
        return Thread.getAllStackTraces().keySet().stream()
                .map(Thread::getName)
                .collect(Collectors.toSet());
    }
}
