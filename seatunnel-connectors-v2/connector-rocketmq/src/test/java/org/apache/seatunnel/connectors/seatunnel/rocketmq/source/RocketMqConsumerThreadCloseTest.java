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

package org.apache.seatunnel.connectors.seatunnel.rocketmq.source;

import org.apache.seatunnel.connectors.seatunnel.rocketmq.common.RocketMqAdminUtil;

import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;

import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import java.io.IOException;
import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

/**
 * Covers the close-ordering fix for {@link RocketMqConsumerThread} and {@link
 * RocketMqSourceReader}: a reader must stop the underlying RocketMQ client of every consumer thread
 * it owns before it interrupts the thread pool. {@code DefaultLitePullConsumer#poll(long)} blocks
 * on network I/O that plain thread interruption does not reliably unblock, so relying on {@code
 * ExecutorService#shutdownNow()} alone could leave a consumer thread stuck mid-poll instead of
 * returning promptly through {@link RocketMqConsumerThread#run()}'s {@code finally} block.
 *
 * <p>Also covers two follow-up hardening fixes to {@link RocketMqSourceReader#close()}: one
 * consumer thread's shutdown failure must not abort closing the rest (a plain {@code forEach} would
 * stop at the first exception), and every consumer thread must be closed concurrently rather than
 * one at a time, since {@code DefaultLitePullConsumer}'s internally synchronized methods mean a
 * sequential close could block the calling thread for {@code pollTimeoutMillis * partitionCount}
 * instead of a single poll timeout.
 */
class RocketMqConsumerThreadCloseTest {

    /**
     * {@link RocketMqConsumerThread#close()} must shut down its RocketMQ client directly, not only
     * rely on the run-loop's own {@code finally} block, which only fires once the thread notices
     * the interrupt - something a blocking {@code poll()} call is not guaranteed to do.
     */
    @Test
    void closeShutsDownUnderlyingConsumerClient() throws Exception {
        DefaultLitePullConsumer mockConsumer = mock(DefaultLitePullConsumer.class);
        RocketMqConsumerThread consumerThread;
        try (MockedStatic<RocketMqAdminUtil> adminUtil =
                Mockito.mockStatic(RocketMqAdminUtil.class)) {
            adminUtil
                    .when(() -> RocketMqAdminUtil.initDefaultLitePullConsumer(any(), anyBoolean()))
                    .thenReturn(mockConsumer);
            consumerThread = new RocketMqConsumerThread(new ConsumerMetadata());
        }
        verify(mockConsumer, never()).shutdown();

        consumerThread.close();

        verify(mockConsumer, times(1)).shutdown();
    }

    /**
     * {@link RocketMqSourceReader#close()} must mark the reader as no longer running and close
     * every consumer thread it has created, in addition to shutting down its executor - dropping
     * the explicit per-thread close would silently reintroduce the hang this fix removes.
     */
    @Test
    @SuppressWarnings("unchecked")
    void closeStopsRunningAndClosesEveryTrackedConsumerThread() throws Exception {
        RocketMqSourceReader reader =
                new RocketMqSourceReader(
                        new ConsumerMetadata(),
                        new ConcurrentHashMap<>(),
                        mock(org.apache.seatunnel.api.source.SourceReader.Context.class));

        RocketMqConsumerThread mockConsumerThread = mock(RocketMqConsumerThread.class);
        setPrivateField(reader, "running", true);
        Map<Object, RocketMqConsumerThread> consumerThreads =
                (Map<Object, RocketMqConsumerThread>) getPrivateField(reader, "consumerThreads");
        consumerThreads.put(new Object(), mockConsumerThread);

        reader.close();

        verify(mockConsumerThread, times(1)).close();
        assertFalse((boolean) getPrivateField(reader, "running"));
        ExecutorService executorService =
                (ExecutorService) getPrivateField(reader, "executorService");
        assertTrue(executorService.isShutdown());
    }

    /**
     * One consumer thread's {@code close()} failure must not prevent the reader from closing every
     * other tracked consumer thread or from shutting down its executor - a plain {@code forEach}
     * would abort at the first exception and leak every remaining consumer thread plus skip {@code
     * executorService.shutdownNow()} entirely.
     */
    @Test
    @SuppressWarnings("unchecked")
    void closeIsolatesOneThreadsFailureFromTheRest() throws Exception {
        RocketMqSourceReader reader =
                new RocketMqSourceReader(
                        new ConsumerMetadata(),
                        new ConcurrentHashMap<>(),
                        mock(org.apache.seatunnel.api.source.SourceReader.Context.class));

        RocketMqConsumerThread failingThread = mock(RocketMqConsumerThread.class);
        RocketMqConsumerThread healthyThread = mock(RocketMqConsumerThread.class);
        Mockito.doThrow(new RuntimeException("simulated shutdown failure"))
                .when(failingThread)
                .close();
        Map<Object, RocketMqConsumerThread> consumerThreads =
                (Map<Object, RocketMqConsumerThread>) getPrivateField(reader, "consumerThreads");
        consumerThreads.put(new Object(), failingThread);
        consumerThreads.put(new Object(), healthyThread);

        reader.close();

        verify(failingThread, times(1)).close();
        verify(healthyThread, times(1)).close();
        ExecutorService executorService =
                (ExecutorService) getPrivateField(reader, "executorService");
        assertTrue(executorService.isShutdown());
    }

    /**
     * Consumer threads must be closed concurrently, not one at a time: {@code
     * DefaultLitePullConsumer#shutdown()} is internally synchronized and can block behind an
     * in-flight {@code poll()}, so closing sequentially would let the calling thread's wait grow
     * with the partition count instead of staying bounded by a single poll timeout. Proven here by
     * a rendezvous: if every mocked thread's {@code close()} is entered before any of them is
     * allowed to return, they were not run one at a time.
     */
    @Test
    @SuppressWarnings("unchecked")
    void closeShutsDownEveryConsumerThreadConcurrently() throws Exception {
        int threadCount = 3;
        RocketMqSourceReader reader =
                new RocketMqSourceReader(
                        new ConsumerMetadata(),
                        new ConcurrentHashMap<>(),
                        mock(org.apache.seatunnel.api.source.SourceReader.Context.class));
        Map<Object, RocketMqConsumerThread> consumerThreads =
                (Map<Object, RocketMqConsumerThread>) getPrivateField(reader, "consumerThreads");

        CountDownLatch allEnteredClose = new CountDownLatch(threadCount);
        CountDownLatch releaseClose = new CountDownLatch(1);
        for (int i = 0; i < threadCount; i++) {
            RocketMqConsumerThread thread = mock(RocketMqConsumerThread.class);
            Mockito.doAnswer(
                            invocation -> {
                                allEnteredClose.countDown();
                                releaseClose.await(5, TimeUnit.SECONDS);
                                return null;
                            })
                    .when(thread)
                    .close();
            consumerThreads.put(new Object(), thread);
        }

        Thread closer =
                new Thread(
                        () -> {
                            try {
                                reader.close();
                            } catch (IOException e) {
                                throw new RuntimeException(e);
                            }
                        });
        closer.start();
        try {
            // If close() ran the mocked threads sequentially, only the first would ever enter
            // close() - it would block on releaseClose forever, since nothing releases it yet,
            // and this latch would time out instead of reaching zero.
            assertTrue(
                    allEnteredClose.await(5, TimeUnit.SECONDS),
                    "expected every consumer thread to enter close() concurrently");
        } finally {
            releaseClose.countDown();
            closer.join(TimeUnit.SECONDS.toMillis(5));
        }
    }

    private static Object getPrivateField(Object target, String name) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        return field.get(target);
    }

    private static void setPrivateField(Object target, String name, Object value) throws Exception {
        Field field = target.getClass().getDeclaredField(name);
        field.setAccessible(true);
        field.set(target, value);
    }
}
