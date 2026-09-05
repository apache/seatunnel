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

import java.lang.reflect.Field;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

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
