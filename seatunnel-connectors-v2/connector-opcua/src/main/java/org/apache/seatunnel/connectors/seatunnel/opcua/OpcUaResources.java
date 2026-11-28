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

package org.apache.seatunnel.connectors.seatunnel.opcua;

import org.eclipse.milo.opcua.stack.client.UaStackClientConfigBuilder;
import org.eclipse.milo.opcua.stack.core.channel.EncodingLimits;

import io.netty.channel.DefaultSelectStrategyFactory;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.HashedWheelTimer;
import io.netty.util.concurrent.DefaultEventExecutorChooserFactory;

import java.nio.channels.spi.SelectorProvider;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.eclipse.milo.opcua.stack.core.types.builtin.unsigned.Unsigned.uint;

/** Each reader owns these bounded SDK facilities and shuts down only its own resources. */
final class OpcUaResources implements AutoCloseable {
    private static final int MAX_TASKS = 256;
    private final AtomicBoolean overflow = new AtomicBoolean();
    private final ThreadPoolExecutor callbacks =
            new ThreadPoolExecutor(
                    2,
                    2,
                    0L,
                    TimeUnit.MILLISECONDS,
                    new ArrayBlockingQueue<>(MAX_TASKS),
                    threads("opcua-sdk"),
                    (task, executor) -> reject());
    private final ScheduledThreadPoolExecutor scheduled = new LimitedSchedule();
    private final NioEventLoopGroup transport =
            new NioEventLoopGroup(
                    1,
                    command -> threads("opcua-transport").newThread(command).start(),
                    DefaultEventExecutorChooserFactory.INSTANCE,
                    SelectorProvider.provider(),
                    DefaultSelectStrategyFactory.INSTANCE,
                    (task, executor) -> reject(),
                    ignored -> new ArrayBlockingQueue<>(MAX_TASKS),
                    ignored -> new ArrayBlockingQueue<>(MAX_TASKS));
    private final HashedWheelTimer timer =
            new HashedWheelTimer(
                    threads("opcua-timer"), 20, TimeUnit.MILLISECONDS, 512, false, MAX_TASKS);
    private final AtomicBoolean closed = new AtomicBoolean();

    OpcUaResources() {
        scheduled.setRemoveOnCancelPolicy(true);
        scheduled.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
    }

    void configure(UaStackClientConfigBuilder builder, OpcUaSettings settings) {
        builder.setExecutor(callbacks);
        builder.setScheduledExecutor(scheduled);
        builder.setEventLoop(transport);
        builder.setWheelTimer(timer);
        builder.setConnectTimeout(uint(settings.requestTimeout));
        builder.setRequestTimeout(uint(settings.requestTimeout));
        builder.setEncodingLimits(
                new EncodingLimits(
                        settings.maxChunkSize,
                        settings.maxChunkCount,
                        settings.maxMessageSize,
                        64));
    }

    static ThreadFactory threads(String name) {
        return work -> {
            Thread thread = new Thread(work, name);
            thread.setDaemon(true);
            return thread;
        };
    }

    private void reject() {
        overflow.set(true);
        throw new RejectedExecutionException("OPC UA task queue limit");
    }

    void requireHealthy() throws OpcUaFailure {
        if (overflow.get()) {
            throw OpcUaFailure.problem("SDK task queue exhausted");
        }
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            try {
                transport.shutdownGracefully(0, 1, TimeUnit.SECONDS);
            } finally {
                callbacks.shutdownNow();
                scheduled.shutdownNow();
                timer.stop();
            }
        }
    }

    private final class LimitedSchedule extends ScheduledThreadPoolExecutor {
        private LimitedSchedule() {
            super(1, threads("opcua-scheduler"), (task, executor) -> OpcUaResources.this.reject());
        }

        @Override
        public synchronized ScheduledFuture<?> schedule(Runnable work, long delay, TimeUnit unit) {
            checkCapacity();
            return super.schedule(work, delay, unit);
        }

        @Override
        public synchronized <V> ScheduledFuture<V> schedule(
                Callable<V> work, long delay, TimeUnit unit) {
            checkCapacity();
            return super.schedule(work, delay, unit);
        }

        @Override
        public synchronized ScheduledFuture<?> scheduleAtFixedRate(
                Runnable work, long delay, long period, TimeUnit unit) {
            checkCapacity();
            return super.scheduleAtFixedRate(work, delay, period, unit);
        }

        @Override
        public synchronized ScheduledFuture<?> scheduleWithFixedDelay(
                Runnable work, long delay, long period, TimeUnit unit) {
            checkCapacity();
            return super.scheduleWithFixedDelay(work, delay, period, unit);
        }

        private void checkCapacity() {
            if (getQueue().size() >= MAX_TASKS) {
                reject();
            }
        }
    }
}
