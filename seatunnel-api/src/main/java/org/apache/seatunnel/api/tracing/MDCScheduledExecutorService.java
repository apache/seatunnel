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

package org.apache.seatunnel.api.tracing;

import java.util.concurrent.Callable;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * ScheduledExecutorService that sets MDC context before calling the delegate and clears it
 * afterwards.
 */
public class MDCScheduledExecutorService extends MDCExecutorService
        implements ScheduledExecutorService {
    private final MDCContext context;
    private final ScheduledExecutorService delegate;

    public MDCScheduledExecutorService(MDCContext context, ScheduledExecutorService delegate) {
        super(context, delegate);
        this.context = context;
        this.delegate = delegate;
    }

    @Override
    public ScheduledFuture<?> schedule(Runnable command, long delay, TimeUnit unit) {
        return delegate.schedule(
                new MDCRunnable(() -> MDCContext.of(context), command), delay, unit);
    }

    @Override
    public <V> ScheduledFuture<V> schedule(Callable<V> callable, long delay, TimeUnit unit) {
        return delegate.schedule(
                new MDCCallable<>(() -> MDCContext.of(context), callable), delay, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleAtFixedRate(
            Runnable command, long initialDelay, long period, TimeUnit unit) {
        return delegate.scheduleAtFixedRate(
                new MDCRunnable(() -> MDCContext.of(context), command), initialDelay, period, unit);
    }

    @Override
    public ScheduledFuture<?> scheduleWithFixedDelay(
            Runnable command, long initialDelay, long delay, TimeUnit unit) {
        return delegate.scheduleWithFixedDelay(
                new MDCRunnable(() -> MDCContext.of(context), command), initialDelay, delay, unit);
    }
}
