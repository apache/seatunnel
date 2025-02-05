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

import java.util.Comparator;
import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ScheduledExecutorService;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Stream;

/**
 * Tracer for MDC context.
 *
 * <p>It wraps the given {@link Runnable}, {@link Callable}, {@link Executor}, {@link
 * ExecutorService}, {@link ScheduledExecutorService} to trace the MDC context.
 *
 * <p>It is useful to trace the MDC context in the asynchronous execution. For example, when you
 * submit a task to the {@link ExecutorService}, the MDC context is not propagated to the worker
 * thread.
 *
 * <p>It is recommended to use the {@link MDCTracer} to wrap the task to trace the MDC context.
 *
 * <pre>{@code
 * MDCContext mdcContext = MDCContext.of(1);
 * ExecutorService executorService = Executors.newFixedThreadPool(10);
 * executorService.submit(MDCTracer.tracing(mdcContext, () -> {
 *    // Your task
 *    logger.info("Task is running");
 *    return null;
 *    }));
 *
 * }</pre>
 */
public class MDCTracer {

    public static MDCRunnable tracing(Runnable delegate) {
        return tracing(MDCContext.current(), delegate);
    }

    public static MDCRunnable tracing(Long jobId, Runnable delegate) {
        return tracing(MDCContext.of(jobId), delegate);
    }

    public static MDCRunnable tracing(MDCContext context, Runnable delegate) {
        if (delegate instanceof MDCRunnable) {
            throw new IllegalArgumentException("Already an MDCRunnable");
        }
        return new MDCRunnable(context, delegate);
    }

    public static <V> MDCCallable<V> tracing(Callable<V> delegate) {
        return tracing(MDCContext.current(), delegate);
    }

    public static <V> MDCCallable<V> tracing(Long jobId, Callable<V> delegate) {
        return tracing(MDCContext.of(jobId), delegate);
    }

    public static <V> MDCCallable<V> tracing(MDCContext context, Callable<V> delegate) {
        if (delegate instanceof MDCCallable) {
            throw new IllegalArgumentException("Already an MDCCallable");
        }
        return new MDCCallable<>(context, delegate);
    }

    public static MDCExecutor tracing(Executor delegate) {
        return tracing(MDCContext.current(), delegate);
    }

    public static MDCExecutor tracing(Long jobId, Executor delegate) {
        return tracing(MDCContext.of(jobId), delegate);
    }

    public static MDCExecutor tracing(MDCContext context, Executor delegate) {
        if (delegate instanceof MDCExecutor) {
            throw new IllegalArgumentException("Already an MDCExecutor");
        }
        return new MDCExecutor(context, delegate);
    }

    public static MDCExecutorService tracing(ExecutorService delegate) {
        return tracing(MDCContext.current(), delegate);
    }

    public static MDCExecutorService tracing(Long jobId, ExecutorService delegate) {
        return tracing(MDCContext.of(jobId), delegate);
    }

    public static MDCExecutorService tracing(MDCContext context, ExecutorService delegate) {
        if (delegate instanceof MDCExecutor) {
            throw new IllegalArgumentException("Already an MDCExecutor");
        }
        return new MDCExecutorService(context, delegate);
    }

    public static MDCScheduledExecutorService tracing(ScheduledExecutorService delegate) {
        return tracing(MDCContext.current(), delegate);
    }

    public static MDCScheduledExecutorService tracing(
            Long jobId, ScheduledExecutorService delegate) {
        return tracing(MDCContext.of(jobId), delegate);
    }

    public static MDCScheduledExecutorService tracing(
            MDCContext context, ScheduledExecutorService delegate) {
        if (delegate instanceof MDCExecutor) {
            throw new IllegalArgumentException("Already an MDCExecutor");
        }
        return new MDCScheduledExecutorService(context, delegate);
    }

    public static <T> MDCConsumer<T> tracing(Consumer<T> delegate) {
        return tracing(MDCContext.current(), delegate);
    }

    public static <T> MDCConsumer<T> tracing(Long jobId, Consumer<T> delegate) {
        return tracing(MDCContext.of(jobId), delegate);
    }

    public static <T> MDCConsumer<T> tracing(MDCContext context, Consumer<T> delegate) {
        if (delegate instanceof MDCConsumer) {
            throw new IllegalArgumentException("Already an MDCConsumer");
        }
        return new MDCConsumer<>(context, delegate);
    }

    public static <T, R> MDCFunction<T, R> tracing(Function<T, R> delegate) {
        return tracing(MDCContext.current(), delegate);
    }

    public static <T, R> MDCFunction<T, R> tracing(Long jobId, Function<T, R> delegate) {
        return tracing(MDCContext.of(jobId), delegate);
    }

    public static <T, R> MDCFunction<T, R> tracing(MDCContext context, Function<T, R> delegate) {
        if (delegate instanceof MDCFunction) {
            throw new IllegalArgumentException("Already an MDCFunction");
        }
        return new MDCFunction<>(context, delegate);
    }

    public static <T> MDCPredicate<T> tracing(Predicate<T> delegate) {
        return tracing(MDCContext.current(), delegate);
    }

    public static <T> MDCPredicate<T> tracing(Long jobId, Predicate<T> delegate) {
        return tracing(MDCContext.of(jobId), delegate);
    }

    public static <T> MDCPredicate<T> tracing(MDCContext context, Predicate<T> delegate) {
        if (delegate instanceof MDCPredicate) {
            throw new IllegalArgumentException("Already an MDCPredicate");
        }
        return new MDCPredicate<>(context, delegate);
    }

    public static <T> MDCComparator<T> tracing(Comparator<T> delegate) {
        return tracing(MDCContext.current(), delegate);
    }

    public static <T> MDCComparator<T> tracing(Long jobId, Comparator<T> delegate) {
        return tracing(MDCContext.of(jobId), delegate);
    }

    public static <T> MDCComparator<T> tracing(MDCContext context, Comparator<T> delegate) {
        if (delegate instanceof MDCComparator) {
            throw new IllegalArgumentException("Already an MDCComparator");
        }
        return new MDCComparator<>(context, delegate);
    }

    public static <T> MDCSupplier<T> tracing(Supplier<T> delegate) {
        return tracing(MDCContext.current(), delegate);
    }

    public static <T> MDCSupplier<T> tracing(Long jobId, Supplier<T> delegate) {
        return tracing(MDCContext.of(jobId), delegate);
    }

    public static <T> MDCSupplier<T> tracing(MDCContext context, Supplier<T> delegate) {
        if (delegate instanceof MDCSupplier) {
            throw new IllegalArgumentException("Already an MDCSupplier");
        }
        return new MDCSupplier<>(context, delegate);
    }

    public static <T> MDCStream<T> tracing(Stream<T> delegate) {
        return tracing(MDCContext.current(), delegate);
    }

    public static <T> MDCStream<T> tracing(Long jobId, Stream<T> delegate) {
        return tracing(MDCContext.of(jobId), delegate);
    }

    public static <T> MDCStream<T> tracing(MDCContext context, Stream<T> delegate) {
        if (delegate instanceof MDCStream) {
            throw new IllegalArgumentException("Already an MDCStream");
        }
        return new MDCStream<>(context, delegate);
    }
}
