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

import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

/** A {@link java.util.concurrent.CompletableFuture} with own executor. */
public class CompletableFuture<T> extends java.util.concurrent.CompletableFuture<T> {

    public static final Executor EXECUTOR =
            new ThreadPoolExecutor(
                    Math.min(8, Runtime.getRuntime().availableProcessors()),
                    Integer.MAX_VALUE,
                    60L,
                    TimeUnit.SECONDS,
                    new SynchronousQueue<>(),
                    new ThreadFactory() {
                        private final AtomicInteger seq = new AtomicInteger();

                        @Override
                        public Thread newThread(Runnable r) {
                            Thread thread =
                                    new Thread(
                                            r,
                                            "SeaTunnel-CompletableFuture-Thread-"
                                                    + seq.getAndIncrement());
                            thread.setDaemon(true);
                            return thread;
                        }
                    });

    public CompletableFuture() {}

    public CompletableFuture(java.util.concurrent.CompletableFuture<T> future) {
        future.whenComplete(
                (value, ex) -> {
                    if (ex != null) {
                        super.completeExceptionally(ex);
                    } else {
                        super.complete(value);
                    }
                });
    }

    public static CompletableFuture<Void> allOf(CompletableFuture<?>... cfs) {
        return new CompletableFuture<>(java.util.concurrent.CompletableFuture.allOf(cfs));
    }

    public static CompletableFuture<Void> allOf(java.util.concurrent.CompletableFuture<?>... cfs) {
        return new CompletableFuture<>(java.util.concurrent.CompletableFuture.allOf(cfs));
    }

    public boolean complete(T value) {
        return super.complete(value);
    }

    public static <U> CompletableFuture<U> completedFuture(U value) {
        return new CompletableFuture<>(
                java.util.concurrent.CompletableFuture.completedFuture(value));
    }

    public static CompletableFuture<Void> runAsync(Runnable runnable) {
        return new CompletableFuture<>(
                java.util.concurrent.CompletableFuture.runAsync(runnable, EXECUTOR));
    }

    public static CompletableFuture<Void> runAsync(Runnable runnable, Executor executor) {
        return new CompletableFuture<>(
                java.util.concurrent.CompletableFuture.runAsync(runnable, executor));
    }

    public CompletableFuture<T> exceptionally(Function<Throwable, ? extends T> fn) {
        return new CompletableFuture<>(super.exceptionally(fn));
    }

    public CompletableFuture<T> whenComplete(BiConsumer<? super T, ? super Throwable> action) {
        return new CompletableFuture<>(super.whenComplete(action));
    }

    public CompletableFuture<Void> thenAccept(Consumer<? super T> action) {
        return new CompletableFuture<>(super.thenAccept(action));
    }

    public static <U> CompletableFuture<U> supplyAsync(Supplier<U> supplier) {
        return new CompletableFuture<>(
                java.util.concurrent.CompletableFuture.supplyAsync(supplier, EXECUTOR));
    }

    public static <U> CompletableFuture<U> supplyAsync(Supplier<U> supplier, Executor executor) {
        return new CompletableFuture<>(
                java.util.concurrent.CompletableFuture.supplyAsync(supplier, executor));
    }

    public <U> CompletableFuture<U> thenApply(Function<? super T, ? extends U> fn) {
        return new CompletableFuture<>(super.thenApply(fn));
    }

    public <U> CompletableFuture<U> thenApplyAsync(Function<? super T, ? extends U> fn) {
        return new CompletableFuture<>(super.thenApplyAsync(fn, EXECUTOR));
    }

    public <U> CompletableFuture<U> thenApplyAsync(
            Function<? super T, ? extends U> fn, Executor executor) {
        return new CompletableFuture<>(super.thenApplyAsync(fn, executor));
    }

    public CompletableFuture<T> whenCompleteAsync(BiConsumer<? super T, ? super Throwable> action) {
        return new CompletableFuture<>(super.whenCompleteAsync(action, EXECUTOR));
    }

    public CompletableFuture<T> whenCompleteAsync(
            BiConsumer<? super T, ? super Throwable> action, Executor executor) {
        return new CompletableFuture<>(super.whenCompleteAsync(action, executor));
    }

    public boolean completeExceptionally(Throwable ex) {
        return super.completeExceptionally(ex);
    }

    public T get() throws InterruptedException, ExecutionException {
        return super.get();
    }

    public T get(long timeout, TimeUnit unit)
            throws InterruptedException, ExecutionException, TimeoutException {
        return super.get(timeout, unit);
    }

    public T join() {
        return super.join();
    }

    public void obtrudeException(Throwable ex) {
        super.obtrudeException(ex);
    }

    public void obtrudeValue(T value) {
        super.obtrudeValue(value);
    }

    @Override
    public boolean cancel(boolean mayInterruptIfRunning) {
        return super.cancel(mayInterruptIfRunning);
    }

    @Override
    public boolean isCancelled() {
        return super.isCancelled();
    }

    public boolean isDone() {
        return super.isDone();
    }
}
