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

package org.apache.seatunnel.api.sink.multitablesink;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Coordinates one schema-change event across every queue worker. The last worker reaching the
 * barrier performs the actual shared-sink schema mutation and then releases the others.
 */
final class SchemaChangeBarrier {

    /**
     * Executes the shared-sink schema mutation once every queue has drained older rows and reached
     * the barrier.
     */
    @FunctionalInterface
    interface Dispatcher {

        void dispatch() throws IOException;
    }

    /** Table path used for log messages and queue diagnostics. */
    private final String tablePath;
    /** Number of queue workers that must reach the barrier before the mutation can run. */
    private final int participantCount;
    /** Callback that performs the actual schema mutation once all queues are parked. */
    private final Dispatcher schemaChangeDispatcher;
    /** Counts queue workers that already reached the barrier. */
    private final AtomicInteger arrivedQueues = new AtomicInteger(0);
    /** Prevents success and failure paths from releasing the barrier twice. */
    private final AtomicBoolean completedOrFailed = new AtomicBoolean(false);
    /** Releases every parked queue worker after the schema change succeeds or fails. */
    private final CountDownLatch completed = new CountDownLatch(1);
    /** Stores the first failure that should be surfaced back to the coordinator thread. */
    private final AtomicReference<Throwable> failure = new AtomicReference<>();

    SchemaChangeBarrier(String tablePath, int participantCount, Dispatcher schemaChangeDispatcher) {
        this.tablePath = tablePath;
        this.participantCount = participantCount;
        this.schemaChangeDispatcher = schemaChangeDispatcher;
    }

    /**
     * Enters the shared barrier for this queue. The last arriving worker performs the actual schema
     * mutation while the others wait behind the same completion latch.
     */
    void reachBarrier() throws IOException {
        if (arrivedQueues.incrementAndGet() == participantCount
                && completedOrFailed.compareAndSet(false, true)) {
            try {
                schemaChangeDispatcher.dispatch();
            } catch (Throwable throwable) {
                failure.compareAndSet(null, throwable);
            } finally {
                completed.countDown();
            }
        }
        awaitCompletion();
    }

    /**
     * Fails the shared barrier when one queue worker dies before reaching it. This keeps the
     * schema-change caller on the original write-failure path instead of waiting forever for a
     * queue that has already stopped making progress.
     */
    void fail(Throwable throwable) {
        failure.compareAndSet(null, throwable);
        if (completedOrFailed.compareAndSet(false, true)) {
            completed.countDown();
        }
    }

    /**
     * Waits until the shared schema change either finishes successfully or fails fast because a
     * queue worker stopped making progress.
     */
    void awaitCompletion() throws IOException {
        try {
            completed.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException(e);
        }
        rethrowFailureIfNeeded();
    }

    /** Returns the source table path for log messages and queue debug output. */
    String getTablePath() {
        return tablePath;
    }

    private void rethrowFailureIfNeeded() throws IOException {
        Throwable throwable = failure.get();
        if (throwable == null) {
            return;
        }
        if (throwable instanceof IOException) {
            throw (IOException) throwable;
        }
        if (throwable instanceof RuntimeException) {
            throw (RuntimeException) throwable;
        }
        throw new RuntimeException(throwable);
    }
}
