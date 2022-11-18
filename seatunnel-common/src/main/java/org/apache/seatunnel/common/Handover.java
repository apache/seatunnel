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

package org.apache.seatunnel.common;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.Closeable;
import java.util.ArrayDeque;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

public final class Handover<T> implements Closeable {
    private static final int DEFAULT_QUEUE_SIZE = 8192;
    private static final long DEFAULT_POLL_INTERVAL_MILLIS = 200;

    private final Lock lock;
    private final Condition isNotFull;
    private final Queue<T> queue;
    private Throwable error;

    public Handover() {
        this.lock = new ReentrantLock();
        this.isNotFull = lock.newCondition();

        this.queue = new ArrayDeque<>(DEFAULT_QUEUE_SIZE);
    }

    public boolean isEmpty() {
        return queue.isEmpty();
    }

    public Optional<T> pollNext() throws Exception {
        if (error != null) {
            rethrowException(error, error.getMessage());
        } else if (!isEmpty()) {
            try {
                lock.lock();
                T record = queue.poll();
                // signal produce() to add more records
                isNotFull.signalAll();
                return Optional.ofNullable(record);
            } finally {
                lock.unlock();
            }
        }
        return Optional.empty();
    }

    public void produce(final T element)
        throws InterruptedException, ClosedException {
        if (error != null) {
            throw new ClosedException();
        }
        try {
            lock.lock();
            while (queue.size() >= DEFAULT_QUEUE_SIZE) {
                // queue size threshold reached, so wait a bit
                isNotFull.await(DEFAULT_POLL_INTERVAL_MILLIS, TimeUnit.MILLISECONDS);
            }
            queue.add(element);
        } finally {
            lock.unlock();
        }
    }

    public void reportError(Throwable t) {
        checkNotNull(t);

        synchronized (lock) {
            // do not override the initial exception
            if (error == null) {
                error = t;
            }
            lock.notifyAll();
        }
    }

    @Override
    public void close() {
        synchronized (lock) {
            if (error == null) {
                error = new ClosedException();
            }
            lock.notifyAll();
        }
    }

    public static void rethrowException(Throwable t, String parentMessage) throws Exception {
        if (t instanceof Error) {
            throw (Error) t;
        } else if (t instanceof Exception) {
            throw (Exception) t;
        } else {
            throw new Exception(parentMessage, t);
        }
    }

    public static final class ClosedException extends Exception {
        private static final long serialVersionUID = 1L;
    }
}
