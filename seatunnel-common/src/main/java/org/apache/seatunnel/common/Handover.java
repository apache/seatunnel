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

import java.io.Closeable;
import java.util.Optional;
import java.util.concurrent.LinkedBlockingQueue;

import static com.google.common.base.Preconditions.checkNotNull;

public final class Handover<T> implements Closeable {
    private static final int DEFAULT_QUEUE_SIZE = 10000;
    private final Object lock = new Object();
    private final LinkedBlockingQueue<T> blockingQueue =
            new LinkedBlockingQueue<>(DEFAULT_QUEUE_SIZE);
    private Throwable error;

    public boolean isEmpty() throws Exception {
        if (error != null) {
            rethrowException(error, error.getMessage());
        }
        return blockingQueue.isEmpty();
    }

    public Optional<T> pollNext() throws Exception {
        if (error != null) {
            rethrowException(error, error.getMessage());
        } else if (!isEmpty()) {
            return Optional.ofNullable(blockingQueue.poll());
        }
        return Optional.empty();
    }

    public void produce(final T element) throws InterruptedException, ClosedException {
        if (error != null) {
            throw new ClosedException();
        }
        blockingQueue.put(element);
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
