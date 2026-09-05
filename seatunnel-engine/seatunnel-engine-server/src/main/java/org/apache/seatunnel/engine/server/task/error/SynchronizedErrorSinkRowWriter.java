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

package org.apache.seatunnel.engine.server.task.error;

import java.util.Objects;

/** Thread-safe wrapper for ErrorSinkRowWriter to serialize write/close calls. */
public final class SynchronizedErrorSinkRowWriter<T> implements ErrorSinkRowWriter<T> {

    private static final long serialVersionUID = 1L;

    private final ErrorSinkRowWriter<T> delegate;
    private final Object lock = new Object();
    private volatile boolean closed;

    public SynchronizedErrorSinkRowWriter(ErrorSinkRowWriter<T> delegate) {
        this.delegate = Objects.requireNonNull(delegate, "delegate must not be null");
    }

    @Override
    public void write(RowErrorContext ctx, T row, Throwable t) throws Exception {
        synchronized (lock) {
            if (closed) {
                throw new IllegalStateException("ErrorSinkRowWriter is already closed");
            }
            delegate.write(ctx, row, t);
        }
    }

    @Override
    public boolean writeAndCheckAccepted(RowErrorContext ctx, T row, Throwable t) throws Exception {
        synchronized (lock) {
            if (closed) {
                throw new IllegalStateException("ErrorSinkRowWriter is already closed");
            }
            return delegate.writeAndCheckAccepted(ctx, row, t);
        }
    }

    @Override
    public void flush() throws Exception {
        synchronized (lock) {
            if (closed) {
                throw new IllegalStateException("ErrorSinkRowWriter is already closed");
            }
            delegate.flush();
        }
    }

    @Override
    public void flush(long checkpointId) throws Exception {
        synchronized (lock) {
            if (closed) {
                throw new IllegalStateException("ErrorSinkRowWriter is already closed");
            }
            delegate.flush(checkpointId);
        }
    }

    @Override
    public void close() throws Exception {
        synchronized (lock) {
            if (closed) {
                return;
            }
            closed = true;
            delegate.close();
        }
    }
}
