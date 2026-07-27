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

import java.io.Serializable;

/** Sink writer used by {@link ErrorHandler} to persist error records. */
public interface ErrorSinkRowWriter<T> extends Serializable, AutoCloseable {

    /**
     * Writes one row-level error to the configured error sink.
     *
     * @param ctx context describing the source stage, plugin, and table
     * @param row original row that caused the row-level error
     * @param t original row-level failure
     */
    void write(RowErrorContext ctx, T row, Throwable t) throws Exception;

    /**
     * Writes one row-level error and reports whether the error sink accepted it.
     *
     * <p>Implementations can return {@code false} when the row is intentionally dropped, for
     * example because of a bounded queue overflow policy.
     */
    default boolean writeAndCheckAccepted(RowErrorContext ctx, T row, Throwable t)
            throws Exception {
        write(ctx, row, t);
        return true;
    }

    /** Flushes pending error rows outside a checkpoint boundary. */
    default void flush() throws Exception {}

    /**
     * Flushes pending error rows for the given checkpoint.
     *
     * <p>The default delegates to {@link #flush()} so simple sinks do not need checkpoint-specific
     * handling.
     */
    default void flush(long checkpointId) throws Exception {
        flush();
    }

    @Override
    void close() throws Exception;
}
