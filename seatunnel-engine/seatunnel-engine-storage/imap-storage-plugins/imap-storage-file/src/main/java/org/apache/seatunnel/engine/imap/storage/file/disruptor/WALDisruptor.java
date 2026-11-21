/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.seatunnel.engine.imap.storage.file.disruptor;

import org.apache.seatunnel.engine.imap.storage.api.exception.IMapStorageException;
import org.apache.seatunnel.engine.imap.storage.file.bean.IMapFileData;
import org.apache.seatunnel.engine.imap.storage.file.config.FileConfiguration;
import org.apache.seatunnel.engine.serializer.api.Serializer;

import org.apache.hadoop.fs.FileSystem;

import com.lmax.disruptor.BlockingWaitStrategy;
import com.lmax.disruptor.EventTranslatorThreeArg;
import com.lmax.disruptor.TimeoutException;
import com.lmax.disruptor.WorkHandler;
import com.lmax.disruptor.dsl.Disruptor;
import com.lmax.disruptor.dsl.ProducerType;
import com.lmax.disruptor.util.DaemonThreadFactory;
import lombok.extern.slf4j.Slf4j;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.LockSupport;

@Slf4j
public class WALDisruptor implements Closeable {

    private volatile Disruptor<FileWALEvent> disruptor;

    /**
     * Sole consumer installed in {@link #WALDisruptor}. Owned by this disruptor for the process
     * lifetime: its fail-close flag is never reset in-process and is cleared only by restarting the
     * engine node. Do not replace this handler to "un-stick" a failed writer; {@code fs.create}
     * would truncate the fixed {@code wal.txt} path.
     */
    private final WALWorkHandler workHandler;

    private static final int DEFAULT_RING_BUFFER_SIZE = 1024;

    private static final int DEFAULT_CLOSE_WAIT_TIME_SECONDS = 5;

    private boolean isClosed = false;

    private static final EventTranslatorThreeArg<FileWALEvent, IMapFileData, WALEventType, Long>
            TRANSLATOR =
                    (event, sequence, data, walEventStatus, requestId) -> {
                        event.setData(data);
                        event.setType(walEventStatus);
                        event.setRequestId(requestId);
                    };

    public WALDisruptor(
            FileSystem fs,
            FileConfiguration fileConfiguration,
            String parentPath,
            Serializer serializer) {
        // todo should support multi thread producer
        this(
                fs,
                fileConfiguration,
                parentPath,
                serializer,
                DEFAULT_RING_BUFFER_SIZE,
                new WALWorkHandler(fs, fileConfiguration, parentPath, serializer));
    }

    /**
     * Alternate constructor for tests that need a small ring buffer or a custom consumer.
     * Production uses the four-arg constructor above.
     */
    public WALDisruptor(
            FileSystem fs,
            FileConfiguration fileConfiguration,
            String parentPath,
            Serializer serializer,
            int ringBufferSize,
            WorkHandler<FileWALEvent> eventHandler) {
        ThreadFactory threadFactory = DaemonThreadFactory.INSTANCE;
        this.disruptor =
                new Disruptor<>(
                        FileWALEvent.FACTORY,
                        ringBufferSize,
                        threadFactory,
                        ProducerType.SINGLE,
                        new BlockingWaitStrategy());
        this.workHandler =
                eventHandler instanceof WALWorkHandler ? (WALWorkHandler) eventHandler : null;
        disruptor.handleEventsWithWorkerPool(eventHandler);
        disruptor.start();
    }

    /**
     * Whether the sole WAL consumer has permanently fail-closed APPEND after a write failure.
     *
     * @return true until process restart once fail-close has tripped
     */
    public boolean isAppendBlockedAfterWriteFailure() {
        return workHandler != null && workHandler.isAppendBlockedAfterWriteFailure();
    }

    /**
     * Publishes one WAL event if a ring slot is free before {@code deadlineNanos}.
     *
     * <p>A full ring used to block inside {@code publishEvent} with no timeout, so a stalled
     * consumer held the write-through caller before any future wait started. A closed ring returns
     * false immediately; the caller must drop the pending request instead of waiting for a result
     * that will never arrive.
     *
     * @param waitNanos maximum time to wait for a ring slot, in the same unit as {@link
     *     System#nanoTime()}; callers must derive this from the shared operation deadline using the
     *     same clock as {@link org.apache.seatunnel.engine.imap.storage.file.IMapFileStorage}
     * @return false when the ring is closed or no slot was claimed before {@code waitNanos} elapses
     */
    public boolean tryPublish(
            IMapFileData message, WALEventType status, Long requestId, long waitNanos) {
        if (isClosed()) {
            return false;
        }
        if (waitNanos <= 0L) {
            return false;
        }
        long deadline = System.nanoTime() + waitNanos;
        while (System.nanoTime() < deadline) {
            if (Thread.currentThread().isInterrupted()) {
                return false;
            }
            if (disruptor.getRingBuffer().tryPublishEvent(TRANSLATOR, message, status, requestId)) {
                return true;
            }
            long remaining = deadline - System.nanoTime();
            if (remaining <= 0L) {
                break;
            }
            LockSupport.parkNanos(Math.min(remaining, 100_000L));
            if (Thread.currentThread().isInterrupted()) {
                return false;
            }
        }
        return false;
    }

    /**
     * Publishes one WAL event, waiting for a ring slot with no caller deadline.
     *
     * <p>Shutdown uses this for the terminal {@code CLOSED} marker. Steady-state appends must use
     * {@link #tryPublish(IMapFileData, WALEventType, Long, long)} so admission stays inside the
     * configured write timeout.
     */
    public boolean tryPublish(IMapFileData message, WALEventType status, Long requestId) {
        if (isClosed()) {
            return false;
        }
        disruptor.getRingBuffer().publishEvent(TRANSLATOR, message, status, requestId);
        return true;
    }

    public boolean tryAppendPublish(IMapFileData message, long requestId) {
        return this.tryPublish(message, WALEventType.APPEND, requestId);
    }

    public boolean isClosed() {
        return isClosed;
    }

    @Override
    public void close() throws IOException {
        // we can wait for 5 seconds, so that backlog can be committed
        try {
            tryPublish(null, WALEventType.CLOSED, 0L);
            isClosed = true;
            disruptor.shutdown(DEFAULT_CLOSE_WAIT_TIME_SECONDS, TimeUnit.SECONDS);
        } catch (TimeoutException e) {
            log.error("WALDisruptor close timeout error", e);
            throw new IMapStorageException("WALDisruptor close timeout error", e);
        }
    }
}
