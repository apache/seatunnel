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

package org.apache.seatunnel.edge.agent.connector;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Discrete-event style {@link AgentInput}.
 *
 * <p><strong>File-backed mode</strong> (one or more non-null paths): on {@link #open()}, loads all
 * NDJSON lines from {@link #paths()} into an in-memory queue (in path order), same as before.
 * {@link #append(String)}, {@link #publish(String)}, and {@link #publish(List)} are not supported.
 *
 * <p><strong>Memory-only mode</strong> (no paths): the queue is filled via {@link #append(String)},
 * {@link #publish(String)}, and {@link #publish(List)} before or after {@link #open()}; {@link
 * #poll(int)} drains up to {@code maxRecords} strings per call. Lines are normalized with {@link
 * JsonLineSupport#normalizeRecord(String)} (blank lines skipped).
 */
public final class EventAgentInput implements AgentInput {

    private final List<Path> paths;
    private final boolean memoryOnly;

    private final Object queueLock = new Object();
    private ArrayDeque<String> queue = new ArrayDeque<>();
    private boolean opened;
    private boolean closed;

    public EventAgentInput(List<Path> paths) {
        List<Path> raw = JsonLineSupport.nonNullPaths(paths);
        List<Path> resolved = new ArrayList<>(raw.size());
        for (Path p : raw) {
            if (p != null) {
                resolved.add(p);
            }
        }
        this.paths = resolved;
        this.memoryOnly = this.paths.isEmpty();
    }

    /** Paths used when not in memory-only mode; immutable view. */
    public List<Path> paths() {
        return Collections.unmodifiableList(paths);
    }

    /**
     * {@code true} when no paths were configured; {@link #append(String)} / {@link #publish} apply.
     */
    public boolean isMemoryOnly() {
        return memoryOnly;
    }

    @Override
    public String id() {
        return AgentInputFactory.TYPE_EVENT;
    }

    /**
     * Appends one NDJSON line to the queue (memory-only mode).
     *
     * @param line raw line; normalized and skipped if blank after trim
     */
    public void append(String line) {
        requireMemoryOnly("append");
        appendInternal(line);
    }

    /**
     * Same as {@link #append(String)} (memory-only mode).
     *
     * @param line raw line; normalized and skipped if blank after trim
     */
    public void publish(String line) {
        requireMemoryOnly("publish");
        appendInternal(line);
    }

    /**
     * Appends multiple NDJSON lines (memory-only mode).
     *
     * @param lines lines to append; must not be {@code null}
     */
    public void publish(List<String> lines) {
        requireMemoryOnly("publish");
        if (lines == null) {
            throw new IllegalArgumentException("lines must not be null");
        }
        ensureNotClosed();
        for (String line : lines) {
            appendInternal(line);
        }
    }

    private void appendInternal(String line) {
        ensureNotClosed();
        String record = JsonLineSupport.normalizeRecord(line);
        if (record == null) {
            return;
        }
        synchronized (queueLock) {
            queue.addLast(record);
        }
    }

    @Override
    public void open() throws IOException {
        ensureNotClosed();
        if (opened) {
            throw new IllegalStateException("already open");
        }
        synchronized (queueLock) {
            if (!memoryOnly) {
                queue = JsonLineSupport.loadAllRecords(paths);
            }
        }
        opened = true;
    }

    @Override
    public List<String> poll(int maxRecords) {
        ensureOpen();
        if (maxRecords <= 0) {
            return Collections.emptyList();
        }
        synchronized (queueLock) {
            if (queue.isEmpty()) {
                return Collections.emptyList();
            }
            int n = Math.min(maxRecords, queue.size());
            List<String> batch = new ArrayList<>(n);
            for (int i = 0; i < n; i++) {
                batch.add(queue.removeFirst());
            }
            return batch;
        }
    }

    @Override
    public void close() {
        if (closed) {
            return;
        }
        closed = true;
        opened = false;
        synchronized (queueLock) {
            queue.clear();
        }
    }

    private void requireMemoryOnly(String operation) {
        if (!memoryOnly) {
            throw new IllegalStateException(
                    operation + "() is only supported when EventAgentInput has no file paths");
        }
    }

    private void ensureOpen() {
        ensureNotClosed();
        if (!opened) {
            throw new IllegalStateException("open() must be called before poll()");
        }
    }

    private void ensureNotClosed() {
        if (closed) {
            throw new IllegalStateException("closed");
        }
    }
}
