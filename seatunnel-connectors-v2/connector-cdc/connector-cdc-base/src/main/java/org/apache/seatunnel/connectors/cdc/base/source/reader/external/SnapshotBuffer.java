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

package org.apache.seatunnel.connectors.cdc.base.source.reader.external;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.AbstractMap;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

/** Ordered reconciliation state with a bounded small-split fast path and ephemeral spill files. */
final class SnapshotBuffer extends AbstractMap<Struct, SourceRecord> implements AutoCloseable {
    private static final int MEMORY_RECORD_LIMIT = 1024;
    private static final long MEMORY_ENCODED_BYTE_LIMIT = 8L * 1024 * 1024;

    private final int recordLimit;
    private final long byteLimit;
    private final Map<Struct, SourceRecord> memory = new LinkedHashMap<>();
    private SnapshotRecordCodec sizeCodec = new SnapshotRecordCodec();
    private DiskSnapshotMap spilled;
    private DiskSnapshotMap failedCleanup;
    private long encodedBytes;
    private boolean closed;

    SnapshotBuffer() {
        this(MEMORY_RECORD_LIMIT, MEMORY_ENCODED_BYTE_LIMIT);
    }

    SnapshotBuffer(int recordLimit, long byteLimit) {
        if (recordLimit < 0 || byteLimit < 0) {
            throw new IllegalArgumentException("Snapshot memory limits must be non-negative");
        }
        this.recordLimit = recordLimit;
        this.byteLimit = byteLimit;
    }

    private void ensureOpen() {
        if (closed) {
            throw new IllegalStateException("Snapshot buffer is closed");
        }
        if (failedCleanup != null) {
            throw new IllegalStateException("Failed snapshot spill still requires cleanup");
        }
    }

    @Override
    public synchronized SourceRecord put(Struct key, SourceRecord record) {
        ensureOpen();
        try {
            if (spilled == null) {
                int size = sizeCodec.encode(record).length;
                if ((!memory.containsKey(key) && memory.size() >= recordLimit)
                        || size > byteLimit - encodedBytes) {
                    DiskSnapshotMap candidate = new DiskSnapshotMap(65536);
                    try {
                        for (Entry<Struct, SourceRecord> entry : memory.entrySet()) {
                            candidate.put(entry.getKey(), entry.getValue());
                        }
                        candidate.flush();
                    } catch (IOException | RuntimeException | Error failure) {
                        try {
                            candidate.close();
                        } catch (IOException cleanupFailure) {
                            failedCleanup = candidate;
                            failure.addSuppressed(cleanupFailure);
                        }
                        throw failure;
                    }
                    spilled = candidate;
                    memory.clear();
                    sizeCodec = null;
                } else {
                    // Conservative cumulative accounting also bounds repeated replacements.
                    encodedBytes += size;
                    return memory.put(key, record);
                }
            }
            return spilled.put(key, record);
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot spill CDC snapshot records", e);
        }
    }

    @Override
    public synchronized SourceRecord get(Object key) {
        ensureOpen();
        return spilled == null ? memory.get(key) : spilled.get(key);
    }

    @Override
    public synchronized SourceRecord remove(Object key) {
        ensureOpen();
        return spilled == null ? memory.remove(key) : spilled.remove(key);
    }

    @Override
    public synchronized int size() {
        ensureOpen();
        return spilled == null ? memory.size() : spilled.size();
    }

    @Override
    public synchronized Set<Entry<Struct, SourceRecord>> entrySet() {
        ensureOpen();
        return spilled == null ? memory.entrySet() : spilled.entrySet();
    }

    @Override
    public synchronized Collection<SourceRecord> values() {
        ensureOpen();
        return spilled == null ? memory.values() : spilled.values();
    }

    synchronized void seal() throws IOException {
        ensureOpen();
        if (spilled != null) {
            spilled.flush();
        }
    }

    synchronized Path spillDirectory() {
        return spilled == null ? null : spilled.directory;
    }

    @Override
    public synchronized void close() throws IOException {
        closed = true;
        memory.clear();
        sizeCodec = null;
        if (spilled != null) {
            // Keep the owner available for retry if deletion fails.
            spilled.close();
        }
        if (failedCleanup != null) {
            failedCleanup.close();
            failedCleanup = null;
        }
    }
}
