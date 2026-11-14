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
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.AbstractMap;
import java.util.AbstractSet;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

/** Ephemeral single-writer disk hash map with a separate linked insertion-order index. */
final class DiskSnapshotMap extends AbstractMap<Struct, SourceRecord> implements AutoCloseable {
    final Path directory;
    final RandomAccessFile index;
    final RandomAccessFile data;
    private final Path indexPath;
    private final Path dataPath;
    private final BoundedSnapshotFile indexPages;
    private final BoundedSnapshotFile dataPages;
    private final int buckets;
    private final long[] bucketHeads;
    private final byte[] scalar = new byte[8];
    private final SnapshotKeyEncoder keys = new SnapshotKeyEncoder();
    private final SnapshotRecordCodec records = new SnapshotRecordCodec(keys);
    private long first;
    private long last;
    private int size;
    private long allocatedNodes;
    private int maxEncodedRecordLength;
    private boolean closed;
    private boolean cleaned;

    public DiskSnapshotMap(int buckets) throws IOException {
        this.buckets = buckets;
        if (buckets <= 0 || buckets > 65536)
            throw new IllegalArgumentException("Invalid bucket count");
        bucketHeads = new long[buckets];
        directory = Files.createTempDirectory("seatunnel-cdc-snapshot-");
        Path localIndexPath = null;
        Path localDataPath = null;
        RandomAccessFile localIndex = null;
        RandomAccessFile localData = null;
        try {
            try {
                Files.setPosixFilePermissions(
                        directory, PosixFilePermissions.fromString("rwx------"));
            } catch (UnsupportedOperationException ignored) {
            }
            localIndexPath = Files.createTempFile(directory, "index-", ".bin");
            localDataPath = Files.createTempFile(directory, "records-", ".bin");
            localIndex = new RandomAccessFile(localIndexPath.toFile(), "rw");
            localData = new RandomAccessFile(localDataPath.toFile(), "rw");
            localIndex.setLength(1);
            localData.writeByte(0);
            indexPath = localIndexPath;
            dataPath = localDataPath;
            index = localIndex;
            data = localData;
            indexPages = new BoundedSnapshotFile(index, 4096, 128);
            dataPages = new BoundedSnapshotFile(data, 16384, 8);
        } catch (IOException | RuntimeException | Error failure) {
            try {
                if (localIndex != null) localIndex.close();
            } catch (IOException e) {
                failure.addSuppressed(e);
            }
            try {
                if (localData != null) localData.close();
            } catch (IOException e) {
                failure.addSuppressed(e);
            }
            try {
                if (localIndexPath != null) Files.deleteIfExists(localIndexPath);
            } catch (IOException e) {
                failure.addSuppressed(e);
            }
            try {
                if (localDataPath != null) Files.deleteIfExists(localDataPath);
            } catch (IOException e) {
                failure.addSuppressed(e);
            }
            try {
                Files.deleteIfExists(directory);
            } catch (IOException e) {
                failure.addSuppressed(e);
            }
            throw failure;
        }
    }

    private void ensureOpen() throws IOException {
        if (closed) throw new IOException("Snapshot map is closed");
    }

    private long bucket(byte[] key) {
        return (Arrays.hashCode(key) & 0x7fffffff) % buckets;
    }

    private long read(long position) throws IOException {
        indexPages.read(position, scalar, 0, 8);
        return ByteBuffer.wrap(scalar).getLong();
    }

    private void write(long position, long value) throws IOException {
        ByteBuffer.wrap(scalar).putLong(value);
        indexPages.write(position, scalar, 0, 8);
    }

    private long find(byte[] key) throws IOException {
        long indexLength = indexPages.length();
        long maximumNodes = allocatedNodes;
        for (long node = bucketHeads[(int) bucket(key)], hops = 0; node != 0; node = read(node)) {
            if (node < 1 || node > indexLength - 36 || ++hops > maximumNodes)
                throw new IOException("Invalid/cyclic snapshot index");
            indexPages.read(node + 32, scalar, 0, 4);
            int length = ByteBuffer.wrap(scalar).getInt();
            if (length < 0 || length > indexLength - node - 36)
                throw new IOException("Invalid snapshot key length");
            if (length == key.length) {
                byte[] stored = new byte[length];
                indexPages.read(node + 36, stored, 0, length);
                if (Arrays.equals(key, stored)) return node;
            }
        }
        return 0;
    }

    private SourceRecord value(long node) throws IOException {
        long offset = read(node + 24);
        if (offset == 0) return null;
        dataPages.read(offset, scalar, 0, 4);
        int length = ByteBuffer.wrap(scalar).getInt();
        if (length < 0
                || length > maxEncodedRecordLength
                || length > dataPages.length() - offset - 4) {
            throw new IOException("Invalid snapshot record length");
        }
        byte[] bytes = new byte[length];
        dataPages.read(offset + 4, bytes, 0, length);
        return records.decode(bytes);
    }

    @Override
    public synchronized SourceRecord put(Struct key, SourceRecord record) {
        try {
            ensureOpen();
            byte[] encodedKey = keys.encode(key);
            byte[] encodedRecord = records.encode(record);
            maxEncodedRecordLength = Math.max(maxEncodedRecordLength, encodedRecord.length);
            long node = find(encodedKey);
            SourceRecord previous = node == 0 ? null : value(node);
            long offset = dataPages.length();
            ByteBuffer.wrap(scalar).putInt(encodedRecord.length);
            dataPages.write(offset, scalar, 0, 4);
            dataPages.write(offset + 4, encodedRecord, 0, encodedRecord.length);
            if (node == 0) {
                node = indexPages.length();
                int bucket = (int) bucket(encodedKey);
                byte[] header =
                        ByteBuffer.allocate(36 + encodedKey.length)
                                .putLong(bucketHeads[bucket])
                                .putLong(0)
                                .putLong(0)
                                .putLong(0)
                                .putInt(encodedKey.length)
                                .put(encodedKey)
                                .array();
                indexPages.write(node, header, 0, header.length);
                bucketHeads[bucket] = node;
                allocatedNodes++;
            }
            if (previous == null) {
                write(node + 8, last);
                write(node + 16, 0);
                if (last == 0) first = node;
                else write(last + 16, node);
                last = node;
                size++;
            }
            write(node + 24, offset);
            return previous;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public synchronized SourceRecord get(Object key) {
        if (key != null && !(key instanceof Struct)) return null;
        try {
            ensureOpen();
            long node = find(keys.encode((Struct) key));
            return node == 0 ? null : value(node);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public synchronized SourceRecord remove(Object key) {
        if (key != null && !(key instanceof Struct)) return null;
        try {
            ensureOpen();
            long node = find(keys.encode((Struct) key));
            SourceRecord previous = node == 0 ? null : value(node);
            if (previous != null) {
                long before = read(node + 8);
                long after = read(node + 16);
                if (before == 0) first = after;
                else write(before + 16, after);
                if (after == 0) last = before;
                else write(after + 8, before);
                write(node + 24, 0);
                size--;
            }
            return previous;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public synchronized int size() {
        return size;
    }

    @Override
    public Set<Map.Entry<Struct, SourceRecord>> entrySet() {
        return new AbstractSet<Map.Entry<Struct, SourceRecord>>() {
            @Override
            public int size() {
                return DiskSnapshotMap.this.size();
            }

            @Override
            public Iterator<Map.Entry<Struct, SourceRecord>> iterator() {
                try {
                    flush();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
                return new Iterator<Map.Entry<Struct, SourceRecord>>() {
                    private long cursor = first;
                    private int remaining = DiskSnapshotMap.this.size();

                    @Override
                    public boolean hasNext() {
                        synchronized (DiskSnapshotMap.this) {
                            if (cursor == 0 && remaining != 0 || cursor != 0 && remaining <= 0)
                                throw new UncheckedIOException(
                                        new IOException("Truncated/cyclic snapshot order index"));
                            return cursor != 0;
                        }
                    }

                    @Override
                    public Map.Entry<Struct, SourceRecord> next() {
                        synchronized (DiskSnapshotMap.this) {
                            if (!hasNext()) throw new NoSuchElementException();
                            try {
                                ensureOpen();
                                if (--remaining < 0
                                        || cursor < 1
                                        || cursor > indexPages.length() - 36)
                                    throw new IOException("Invalid/cyclic snapshot order index");
                                SourceRecord value = value(cursor);
                                cursor = read(cursor + 16);
                                return new SimpleImmutableEntry<>((Struct) value.key(), value);
                            } catch (IOException e) {
                                throw new UncheckedIOException(e);
                            }
                        }
                    }
                };
            }
        };
    }

    public synchronized void flush() throws IOException {
        ensureOpen();
        indexPages.flush();
        dataPages.flush();
    }

    public synchronized long diskBytes() throws IOException {
        flush();
        return indexPages.length() + dataPages.length();
    }

    @Override
    public synchronized void close() throws IOException {
        if (cleaned) return;
        closed = true;
        indexPages.discard();
        dataPages.discard();
        IOException failure = null;
        try {
            index.close();
        } catch (IOException e) {
            failure = e;
        }
        try {
            data.close();
        } catch (IOException e) {
            if (failure == null) failure = e;
            else failure.addSuppressed(e);
        }
        try {
            Files.deleteIfExists(indexPath);
        } catch (IOException e) {
            if (failure == null) failure = e;
            else failure.addSuppressed(e);
        }
        try {
            Files.deleteIfExists(dataPath);
        } catch (IOException e) {
            if (failure == null) failure = e;
            else failure.addSuppressed(e);
        }
        try {
            Files.deleteIfExists(directory);
        } catch (IOException e) {
            if (failure == null) failure = e;
            else failure.addSuppressed(e);
        }
        if (failure == null) cleaned = true;
        if (failure != null) throw failure;
    }
}
