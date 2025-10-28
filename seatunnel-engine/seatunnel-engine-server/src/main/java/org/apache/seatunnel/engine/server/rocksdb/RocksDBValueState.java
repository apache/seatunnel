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

package org.apache.seatunnel.engine.server.rocksdb;

import org.apache.seatunnel.common.exception.CommonError;
import org.apache.seatunnel.engine.serializer.api.Serializer;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

import lombok.extern.slf4j.Slf4j;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.AbstractMap;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.UnaryOperator;

@Slf4j
public class RocksDBValueState<K, V> implements ValueState<K, V> {
    private final RocksDB db;
    private final ColumnFamilyHandle columnFamilyHandle;
    private final Serializer serializer;

    private final ConcurrentHashMap<K, ReentrantLock> lockMap = new ConcurrentHashMap<>();

    RocksDBValueState(RocksDB db, ColumnFamilyHandle columnFamilyHandle, Serializer serializer) {
        this.db = db;
        this.columnFamilyHandle = columnFamilyHandle;
        this.serializer = serializer;
    }

    private byte[] encode(Object object) throws IOException {
        if (object == null) throw CommonError.illegalArgument("object is null", "encode object");
        byte[] payload;
        if (serializer != null) {
            payload = serializer.serialize(object);
        } else {
            payload = encodeObject(object);
        }
        String className = object.getClass().getName();
        return encodeWithClassName(payload, className);
    }

    private static byte[] encodeObject(Object key) throws IOException {
        if (key == null) throw CommonError.illegalArgument("object is null", "encode object");
        try (ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
                ObjectOutputStream objectOutputStream =
                        new ObjectOutputStream(byteArrayOutputStream)) {
            objectOutputStream.writeObject(key);
            objectOutputStream.flush();
            return byteArrayOutputStream.toByteArray();
        }
    }

    @SuppressWarnings("unchecked")
    private K decodeKey(byte[] bytes) throws IOException, ClassNotFoundException {
        if (bytes == null) return null;
        Decoded decoded = decodeWithClassName(bytes);
        Class<?> actualClass = Class.forName(decoded.className);
        if (serializer != null) {
            try {
                return (K) serializer.deserialize(decoded.payload, actualClass);
            } catch (Exception ex) {
                log.warn(
                        "serializer.deserialize failed for key class {}, fallback to Java deserialization",
                        decoded.className,
                        ex);
            }
        }
        return (K) javaDeserialize(decoded.payload);
    }

    @SuppressWarnings("unchecked")
    private V decodeValue(byte[] bytes) throws IOException, ClassNotFoundException {
        if (bytes == null) return null;
        Decoded decoded = decodeWithClassName(bytes);
        Class<?> actualClass = Class.forName(decoded.className);
        if (serializer != null) {
            try {
                return (V) serializer.deserialize(decoded.payload, actualClass);
            } catch (Exception e) {
                log.warn(
                        "serializer.deserialize failed for value class {}, fallback to Java deserialization",
                        decoded.className,
                        e);
            }
        }
        return (V) javaDeserialize(decoded.payload);
    }

    private Object javaDeserialize(byte[] payload) throws IOException, ClassNotFoundException {
        if (payload == null)
            throw CommonError.illegalArgument("payload is null", "deserialize payload");
        try (ByteArrayInputStream byteArrayInputStream = new ByteArrayInputStream(payload);
                ObjectInputStream objectInputStream = new ObjectInputStream(byteArrayInputStream)) {
            return objectInputStream.readObject();
        }
    }

    private ReentrantLock getLock(K key) {
        return lockMap.computeIfAbsent(key, k -> new ReentrantLock());
    }

    private void tryRemoveLock(K key, ReentrantLock lock) {
        if (!lock.isLocked() && !lock.hasQueuedThreads()) {
            lockMap.remove(key, lock);
        }
    }

    public V get(K key) {
        if (key == null) throw new NullPointerException("key is null");
        ReentrantLock lock = getLock(key);
        lock.lock();
        try {
            byte[] rawKey = encode(key);
            byte[] value = db.get(columnFamilyHandle, rawKey);
            return decodeValue(value);
        } catch (Exception e) {
            throw new RocksDBRuntimeException("Failed to get value from RocksDB. key: " + key, e);
        } finally {
            lock.unlock();
        }
    }

    public void put(K key, V value) {
        if (key == null) throw new NullPointerException("key is null");
        ReentrantLock lock = getLock(key);
        lock.lock();
        try {
            byte[] rawKey = encode(key);
            if (value == null) {
                db.delete(columnFamilyHandle, rawKey);
            } else {
                byte[] valueBytes = encode(value);
                db.put(columnFamilyHandle, rawKey, valueBytes);
            }
        } catch (Exception e) {
            throw new RocksDBRuntimeException(
                    String.format(
                            "Failed to put key-value into RocksDB. key: %s, value: %s", key, value),
                    e);
        } finally {
            lock.unlock();
        }
    }

    public void remove(K key) {
        if (key == null) throw new NullPointerException("key is null");
        ReentrantLock lock = getLock(key);
        lock.lock();
        try {
            byte[] rawKey = encode(key);
            db.delete(columnFamilyHandle, rawKey);
        } catch (Exception e) {
            throw new RocksDBRuntimeException("Failed to remove key from RocksDB. key: " + key, e);
        } finally {
            lock.unlock();
            tryRemoveLock(key, lock);
        }
    }

    @Override
    public boolean contains(K key) throws IOException, RocksDBException {
        if (key == null) throw new NullPointerException("key is null");
        ReentrantLock lock = getLock(key);
        lock.lock();
        try {
            byte[] rawKey = encode(key);
            byte[] value = db.get(columnFamilyHandle, rawKey);
            return value != null;
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Iterable<Map.Entry<K, V>> entries() {
        return () -> {
            try {
                return iterator();
            } catch (Exception e) {
                throw new RocksDBRuntimeException("Failed to create iterator for entries", e);
            }
        };
    }

    @Override
    public Iterable<K> keys() {
        return () -> new KeyIterator(iterator());
    }

    @Override
    public Iterable<V> values() throws Exception {
        return () -> new ValueIterator(iterator());
    }

    @Override
    public Iterator<Map.Entry<K, V>> iterator() {
        RocksIterator rocksIter;
        try {
            rocksIter = db.newIterator(columnFamilyHandle);
        } catch (Exception e) {
            throw new RocksDBRuntimeException("Failed to create RocksIterator", e);
        }
        rocksIter.seekToFirst();

        return new AutoCloseableIterator<Map.Entry<K, V>>() {
            private boolean closed = false;

            private void closeIfNeeded() {
                if (!closed) {
                    try {
                        rocksIter.close();
                    } catch (Exception ignored) {
                        log.warn("Failed to close RocksIterator", ignored);
                    } finally {
                        closed = true;
                    }
                }
            }

            @Override
            public boolean hasNext() {
                boolean valid = rocksIter.isValid();
                if (!valid) {
                    closeIfNeeded();
                }
                return valid;
            }

            @Override
            public Map.Entry<K, V> next() {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                try {
                    byte[] keyBytes = rocksIter.key();
                    byte[] valueBytes = rocksIter.value();
                    K k = decodeKey(keyBytes);
                    V v = decodeValue(valueBytes);
                    rocksIter.next();
                    return new AbstractMap.SimpleEntry<>(k, v);
                } catch (Exception e) {
                    closeIfNeeded();
                    throw new RocksDBRuntimeException("Failed to deserialize entry", e);
                }
            }

            @Override
            public void close() {
                closeIfNeeded();
            }

            @Override
            public void remove() {
                throw new UnsupportedOperationException();
            }
        };
    }

    @Override
    public boolean isEmpty() {
        RocksIterator it = db.newIterator(columnFamilyHandle);
        try {
            it.seekToFirst();
            return !it.isValid();
        } finally {
            try {
                it.close();
            } catch (Exception ignored) {
                log.warn("Failed to close RocksIterator", ignored);
            }
        }
    }

    @Override
    public void close() {
        try {
            columnFamilyHandle.close();
        } catch (Exception ignored) {
            log.warn("Failed to close ColumnFamilyHandle", ignored);
        }
    }

    public void compute(K key, UnaryOperator<V> remappingFunction) {
        if (key == null) throw new NullPointerException("key");
        ReentrantLock lock = getLock(key);
        lock.lock();
        try {
            byte[] rawKey = encode(key);
            byte[] rawValue = db.get(columnFamilyHandle, rawKey);
            V oldValue = decodeValue(rawValue);
            V newValue = remappingFunction.apply(oldValue);

            if (newValue == null) {
                db.delete(columnFamilyHandle, rawKey);
            } else {
                byte[] valueBytes = encode(newValue);
                db.put(columnFamilyHandle, rawKey, valueBytes);
            }
        } catch (Exception e) {
            throw new RocksDBRuntimeException(
                    "Failed to compute value for key in RocksDB. key: " + key, e);
        } finally {
            lock.unlock();
        }
    }

    private byte[] encodeWithClassName(byte[] payload, String className) {
        byte[] bytes = className.getBytes(StandardCharsets.UTF_8);
        int payloadLen = payload == null ? 0 : payload.length;
        ByteBuffer byteBuffer = ByteBuffer.allocate(Integer.BYTES + bytes.length + payloadLen);
        byteBuffer.putInt(bytes.length);
        byteBuffer.put(bytes);
        if (payloadLen > 0) byteBuffer.put(payload);
        return byteBuffer.array();
    }

    private Decoded decodeWithClassName(byte[] encoded) {
        if (encoded == null || encoded.length < Integer.BYTES) {
            throw CommonError.illegalArgument(Arrays.toString(encoded), "check framed data");
        }
        ByteBuffer byteBuffer = ByteBuffer.wrap(encoded);
        int len = byteBuffer.getInt();
        if (len < 0 || len > byteBuffer.remaining()) {
            throw CommonError.illegalArgument(Arrays.toString(encoded), "check framed data");
        }
        byte[] className = new byte[len];
        byteBuffer.get(className);
        byte[] payload = new byte[byteBuffer.remaining()];
        byteBuffer.get(payload);
        Decoded decoded = new Decoded();
        decoded.className = new String(className, StandardCharsets.UTF_8);
        decoded.payload = payload;
        return decoded;
    }

    private static class Decoded {
        String className;
        byte[] payload;
    }

    private class KeyIterator implements Iterator<K> {
        private final AutoCloseableIterator<Map.Entry<K, V>> entryIter;

        public KeyIterator(Iterator<Map.Entry<K, V>> iterator) {
            this.entryIter = (AutoCloseableIterator<Map.Entry<K, V>>) iterator;
        }

        @Override
        public boolean hasNext() {
            return entryIter.hasNext();
        }

        @Override
        public K next() {
            return entryIter.next().getKey();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    private class ValueIterator implements Iterator<V> {
        private final AutoCloseableIterator<Map.Entry<K, V>> entryIter;

        public ValueIterator(Iterator<Map.Entry<K, V>> iterator) {
            this.entryIter = (AutoCloseableIterator<Map.Entry<K, V>>) iterator;
        }

        @Override
        public boolean hasNext() {
            return entryIter.hasNext();
        }

        @Override
        public V next() {
            return entryIter.next().getValue();
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }

    public interface AutoCloseableIterator<T> extends Iterator<T>, AutoCloseable {}
}
