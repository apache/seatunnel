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

package org.apache.seatunnel.engine.server.common.statestore;

/**
 * Low-level backend storage SPI.
 *
 * <p>This is not meant to be the public state contract used directly by engine code. Instead, it is
 * the minimal key-value storage contract used to implement real backends such as RocksDB-backed
 * stores.
 *
 * <p>Rather than copying the full Hazelcast {@code IMap} surface, it keeps only the minimum
 * operations that remain meaningful when switching between backend implementations.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface StateStore<K, V> {

    /**
     * Retrieves the value for the given key.
     *
     * @param key key to look up
     * @return stored value, or {@code null} if absent
     */
    V get(K key);

    /**
     * Stores a value for the given key.
     *
     * @param key key to store
     * @param value value to store
     */
    void put(K key, V value);

    /**
     * Stores a value only when the key is currently absent.
     *
     * @param key key to store
     * @param value value to store
     * @return existing value if present, otherwise {@code null}
     */
    V putIfAbsent(K key, V value);

    /**
     * Removes the value for the given key.
     *
     * @param key key to remove
     */
    void remove(K key);

    /**
     * Returns whether the key exists.
     *
     * @param key key to check
     * @return {@code true} if the key exists
     */
    boolean containsKey(K key);

    /**
     * Returns whether the store is empty.
     *
     * @return {@code true} if the store is empty
     */
    boolean isEmpty();

    /**
     * Returns the number of key-value pairs in the store.
     *
     * @return size of the store
     */
    int size();

    /**
     * Returns the default value when the key is absent.
     *
     * @param key key to look up
     * @param defaultValue default value
     * @return stored value or the default value
     */
    default V getOrDefault(K key, V defaultValue) {
        V value = get(key);
        return value != null ? value : defaultValue;
    }
}
