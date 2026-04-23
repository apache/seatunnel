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

package org.apache.seatunnel.engine.server.common.statestore.history;

import org.apache.seatunnel.engine.server.common.statestore.ExpiringStateStore;
import org.apache.seatunnel.engine.server.common.statestore.StateStore;

import java.util.Collection;

/**
 * Store for historical state with retention semantics.
 *
 * <p>This interface is intended for states that need both read access and TTL/retention behavior,
 * such as finished job state, finished job metrics, or finished job DAG info.
 *
 * @param <K> key type
 * @param <V> value type
 */
public interface HistoricalStateStore<K, V> extends StateStore<K, V>, ExpiringStateStore<K, V> {

    /**
     * Plain writes without retention metadata are intentionally not supported.
     *
     * @param key key to store
     * @param value value to store
     */
    @Override
    default void put(K key, V value) {
        throw new UnsupportedOperationException(
                "Historical state requires retention metadata. Use put(key, value, ttl, timeUnit).");
    }

    /**
     * Conditional writes without retention metadata are intentionally not supported.
     *
     * @param key key to store
     * @param value value to store
     * @return never returns normally
     */
    @Override
    default V putIfAbsent(K key, V value) {
        throw new UnsupportedOperationException(
                "Historical state requires retention metadata. Use put(key, value, ttl, timeUnit).");
    }

    /**
     * Returns all non-expired values.
     *
     * @return current valid values
     */
    Collection<V> values();

    /**
     * Returns the number of non-expired values.
     *
     * @return current valid value count
     */
    int size();

    /**
     * Returns whether there are no valid values.
     *
     * @return {@code true} if empty
     */
    boolean isEmpty();

    /**
     * Explicitly purges expired entries.
     *
     * <p>Implementations with native TTL support, such as Hazelcast-backed stores, usually treat
     * this as a no-op that returns {@code 0}. Implementations that manage expiration metadata
     * directly, such as RocksDB-backed stores, perform real cleanup work here.
     *
     * @return number of purged entries
     */
    int purgeExpired();
}
