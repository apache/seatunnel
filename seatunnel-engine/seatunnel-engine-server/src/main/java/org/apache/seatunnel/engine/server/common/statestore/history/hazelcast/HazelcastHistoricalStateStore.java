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

package org.apache.seatunnel.engine.server.common.statestore.history.hazelcast;

import org.apache.seatunnel.engine.server.common.statestore.history.HistoricalStateExpirationListener;
import org.apache.seatunnel.engine.server.common.statestore.history.HistoricalStateListenerRegistration;
import org.apache.seatunnel.engine.server.common.statestore.history.ObservableHistoricalStateStore;

import com.hazelcast.map.IMap;
import com.hazelcast.map.listener.EntryExpiredListener;

import java.util.Collection;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Historical-state store implementation backed by Hazelcast {@link IMap}.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class HazelcastHistoricalStateStore<K, V> implements ObservableHistoricalStateStore<K, V> {

    private final IMap<K, V> iMap;

    public HazelcastHistoricalStateStore(IMap<K, V> iMap) {
        this.iMap = iMap;
    }

    @Override
    public V get(K key) {
        return iMap.get(key);
    }

    @Override
    public V put(K key, V value, long ttl, TimeUnit timeUnit) {
        return iMap.put(key, value, ttl, timeUnit);
    }

    @Override
    public void remove(K key) {
        iMap.remove(key);
    }

    @Override
    public boolean containsKey(K key) {
        return iMap.containsKey(key);
    }

    @Override
    public Collection<V> values() {
        return iMap.values();
    }

    @Override
    public int size() {
        return iMap.size();
    }

    @Override
    public boolean isEmpty() {
        return iMap.isEmpty();
    }

    @Override
    public int purgeExpired() {
        return 0;
    }

    @Override
    public HistoricalStateListenerRegistration addExpirationListener(
            final HistoricalStateExpirationListener<K, V> listener) {
        EntryExpiredListener<K, V> hazelcastListener =
                event -> listener.onExpired(event.getKey(), event.getOldValue());
        final UUID listenerId = iMap.addEntryListener(hazelcastListener, true);
        return () -> iMap.removeEntryListener(listenerId);
    }
}
