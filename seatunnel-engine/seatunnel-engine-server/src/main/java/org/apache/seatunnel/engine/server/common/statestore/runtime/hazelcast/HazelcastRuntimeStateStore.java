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

package org.apache.seatunnel.engine.server.common.statestore.runtime.hazelcast;

import org.apache.seatunnel.engine.server.common.statestore.runtime.RuntimeStateStore;

import com.hazelcast.map.IMap;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * Runtime-state store implementation backed by Hazelcast {@link IMap}.
 *
 * @param <K> key type
 * @param <V> value type
 */
public class HazelcastRuntimeStateStore<K, V> implements RuntimeStateStore<K, V> {

    private final IMap<K, V> iMap;

    public HazelcastRuntimeStateStore(IMap<K, V> iMap) {
        this.iMap = iMap;
    }

    @Override
    public V get(K key) {
        return iMap.get(key);
    }

    @Override
    public void put(K key, V value) {
        iMap.put(key, value);
    }

    @Override
    public V putIfAbsent(K key, V value) {
        return iMap.putIfAbsent(key, value);
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
    public Set<Map.Entry<K, V>> entrySet() {
        return iMap.entrySet();
    }

    @Override
    public Collection<V> values() {
        return iMap.values();
    }

    @Override
    public boolean isEmpty() {
        return iMap.isEmpty();
    }

    @Override
    public int size() {
        return iMap.size();
    }
}
