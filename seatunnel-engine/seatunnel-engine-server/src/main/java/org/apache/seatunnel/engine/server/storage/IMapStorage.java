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

package org.apache.seatunnel.engine.server.storage;

import com.hazelcast.map.IMap;
import com.hazelcast.map.listener.MapListener;

import javax.annotation.Nonnull;

import java.util.Collection;
import java.util.EventListener;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

public class IMapStorage<K, V> implements MapStorage<K, V> {
    private final IMap<K, V> iMap;

    public IMapStorage(IMap<K, V> iMap) {
        this.iMap = iMap;
    }

    @Override
    public V putIfAbsent(K key, V value) {
        return iMap.putIfAbsent(key, value);
    }

    @Override
    public V compute(K key, BiFunction<K, V, V> remappingFunction) {
        return iMap.compute(key, remappingFunction);
    }

    @Override
    public void remove(Object key) {
        iMap.remove(key);
    }

    @Override
    public V get(Object key) {
        return iMap.get(key);
    }

    @Override
    public void put(K key, V value) {
        iMap.put(key, value);
    }

    @Override
    public void set(K key, V value) {
        iMap.put(key, value);
    }

    @Override
    public Set<Map.Entry<K, V>> entrySet() {
        return iMap.entrySet();
    }

    @Override
    public void forEach(BiConsumer<? super K, ? super V> action) {
        iMap.forEach(action);
    }

    @Override
    public UUID addEntryListener(@Nonnull EventListener listener, boolean includeValue) {
        return iMap.addEntryListener((MapListener) listener, includeValue);
    }

    @Override
    public Collection<V> values() {
        return iMap.values();
    }

    @Override
    public V getOrDefault(Object key, V defaultValue) {
        return iMap.getOrDefault(key, defaultValue);
    }

    @Override
    public V computeIfAbsent(@Nonnull K key, @Nonnull Function<? super K, ? extends V> func) {
        return iMap.computeIfAbsent(key, func);
    }

    @Override
    public V put(@Nonnull K key, @Nonnull V value, long ttl, @Nonnull TimeUnit timeUnit) {
        return iMap.put(key, value, ttl, timeUnit);
    }

    @Override
    public boolean isEmpty() {
        return iMap.isEmpty();
    }

    @Override
    public boolean containsKey(@Nonnull Object key) {
        return iMap.containsKey(key);
    }

    @Override
    public int size() {
        return iMap.size();
    }
}
