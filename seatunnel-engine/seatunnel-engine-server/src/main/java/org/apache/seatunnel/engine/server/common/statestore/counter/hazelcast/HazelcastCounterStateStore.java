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

package org.apache.seatunnel.engine.server.common.statestore.counter.hazelcast;

import org.apache.seatunnel.engine.server.common.statestore.counter.CounterStateStore;

import com.hazelcast.map.IMap;

/**
 * Counter-store implementation backed by Hazelcast {@link IMap}.
 *
 * @param <K> key type
 */
public class HazelcastCounterStateStore<K> implements CounterStateStore<K> {

    private final IMap<K, Long> iMap;

    public HazelcastCounterStateStore(IMap<K, Long> iMap) {
        this.iMap = iMap;
    }

    @Override
    public boolean initializeIfAbsent(K key, long initialValue) {
        return iMap.putIfAbsent(key, initialValue) == null;
    }

    @Override
    public Long get(K key) {
        return iMap.get(key);
    }

    @Override
    public Long incrementAndGet(K key) {
        return iMap.compute(key, (ignored, current) -> current == null ? null : current + 1L);
    }

    @Override
    public Long addAndGet(K key, long delta) {
        return iMap.compute(key, (ignored, current) -> current == null ? null : current + delta);
    }

    @Override
    public void set(K key, long value) {
        iMap.put(key, value);
    }

    @Override
    public void remove(K key) {
        iMap.remove(key);
    }
}
