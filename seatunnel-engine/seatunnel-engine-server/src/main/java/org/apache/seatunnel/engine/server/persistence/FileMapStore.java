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

package org.apache.seatunnel.engine.server.persistence;

import org.apache.seatunnel.shade.com.google.common.collect.Maps;

import org.apache.seatunnel.engine.common.utils.FactoryUtil;
import org.apache.seatunnel.engine.imap.storage.api.ExpirableValue;
import org.apache.seatunnel.engine.imap.storage.api.IMapStorage;
import org.apache.seatunnel.engine.imap.storage.api.IMapStorageFactory;
import org.apache.seatunnel.engine.server.common.statestore.EngineStateStoreNames;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.EntryLoader.MetadataAwareValue;
import com.hazelcast.map.EntryStore;
import com.hazelcast.map.MapLoaderLifecycleSupport;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Persists engine IMaps to {@link IMapStorage}.
 *
 * <p>It is an {@link EntryStore} so that Hazelcast passes the expiration time of every stored
 * entry. Hazelcast does not delete an entry from the map store when it expires, so the expiration
 * time is stored with the entry and handed back on load; otherwise an entry put with a TTL would be
 * loaded again after a restart and never expire.
 */
@Slf4j
public class FileMapStore implements EntryStore<Object, Object>, MapLoaderLifecycleSupport {

    private IMapStorage mapStorage;

    @Override
    public void init(HazelcastInstance hazelcastInstance, Properties properties, String mapName) {
        if (EngineStateStoreNames.RUNNING_JOB_METRICS.equals(mapName)) {
            this.mapStorage = NoOpMapStorage.INSTANCE;
            log.info(
                    "Skip persistence for map '{}' because runtime metrics snapshots are auxiliary "
                            + "observability state and should not write to persistent IMAP storage.",
                    mapName);
            return;
        }

        Map<String, Object> initMap = new HashMap<>(Maps.fromProperties(properties));
        String storageType = (String) initMap.get("type");
        try {
            this.mapStorage =
                    FactoryUtil.discoverFactory(
                                    Thread.currentThread().getContextClassLoader(),
                                    IMapStorageFactory.class,
                                    storageType)
                            .create(initMap);
        } catch (RuntimeException e) {
            log.error(
                    "Failed to initialize IMap storage for map '{}', type='{}'. "
                            + "Cluster state will NOT be persisted.",
                    mapName,
                    storageType,
                    e);
            throw e;
        }
    }

    @Override
    public void destroy() {
        mapStorage.destroy(false);
    }

    @Override
    public void store(Object key, MetadataAwareValue<Object> value) {
        mapStorage.store(key, value.getValue(), value.getExpirationTime());
    }

    @Override
    public void storeAll(Map<Object, MetadataAwareValue<Object>> map) {
        Map<Object, ExpirableValue> values = new HashMap<>(map.size());
        map.forEach(
                (key, value) ->
                        values.put(
                                key,
                                new ExpirableValue(value.getValue(), value.getExpirationTime())));
        mapStorage.storeAllWithExpirationTime(values);
    }

    @Override
    public void delete(Object key) {
        mapStorage.delete(key);
    }

    @Override
    public void deleteAll(Collection<Object> keys) {
        mapStorage.deleteAll(keys);
    }

    @SneakyThrows
    @Override
    public MetadataAwareValue<Object> load(Object key) {
        return null;
    }

    /**
     * Loads the requested keys with the expiration times they were stored with, so entries put with
     * a TTL keep expiring after a restart. Keys that are missing or already expired are left out,
     * and Hazelcast does not load them.
     */
    @SneakyThrows
    @Override
    public Map<Object, MetadataAwareValue<Object>> loadAll(Collection<Object> keys) {
        Map<Object, ExpirableValue> allMap = mapStorage.loadAllWithExpirationTime();
        Map<Object, MetadataAwareValue<Object>> retMap = new HashMap<>();
        keys.forEach(
                key -> {
                    ExpirableValue value = allMap.get(key);
                    if (value != null) {
                        retMap.put(
                                key,
                                new MetadataAwareValue<>(
                                        value.getValue(), value.getExpirationTime()));
                    }
                });

        return Collections.unmodifiableMap(retMap);
    }

    @Override
    public Iterable<Object> loadAllKeys() {
        return mapStorage.loadAllKeys();
    }

    private static final class NoOpMapStorage implements IMapStorage {
        private static final NoOpMapStorage INSTANCE = new NoOpMapStorage();

        @Override
        public void initialize(Map<String, Object> properties) {
            // no-op
        }

        @Override
        public boolean store(Object key, Object value) {
            return true;
        }

        @Override
        public java.util.Set<Object> storeAll(Map<Object, Object> map) {
            return Collections.emptySet();
        }

        @Override
        public boolean delete(Object key) {
            return true;
        }

        @Override
        public java.util.Set<Object> deleteAll(Collection<Object> keys) {
            return Collections.emptySet();
        }

        @Override
        public Map<Object, Object> loadAll() {
            return Collections.emptyMap();
        }

        @Override
        public java.util.Set<Object> loadAllKeys() {
            return Collections.emptySet();
        }

        @Override
        public void destroy(boolean deleteAllFileFlag) {
            // no-op
        }
    }
}
