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

import org.apache.seatunnel.shade.com.google.common.annotations.VisibleForTesting;
import org.apache.seatunnel.shade.com.google.common.collect.Maps;

import org.apache.seatunnel.common.config.Common;
import org.apache.seatunnel.common.utils.FileUtils;
import org.apache.seatunnel.common.utils.TemporaryClassLoaderContext;
import org.apache.seatunnel.engine.common.utils.FactoryUtil;
import org.apache.seatunnel.engine.imap.storage.api.IMapStorage;
import org.apache.seatunnel.engine.imap.storage.api.IMapStorageFactory;
import org.apache.seatunnel.engine.server.common.statestore.EngineStateStoreNames;

import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.map.MapLoaderLifecycleSupport;
import com.hazelcast.map.MapStore;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.Path;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

@Slf4j
public class FileMapStore implements MapStore<Object, Object>, MapLoaderLifecycleSupport {

    private IMapStorage mapStorage;

    @Override
    public void init(HazelcastInstance hazelcastInstance, Properties properties, String mapName) {
        init(hazelcastInstance, properties, mapName, Common.appStarterDir().resolve("zeta"));
    }

    /**
     * Initializes the map store with an explicit Zeta starter directory for isolated classloader
     * verification.
     */
    @VisibleForTesting
    void init(
            HazelcastInstance hazelcastInstance,
            Properties properties,
            String mapName,
            Path zetaDirectory) {
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
        ClassLoader storageClassLoader = Thread.currentThread().getContextClassLoader();
        try {
            List<URL> storageJars =
                    FileUtils.searchJarFilesForStorage(
                            zetaDirectory, properties.getProperty("storage.type"));
            if (!storageJars.isEmpty()) {
                storageClassLoader =
                        new URLClassLoader(storageJars.toArray(new URL[0]), storageClassLoader);
            }
        } catch (IOException e) {
            throw new IllegalStateException("Failed to load Zeta storage jars", e);
        }

        try (TemporaryClassLoaderContext ignored =
                TemporaryClassLoaderContext.of(storageClassLoader)) {
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
    }

    @Override
    public void destroy() {
        mapStorage.destroy(false);
    }

    @Override
    public void store(Object key, Object value) {
        mapStorage.store(key, value);
    }

    @Override
    public void storeAll(Map<Object, Object> map) {
        mapStorage.storeAll(map);
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
    public Object load(Object key) {
        return null;
    }

    @SneakyThrows
    @Override
    public Map<Object, Object> loadAll(Collection<Object> keys) {
        Map<Object, Object> allMap = mapStorage.loadAll();
        Map<Object, Object> retMap = new HashMap<>();
        keys.forEach(key -> retMap.put(key, allMap.get(key)));

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
